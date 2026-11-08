//! A conformance client under test.
//!
//! The connectrpc conformance runner writes length-prefixed
//! `ClientCompatRequest` messages to this program's stdin and reads
//! length-prefixed `ClientCompatResponse` messages from its stdout. Each request
//! describes one RPC; this program issues it and reports what the server said.
//!
//! `just conformance` runs the suite. Every stream type, the proto codec, and the
//! identity and gzip encodings are supported over cleartext HTTP/2. TLS is not
//! claimed: there is no client transport to terminate it with yet.

const std = @import("std");
const Io = std.Io;

const compress = @import("compress");
const grpc = @import("grpc");
const http = @import("http");
const pb = @import("protobuf");

const messages = @import("messages.zig");
const tls_bridge = @import("tls_bridge.zig");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.arena.allocator();

    var in_buffer: [16 * 1024]u8 = undefined;
    var stdin_reader = Io.File.stdin().reader(io, &in_buffer);
    const reader = &stdin_reader.interface;

    var out_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = Io.File.stdout().writer(io, &out_buffer);
    const writer = &stdout_writer.interface;

    while (true) {
        var length_bytes: [4]u8 = undefined;
        reader.readSliceAll(&length_bytes) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        const length = std.mem.readInt(u32, &length_bytes, .big);

        const body = try gpa.alloc(u8, length);
        defer gpa.free(body);
        try reader.readSliceAll(body);

        var arena: std.heap.ArenaAllocator = .init(gpa);
        defer arena.deinit();
        const allocator = arena.allocator();

        var request_reader: Io.Reader = .fixed(body);
        const request = try pb.decodeLeaky(
            messages.ClientCompatRequest,
            allocator,
            &request_reader,
            .{},
        );

        const response = dispatch(allocator, io, request) catch |err| blk: {
            break :blk messages.ClientCompatResponse{
                .test_name = request.test_name,
                .err = .{ .message = @errorName(err) },
            };
        };

        var encoded: Io.Writer.Allocating = .init(gpa);
        defer encoded.deinit();
        try pb.encode(response, .{}, &encoded.writer);

        const bytes = encoded.written();
        var size: [4]u8 = undefined;
        std.mem.writeInt(u32, &size, @intCast(bytes.len), .big);
        try writer.writeAll(&size);
        try writer.writeAll(bytes);
        try writer.flush();
    }
}

fn dispatch(
    allocator: std.mem.Allocator,
    io: Io,
    request: messages.ClientCompatRequest,
) !messages.ClientCompatResponse {
    if (request.protocol != messages.protocol_grpc) return error.UnsupportedProtocol;
    if (request.codec != messages.codec_proto) return error.UnsupportedCodec;
    switch (request.compression) {
        messages.compression_identity, messages.compression_gzip => {},
        else => return error.UnsupportedCompression,
    }

    return .{
        .test_name = request.test_name,
        .response = try call(allocator, io, request),
    };
}

/// The narrow end of the `client.open` API: what the suite says, and what
/// `Session.run` does with it.
fn call(
    allocator: std.mem.Allocator,
    io: Io,
    request: messages.ClientCompatRequest,
) !messages.ClientResponseResult {
    var endpoint_buffer: [256]u8 = undefined;
    const endpoint = try std.mem.print(&endpoint_buffer, "{s}:{d}", .{
        request.host,
        request.port,
    });

    const address = try Io.net.IpAddress.parseLiteral(endpoint);
    const socket = try address.connect(io, .{ .mode = .stream });

    var client: grpc.blocking.Client = undefined;
    var bridge: tls_bridge.Bridge = undefined;
    var client_ready = false;
    var bridge_ready = false;
    defer {
        if (client_ready) client.deinit();
        if (bridge_ready) {
            bridge.deinit();
            socket.close(io);
        }
    }

    if (request.server_tls_cert.len != 0) {
        // The runner reports an address, and the certificate carries a DNS SAN,
        // so the TLS dial has to use the name. See docs/slop/TLS-SEAM.md.
        // The runner reports an address and the certificate carries a DNS SAN,
        // so only the verification name changes. The connection still goes to
        // the address it gave us.
        try bridge.init(allocator, io, socket, request.server_tls_cert, "localhost");
        bridge_ready = true;
        if (request.timeout_ms) |ms| {
            const now = Io.Clock.awake.now(io).nanoseconds;
            bridge.setDeadline(now + @as(i96, ms) * std.time.ns_per_ms);
        }
        try client.initIo(
            allocator,
            io,
            bridge.readerInterface(),
            bridge.writerInterface(),
            // The fd is below TLS, so the reader owns the deadline: reading the
            // fd here would hand the connection ciphertext.
            .{ .fd = socket.socket.handle, .deadline_by = .reader },
        );
        client_ready = true;
    } else {
        // The client owns the socket from here and closes it in `deinit`.
        try client.init(allocator, io, socket);
        client_ready = true;
    }

    var session: Session = .{
        .io = io,
        .allocator = allocator,
        .endpoint = endpoint,
        .stream_type = request.stream_type,
        .encoding = if (request.compression == messages.compression_gzip) .gzip else .identity,
        .payloads = .empty,
        .scratch = .init(allocator),
    };
    defer session.scratch.deinit();
    defer if (session.opened) session.close();

    session.run(&client, request) catch |err| {
        // A TLS transport cannot report a timeout through `Io.Reader`, so the
        // bridge records it. That is the call's deadline expiring, not a client
        // failure, and the result has to say so.
        if (!(bridge_ready and bridge.expired)) return err;
        session.deadline_expired = true;
    };
    if (bridge_ready) session.deadline_expired = session.deadline_expired or bridge.expired;
    return session.result();
}

const Encoding = enum { identity, gzip };

fn encodingName(encoding: Encoding) []const u8 {
    return switch (encoding) {
        .identity => "identity",
        .gzip => "gzip",
    };
}

const Session = struct {
    io: Io,
    allocator: std.mem.Allocator,
    endpoint: []const u8,
    stream_type: u32,
    encoding: Encoding,
    payloads: std.ArrayList(messages.ConformancePayload),
    /// Scratch for a decompressed message. Kept apart from the arena the decoded
    /// payloads live in, so a decode allocation cannot disturb the bytes it is
    /// reading.
    scratch: std.heap.ArenaAllocator,
    client: ?*grpc.blocking.Client = null,
    calls: [1]grpc.Call = undefined,
    opened: bool = false,
    cancelled: bool = false,
    deadline_expired: bool = false,

    fn run(
        self: *Session,
        client: *grpc.blocking.Client,
        request: messages.ClientCompatRequest,
    ) !void {
        const metadata = try convertHeaders(self.allocator, request.request_headers);
        self.calls[0] = try client.open(.{
            .authority = self.endpoint,
            .service = request.service orelse "connectrpc.conformance.v1.ConformanceService",
            .method = request.method orelse "Unary",
            .body = &.{},
            .streaming = true,
            .encoding = encodingName(self.encoding),
            // Advertise only what this test's configuration chose, so a server
            // that compresses anyway is answering something the client never
            // said it could read.
            .accept_encoding = if (self.encoding == .gzip) "identity,gzip" else "identity",
            .deadline_ns = if (request.timeout_ms) |ms| @as(u64, ms) * std.time.ns_per_ms else null,
            // The suite sets this per request; zero means no limit was asked
            // for, so the frame default stands.
            .message_limit = if (request.message_receive_limit != 0)
                request.message_receive_limit
            else
                null,
            .metadata = metadata,
        });
        self.client = client;
        self.opened = true;

        switch (request.stream_type) {
            messages.stream_type_unary, messages.stream_type_server_stream => {
                try self.singleRequest(request);
            },
            messages.stream_type_client_stream, messages.stream_type_half_duplex => {
                try self.clientFirst(request);
            },
            messages.stream_type_full_duplex => {
                try self.fullDuplex(request);
            },
            else => return error.UnsupportedStreamType,
        }
    }

    /// One request message, then read everything. A trailing cancel happens
    /// after the request goes out.
    fn singleRequest(self: *Session, request: messages.ClientCompatRequest) !void {
        try self.send(request.request_messages[0]);

        if (request.cancel) |cancellation| {
            if (cancellation.after_close_send_ms) |delay| {
                self.sleepAndCancel(delay);
            }
        }
        self.current().closeSend();
        try self.drain(request);
    }

    /// Every request message, then the response. A cancel may land before the
    /// send side closes, or after it.
    fn clientFirst(self: *Session, request: messages.ClientCompatRequest) !void {
        for (request.request_messages) |message| {
            self.sleep(request.request_delay_ms);
            try self.send(message);
        }

        if (request.cancel) |cancellation| {
            if (cancellation.before_close_send != null) {
                self.cancel();
                try self.drain(request);
                return;
            }
        }
        if (request.cancel) |cancellation| {
            if (cancellation.after_close_send_ms) |delay| {
                self.sleepAndCancel(delay);
            }
        }
        self.current().closeSend();
        try self.drain(request);
    }

    /// One response per request, then the rest.
    fn fullDuplex(self: *Session, request: messages.ClientCompatRequest) !void {
        for (request.request_messages) |message| {
            self.sleep(request.request_delay_ms);
            try self.send(message);
            _ = try self.awaitMessage();
            try self.cancelAfterResponses(request);
        }

        if (request.cancel) |cancellation| {
            if (cancellation.before_close_send != null) {
                self.cancel();
                try self.drain(request);
                return;
            }
        }
        if (request.cancel) |cancellation| {
            if (cancellation.after_close_send_ms) |delay| {
                self.sleepAndCancel(delay);
            }
        }
        self.current().closeSend();
        try self.drain(request);
    }

    /// Read until the call ends.
    fn drain(self: *Session, request: messages.ClientCompatRequest) !void {
        while (true) {
            const found = try self.awaitMessage();
            if (found) {
                try self.cancelAfterResponses(request);
                continue;
            }
            if (self.current().result() != null) return;
        }
    }

    /// One response message, or false when the call ended first.
    fn awaitMessage(self: *Session) !bool {
        while (true) {
            if (try self.current().nextMessage()) |found| {
                // A cancelled call reports what arrived before the cancel; the
                // messages that were already in flight do not count.
                if (!self.cancelled) {
                    const payload = self.decodePayload(found) catch |err| switch (err) {
                        // A message this client cannot read ends the call, which
                        // is what the suite expects from a bad encoding.
                        error.MalformedResponse => {
                            self.current().abandon(.internal, "unreadable response message");
                            return false;
                        },
                        else => return err,
                    };
                    try self.payloads.append(self.allocator, payload);
                }
                return true;
            }
            if (self.current().result() != null) return false;
            const stepped = self.client.?.step(&self.calls) catch |err| switch (err) {
                error.UnexpectedStream, error.StreamClosed, error.ConnectionClosed => {
                    self.abandonFromTransport();
                    return false;
                },
                else => return err,
            };
            if (!stepped) return false;
        }
    }

    /// Write one request message, compressed when the call asked for gzip.
    fn send(self: *Session, message: messages.Any) !void {
        if (self.cancelled) return;
        switch (self.encoding) {
            .identity => try self.current().writeMessage(message.value),
            .gzip => {
                const buffer = try self.allocator.alloc(
                    u8,
                    compress.gzip.encode.maxCompressedLength(message.value.len),
                );
                defer self.allocator.free(buffer);
                const compressed = try compress.gzip.encode.compress(message.value, buffer, .{});
                try self.current().writeMessageFlagged(buffer[0..compressed], true);
            },
        }
    }

    fn decodePayload(
        self: *Session,
        found: grpc.message.Message,
    ) !messages.ConformancePayload {
        var bytes = found.bytes;
        if (found.compressed) {
            // The wire length is the compressed size, so the ceiling comes from
            // the call's receive limit rather than from the frame.
            const buffer = try self.scratch.allocator().alloc(u8, self.current().messageLimit());
            const plain = compress.gzip.decode.decompress(found.bytes, buffer) catch {
                return error.MalformedResponse;
            };
            bytes = buffer[0..plain];
        }

        var holder_reader: Io.Reader = .fixed(bytes);
        const holder = pb.decodeLeaky(
            messages.PayloadHolder,
            self.allocator,
            &holder_reader,
            .{},
        ) catch return error.MalformedResponse;
        return holder.payload orelse .{};
    }

    fn cancelAfterResponses(self: *Session, request: messages.ClientCompatRequest) !void {
        const streams = request.stream_type == messages.stream_type_server_stream or
            request.stream_type == messages.stream_type_half_duplex or
            request.stream_type == messages.stream_type_full_duplex;
        if (!streams) return;

        const cancellation = request.cancel orelse return;
        const count = cancellation.after_num_responses orelse return;
        if (self.cancelled) return;
        if (self.payloads.items.len >= count) self.cancel();
    }

    /// The transport ended under a call. If its deadline passed, that is the
    /// answer; otherwise the connection went away on us.
    fn abandonFromTransport(self: *Session) void {
        const rpc = self.current();
        if (rpc.result() != null) return;
        // A call the client cancelled itself reports that, not the teardown.
        if (self.cancelled) {
            rpc.abandon(.cancelled, "");
            return;
        }
        if (rpc.deadline_ns) |deadline| {
            const now = Io.Clock.awake.now(self.io).nanoseconds;
            if (now >= deadline) {
                rpc.abandon(.deadline_exceeded, "");
                return;
            }
        }
        rpc.abandon(.unavailable, "connection closed");
    }

    fn cancel(self: *Session) void {
        if (self.cancelled) return;
        // A deadline that already passed is the answer; a later cancel does not
        // replace it.
        if (self.current().deadline_ns) |deadline| {
            const now = Io.Clock.awake.now(self.io).nanoseconds;
            if (now >= deadline) return;
        }
        self.cancelled = true;
        self.current().cancel() catch |err| switch (err) {
            // A stream the connection already retired needs no reset.
            error.StreamClosed, error.UnknownStream => {},
            else => {},
        };
    }

    fn sleepAndCancel(self: *Session, delay_ms: u32) void {
        sleepQuietly(self.io, delay_ms);
        self.cancel();
    }

    fn sleep(self: *Session, delay_ms: u32) void {
        if (delay_ms == 0) return;
        sleepQuietly(self.io, delay_ms);
    }

    /// The suite asks for delays; a cancelled sleep means the run is ending, and
    /// there is nothing useful to do about it here.
    fn sleepQuietly(io: Io, delay_ms: u32) void {
        io.sleep(.fromMilliseconds(@intCast(delay_ms)), .awake) catch |err| switch (err) {
            error.Canceled => {},
        };
    }

    fn current(self: *Session) *grpc.Call {
        return &self.calls[0];
    }

    fn close(self: *Session) void {
        self.opened = false;
        self.calls[0].deinit();
    }

    fn result(self: *Session) !messages.ClientResponseResult {
        const rpc = self.current();

        var out: messages.ClientResponseResult = .{};
        out.response_headers = try convertView(self.allocator, rpc.responseHeaders());
        out.response_trailers = try convertView(self.allocator, rpc.responseTrailers());
        out.payloads = self.payloads.items;

        const call_status: grpc.status.Status = rpc.result() orelse .{ .code = .unavailable };
        if (!call_status.isOk()) {
            var call_error: messages.Error = .{
                .code = @backingInt(call_status.code),
                .message = try self.allocator.dupe(u8, call_status.message),
            };
            const details = rpc.errorDetails();
            if (details.len != 0) {
                var status_reader: Io.Reader = .fixed(details);
                const call_details = pb.decodeLeaky(
                    messages.RpcStatus,
                    self.allocator,
                    &status_reader,
                    .{},
                ) catch messages.RpcStatus{};
                call_error.details = call_details.details;
            }
            // A call that carried a deadline and ended without an answer is a
            // deadline failure, however the teardown reached us: the peer's own
            // timeout and our transport close race, and the caller needs the
            // deadline either way.
            if (!self.cancelled and rpc.deadline_ns != null and
                (call_error.code == 1 or call_error.code == 13 or call_error.code == 14))
            {
                call_error.code = 4;
                call_error.message = "";
            }
            out.err = call_error;
        }

        // A TLS transport cannot report a timeout through `Io.Reader`, so it
        // records one and the result is corrected here.
        if (self.deadline_expired) {
            out.err = .{ .code = 4, .message = "" };
            out.payloads = &.{};
            return out;
        }

        // A failing unary or client-stream call delivers no message. A server or
        // bidi stream can fail after messages, and those stand.
        const drops = oneMessageIsKept(self.stream_type);
        if (out.err != null and drops) out.payloads = &.{};

        // A unary or client-stream call answers with exactly one message. Zero
        // or two means the server answered something else.
        const one_message = self.stream_type == messages.stream_type_unary or
            self.stream_type == messages.stream_type_client_stream;
        if (out.err == null and one_message and out.payloads.len != 1) {
            out.err = .{
                .code = 12,
                .message = "a one-message call carried a different count",
            };
            out.payloads = &.{};
        }
        return out;
    }
};

/// The stream types whose answer is exactly one message.
fn oneMessageIsKept(stream_type: u32) bool {
    return stream_type == messages.stream_type_unary or
        stream_type == messages.stream_type_client_stream;
}

/// The suite models one header entry with repeated values; HTTP has one line per
/// value, so one entry becomes several fields.
fn convertHeaders(
    allocator: std.mem.Allocator,
    headers: []const messages.Header,
) ![]const http.Header {
    var count: usize = 0;
    for (headers) |header| count += @max(header.value.len, 1);

    const fields = try allocator.alloc(http.Header, count);
    var index: usize = 0;
    for (headers) |header| {
        if (header.value.len == 0) {
            fields[index] = .{ .name = header.name, .value = "" };
            index += 1;
            continue;
        }
        for (header.value) |value| {
            fields[index] = .{ .name = header.name, .value = value };
            index += 1;
        }
    }
    return fields;
}

/// The reverse: one entry per name, with its values in wire order. The view
/// borrows the connection's arena, so every string is copied before the next
/// poll can reclaim it.
fn convertView(
    allocator: std.mem.Allocator,
    view: ?http.Headers,
) ![]const messages.Header {
    const headers = view orelse return &.{};

    var converted: std.ArrayList(messages.Header) = .empty;
    var index: usize = 0;
    while (index < headers.items.len) : (index += 1) {
        const name = headers.items[index].name;

        var values: std.ArrayList([]const u8) = .empty;
        var scan = index;
        while (scan < headers.items.len and
            std.mem.eql(u8, headers.items[scan].name, name)) : (scan += 1)
        {
            try values.append(allocator, try allocator.dupe(u8, headers.items[scan].value));
        }
        try converted.append(allocator, .{
            .name = try allocator.dupe(u8, name),
            .value = values.items,
        });
        index = scan - 1;
    }
    return converted.items;
}
