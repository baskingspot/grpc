//! A server under test for the connectrpc conformance runner.
//!
//! In server mode the runner writes one `ServerCompatRequest` to this program's
//! stdin and then expects one length-prefixed `ServerCompatResponse` on stdout,
//! carrying the host and port it should dial. This program starts a gRPC server,
//! reports where it listens, and serves the conformance service until the runner
//! sends SIGTERM or closes stdin.
//!
//! Plaintext HTTP/2 only for now. A `use_tls` request is refused rather than
//! silently served in the clear, because a client that asked for TLS would then
//! fail verification for a reason that is not its own.

const std = @import("std");
const grpc = @import("grpc");
const http = @import("http");
const pb = @import("protobuf");
const compress = @import("compress");
const messages = @import("messages.zig");

const Io = std.Io;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa: std.mem.Allocator = init.arena.allocator();

    var in_buffer: [16 * 1024]u8 = undefined;
    var stdin_reader = Io.File.stdin().reader(io, &in_buffer);
    const reader = &stdin_reader.interface;

    var out_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = Io.File.stdout().writer(io, &out_buffer);
    const writer = &stdout_writer.interface;

    var length_bytes: [4]u8 = undefined;
    try reader.readSliceAll(&length_bytes);
    const length = std.mem.readInt(u32, &length_bytes, .big);
    const body = try gpa.alloc(u8, length);
    try reader.readSliceAll(body);

    var request_reader: Io.Reader = .fixed(body);
    const request = try pb.decodeLeaky(messages.ServerCompatRequest, gpa, &request_reader, .{});

    if (request.protocol != messages.protocol_grpc) return error.UnsupportedProtocol;
    if (request.use_tls) return error.UnsupportedTls;
    if (request.message_receive_limit != 0 and
        request.message_receive_limit < 4 * 1024 * 1024)
    {
        // The limit is a client-side option here: a message above it is what the
        // runner will ask us to reject. The engine's body cap is the only knob
        // this server has today.
    }

    var app: App = .{ .io = io, .limit = request.message_receive_limit };
    var server: grpc.server.ReadinessServer = undefined;
    try server.init(
        io,
        try Io.net.IpAddress.parseLiteral("127.0.0.1:0"),
        grpc.server.Handler.init(&app, App.handle),
        .{ .allocator = gpa, .max_connections = 1024 },
    );
    defer server.deinit();

    const response: messages.ServerCompatResponse = .{
        .host = "127.0.0.1",
        .port = server.address().getPort(),
    };

    var encoded: Io.Writer.Allocating = .init(gpa);
    defer encoded.deinit();
    try pb.encode(response, .{}, &encoded.writer);
    var size: [4]u8 = undefined;
    std.mem.writeInt(u32, &size, @intCast(encoded.written().len), .big);
    try writer.writeAll(&size);
    try writer.writeAll(encoded.written());
    try writer.flush();

    // The runner stops the process with SIGTERM; serving until the loop ends is
    // enough, and stdin closing is the other natural end.
    try server.run();
}

/// The conformance service. Unary and IdempotentUnary answer; everything else
/// is UNIMPLEMENTED, which the runner reports as a case rather than a hang.
const App = struct {
    io: Io,
    /// The largest message the runner says it will send. A bigger one must be
    /// rejected with RESOURCE_EXHAUSTED, which several cases check.
    limit: usize = 0,

    fn handle(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) anyerror!void {
        const slash = std.mem.findScalarLast(u8, request.path, '/');
        const method = if (slash) |at| request.path[at + 1 ..] else request.path;

        if (std.mem.eql(u8, method, "Unary") or std.mem.eql(u8, method, "IdempotentUnary")) {
            return self.unary(request, response);
        }
        if (std.mem.eql(u8, method, "ClientStream")) {
            return self.clientStream(request, response);
        }
        if (std.mem.eql(u8, method, "ServerStream")) {
            return self.serverStream(request, response);
        }
        if (std.mem.eql(u8, method, "BidiStream")) {
            return self.bidiStream(request, response);
        }
        if (std.mem.eql(u8, method, "Unimplemented")) {
            response.status = .{
                .code = .unimplemented,
                .message = "unimplemented on purpose",
            };
            return;
        }
        response.status = .{ .code = .unimplemented, .message = "not implemented yet" };
    }

    fn unary(self: *App, request: grpc.server.Request, response: *grpc.server.Response) !void {
        const kind = "UnaryRequest";
        if (request.messages.len != 1) {
            response.status = .{ .code = .internal, .message = "unary takes one message" };
            return;
        }
        const arena = request.arena;

        // A message the client compressed arrives compressed. The call layer
        // flags it and leaves the bytes alone, so the codec above it decodes.
        const maybe_wire = try self.plain(arena, request.messages[0], response);
        const wire = maybe_wire orelse return;

        var reader: Io.Reader = .fixed(wire);
        const decoded = try pb.decodeLeaky(messages.UnaryRequest, arena, &reader, .{});
        const definition: messages.UnaryResponseDefinition =
            decoded.response_definition orelse .{};

        response.headers = try convert(arena, definition.response_headers);
        response.metadata = try convert(arena, definition.response_trailers);

        if (definition.err) |failure| {
            return self.fail(
                kind,
                arena,
                request,
                response,
                @enumFromInt(@as(u8, @intCast(failure.code))),
                failure.message orelse "",
            );
        }
        if (definition.response_delay_ms != 0) {
            // A blocking engine has one thread, so this sleeps the loop. That
            // is the test's intent: a slow server.
            const delay_ms = definition.response_delay_ms;
            self.io.sleep(.fromMilliseconds(delay_ms), .awake) catch |err| switch (err) {
                error.Canceled => return,
            };
        }

        const data = if (definition.response_data.len != 0)
            definition.response_data
        else
            decoded.request_data;

        const requests = try arena.alloc(messages.Any, 1);
        requests[0] = .{
            .type_url = "type.googleapis.com/connectrpc.conformance.v1.UnaryRequest",
            .value = wire,
        };

        const reply: messages.UnaryResponse = .{
            .payload = .{
                .data = data,
                .request_info = .{
                    .request_headers = try headersOf(arena, request.metadata),
                    .requests = requests,
                },
            },
        };

        var encoded: Io.Writer.Allocating = .init(arena);
        try pb.encode(reply, .{}, &encoded.writer);
        const messages_out = try arena.alloc([]const u8, 1);
        messages_out[0] = encoded.written();
        response.messages = messages_out;
    }

    /// Every request message has arrived by the time a buffered engine calls
    /// the handler, so a client stream is one answer built from the last
    /// request's definition.
    fn clientStream(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) !void {
        const kind = "ClientStreamRequest";
        const arena = request.arena;
        if (request.messages.len == 0) {
            response.status = .{
                .code = .internal,
                .message = "client stream with no messages",
            };
            return;
        }
        const wire = try self.plain(arena, request.messages[0], response);
        if (wire == null) return;

        var reader: Io.Reader = .fixed(wire.?);
        const decoded = try pb.decodeLeaky(messages.ClientStreamRequest, arena, &reader, .{});
        const definition: messages.UnaryResponseDefinition =
            decoded.response_definition orelse .{};

        response.headers = try convert(arena, definition.response_headers);
        response.metadata = try convert(arena, definition.response_trailers);
        if (definition.err) |failure| {
            return self.fail(
                kind,
                arena,
                request,
                response,
                @enumFromInt(@as(u8, @intCast(failure.code))),
                failure.message orelse "",
            );
        }

        const requests = try arena.alloc(messages.Any, request.messages.len);
        for (request.messages, 0..) |one, at| {
            const body = (try self.plain(arena, one, response)) orelse return;
            requests[at] = .{
                .type_url = "type.googleapis.com/connectrpc.conformance.v1.ClientStreamRequest",
                .value = body,
            };
        }

        // The answer echoes the last request's data unless the definition
        // carries its own.
        const data = if (definition.response_data.len != 0)
            definition.response_data
        else blk: {
            var last_reader: Io.Reader = .fixed(requests[requests.len - 1].value);
            const last_decoded = try pb.decodeLeaky(
                messages.ClientStreamRequest,
                arena,
                &last_reader,
                .{},
            );
            break :blk last_decoded.request_data;
        };
        const payload: messages.ConformancePayload = .{
            .data = data,
            .request_info = .{
                .request_headers = try headersOf(arena, request.metadata),
                .timeout_ms = timeoutOf(request),
                .requests = requests,
            },
        };
        const reply: messages.UnaryResponse = .{ .payload = payload };
        var encoded: Io.Writer.Allocating = .init(arena);
        try pb.encode(reply, .{}, &encoded.writer);
        const out = try arena.alloc([]const u8, 1);
        out[0] = encoded.written();
        response.messages = out;
    }

    /// One request, many replies: every entry in the definition becomes a
    /// response message.
    fn serverStream(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) !void {
        const kind = "ServerStreamRequest";
        const arena = request.arena;
        if (request.messages.len == 0) {
            // A call can arrive with no message frames at all. Answer with the
            // definition it should have carried: nothing to say.
            return self.fail(kind, arena, request, response, .internal, "no request message");
        }
        const wire = (try self.plain(arena, request.messages[0], response)) orelse return;

        var reader: Io.Reader = .fixed(wire);
        const decoded = try pb.decodeLeaky(messages.ServerStreamRequest, arena, &reader, .{});
        const definition: messages.StreamResponseDefinition =
            decoded.response_definition orelse .{};

        response.headers = try convert(arena, definition.response_headers);
        response.metadata = try convert(arena, definition.response_trailers);
        if (definition.err) |failure| {
            return self.fail(
                kind,
                arena,
                request,
                response,
                @enumFromInt(@as(u8, @intCast(failure.code))),
                failure.message orelse "",
            );
        }

        // No entries means no response messages. The suite has a case for it.
        const datas = definition.response_data;
        const out = try arena.alloc([]const u8, datas.len);
        for (datas, 0..) |data, at| {
            var encoded: Io.Writer.Allocating = .init(arena);
            const reply: messages.UnaryResponse = .{ .payload = .{
                .data = data,
                .request_info = if (at == 0)
                    .{
                        .request_headers = try headersOf(arena, request.metadata),
                        .timeout_ms = timeoutOf(request),
                        .requests = try oneAny("ServerStreamRequest", arena, wire),
                    }
                else
                    .{},
            } };
            try pb.encode(reply, .{}, &encoded.writer);
            out[at] = try arena.dupe(u8, encoded.written());
        }
        response.messages = out;
    }

    /// Every request in, every reply out, in order. The buffered engine means a
    /// full-duplex call cannot answer mid-stream; it arrives complete.
    fn bidiStream(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) !void {
        const kind = "BidiStreamRequest";
        const arena = request.arena;
        if (request.messages.len == 0) {
            // A call can arrive with no message frames at all. Answer with the
            // definition it should have carried: nothing to say.
            return self.fail(kind, arena, request, response, .internal, "no request message");
        }
        const bodies = try arena.alloc([]const u8, request.messages.len);
        for (request.messages, 0..) |one, at| {
            bodies[at] = (try self.plain(arena, one, response)) orelse return;
        }

        var first_reader: Io.Reader = .fixed(bodies[0]);
        const first = try pb.decodeLeaky(messages.BidiStreamRequest, arena, &first_reader, .{});
        const definition: messages.StreamResponseDefinition =
            first.response_definition orelse .{};

        response.headers = try convert(arena, definition.response_headers);
        response.metadata = try convert(arena, definition.response_trailers);
        if (definition.err) |failure| {
            return self.fail(
                kind,
                arena,
                request,
                response,
                @enumFromInt(@as(u8, @intCast(failure.code))),
                failure.message orelse "",
            );
        }

        // No entries means no response messages, the same rule the server
        // stream follows.
        const datas = definition.response_data;

        const described = try arena.alloc(messages.Any, request.messages.len);
        for (bodies, 0..) |body, at| {
            described[at] = .{
                .type_url = "type.googleapis.com/connectrpc.conformance.v1.BidiStreamRequest",
                .value = body,
            };
        }

        const out = try arena.alloc([]const u8, datas.len);
        for (datas, 0..) |data, at| {
            var encoded: Io.Writer.Allocating = .init(arena);
            const reply: messages.UnaryResponse = .{ .payload = .{
                .data = data,
                .request_info = if (at == 0)
                    .{
                        .request_headers = try headersOf(arena, request.metadata),
                        .timeout_ms = timeoutOf(request),
                        .requests = described,
                    }
                else
                    .{},
            } };
            try pb.encode(reply, .{}, &encoded.writer);
            out[at] = try arena.dupe(u8, encoded.written());
        }
        response.messages = out;
    }

    /// The wrapped request bytes the suite expects in `request_info`.
    fn oneAny(
        comptime kind: []const u8,
        arena: std.mem.Allocator,
        wire: []const u8,
    ) ![]const messages.Any {
        const requests = try arena.alloc(messages.Any, 1);
        requests[0] = .{
            .type_url = "type.googleapis.com/connectrpc.conformance.v1." ++ kind,
            .value = wire,
        };
        return requests;
    }

    /// True when a message is over the limit the runner announced, in which case
    /// the answer is already the error the suite expects.
    fn overLimit(
        self: *App,
        comptime kind: []const u8,
        arena: std.mem.Allocator,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) !bool {
        if (self.limit == 0) return false;
        for (request.messages) |one| {
            // The limit applies to the message, not to its compressed size.
            var scratch: grpc.server.Response = .{};
            const body = if (one.compressed)
                (try self.plain(arena, one, &scratch)) orelse one.bytes
            else
                one.bytes;
            if (body.len <= self.limit) continue;
            try self.fail(
                kind,
                arena,
                request,
                response,
                .resource_exhausted,
                "message is larger than the server limit",
            );
            return true;
        }
        return false;
    }

    /// An error with a `RequestInfo` detail. The suite checks the detail on
    /// every error it asked for, so it travels in `grpc-status-details-bin`.
    fn fail(
        self: *App,
        comptime kind: []const u8,
        arena: std.mem.Allocator,
        request: grpc.server.Request,
        response: *grpc.server.Response,
        code: grpc.status.Code,
        message: []const u8,
    ) !void {
        response.status = .{ .code = code, .message = message };

        const requests = try arena.alloc(messages.Any, request.messages.len);
        for (request.messages, 0..) |one, at| {
            // The suite compares against the message it sent, so a compressed
            // one must be described decompressed.
            requests[at] = .{
                .type_url = "type.googleapis.com/connectrpc.conformance.v1." ++ kind,
                .value = one.bytes,
            };
            if (one.compressed) {
                var scratch: grpc.server.Response = .{};
                if (try self.plain(arena, one, &scratch)) |body| requests[at].value = body;
            }
        }
        const status_message: messages.RpcStatus = .{
            .code = @intCast(@intFromEnum(code)),
            .message = message,
            .details = try oneAnyPayload(arena, .{
                .request_headers = try headersOf(arena, request.metadata),
                .timeout_ms = timeoutOf(request),
                .requests = requests,
            }),
        };
        var encoded: Io.Writer.Allocating = .init(arena);
        try pb.encode(status_message, .{}, &encoded.writer);
        response.details = try arena.dupe(u8, encoded.written());
    }

    /// The deadline the client asked for, in milliseconds, as the suite wants it
    /// echoed back.
    fn timeoutOf(request: grpc.server.Request) ?i64 {
        const raw = request.metadata.get("grpc-timeout") orelse return null;
        if (raw.len < 2) return null;
        const amount = std.fmt.parseInt(i64, raw[0 .. raw.len - 1], 10) catch return null;
        return switch (raw[raw.len - 1]) {
            'n' => @divTrunc(amount, 1_000_000),
            'u' => @divTrunc(amount, 1_000),
            'm' => amount,
            'S' => amount * 1_000,
            'M' => amount * 60_000,
            'H' => amount * 3_600_000,
            else => null,
        };
    }

    /// `request_info` wrapped as the single detail the suite looks for.
    fn oneAnyPayload(
        arena: std.mem.Allocator,
        request_info: messages.ConformancePayload.RequestInfo,
    ) ![]const messages.Any {
        var encoded: Io.Writer.Allocating = .init(arena);
        try pb.encode(request_info, .{}, &encoded.writer);
        const out = try arena.alloc(messages.Any, 1);
        out[0] = .{
            .type_url = "type.googleapis.com/connectrpc.conformance.v1." ++
                "ConformancePayload.RequestInfo",
            .value = try arena.dupe(u8, encoded.written()),
        };
        return out;
    }

    /// The request message with any compression removed, or null when the answer
    /// is already an error status.
    fn plain(
        self: *App,
        arena: std.mem.Allocator,
        one: grpc.server.Message,
        response: *grpc.server.Response,
    ) !?[]const u8 {
        _ = self;
        if (!one.compressed) return one.bytes;
        const buffer = try arena.alloc(u8, 4 * 1024 * 1024);
        const n = compress.gzip.decode.decompress(one.bytes, buffer) catch {
            response.status = .{ .code = .internal, .message = "cannot decode gzip" };
            return null;
        };
        return buffer[0..n];
    }

    fn convert(arena: std.mem.Allocator, fields: []const messages.Header) ![]const http.Header {
        var count: usize = 0;
        for (fields) |field| count += @max(field.value.len, 1);
        const out = try arena.alloc(http.Header, count);
        var index: usize = 0;
        for (fields) |field| {
            if (field.value.len == 0) {
                out[index] = .{ .name = field.name, .value = "" };
                index += 1;
                continue;
            }
            for (field.value) |value| {
                out[index] = .{ .name = field.name, .value = value };
                index += 1;
            }
        }
        return out;
    }

    fn headersOf(arena: std.mem.Allocator, fields: http.Headers) ![]const messages.Header {
        var out: std.ArrayList(messages.Header) = .empty;
        var index: usize = 0;
        while (index < fields.items.len) : (index += 1) {
            const name = fields.items[index].name;
            var values: std.ArrayList([]const u8) = .empty;
            var scan = index;
            while (scan < fields.items.len and
                std.mem.eql(u8, fields.items[scan].name, name)) : (scan += 1)
            {
                try values.append(arena, fields.items[scan].value);
            }
            try out.append(arena, .{ .name = name, .value = values.items });
            index = scan - 1;
        }
        return out.items;
    }
};
