//! A blocking gRPC unary call over `std.Io`.
//!
//! This is the convenience over `Call`: it runs the socket-facing loop, and the
//! caller owns the stream. It sends one request body and returns one response
//! message, so a streaming RPC drives `Call` and `h2.client.Connection`
//! directly.
//!
//! The deadline field goes on the wire but is not enforced here. A caller that
//! needs the deadline to be a real one runs `Call` under its own `Io` timer.

const std = @import("std");
const http = @import("http");
const head = @import("head.zig");
const message = @import("message.zig");
const status = @import("status.zig");
const timeout = @import("timeout.zig");
const Call = @import("Call.zig");

const Io = std.Io;
const h2 = http.h2;
const sim = @import("sim.zig");

/// The module is called `status`, so a method of that name needs a type alias
/// to stay unambiguous.
const Status = status.Status;

pub const Request = struct {
    /// `:authority`, as in `host:port`.
    authority: []const u8,
    service: []const u8,
    method: []const u8,
    /// Framed request messages, from `message.write`.
    body: []const u8 = &.{},
    /// `.https` when the transport under the reader and writer is TLS.
    scheme: http.client.Scheme = .http,
    deadline_ns: ?u64 = null,
    metadata: []const http.Header = &.{},
};

// ast-grep-ignore: public-struct-field-default -- caller-owned buffers; the sizes are the policy
pub const Config = struct {
    allocator: std.mem.Allocator,
    /// Transport read scratch.
    read_buffer: []u8,
    /// The transport writer's buffer.
    write_buffer: []u8,
    /// One frame of h2 output.
    frame_buffer: []u8,
    /// Request head field storage.
    fields: []http.Header,
    /// Scratch for the path and the encoded deadline.
    scratch: []u8,
    /// Destination for the response message.
    message: []u8,
};

pub const Response = struct {
    status: Status,
    /// The response message with the length prefix removed, a prefix of
    /// `Config.message`. Empty when the call carried no message.
    message: []const u8,
};

pub const Error = error{
    /// A unary call got a second message. The RPC is not unary.
    TooManyMessages,
    /// The message did not fit `Config.message`.
    MessageTooLarge,
    /// The connection stopped accepting input.
    InputFull,
};

/// Send one request and read one response over a caller-owned stream.
pub fn unary(io: Io, stream: Io.net.Stream, request: Request, config: Config) !Response {
    var stream_reader = stream.reader(io, &.{});
    var stream_writer = stream.writer(io, config.write_buffer);
    return unaryIo(&stream_reader.interface, &stream_writer.interface, request, config);
}

/// The same call over plaintext reader and writer interfaces. A TLS adapter
/// calls this after its handshake, which is why this takes no stream.
pub fn unaryIo(
    reader: *Io.Reader,
    writer: *Io.Writer,
    request: Request,
    config: Config,
) !Response {
    var conn: h2.client.Connection = undefined;
    try conn.init(.{ .allocator = config.allocator });
    defer conn.deinit();

    const target = try head.path(config.scratch, .{
        .service = request.service,
        .method = request.method,
    });
    var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
    const fields = try head.fields(config.fields, &timeout_buf, .{
        .service = request.service,
        .method = request.method,
        .deadline_ns = request.deadline_ns,
        .metadata = request.metadata,
    });

    const stream = try conn.open(.{
        .method_token = "POST",
        .scheme = request.scheme,
        .authority = request.authority,
        .target = target,
        .headers = fields,
    }, false);

    var call = Call.init(config.allocator, stream, request.body, .{});
    defer call.deinit();

    try drive(&conn, reader, writer, &call, config);
    return collect(&call, config);
}

fn drive(
    conn: *h2.client.Connection,
    reader: *Io.Reader,
    writer: *Io.Writer,
    call: *Call,
    config: Config,
) !void {
    while (call.result() == null) {
        try flush(conn, writer, config.frame_buffer);
        _ = try call.pump();
        try flush(conn, writer, config.frame_buffer);
        while (try call.step(conn)) {}
        if (call.result() != null) return;

        var iovecs: [1][]u8 = .{config.read_buffer};
        const n = reader.readVec(&iovecs) catch |err| switch (err) {
            error.EndOfStream => return,
            else => return err,
        };
        if (n == 0) return;

        var offset: usize = 0;
        while (offset < n) {
            const taken = conn.receive(config.read_buffer[offset..n]);
            if (taken == 0) return error.InputFull;
            offset += taken;
        }
    }
}

fn flush(conn: *h2.client.Connection, writer: *Io.Writer, buffer: []u8) !void {
    while (true) {
        const n = try conn.nextFrame(buffer);
        if (n == 0) break;
        try writer.writeAll(buffer[0..n]);
    }
    try writer.flush();
}

fn collect(call: *Call, config: Config) !Response {
    const closed: Status = .{ .code = .unavailable, .message = "connection closed" };
    const first = (try call.nextMessage()) orelse return .{
        .status = call.result() orelse closed,
        .message = &.{},
    };
    if (first.bytes.len > config.message.len) return error.MessageTooLarge;
    // Copy before asking for a second message: the next walk may clear the
    // buffer this slice points into.
    @memcpy(config.message[0..first.bytes.len], first.bytes);
    const body = config.message[0..first.bytes.len];
    if ((try call.nextMessage()) != null) return error.TooManyMessages;
    return .{
        .status = call.result() orelse closed,
        .message = body,
    };
}

test "a unary call over a loopback socket" {
    const testing = std.testing;

    const address = try Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var listener = try address.listen(testing.io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(testing.io);

    var client_address = address;
    client_address.setPort(listener.socket.address.getPort());

    var reply_buffer: [64]u8 = undefined;
    const reply_body = try message.write(&reply_buffer, "world", false);

    var server: sim.Server = .{
        .io = testing.io,
        .listener = &listener,
        .allocator = testing.allocator,
        .reply = .{ .body = reply_body, .code = "0" },
    };
    const thread = try std.Thread.spawn(.{}, sim.Server.run, .{&server});

    const stream = try client_address.connect(testing.io, .{ .mode = .stream });
    var joined = false;
    defer if (!joined) {
        stream.close(testing.io);
        thread.join();
    };

    var read_buffer: [16 * 1024]u8 = undefined;
    var write_buffer: [4 * 1024]u8 = undefined;
    var frame_buffer: [16 * 1024 + 9]u8 = undefined;
    var fields: [16]http.Header = undefined;
    var scratch: [128]u8 = undefined;
    var response_message: [256]u8 = undefined;

    var request_buffer: [64]u8 = undefined;
    const request_body = try message.write(&request_buffer, "hello", false);

    const response = try unary(testing.io, stream, .{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .body = request_body,
    }, .{
        .allocator = testing.allocator,
        .read_buffer = &read_buffer,
        .write_buffer = &write_buffer,
        .frame_buffer = &frame_buffer,
        .fields = &fields,
        .scratch = &scratch,
        .message = &response_message,
    });

    try testing.expectEqual(status.Code.ok, response.status.code);
    try testing.expectEqualStrings("world", response.message);
    try testing.expectEqualStrings("/helloworld.Greeter/SayHello", server.received.targetPath());
    try testing.expectEqualSlices(u8, request_body, server.received.bodyBytes());

    stream.close(testing.io);
    thread.join();
    joined = true;
    try testing.expect(server.result == null);
}

/// A server-streaming call in progress: the connection, the call, and the
/// socket, so the caller pulls one message at a time.
///
/// `init` takes a pointer because the h2 stream inside the call points at the
/// connection, and a moved struct would break that pointer. The caller keeps it
/// in place and calls `deinit`.
pub const ServerStream = struct {
    io: Io,
    socket: Io.net.Stream,
    transport_reader: Io.net.Stream.Reader,
    transport_writer: Io.net.Stream.Writer,
    conn: h2.client.Connection,
    call: Call,
    config: Config,
    finished: bool = false,

    pub fn init(
        self: *ServerStream,
        io: Io,
        socket: Io.net.Stream,
        request: Request,
        config: Config,
    ) !void {
        self.* = .{
            .io = io,
            .socket = socket,
            .transport_reader = socket.reader(io, &.{}),
            .transport_writer = socket.writer(io, config.write_buffer),
            .conn = undefined,
            .call = undefined,
            .config = config,
        };
        try self.conn.init(.{ .allocator = config.allocator });
        errdefer self.conn.deinit();
        try self.start(request);
    }

    pub fn deinit(self: *ServerStream) void {
        self.call.deinit();
        self.conn.deinit();
        self.socket.close(self.io);
        self.* = undefined;
    }

    /// The next message, or null when the stream ended. Read `status` after
    /// null: it is the answer, and it is UNIMPLEMENTED when the peer sent a
    /// compressed message this layer cannot read.
    pub fn next(self: *ServerStream) !?message.Message {
        while (true) {
            if (try self.call.nextMessage()) |found| return found;
            if (self.call.result() != null) return null;

            try self.flush();
            _ = try self.call.pump();
            try self.flush();

            var progressed = false;
            while (try self.call.step(&self.conn)) progressed = true;
            try self.flush();
            // Re-check the messages and the status before waiting on the wire.
            // Reading after a finished call blocks until a peer that has
            // already said everything it intends to say decides to hang up.
            if (progressed) continue;

            var iovecs: [1][]u8 = .{self.config.read_buffer};
            const n = self.transport_reader.interface.readVec(&iovecs) catch |err| switch (err) {
                error.EndOfStream => {
                    self.call.abandon(.unavailable, "connection closed");
                    return null;
                },
                else => return err,
            };
            if (n == 0) {
                self.call.abandon(.unavailable, "connection closed");
                return null;
            }
            try self.receive(n);
        }
    }

    /// The answer, or null while messages are still arriving.
    pub fn status(self: *const ServerStream) ?Status {
        return self.call.result();
    }

    fn start(self: *ServerStream, request: Request) !void {
        const target = try head.path(self.config.scratch, .{
            .service = request.service,
            .method = request.method,
        });
        var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
        const fields = try head.fields(self.config.fields, &timeout_buf, .{
            .service = request.service,
            .method = request.method,
            .deadline_ns = request.deadline_ns,
            .metadata = request.metadata,
        });

        const stream = try self.conn.open(.{
            .method_token = "POST",
            .scheme = request.scheme,
            .authority = request.authority,
            .target = target,
            .headers = fields,
        }, false);
        self.call = Call.init(self.config.allocator, stream, request.body, .{});
    }

    fn flush(self: *ServerStream) !void {
        const writer = &self.transport_writer.interface;
        while (true) {
            const n = try self.conn.nextFrame(self.config.frame_buffer);
            if (n == 0) break;
            try writer.writeAll(self.config.frame_buffer[0..n]);
        }
        try writer.flush();
    }

    fn receive(self: *ServerStream, n: usize) !void {
        var offset: usize = 0;
        while (offset < n) {
            const taken = self.conn.receive(self.config.read_buffer[offset..n]);
            if (taken == 0) return error.InputFull;
            offset += taken;
        }
    }
};

test "a server stream yields each message and then the status" {
    const testing = std.testing;

    const address = try Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var listener = try address.listen(testing.io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(testing.io);

    var client_address = address;
    client_address.setPort(listener.socket.address.getPort());

    var reply_buffer: [128]u8 = undefined;
    var reply_len: usize = 0;
    for ([_][]const u8{ "one", "two" }) |text| {
        const frame = try message.write(reply_buffer[reply_len..], text, false);
        reply_len += frame.len;
    }

    var server: sim.Server = .{
        .io = testing.io,
        .listener = &listener,
        .allocator = testing.allocator,
        .reply = .{ .body = reply_buffer[0..reply_len], .code = "0" },
    };
    const thread = try std.Thread.spawn(.{}, sim.Server.run, .{&server});

    const socket = try client_address.connect(testing.io, .{ .mode = .stream });

    var read_buffer: [16 * 1024]u8 = undefined;
    var write_buffer: [4 * 1024]u8 = undefined;
    var frame_buffer: [16 * 1024 + 9]u8 = undefined;
    var fields: [16]http.Header = undefined;
    var scratch: [128]u8 = undefined;
    var response_message: [256]u8 = undefined;

    var request_buffer: [64]u8 = undefined;
    const request_body = try message.write(&request_buffer, "hello", false);

    var streaming: ServerStream = undefined;
    try streaming.init(testing.io, socket, .{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .body = request_body,
    }, .{
        .allocator = testing.allocator,
        .read_buffer = &read_buffer,
        .write_buffer = &write_buffer,
        .frame_buffer = &frame_buffer,
        .fields = &fields,
        .scratch = &scratch,
        .message = &response_message,
    });

    const first = (try streaming.next()).?;
    try testing.expectEqualStrings("one", first.bytes);
    const second = (try streaming.next()).?;
    try testing.expectEqualStrings("two", second.bytes);
    try testing.expect((try streaming.next()) == null);
    try testing.expectEqual(status.Code.ok, streaming.status().?.code);

    streaming.deinit();
    thread.join();
    try testing.expect(server.result == null);
}
