//! A blocking gRPC call over `std.Io`.
//!
//! This is the convenience over `Call`: it runs the socket-facing loop, and the
//! caller owns the stream. `unary` sends one request body and returns one
//! response message. `ServerStream` pulls one message at a time. A streaming
//! RPC that wants to write between reads drives `Call` and
//! `h2.client.Connection` directly.
//!
//! Deadlines go on the wire as `grpc-timeout` and are not enforced here. A
//! caller that needs the deadline to be real runs the call under its own `Io`
//! timer.

const std = @import("std");
const http = @import("http");
const head = @import("head.zig");
const message = @import("message.zig");
const status = @import("status.zig");
const timeout = @import("timeout.zig");
const Call = @import("Call.zig");
const sim = @import("sim.zig");

const Io = std.Io;
const h2 = http.h2;

/// The module is called `status`, so a method of that name needs a type alias
/// to stay unambiguous.
const Status = status.Status;

/// One transport read. A DATA frame carries at most 16 KiB, so most reads end
/// on a frame boundary.
const read_capacity = 16 * 1024;
/// The transport writer's buffer.
const write_capacity = 4 * 1024;
/// h2 caps an outbound frame at 16 KiB, so one frame and its nine-byte header
/// fit. A buffer smaller than a queued frame makes `nextFrame` fail rather than
/// overrun.
const frame_capacity = 16 * 1024 + 9;

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

pub const Config = struct {
    allocator: std.mem.Allocator,
    /// Destination for the response: the message first, then the status text.
    /// Both `Response.message` and `Response.status.message` borrow it, so size
    /// it for the two together. A response that does not fit is
    /// `error.MessageTooLarge` rather than a truncated one.
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
    var write_buffer: [write_capacity]u8 = undefined;
    var stream_reader = stream.reader(io, &.{});
    var stream_writer = stream.writer(io, &write_buffer);
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
    var read_buffer: [read_capacity]u8 = undefined;
    const frame_buffer = try config.allocator.alloc(u8, frame_capacity);
    defer config.allocator.free(frame_buffer);

    var conn: h2.client.Connection = undefined;
    try conn.init(.{ .allocator = config.allocator });
    defer conn.deinit();

    const stream = try openCall(&conn, config.allocator, request);
    var call = Call.init(config.allocator, stream.call, stream.body, .{});
    defer call.deinit();

    var pump: Pump = .{
        .transport = .{
            .conn = &conn,
            .reader = reader,
            .writer = writer,
            .read_buffer = &read_buffer,
            .frame_buffer = frame_buffer,
        },
        .call = &call,
    };
    try pump.run();
    return collect(&call, config.message);
}

/// A connection that stays open across calls.
///
/// One call at a time: `unary` opens a stream, finishes it, and returns with
/// the connection ready for the next call. Concurrent calls need a driver that
/// polls once and hands each event to its stream, which this is not.
///
/// `init` takes a pointer because the h2 stream inside a call points at the
/// connection, and a moved struct would break that pointer.
pub const Client = struct {
    io: Io,
    allocator: std.mem.Allocator,
    socket: Io.net.Stream,
    transport_reader: Io.net.Stream.Reader,
    transport_writer: Io.net.Stream.Writer,
    read_buffer: [read_capacity]u8 = undefined,
    write_buffer: [write_capacity]u8 = undefined,
    frame: []u8 = &.{},
    conn: h2.client.Connection,

    /// The client owns the socket from here: `deinit` closes it.
    pub fn init(
        self: *Client,
        allocator: std.mem.Allocator,
        io: Io,
        socket: Io.net.Stream,
    ) !void {
        self.* = .{
            .io = io,
            .allocator = allocator,
            .socket = socket,
            .transport_reader = socket.reader(io, &.{}),
            .transport_writer = socket.writer(io, &self.write_buffer),
            .conn = undefined,
        };
        self.frame = try allocator.alloc(u8, frame_capacity);
        errdefer allocator.free(self.frame);
        try self.conn.init(.{ .allocator = allocator });
    }

    pub fn deinit(self: *Client) void {
        self.conn.deinit();
        self.allocator.free(self.frame);
        self.socket.close(self.io);
        self.* = undefined;
    }

    /// Open a call without waiting for its status. The call borrows this
    /// client's connection, so it must be dropped before the client.
    pub fn open(self: *Client, request: Request) !Call {
        const opened = try openCall(&self.conn, self.allocator, request);
        return Call.init(self.allocator, opened.call, opened.body, .{});
    }

    /// Drive every call one step: flush output, pump each request, hand each
    /// queued event to each call, and read when the connection wants input.
    ///
    /// False means no call can make progress right now. An event for a stream
    /// that is not in `calls` is dropped, so the slice must hold every call the
    /// connection carries.
    pub fn step(self: *Client, calls: []Call) !bool {
        var transport: Transport = .{
            .conn = &self.conn,
            .reader = &self.transport_reader.interface,
            .writer = &self.transport_writer.interface,
            .read_buffer = &self.read_buffer,
            .frame_buffer = self.frame,
        };

        var handled = false;
        try transport.flush();
        for (calls) |*call| _ = try call.pump();
        try transport.flush();

        while (try self.conn.poll()) |event| {
            for (calls) |*call| try call.handle(event);
            handled = true;
        }
        try transport.flush();
        if (handled) return true;
        if (!self.conn.wantsInput()) return false;
        return transport.read();
    }

    /// One unary call on the open connection.
    pub fn unary(self: *Client, request: Request, reply: []u8) !Response {
        const opened = try openCall(&self.conn, self.allocator, request);

        var call = Call.init(self.allocator, opened.call, opened.body, .{});
        defer call.deinit();

        var pump: Pump = .{
            .transport = .{
                .conn = &self.conn,
                .reader = &self.transport_reader.interface,
                .writer = &self.transport_writer.interface,
                .read_buffer = &self.read_buffer,
                .frame_buffer = self.frame,
            },
            .call = &call,
        };
        try pump.run();
        return collect(&call, reply);
    }
};

/// A server-streaming call in progress: the connection, the call, and the
/// socket, so the caller pulls one message at a time.
///
/// `init` takes a pointer because the h2 stream inside the call points at the
/// connection, and a moved struct would break that pointer. The caller keeps it
/// in place and calls `deinit`.
pub const ServerStream = struct {
    io: Io,
    allocator: std.mem.Allocator,
    socket: Io.net.Stream,
    transport_reader: Io.net.Stream.Reader,
    transport_writer: Io.net.Stream.Writer,
    read_buffer: [read_capacity]u8 = undefined,
    write_buffer: [write_capacity]u8 = undefined,
    frame_buffer: []u8 = &.{},
    conn: h2.client.Connection,
    call: Call,

    /// The stream owns the socket from here: `deinit` closes it.
    pub fn init(
        self: *ServerStream,
        allocator: std.mem.Allocator,
        io: Io,
        socket: Io.net.Stream,
        request: Request,
    ) !void {
        self.* = .{
            .io = io,
            .allocator = allocator,
            .socket = socket,
            .transport_reader = socket.reader(io, &.{}),
            .transport_writer = socket.writer(io, &self.write_buffer),
            .frame_buffer = &.{},
            .conn = undefined,
            .call = undefined,
        };
        self.frame_buffer = try allocator.alloc(u8, frame_capacity);
        errdefer allocator.free(self.frame_buffer);

        try self.conn.init(.{ .allocator = allocator });
        errdefer self.conn.deinit();

        const opened = try openCall(&self.conn, allocator, request);
        self.call = Call.init(allocator, opened.call, opened.body, .{});
    }

    pub fn deinit(self: *ServerStream) void {
        self.call.deinit();
        self.conn.deinit();
        self.allocator.free(self.frame_buffer);
        self.socket.close(self.io);
        self.* = undefined;
    }

    /// The next message, or null when the stream ended. Read `status` after
    /// null: it is the answer, and it is UNIMPLEMENTED when the peer sent a
    /// compressed message this layer cannot read.
    ///
    /// The message borrows the call's buffer and is valid until the next
    /// `next` call.
    pub fn next(self: *ServerStream) !?message.Message {
        while (true) {
            if (try self.call.nextMessage()) |found| return found;
            if (self.call.result() != null) return null;

            var pump: Pump = .{
                .transport = .{
                    .conn = &self.conn,
                    .reader = &self.transport_reader.interface,
                    .writer = &self.transport_writer.interface,
                    .read_buffer = &self.read_buffer,
                    .frame_buffer = self.frame_buffer,
                },
                .call = &self.call,
            };
            try pump.run();
        }
    }

    /// The answer, or null while messages are still arriving.
    pub fn status(self: *const ServerStream) ?Status {
        return self.call.result();
    }
};

/// A request head waiting for its call. `scratch` holds the `:path` and the
/// encoded deadline, and must outlive the call.
const Opened = struct {
    call: h2.client.Stream,
    body: []const u8,
};

fn openCall(
    conn: *h2.client.Connection,
    allocator: std.mem.Allocator,
    request: Request,
) !Opened {
    const path_len = try std.math.add(
        usize,
        try std.math.add(usize, request.service.len, request.method.len),
        2,
    );
    const scratch = try allocator.alloc(u8, path_len);
    defer allocator.free(scratch);

    const target = try head.path(scratch, .{
        .service = request.service,
        .method = request.method,
    });

    const field_count = head.fieldCount(.{
        .service = request.service,
        .method = request.method,
        .deadline_ns = request.deadline_ns,
        .metadata = request.metadata,
    });
    const fields = try allocator.alloc(http.Header, field_count);
    defer allocator.free(fields);

    var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
    const written = try head.fields(fields, &timeout_buf, .{
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
        .headers = written,
    }, false);

    return .{ .call = stream, .body = request.body };
}

/// The bytes between one connection and one transport. It knows nothing about
/// a call, so a driver that carries several calls uses it directly.
const Transport = struct {
    conn: *h2.client.Connection,
    reader: *Io.Reader,
    writer: *Io.Writer,
    read_buffer: []u8,
    frame_buffer: []u8,

    fn flush(self: *Transport) !void {
        while (true) {
            const n = try self.conn.nextFrame(self.frame_buffer);
            if (n == 0) break;
            try self.writer.writeAll(self.frame_buffer[0..n]);
        }
        try self.writer.flush();
    }

    /// Read one chunk into the connection. False on end of stream.
    fn read(self: *Transport) !bool {
        var iovecs: [1][]u8 = .{self.read_buffer};
        const n = self.reader.readVec(&iovecs) catch |err| switch (err) {
            error.EndOfStream => return false,
            else => return err,
        };
        if (n == 0) return false;

        var offset: usize = 0;
        while (offset < n) {
            const taken = self.conn.receive(self.read_buffer[offset..n]);
            if (taken == 0) return error.InputFull;
            offset += taken;
        }
        return true;
    }
};

/// One call driven over a transport.
const Pump = struct {
    transport: Transport,
    call: *Call,

    /// Drive the call until it has a status. Reads only when the connection
    /// wants input, because a null poll means nothing is queued now, not that
    /// the peer has nothing more to say.
    fn run(self: *Pump) !void {
        while (self.call.result() == null) {
            if (try self.advance()) continue;
            if (!self.transport.conn.wantsInput()) {
                self.call.abandon(.unavailable, "connection closed");
                return;
            }
            if (!try self.transport.read()) {
                self.call.abandon(.unavailable, "connection closed");
                return;
            }
        }
    }

    /// Push queued bytes out and take queued events in. True when an event was
    /// handled: the caller must re-check messages and status before reading.
    fn advance(self: *Pump) !bool {
        try self.transport.flush();
        _ = try self.call.pump();
        try self.transport.flush();

        var handled = false;
        while (try self.transport.conn.poll()) |event| {
            try self.call.handle(event);
            handled = true;
        }
        try self.transport.flush();
        return handled;
    }
};

fn collect(call: *Call, reply: []u8) !Response {
    const closed: Status = .{ .code = .unavailable, .message = "connection closed" };

    const first = (try call.nextMessage()) orelse {
        var result: Response = .{ .status = call.result() orelse closed, .message = &.{} };
        try copyStatusText(&result.status, reply, 0);
        return result;
    };
    if (first.bytes.len > reply.len) return error.MessageTooLarge;
    // Copy before asking for a second message: the next walk may clear the
    // buffer this slice points into.
    @memcpy(reply[0..first.bytes.len], first.bytes);
    const body = reply[0..first.bytes.len];
    if ((try call.nextMessage()) != null) return error.TooManyMessages;

    var result: Response = .{ .status = call.result() orelse closed, .message = body };
    try copyStatusText(&result.status, reply, body.len);
    return result;
}

/// Move the status text into caller storage, after the message. The call's own
/// arena is freed when it returns, so a status that borrowed it would leave the
/// caller holding a dangling slice.
fn copyStatusText(call_status: *Status, reply: []u8, offset: usize) !void {
    const text = call_status.message;
    if (text.len == 0) return;
    if (offset + text.len > reply.len) return error.MessageTooLarge;
    @memcpy(reply[offset..][0..text.len], text);
    call_status.message = reply[offset..][0..text.len];
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

    var request_buffer: [64]u8 = undefined;
    const request_body = try message.write(&request_buffer, "hello", false);
    var response_message: [256]u8 = undefined;

    const response = try unary(testing.io, stream, .{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .body = request_body,
    }, .{ .allocator = testing.allocator, .message = &response_message });

    try testing.expectEqual(status.Code.ok, response.status.code);
    try testing.expectEqualStrings("world", response.message);
    try testing.expectEqualStrings("/helloworld.Greeter/SayHello", server.received.targetPath());
    try testing.expectEqualSlices(u8, request_body, server.received.bodyBytes());

    stream.close(testing.io);
    thread.join();
    joined = true;
    try testing.expect(server.result == null);
}

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

    var request_buffer: [64]u8 = undefined;
    const request_body = try message.write(&request_buffer, "hello", false);

    var streaming: ServerStream = undefined;
    try streaming.init(testing.allocator, testing.io, socket, .{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .body = request_body,
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

test "one connection serves two calls in sequence" {
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

    const socket = try client_address.connect(testing.io, .{ .mode = .stream });

    var client: Client = undefined;
    try client.init(testing.allocator, testing.io, socket);

    var request_buffer: [64]u8 = undefined;
    const request_body = try message.write(&request_buffer, "hello", false);
    var first_reply: [256]u8 = undefined;
    var second_reply: [256]u8 = undefined;

    const first = try client.unary(.{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .body = request_body,
    }, &first_reply);
    const second = try client.unary(.{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .body = request_body,
    }, &second_reply);

    try testing.expectEqual(status.Code.ok, first.status.code);
    try testing.expectEqualStrings("world", first.message);
    try testing.expectEqual(status.Code.ok, second.status.code);
    try testing.expectEqualStrings("world", second.message);

    client.deinit();
    thread.join();
    try testing.expect(server.result == null);
}

test "two calls in flight on one connection" {
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

    const socket = try client_address.connect(testing.io, .{ .mode = .stream });

    var client: Client = undefined;
    try client.init(testing.allocator, testing.io, socket);

    var request_buffer: [64]u8 = undefined;
    const request_body = try message.write(&request_buffer, "hello", false);
    var calls: [2]Call = .{
        try client.open(.{
            .authority = "127.0.0.1",
            .service = "helloworld.Greeter",
            .method = "First",
            .body = request_body,
        }),
        try client.open(.{
            .authority = "127.0.0.1",
            .service = "helloworld.Greeter",
            .method = "Second",
            .body = request_body,
        }),
    };
    defer for (&calls) |*call| call.deinit();

    // Both calls are open before either finishes: one poll feeds both.
    var rounds: usize = 0;
    while (true) {
        rounds += 1;
        try testing.expect(rounds < 1000);
        if (try client.step(&calls)) continue;

        var pending = false;
        for (&calls) |*call| {
            if (call.result() == null) pending = true;
        }
        if (!pending) break;
        // No progress and a call still waiting means the driver lost an event.
        return error.NoProgress;
    }

    for (&calls) |*call| {
        try testing.expectEqual(status.Code.ok, call.result().?.code);
        const found = (try call.nextMessage()).?;
        try testing.expectEqualStrings("world", found.bytes);
    }

    client.deinit();
    thread.join();
    try testing.expect(server.result == null);
}
