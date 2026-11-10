//! A readiness client: one connection, several calls in flight, no threads.
//!
//! A session owns the call slots and dispatches HTTP events to each call.
//! The shared HTTP connection driver handles socket reads, frame writes, and
//! deadline waits. Each call keeps its own absolute deadline.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const testing = std.testing;

const http = @import("http");
const h2 = http.h2;
const Driver = h2.client.Driver;

const blocking = @import("blocking.zig");
const Call = @import("Call.zig");
const message = @import("message.zig");
const server_mod = @import("server.zig");
const status = @import("status.zig");

const read_capacity = 16 * 1024;
const frame_capacity = 16 * 1024 + 9;

pub const Options = struct {
    allocator: Allocator,
    /// Calls this session can carry at once.
    max_calls: usize = 16,
    /// The largest message each call accepts, before decompression.
    message_limit: u32 = message.default_limit,
};

pub const Error = error{
    /// Every call slot is in use.
    TooManyCalls,
    /// The peer ended the connection with calls still open.
    ConnectionClosed,
    /// The connection will not take more bytes from this driver.
    InputFull,
};

pub const Session = struct {
    io: Io,
    allocator: Allocator,
    socket: Io.net.Stream,
    transport_reader: Io.net.Stream.Reader,
    transport_writer: Io.net.Stream.Writer,
    read_buffer: [read_capacity]u8 = undefined,
    frame: []u8 = &.{},
    conn: h2.client.Connection,
    driver: Driver,
    calls: []Call = &.{},
    used: []bool = &.{},
    message_limit: u32,
    opened: bool = false,

    /// On success, the session owns the socket and `deinit` closes it.
    /// On failure, the caller retains the socket.
    pub fn init(
        self: *Session,
        options: Options,
        io: Io,
        socket: Io.net.Stream,
    ) !void {
        self.* = .{
            .io = io,
            .allocator = options.allocator,
            .socket = socket,
            .transport_reader = socket.reader(io, &.{}),
            .transport_writer = socket.writer(io, &.{}),
            .conn = undefined,
            .driver = undefined,
            .message_limit = options.message_limit,
        };
        self.frame = try options.allocator.alloc(u8, frame_capacity);
        errdefer options.allocator.free(self.frame);
        self.calls = try options.allocator.alloc(Call, options.max_calls);
        errdefer options.allocator.free(self.calls);
        self.used = try options.allocator.alloc(bool, options.max_calls);
        errdefer options.allocator.free(self.used);
        @memset(self.used, false);

        try self.conn.init(.{ .allocator = options.allocator });
        self.driver.init(
            &self.conn,
            &self.transport_reader.interface,
            &self.transport_writer.interface,
            .{
                .read_buffer = &self.read_buffer,
                .frame_buffer = self.frame,
                .wait = .{ .socket = .{ .io = io, .fd = socket.socket.handle } },
            },
        );
        self.opened = true;
    }

    pub fn deinit(self: *Session) void {
        for (self.calls, 0..) |*call, index| {
            if (self.used[index]) call.deinit();
        }
        self.conn.deinit();
        self.allocator.free(self.used);
        self.allocator.free(self.calls);
        self.allocator.free(self.frame);
        self.socket.close(self.io);
        self.* = undefined;
    }

    /// Open a call on this connection. The call belongs to the session until
    /// `close` is called on its slot.
    pub fn open(self: *Session, request: blocking.Request) !*Call {
        if (self.driver.isClosed()) return error.ConnectionClosed;
        const deadline = blocking.absoluteDeadline(self.io, request.deadline_ns);
        var slot: ?usize = null;
        for (self.used, 0..) |in_use, index| {
            if (!in_use) {
                slot = index;
                break;
            }
        }
        const index = slot orelse return error.TooManyCalls;

        const opened = try blocking.openCall(&self.conn, self.allocator, request);
        self.calls[index] = try Call.init(
            self.allocator,
            opened.call,
            opened.body,
            blocking.callOptions(request),
        );
        self.calls[index].deadline_ns = deadline;
        self.used[index] = true;
        return &self.calls[index];
    }

    /// Hand a slot back. Its call is finished or abandoned.
    pub fn close(self: *Session, call: *Call) void {
        for (self.calls, 0..) |*slot, index| {
            if (slot == call) {
                if (self.used[index]) {
                    self.used[index] = false;
                    call.deinit();
                }
                return;
            }
        }
    }

    /// Calls still waiting for a status.
    pub fn running(self: *const Session) usize {
        var count: usize = 0;
        for (self.calls, 0..) |*call, index| {
            if (self.used[index] and call.result() == null) count += 1;
        }
        return count;
    }

    /// Advance every call and return after output or event progress.
    /// Otherwise, wait for input until the nearest call deadline.
    ///
    /// False means no call is still waiting for a status.
    pub fn wait(self: *Session) !bool {
        if (self.driver.isClosed()) {
            self.endCalls();
            return false;
        }
        while (true) {
            const now = Io.Clock.now(.awake, self.io).nanoseconds;
            for (self.calls, 0..) |*call, index| {
                if (self.used[index]) _ = blocking.expireCall(call, now);
            }
            var wrote = self.conn.hasPendingOutput();
            try self.driver.flush();
            for (self.calls, 0..) |*call, index| {
                if (self.used[index]) _ = try call.pump();
            }
            wrote = self.conn.hasPendingOutput() or wrote;
            try self.driver.flush();

            var handled = false;
            while (try self.conn.poll()) |event| {
                const observed = Io.Clock.now(.awake, self.io).nanoseconds;
                var matched = false;
                for (self.calls, 0..) |*call, index| {
                    if (!self.used[index]) continue;
                    _ = blocking.expireCall(call, observed);
                    if (event == .data and event.data.stream == call.stream.id) matched = true;
                    try call.handle(event);
                }
                if (event == .data and !matched) try event.data.lease.release();
                handled = true;
            }
            try self.driver.flush();

            const remaining = self.running();
            if (remaining == 0) return false;
            if (handled or wrote) return true;

            if (!self.conn.wantsInput() and !self.driver.hasPendingInput()) {
                self.endCalls();
                return false;
            }

            switch (try self.tryRead()) {
                .ready => return true,
                .timeout => {
                    const expired_at = Io.Clock.now(.awake, self.io).nanoseconds;
                    for (self.calls, 0..) |*call, index| {
                        if (self.used[index]) _ = blocking.expireCall(call, expired_at);
                    }
                    return self.running() != 0;
                },
                .eof => {
                    self.endCalls();
                    return false;
                },
            }
        }
    }

    fn endCalls(self: *Session) void {
        const now = Io.Clock.now(.awake, self.io).nanoseconds;
        for (self.calls, 0..) |*call, index| {
            if (!self.used[index]) continue;
            _ = blocking.expireCall(call, now);
            call.abandon(.unavailable, "connection closed");
        }
    }

    /// Give the HTTP driver the earliest deadline among unfinished calls.
    fn tryRead(self: *Session) !Driver.ReadResult {
        var nearest: ?i96 = null;
        for (self.calls, 0..) |*call, index| {
            if (!self.used[index] or call.result() != null) continue;
            const deadline = call.deadline_ns orelse continue;
            if (nearest == null or deadline < nearest.?) nearest = deadline;
        }

        return self.driver.read(nearest) catch |err| {
            if (!self.driver.isClosed()) return err;
            return .eof;
        };
    }
};

test {
    _ = @import("sim.zig");
}

/// Serves the readiness engine until the test stops it.
const Harness = struct {
    server: server_mod.ReadinessServer,
    result: ?anyerror = null,

    fn init(self: *Harness, handler: server_mod.Handler) !void {
        try self.server.init(
            testing.io,
            try Io.net.IpAddress.parseLiteral("127.0.0.1:0"),
            handler,
            .{ .allocator = testing.allocator, .max_connections = 4 },
        );
        self.result = null;
    }

    fn start(self: *Harness) !std.Thread {
        return std.Thread.spawn(.{}, Harness.run, .{self});
    }

    fn run(self: *Harness) void {
        self.server.run() catch |err| {
            self.result = err;
        };
    }

    fn stop(self: *Harness, thread: std.Thread) void {
        self.server.requestStop();
        thread.join();
    }
};

const Echo = struct {
    fn handle(_: *Echo, request: server_mod.Request, response: *server_mod.Response) anyerror!void {
        // Echo each message back, which covers a unary call and a stream alike.
        var out: std.ArrayList([]const u8) = .empty;
        for (request.messages) |one| try out.append(request.arena, one.bytes);
        response.messages = out.items;
    }
};

const Slow = struct {
    io: Io,

    fn handle(self: *Slow, _: server_mod.Request, response: *server_mod.Response) anyerror!void {
        // A handler that sleeps blocks the server's single thread, which is the
        // point: the client has to give up on its own.
        self.io.sleep(.fromMilliseconds(500), .awake) catch |err| switch (err) {
            error.Canceled => return,
        };
        response.messages = &.{"late"};
    }
};

test "two calls in flight on one readiness session" {
    var echo: Echo = .{};
    var harness: Harness = undefined;
    try harness.init(server_mod.Handler.init(&echo, Echo.handle));
    defer harness.server.deinit();

    const thread = try harness.start();
    var joined = false;
    defer if (!joined) harness.stop(thread);

    const socket = try harness.server.address().connect(testing.io, .{ .mode = .stream });
    var session: Session = undefined;
    try session.init(.{ .allocator = testing.allocator }, testing.io, socket);
    var closed = false;
    defer if (!closed) session.deinit();

    var first_buffer: [64]u8 = undefined;
    var second_buffer: [64]u8 = undefined;
    const first_body = try message.write(&first_buffer, "first", false);
    const second_body = try message.write(&second_buffer, "second", false);

    const first = try session.open(.{
        .authority = "127.0.0.1",
        .service = "probe.Probe",
        .method = "Echo",
        .body = first_body,
    });
    const second = try session.open(.{
        .authority = "127.0.0.1",
        .service = "probe.Probe",
        .method = "Echo",
        .body = second_body,
    });
    // Both calls are open before either answer arrives: one poll feeds both.
    try testing.expectEqual(@as(usize, 2), session.running());

    while (try session.wait()) {}

    try testing.expectEqual(status.Code.ok, first.result().?.code);
    try testing.expectEqualStrings("first", (try first.nextMessage()).?.bytes);
    try testing.expectEqual(status.Code.ok, second.result().?.code);
    try testing.expectEqualStrings("second", (try second.nextMessage()).?.bytes);

    session.deinit();
    closed = true;
    harness.stop(thread);
    joined = true;
    try testing.expect(harness.result == null);
}

test "readiness yields after outbound progress before it reads EOF" {
    const address: Io.net.IpAddress = try .parse("127.0.0.1", 0);
    var listener = try address.listen(testing.io, .{ .mode = .stream });
    defer listener.deinit(testing.io);
    const socket = try listener.socket.address.connect(testing.io, .{ .mode = .stream });
    var initialized = false;
    defer if (!initialized) socket.close(testing.io);
    var session: Session = undefined;
    try session.init(.{ .allocator = testing.allocator }, testing.io, socket);
    initialized = true;
    defer session.deinit();

    var reader: Io.Reader = .fixed("\x00\x00\x00\x04\x00\x00\x00\x00\x00");
    var wire: [4096]u8 = undefined;
    var writer: Io.Writer = .fixed(&wire);
    session.driver.init(&session.conn, &reader, &writer, .{
        .read_buffer = &session.read_buffer,
        .frame_buffer = session.frame,
    });
    _ = try session.driver.read(null);
    while (try session.conn.poll()) |_| {}
    try session.driver.flush();

    const call = try session.open(.{
        .authority = "localhost",
        .service = "test.Service",
        .method = "Echo",
        .streaming = true,
    });
    try call.writeMessage("ping");
    try testing.expect(try session.wait());
    try testing.expect(call.result() == null);
    call.closeSend();
    try testing.expect(try session.wait());
    try testing.expect(call.result() == null);
    try testing.expect(!try session.wait());
    try testing.expectEqual(status.Code.unavailable, call.result().?.code);
}

test "readiness wait yields before the caller closes its send side" {
    var echo: Echo = .{};
    var harness: Harness = undefined;
    try harness.init(server_mod.Handler.init(&echo, Echo.handle));
    defer harness.server.deinit();
    const thread = try harness.start();
    defer harness.stop(thread);
    const socket = try harness.server.address().connect(testing.io, .{ .mode = .stream });
    var session: Session = undefined;
    try session.init(.{ .allocator = testing.allocator }, testing.io, socket);
    defer session.deinit();
    var warmup_buffer: [32]u8 = undefined;
    const warmup = try session.open(.{
        .authority = "localhost",
        .service = "test.Service",
        .method = "Echo",
        .body = try message.write(&warmup_buffer, "warmup", false),
    });
    while (try session.wait()) {}
    try testing.expectEqual(status.Code.ok, warmup.result().?.code);
    session.close(warmup);

    const call = try session.open(.{
        .authority = "localhost",
        .service = "test.Service",
        .method = "Echo",
        .streaming = true,
        .deadline_ns = std.time.ns_per_s,
    });
    try call.writeMessage("ping");
    try testing.expect(try session.wait());
    try testing.expect(call.result() == null);
    call.closeSend();
    while (try session.wait()) {}
    try testing.expectEqual(status.Code.ok, call.result().?.code);
    try testing.expectEqualStrings("ping", (try call.nextMessage()).?.bytes);
}

test "a readiness call gives up on its deadline" {
    var slow: Slow = .{ .io = testing.io };
    var harness: Harness = undefined;
    try harness.init(server_mod.Handler.init(&slow, Slow.handle));
    defer harness.server.deinit();

    const thread = try harness.start();
    var joined = false;
    defer if (!joined) harness.stop(thread);

    const socket = try harness.server.address().connect(testing.io, .{ .mode = .stream });
    var session: Session = undefined;
    try session.init(.{ .allocator = testing.allocator }, testing.io, socket);
    var closed = false;
    defer if (!closed) session.deinit();

    var request_buffer: [64]u8 = undefined;
    const body = try message.write(&request_buffer, "hello", false);

    const call = try session.open(.{
        .authority = "127.0.0.1",
        .service = "probe.Slow",
        .method = "Wait",
        .body = body,
        .deadline_ns = 100 * std.time.ns_per_ms,
    });

    while (try session.wait()) {}

    try testing.expectEqual(status.Code.deadline_exceeded, call.result().?.code);

    session.deinit();
    closed = true;
    harness.stop(thread);
    joined = true;
    try testing.expect(harness.result == null);
}
