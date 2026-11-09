//! A readiness client: one connection, several calls in flight, no threads.
//!
//! The blocking client serves one call at a time on the calling thread. This one
//! opens many calls on the same connection, waits with `poll(2)` on the socket,
//! and hands each event to every call. Nothing here blocks except the socket
//! write, which the kernel accepts because a frame is at most 17 KiB.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const testing = std.testing;

const http = @import("http");
const h2 = http.h2;

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
    calls: []Call = &.{},
    used: []bool = &.{},
    message_limit: u32,
    opened: bool = false,

    /// The session owns the socket: `deinit` closes it.
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
        self.calls[index].deadline_ns = blocking.absoluteDeadline(self.io, request.deadline_ns);
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

    /// Advance every call: write what is queued, hand each event to each call,
    /// and wait for readiness until the nearest deadline or the next event.
    ///
    /// False means no call is still waiting for a status.
    pub fn wait(self: *Session) !bool {
        while (true) {
            const now = Io.Clock.now(.awake, self.io).nanoseconds;
            for (self.calls, 0..) |*call, index| {
                if (self.used[index]) _ = blocking.expireCall(call, now);
            }
            try self.flush();
            for (self.calls, 0..) |*call, index| {
                if (self.used[index]) _ = try call.pump();
            }
            try self.flush();

            var handled = false;
            while (try self.conn.poll()) |event| {
                const observed = Io.Clock.now(.awake, self.io).nanoseconds;
                for (self.calls, 0..) |*call, index| {
                    if (!self.used[index]) continue;
                    _ = blocking.expireCall(call, observed);
                    try call.handle(event);
                }
                handled = true;
            }
            try self.flush();

            const remaining = self.running();
            if (remaining == 0) return false;
            if (handled) continue;

            if (!self.conn.wantsInput()) {
                for (self.calls, 0..) |*call, index| {
                    if (self.used[index] and call.result() == null) {
                        call.abandon(.unavailable, "connection closed");
                    }
                }
                return false;
            }

            switch (try self.tryRead()) {
                .ready => {},
                .timeout => {
                    const expired_at = Io.Clock.now(.awake, self.io).nanoseconds;
                    for (self.calls, 0..) |*call, index| {
                        if (self.used[index]) _ = blocking.expireCall(call, expired_at);
                    }
                },
                .eof => {
                    for (self.calls, 0..) |*call, index| {
                        if (self.used[index] and call.result() == null) {
                            call.abandon(.unavailable, "connection closed");
                        }
                    }
                    return false;
                },
            }
        }
    }

    /// Wait for the socket, bounded by the nearest call deadline. This is
    /// `Io.Batch.awaitConcurrent` with one `net_read`, the same primitive the
    /// blocking driver uses, rather than a hand-rolled poll.
    fn tryRead(self: *Session) !Outcome {
        var nearest: ?i96 = null;
        for (self.calls, 0..) |*call, index| {
            if (!self.used[index] or call.result() != null) continue;
            const deadline = call.deadline_ns orelse continue;
            if (nearest == null or deadline < nearest.?) nearest = deadline;
        }

        var timeout: Io.Timeout = .none;
        if (nearest) |deadline| {
            const now = Io.Clock.now(.awake, self.io).nanoseconds;
            if (now >= deadline) return .timeout;
            timeout = .{ .deadline = Io.Timestamp
                .fromNanoseconds(deadline)
                .withClock(Io.Clock.awake) };
        }

        var storage: [1]Io.Operation.Storage = undefined;
        var batch: Io.Batch = .init(&storage);
        var iovecs: [1][]u8 = .{&self.read_buffer};
        _ = batch.add(.{ .net_read = .{
            .socket_handle = self.socket.socket.handle,
            .data = &iovecs,
        } });
        batch.awaitConcurrent(self.io, timeout) catch |err| switch (err) {
            error.Timeout, error.Canceled => return .timeout,
            error.ConcurrencyUnavailable => return error.DeadlineUnsupported,
        };

        while (batch.next()) |completion| {
            switch (completion.result) {
                .net_read => |result| {
                    const ready = result catch |err| switch (err) {
                        error.ConnectionResetByPeer => return .eof,
                        else => return err,
                    };
                    if (ready.data_len == 0) return .eof;

                    var offset: usize = 0;
                    while (offset < ready.data_len) {
                        const taken = self.conn.receive(self.read_buffer[offset..ready.data_len]);
                        if (taken == 0) return error.InputFull;
                        offset += taken;
                    }
                    return .ready;
                },
                else => {},
            }
        }
        return .timeout;
    }

    const Outcome = enum { ready, timeout, eof };

    fn flush(self: *Session) !void {
        const writer = &self.transport_writer.interface;
        while (true) {
            const n = try self.conn.nextFrame(self.frame);
            if (n == 0) break;
            try writer.writeAll(self.frame[0..n]);
        }
        try writer.flush();
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
