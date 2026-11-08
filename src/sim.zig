//! A two-sided HTTP/2 pair for tests: a client connection and a server
//! session wired to each other through byte buffers and nothing else.
//!
//! There is no socket, no thread, and no handler. `Session` is h2's sans-I/O
//! server machine, so the bytes that cross this pair are the bytes a wire would
//! carry, and a test can put a call through a real exchange without a process.

const std = @import("std");
const http = @import("http");
const Io = std.Io;

const Session = http.h2.Session;
const Connection = http.h2.client.Connection;
const engine = http.h2.engine.blocking;
const Handler = http.h2.engine.Handler;
const Exchange = http.h2.engine.Exchange;

pub const Pair = struct {
    allocator: std.mem.Allocator,
    client: Connection,
    server: Session,

    pub fn init(allocator: std.mem.Allocator, streams: u16) !Pair {
        var pair: Pair = .{
            .allocator = allocator,
            .client = undefined,
            .server = undefined,
        };
        try pair.client.init(.{ .allocator = allocator, .max_streams = streams });
        errdefer pair.client.deinit();
        pair.server = try Session.init(allocator, .{ .role = .server, .max_streams = streams });
        return pair;
    }

    pub fn deinit(self: *Pair) void {
        self.client.deinit();
        self.server.deinit(self.allocator);
        self.* = undefined;
    }

    /// Move every byte the client has queued into the server.
    pub fn toServer(self: *Pair) !void {
        var buffer: [16 * 1024]u8 = undefined;
        while (true) {
            const n = try self.client.nextFrame(&buffer);
            if (n == 0) break;
            var offset: usize = 0;
            while (offset < n) {
                const taken = self.server.receive(buffer[offset..n]);
                if (taken == 0) return error.InputFull;
                offset += taken;
            }
        }
    }

    /// Move every byte the server has queued into the client.
    pub fn toClient(self: *Pair) !void {
        var buffer: [16 * 1024]u8 = undefined;
        while (true) {
            const n = self.server.send(&buffer);
            if (n == 0) break;
            var offset: usize = 0;
            while (offset < n) {
                const taken = self.client.receive(buffer[offset..n]);
                if (taken == 0) return error.InputFull;
                offset += taken;
            }
        }
    }

    /// Exchange SETTINGS both ways, so a stream can be opened afterwards.
    pub fn handshake(self: *Pair) !void {
        try self.toClient();
        try self.toServer();

        var seen_settings = false;
        while (true) {
            const event = (try self.server.nextEvent(self.allocator)) orelse return error.NoEvent;
            switch (event) {
                .request => return error.UnexpectedRequest,
                .settings => {
                    seen_settings = true;
                    try self.toClient();
                    break;
                },
                else => {},
            }
        }
        if (!seen_settings) return error.NoSettings;
        _ = try self.client.poll();
    }

    /// The next request the server sees, skipping anything queued before it.
    pub fn nextRequest(self: *Pair) !u31 {
        while (true) {
            const event = (try self.server.nextEvent(self.allocator)) orelse return error.NoEvent;
            switch (event) {
                .request => |value| return value.stream_id,
                else => {},
            }
        }
    }
};

/// What a test server answers with.
pub const Reply = struct {
    /// Framed response messages.
    body: []const u8 = &.{},
    /// The `grpc-status` value, as text.
    code: []const u8 = "0",
    /// The `grpc-message` value.
    text: []const u8 = "",
};

/// What a test server saw.
pub const Received = struct {
    body: [512]u8 = undefined,
    body_len: usize = 0,
    target: [128]u8 = undefined,
    target_len: usize = 0,

    pub fn bodyBytes(self: *const Received) []const u8 {
        return self.body[0..self.body_len];
    }

    pub fn targetPath(self: *const Received) []const u8 {
        return self.target[0..self.target_len];
    }
};

/// A one-shot gRPC server over a socket, driven by a raw h2 server session.
///
/// It accepts one connection, answers one call with `reply`, and runs until the
/// peer closes. It is a test peer: fixed buffers, one call, no dispatch.
/// A one-shot gRPC server over a socket, on http's blocking h2 engine.
///
/// There is one handler, one call, and fixed buffers: this is the test peer,
/// not a router. It exists so the client is exercised against a real server
/// machine, including the trailing HEADERS that carry `grpc-status`.
pub const Server = struct {
    io: Io,
    listener: *Io.net.Server,
    allocator: std.mem.Allocator,
    reply: Reply,
    received: Received = .{},
    /// The trailer section is borrowed by the engine until the exchange
    /// completes, which is after `handle` returns, so it lives here.
    trailers: [2]http.Header = undefined,
    result: ?anyerror = null,

    pub fn run(self: *Server) void {
        const stream = self.listener.accept(self.io) catch |err| {
            self.result = err;
            return;
        };
        defer stream.close(self.io);

        var read_buffer: [4096]u8 = undefined;
        var write_buffer: [4096]u8 = undefined;
        engine.serve(self.io, stream, self.handler(), .{
            .allocator = self.allocator,
            .read_buffer = &read_buffer,
            .write_buffer = &write_buffer,
            .max_streams = 4,
            .common = .{ .body_bytes = 1024 },
        }) catch |err| {
            self.result = err;
        };
    }

    fn handler(self: *Server) Handler {
        return .{ .ptr = self, .vtable = &.{
            .handle = handle,
            .complete = Handler.noopComplete,
            .report_error = reportError,
            .policy_response = Handler.defaultPolicyResponse,
        } };
    }

    fn handle(ptr: *anyopaque, exchange: *Exchange) anyerror!void {
        const self: *Server = @ptrCast(@alignCast(ptr));

        const path = exchange.request.head.path orelse "";
        const path_len = @min(path.len, self.received.target.len);
        @memcpy(self.received.target[0..path_len], path[0..path_len]);
        self.received.target_len = path_len;

        const body = exchange.request.body;
        const body_len = @min(body.len, self.received.body.len);
        @memcpy(self.received.body[0..body_len], body[0..body_len]);
        self.received.body_len = body_len;

        // The status goes in the trailing section, which is the whole reason a
        // gRPC server cannot be an ordinary request/response handler.
        self.trailers = .{
            .{ .name = "grpc-status", .value = self.reply.code },
            .{ .name = "grpc-message", .value = self.reply.text },
        };
        try exchange.respond(.{
            .status = .ok,
            .content_type = "application/grpc",
            .trailers = &self.trailers,
        }, self.reply.body);
    }

    fn reportError(ptr: *anyopaque, err: anyerror) void {
        const self: *Server = @ptrCast(@alignCast(ptr));
        self.result = err;
    }
};
