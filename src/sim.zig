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
pub const Server = struct {
    io: Io,
    listener: *Io.net.Server,
    allocator: std.mem.Allocator,
    reply: Reply,
    received: Received = .{},
    result: ?anyerror = null,

    pub fn run(self: *Server) void {
        self.acceptAndServe() catch |err| {
            self.result = err;
        };
    }

    fn acceptAndServe(self: *Server) !void {
        const stream = try self.listener.accept(self.io);
        defer stream.close(self.io);
        try self.serve(stream);
    }

    fn serve(self: *Server, stream: Io.net.Stream) !void {
        var session = try Session.init(self.allocator, .{ .role = .server });
        defer session.deinit(self.allocator);

        var read_buffer: [16 * 1024]u8 = undefined;
        var write_buffer: [4 * 1024]u8 = undefined;
        var stream_reader = stream.reader(self.io, &.{});
        var stream_writer = stream.writer(self.io, &write_buffer);
        const reader = &stream_reader.interface;
        const writer = &stream_writer.interface;

        var answered = false;
        while (true) {
            while (true) {
                const n = session.send(&read_buffer);
                if (n == 0) break;
                try writer.writeAll(read_buffer[0..n]);
            }
            try writer.flush();

            while (try session.nextEvent(self.allocator)) |event| {
                switch (event) {
                    .request => |request| {
                        const target = request.head.path orelse "";
                        const copy = @min(target.len, self.received.target.len);
                        @memcpy(self.received.target[0..copy], target[0..copy]);
                        self.received.target_len = copy;
                        if (request.end_stream) answered = try self.answer(&session, request.stream_id, answered);
                    },
                    .data => |data| {
                        const room = self.received.body.len - self.received.body_len;
                        const copy = @min(data.bytes.len, room);
                        @memcpy(self.received.body[self.received.body_len..][0..copy], data.bytes[0..copy]);
                        self.received.body_len += copy;
                        try session.release(self.allocator, data.stream_id, data.flow_len);
                        if (data.end_stream) {
                            answered = try self.answer(&session, data.stream_id, answered);
                        }
                    },
                    // The server session reports a peer reset and a GOAWAY; a
                    // clean stream end shows up as END_STREAM on the DATA.
                    .reset, .goaway => return,
                    else => {},
                }
            }

            var iovecs: [1][]u8 = .{&read_buffer};
            const n = reader.readVec(&iovecs) catch |err| switch (err) {
                error.EndOfStream => return,
                else => return err,
            };
            if (n == 0) return;
            var offset: usize = 0;
            while (offset < n) {
                const taken = session.receive(read_buffer[offset..n]);
                if (taken == 0) return error.InputFull;
                offset += taken;
            }
        }
    }

    fn answer(
        self: *Server,
        session: *Session,
        id: u31,
        already: bool,
    ) !bool {
        if (already) return true;
        try session.submitResponse(self.allocator, id, .{
            .status = .ok,
            .content_type = "application/grpc",
        }, false);

        var sent: usize = 0;
        while (sent < self.reply.body.len) {
            const n = try session.submitData(self.allocator, id, self.reply.body[sent..], false);
            if (n == 0) break;
            sent += n;
        }

        const trailers = [_]http.Header{
            .{ .name = "grpc-status", .value = self.reply.code },
            .{ .name = "grpc-message", .value = self.reply.text },
        };
        try session.submitTrailers(self.allocator, id, &trailers);
        return true;
    }
};
