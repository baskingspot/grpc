//! A two-sided HTTP/2 pair for tests: a client connection and a server
//! session wired to each other through byte buffers and nothing else.
//!
//! There is no socket, no thread, and no handler. `Session` is h2's sans-I/O
//! server machine, so the bytes that cross this pair are the bytes a wire would
//! carry, and a test can put a call through a real exchange without a process.

const std = @import("std");
const http = @import("http");

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
