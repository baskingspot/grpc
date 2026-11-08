//! A TLS-capable blocking gRPC client, in its own module.
//!
//! The core `grpc` module links no crypto: `message`, `status`, and `Call` carry
//! bytes and know nothing about a socket. This module is the opt-in half. It
//! names `http_tls`, which names `tls`, so an h2c-only build fetches neither.
//!
//! It is a thin layer on purpose. `http_tls.client.Connection` owns the record
//! layer, the handshake, and the deadline; this file opens a gRPC connection over
//! it and reports an expired deadline as the status the specification gives it.

const std = @import("std");
const grpc = @import("grpc");
const http_tls = @import("http_tls");

const Io = std.Io;

/// `ca_pem`, `host_name`, `now_sec`, and `alpn`. PEM bytes the caller already
/// holds, and the clock the caller supplies.
pub const Options = http_tls.client.Connection.Options;

pub const Connection = struct {
    io: Io,
    socket: Io.net.Stream,
    tls: http_tls.client.Connection = undefined,
    client: grpc.blocking.Client = undefined,
    open: bool = false,

    /// Handshakes, then presents a gRPC client over the plaintext side. The
    /// connection owns the socket from here: `deinit` closes it.
    ///
    /// `init` takes a pointer because the gRPC client holds the addresses of the
    /// transport's reader and writer.
    pub fn init(
        self: *Connection,
        allocator: std.mem.Allocator,
        io: Io,
        socket: Io.net.Stream,
        options: Options,
    ) !void {
        self.io = io;
        self.socket = socket;
        try self.tls.init(allocator, io, socket, options);
        errdefer self.tls.deinit();
        try self.client.initIo(
            allocator,
            io,
            self.tls.reader(),
            self.tls.writer(),
            .{
                .fd = socket.socket.handle,
                // The record layer is between the socket and the bytes, so the
                // transport owns the deadline. Polling the fd here would read
                // ciphertext.
                .deadline_by = .reader,
            },
        );
        self.open = true;
    }

    pub fn deinit(self: *Connection) void {
        if (self.open) {
            self.open = false;
            self.client.deinit();
            self.tls.deinit();
            self.socket.close(self.io);
        }
        self.* = undefined;
    }

    /// Absolute monotonic nanoseconds, or null.
    pub fn setDeadline(self: *Connection, absolute_ns: ?i96) void {
        self.tls.setDeadline(absolute_ns);
    }

    /// True when the last wait ended because the deadline passed.
    pub fn expired(self: *const Connection) bool {
        return self.tls.expired;
    }

    /// One unary call. A deadline that expires is DEADLINE_EXCEEDED, not the
    /// read failure the transport surfaces, because `Io.Reader.Error` has no
    /// timeout to report.
    pub fn unary(
        self: *Connection,
        request: grpc.blocking.Request,
        reply: []u8,
    ) !grpc.blocking.Response {
        defer self.tls.setDeadline(null);
        if (request.deadline_ns) |relative| {
            const now = Io.Clock.awake.now(self.io).nanoseconds;
            self.tls.setDeadline(now + @as(i96, @intCast(relative)));
        }
        return self.client.unary(request, reply) catch |err| {
            if (self.tls.expired) return .{
                .status = .{ .code = .deadline_exceeded },
                .message = &.{},
            };
            return err;
        };
    }
};

test {
    _ = Connection;
}
