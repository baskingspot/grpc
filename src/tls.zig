//! TLS client and server adapters for gRPC, in an opt-in module.
//!
//! The core `grpc` module links no crypto: `message`, `status`, and `Call` carry
//! bytes and know nothing about a socket. This module is the opt-in half. It
//! names `http_tls`, which names `tls`.
//!
//! `http_tls.client.Connection` owns the record
//! layer, the handshake, and the deadline. The client reports an expired
//! deadline as DEADLINE_EXCEEDED. The server uses `grpc.server.Adapter` on the
//! HTTP/2 TLS readiness engine, with no separate protocol implementation.

const std = @import("std");
const builtin = @import("builtin");
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

const ServerEngine = switch (builtin.os.tag) {
    .linux => http_tls.h2.engine.tls.epoll,
    .macos => http_tls.h2.engine.tls.kqueue,
    else => @compileError("The TLS readiness server requires Linux or macOS"),
};

/// The HTTP/2 engine configuration, including the TLS transport policy.
/// The caller supplies a certificate chain, a signer, and ALPN `h2`.
/// `common.body_bytes` defaults to 64 KiB and caps the total upload per call.
/// Streaming input remains in the call arena until completion.
pub const ServerOptions = ServerEngine.Config;

/// Buffered or streaming gRPC handlers on the HTTP/2 TLS readiness engine.
/// The server borrows the certificate chain and signer until `deinit`.
pub const ReadinessServer = struct {
    adapter: grpc.server.Adapter,
    engine: ServerEngine.Server,

    /// Keep `self` at one address after initialization. The HTTP handler
    /// holds a pointer to its adapter. This initializer forces buffered body mode.
    pub fn init(
        self: *ReadinessServer,
        io: Io,
        bind_address: Io.net.IpAddress,
        handler: grpc.server.Handler,
        options: ServerOptions,
    ) !void {
        var buffered = options;
        buffered.body_mode = .buffered;
        return self.initAdapter(io, bind_address, .init(handler), buffered);
    }

    /// Keep `self` at a stable address until `deinit`. `common.body_bytes`
    /// caps the total upload and retained input, not each message.
    pub fn initStreaming(
        self: *ReadinessServer,
        io: Io,
        bind_address: Io.net.IpAddress,
        handler: grpc.server.StreamingHandler,
        options: ServerOptions,
    ) !void {
        var streaming = options;
        streaming.body_mode = .stream;
        return self.initAdapter(io, bind_address, .initStreaming(io, handler), streaming);
    }

    fn initAdapter(
        self: *ReadinessServer,
        io: Io,
        bind_address: Io.net.IpAddress,
        adapter: grpc.server.Adapter,
        options: ServerOptions,
    ) !void {
        self.adapter = adapter;
        self.engine = try ServerEngine.Server.init(
            io,
            bind_address,
            self.adapter.engine(),
            options,
        );
    }

    pub fn deinit(self: *ReadinessServer) void {
        self.engine.deinit();
        self.* = undefined;
    }

    pub fn address(self: *const ReadinessServer) Io.net.IpAddress {
        return self.engine.address();
    }

    pub fn requestStop(self: *ReadinessServer) void {
        self.engine.requestStop();
    }

    pub fn run(self: *ReadinessServer) !void {
        try self.engine.run();
    }
};

test {
    _ = Connection;
    _ = ReadinessServer;
}
