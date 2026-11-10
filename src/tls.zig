//! TLS client and server adapters for gRPC, in an opt-in module.
//!
//! The core `grpc` module links no crypto: `message`, `status`, and `Call` carry
//! bytes and know nothing about a socket. This module is the opt-in half. It
//! names `http_tls`, which names `tls`.
//!
//! The TLS integration owns records, the handshake, and read deadlines.
//! HTTP enforces ALPN. gRPC reports call expiry as DEADLINE_EXCEEDED.
//! The server uses `grpc.server.Adapter` on the HTTP/2 TLS readiness engine.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

const grpc = @import("grpc");
const time = grpc.time;
const http_tls = @import("http_tls");
/// `ca_pem`, `host_name`, `now_sec`, and `alpn`. PEM bytes the caller already
/// holds, and the clock the caller supplies.
pub const Options = http_tls.client.Connection.Options;

pub const Connection = struct {
    io: Io,
    socket: Io.net.Stream,
    tls: http_tls.client.Connection = undefined,
    client: grpc.blocking.Client = undefined,
    open: bool = false,

    /// Handshakes, then presents a gRPC client over the plaintext side.
    /// On success, this connection owns the socket and `deinit` closes it.
    /// On failure, the caller retains the socket.
    ///
    /// `init` takes a pointer because the gRPC client holds the addresses of the
    /// transport's reader and writer.
    pub fn init(
        self: *Connection,
        allocator: Allocator,
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
                .read_deadline = self.tls.readDeadline(),
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

    /// Set an absolute awake-clock deadline, or clear it with `.none`.
    /// A gRPC call replaces this deadline with its own before each read wait.
    pub fn setDeadline(self: *Connection, deadline: time.OptionalTimestamp) void {
        self.tls.setDeadline(deadline);
    }

    /// True when the most recent TLS read failed because its deadline passed.
    /// This cause survives the deadline cleanup at the end of `unary`.
    pub fn expired(self: *const Connection) bool {
        const cause = self.tls.readError() orelse return false;
        return cause == error.TimedOut;
    }

    /// One unary call. A deadline that expires is DEADLINE_EXCEEDED, not the
    /// read failure the transport surfaces, because `Io.Reader.Error` has no
    /// timeout to report.
    pub fn unary(
        self: *Connection,
        request: grpc.blocking.UnaryRequest,
        reply: []u8,
    ) !grpc.blocking.Response {
        defer self.tls.setDeadline(.none);
        return self.client.unary(request, reply);
    }
};

const ServerEngine = switch (builtin.os.tag) {
    .linux => http_tls.h2.engine.tls.epoll,
    .macos => http_tls.h2.engine.tls.kqueue,
    else => @compileError("The TLS readiness server requires Linux or macOS"),
};

/// The HTTP/2 engine configuration, including the TLS transport policy.
/// The caller supplies a certificate chain, a signer, and ALPN `h2`.
/// `common.body_bytes` defaults to 64 KiB of unread streaming input.
/// Buffered mode applies this limit to the complete request body.
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
    /// caps unread input. `Stream.message_limit` bounds message assembly.
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
