//! TLS client and server adapters for gRPC, in an opt-in module.
//!
//! The core `grpc` module links no crypto: `message`, `status`, and `Call` carry
//! bytes and know nothing about a socket. This module is the opt-in half. It
//! names `http_tls`, which names `tls`.
//!
//! The TLS integration owns records, the handshake, and read deadlines.
//! HTTP enforces ALPN. gRPC reports call expiry as DEADLINE_EXCEEDED.
//! `serve` and `serveStreaming` accept on the caller's `Io`, handshakes each
//! connection through the blocking `tls_std` server, and run the same handlers
//! as `grpc.server.serve` over the plaintext side. The `ReadinessServer` below
//! is the experimental readiness engine path.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const mem = std.mem;
const builtin = @import("builtin");

const grpc = @import("grpc");
const http = @import("http");
const h2_engine = http.h2.engine;
const http_tls = @import("http_tls");
/// `ca_pem`, `host_name`, `now_sec`, and `alpn`. PEM bytes the caller already
/// holds, and the clock the caller supplies.
pub const Options = http_tls.client.Connection.Options;
const time = @import("stdx").time;
const tls = @import("tls");
const tls_std = @import("tls_std");

pub const Connection = struct {
    io: Io,
    socket: Io.net.Stream,
    tls: http_tls.client.Connection = undefined,
    client: grpc.Client = undefined,
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
                .fd = .init(socket.socket.handle),
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
        request: grpc.UnaryRequest,
        reply: []u8,
    ) !grpc.Response {
        defer self.tls.setDeadline(.none);
        return self.client.unary(request, reply);
    }

    /// `unary`, keeping headers, trailers, and error details. The caller
    /// must `deinit` the result.
    pub fn unaryCaptured(
        self: *Connection,
        request: grpc.UnaryRequest,
        reply: []u8,
    ) !grpc.Captured {
        defer self.tls.setDeadline(.none);
        return self.client.unaryCaptured(request, reply);
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
///
/// Experimental: this surface will be removed once the `Io`-based `serve`
/// path carries TLS. The readiness engines stay in `baskingspot/http`.
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
        return self.initAdapter(io, bind_address, .init(io, handler), buffered);
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

/// Options for `serve` and `serveStreaming`.
pub const ServeOptions = struct {
    allocator: Allocator,
    /// Certificate chain, leaf first, DER. Borrowed until `serve` returns.
    /// `tls.CertificateChain.fromPem` builds one from PEM.
    cert_chain: []const []const u8,
    /// Signs CertificateVerify. The private key that produced it must outlive
    /// `serve`. Borrowed.
    signer: tls.signature.Signer,
    /// The largest request body the engine buffers before the handler reads
    /// it. The TLS transport decrypts on demand, so this bounds plaintext
    /// assembly, not wire bytes.
    body_bytes: usize = 64 * 1024,
    max_streams: u16 = 64,
};

/// Accept connections until an unrecoverable accept error. Each connection
/// handshakes on its own task, then runs buffered handlers over the plaintext
/// side, so a handler may block, take locks, and call back into a client.
///
/// The handshake offers and requires ALPN `h2`; a peer that picks anything
/// else, or nothing, ends its connection quietly.
pub fn serve(
    io: Io,
    listener: *Io.net.Server,
    handler: grpc.server.Handler,
    options: ServeOptions,
) Io.net.Server.AcceptError!void {
    return serveLoop(io, listener, grpc.server.Adapter.init(io, handler), options);
}

/// The same loop with streaming handlers.
pub fn serveStreaming(
    io: Io,
    listener: *Io.net.Server,
    handler: grpc.server.StreamingHandler,
    options: ServeOptions,
) Io.net.Server.AcceptError!void {
    return serveLoop(io, listener, grpc.server.Adapter.initStreaming(io, handler), options);
}

/// The accept loop behind `serve` and `serveStreaming`. A connection's own
/// failure, including a refused handshake, ends its task quietly; the error
/// set is the listener's accept errors, the same contract `grpc.server.serve`
/// gives.
fn serveLoop(
    io: Io,
    listener: *Io.net.Server,
    adapter: grpc.server.Adapter,
    options: ServeOptions,
) Io.net.Server.AcceptError!void {
    var group: Io.Group = .init;
    defer group.await(io) catch |err| switch (err) {
        error.Canceled => {},
    };

    while (true) {
        const stream = listener.accept(io) catch |err| switch (err) {
            error.ConnectionAborted, error.WouldBlock => continue,
            error.Canceled => return,
            else => return err,
        };
        group.concurrent(io, serveConnectionTask, .{
            io,
            stream,
            adapter,
            options,
        }) catch |err| switch (err) {
            error.ConcurrencyUnavailable => serveConnection(
                io,
                stream,
                adapter,
                options,
            ) catch continue,
        };
    }
}

fn serveConnectionTask(
    io: Io,
    stream: Io.net.Stream,
    adapter: grpc.server.Adapter,
    options: ServeOptions,
) Io.Cancelable!void {
    serveConnection(io, stream, adapter, options) catch return;
}

/// One TLS connection: handshake, then the h2 engine over the plaintext pair.
fn serveConnection(
    io: Io,
    stream: Io.net.Stream,
    adapter: grpc.server.Adapter,
    options: ServeOptions,
) !void {
    var conn: tls_std.Server = undefined;
    try conn.accept(io, stream, .{
        .cert_chain = options.cert_chain,
        .signer = options.signer,
        .alpn = &.{"h2"},
    });
    defer conn.deinit();
    defer conn.close();

    const alpn = conn.selectedAlpn() orelse return error.NoAlpn;
    if (!mem.eql(u8, alpn, "h2")) return error.WrongAlpn;

    const read_buffer = try options.allocator.alloc(u8, 16 * 1024);
    defer options.allocator.free(read_buffer);
    // The TLS writer stages its own records; the engine needs one byte so the
    // config is not empty.
    var write_buffer: [1]u8 = undefined;

    var conn_adapter = adapter;
    try h2_engine.blocking.serveIo(
        io,
        conn.reader(),
        conn.writer(),
        conn_adapter.engine(),
        .{
            .allocator = options.allocator,
            .read_buffer = read_buffer,
            .write_buffer = &write_buffer,
            .wait_fd = .init(conn.socketHandle()),
            .max_streams = options.max_streams,
            .common = .{ .body_bytes = options.body_bytes },
            .body_mode = switch (adapter.handler) {
                .buffered => .buffered,
                .streaming => .stream,
            },
        },
    );
}

test {
    _ = Connection;
    _ = ReadinessServer;
}
