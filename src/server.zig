//! A gRPC server over http's blocking HTTP/2 engine.
//!
//! The engine buffers a whole request body before the handler runs, so this
//! serves unary calls: one message in, one out. A streaming RPC needs an engine
//! that hands over the body as it arrives, which http's does not do yet.
//!
//! The status travels in the trailing HEADERS section, never in `:status`.
//! A call that fails is still HTTP 200 with `grpc-status` set, which is the one
//! thing a gRPC server cannot share with an ordinary request/response handler.

const std = @import("std");
const http = @import("http");
const message = @import("message.zig");
const status = @import("status.zig");

const Io = std.Io;
const builtin = @import("builtin");
const exchange_mod = http.h2.engine;

/// The readiness engine http ships for this OS. epoll and kqueue are better
/// than poll where they exist, and http exposes each only on its own platform.
const Backend = switch (builtin.os.tag) {
    .linux => exchange_mod.epoll,
    .macos => exchange_mod.kqueue,
    else => exchange_mod.poll,
};
const Exchange = exchange_mod.Exchange;

pub const Request = struct {
    /// The `:path` as it arrived: `/helloworld.Greeter/SayHello`.
    path: []const u8,
    /// Every request message, in wire order, with the length prefix removed.
    /// Borrows the request body, so it is valid for the length of the handler
    /// call. A unary RPC sees one; a client stream sees as many as the client
    /// sent before it closed, up to the engine's body limit.
    messages: []const Message,
    /// The regular fields of the request head.
    metadata: http.Headers,
    /// Per-stream storage that lives until the response is complete. Allocate
    /// the response message here: the server reads it after the handler
    /// returns, so a handler's stack buffer would already be gone.
    arena: std.mem.Allocator,
};

/// One message with its framing removed.
pub const Message = struct {
    compressed: bool,
    bytes: []const u8,
};

pub const Response = struct {
    /// Every response message, in wire order. They must outlive the handler
    /// call, so allocate from `Request.arena` or point at static bytes. The
    /// server adds each length prefix and sends them in one body.
    messages: []const []const u8 = &.{},
    status: status.Status = .{ .code = .ok },
    /// Trailing fields to add beside `grpc-status`. Borrowed through the call.
    metadata: []const http.Header = &.{},
};

/// What the server calls for each request.
pub const Handler = struct {
    ptr: *anyopaque,
    call: *const fn (ptr: *anyopaque, request: Request, response: *Response) anyerror!void,

    pub fn init(
        ptr: anytype,
        comptime function: fn (@TypeOf(ptr), Request, *Response) anyerror!void,
    ) Handler {
        const Capture = struct {
            fn invoke(raw: *anyopaque, request: Request, response: *Response) anyerror!void {
                const typed: @TypeOf(ptr) = @ptrCast(@alignCast(raw));
                return function(typed, request, response);
            }
        };
        return .{ .ptr = ptr, .call = Capture.invoke };
    }
};

pub const Options = struct {
    allocator: std.mem.Allocator,
    /// The largest request body the engine buffers. A longer request is refused
    /// by the engine before the handler sees it.
    body_bytes: usize = 64 * 1024,
    max_streams: u16 = 64,
};

/// What a caller that owns its own transport loop hands to http's engines. The
/// TLS readiness engines in `http_tls` take an engine handler, so a TLS gRPC
/// server is this adapter plus their loop; `serve` below is the plaintext one.
///
/// The adapter holds no allocation of its own: keep it in the server's state and
/// call `engine` once, the way http's own examples keep their handler.
pub const Adapter = struct {
    handler: Handler,

    pub fn init(handler_value: Handler) Adapter {
        return .{ .handler = handler_value };
    }

    pub fn engine(self: *Adapter) exchange_mod.Handler {
        return .{
            .ptr = self,
            .vtable = &.{
                .handle = handle,
                .complete = exchange_mod.Handler.noopComplete,
                .report_error = reportError,
                .policy_response = exchange_mod.Handler.defaultPolicyResponse,
            },
        };
    }

    fn reportError(_: *anyopaque, _: anyerror) void {}

    fn handle(ptr: *anyopaque, exchange: *Exchange) anyerror!void {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        return dispatch(self.handler, exchange);
    }
};

/// The fixed answers for a request this server cannot route.
pub const Errors = struct {
    /// The request had no `content-type: application/grpc...`.
    pub const unsupported_media_type: status.Status = .{
        .code = .internal,
        .message = "content-type is not application/grpc",
    };
    /// The body was not exactly one framed message.
    pub const not_unary: status.Status = .{
        .code = .internal,
        .message = "this server answers one message per call",
    };
    pub const malformed: status.Status = .{
        .code = .internal,
        .message = "malformed request message",
    };
};

// ast-grep-ignore: public-struct-field-default -- options bag; defaults are the policy
pub const ReadinessOptions = struct {
    allocator: std.mem.Allocator,
    /// Slots, and the connections they can carry at once.
    max_connections: usize = 128,
    max_streams: u16 = 64,
    /// The largest request body the engine buffers.
    body_bytes: usize = 64 * 1024,
    read_buffer_bytes: usize = 16 * 1024,
    write_buffer_bytes: usize = 16 * 1024,
    /// Zero disables each of these. The engine applies them per connection.
    idle_timeout: Io.Duration = .zero,
    head_timeout: Io.Duration = .zero,
    body_timeout: Io.Duration = .zero,
    total_timeout: Io.Duration = .zero,
};

/// A gRPC server on the readiness engine: many connections on one thread, with
/// the engine's deadlines, accepting on the address given.
///
/// `init` takes a pointer because the engine holds the address of the adapter
/// below it, and a moved struct would break that pointer.
pub const ReadinessServer = struct {
    adapter: Adapter,
    inner: Backend.Server,
    listening: bool = false,

    pub fn init(
        self: *ReadinessServer,
        io: Io,
        bind: Io.net.IpAddress,
        handler: Handler,
        options: ReadinessOptions,
    ) !void {
        self.adapter = .init(handler);
        self.inner = try Backend.Server.init(io, bind, self.adapter.engine(), .{
            .allocator = options.allocator,
            .max_connections = options.max_connections,
            .read_buffer_bytes = options.read_buffer_bytes,
            .write_buffer_bytes = options.write_buffer_bytes,
            .max_streams = options.max_streams,
            .common = .{ .body_bytes = options.body_bytes },
            .idle_timeout = options.idle_timeout,
            .head_timeout = options.head_timeout,
            .body_timeout = options.body_timeout,
            .total_timeout = options.total_timeout,
        });
        self.listening = true;
    }

    pub fn deinit(self: *ReadinessServer) void {
        if (self.listening) {
            self.listening = false;
            self.inner.deinit();
        }
        self.* = undefined;
    }

    /// The address the kernel gave, which is how a caller finds the port when
    /// it asked for zero.
    pub fn address(self: *const ReadinessServer) Io.net.IpAddress {
        return self.inner.address();
    }

    /// Serve until the engine stops or a wait fails.
    pub fn run(self: *ReadinessServer) !void {
        return self.inner.run();
    }

    /// Ask `run` to finish: it stops accepting, drains the open connections,
    /// and returns.
    pub fn requestStop(self: *ReadinessServer) void {
        self.inner.requestStop();
    }
};

/// Accept connections until an unrecoverable accept error, serving each one on
/// the `Io` executor.
///
/// The group is awaited once the loop ends; until then every accepted
/// connection is one concurrent task. An implementation without concurrency
/// falls back to serving the connection inline, which is what the previous
/// version did for every connection.
pub fn serve(
    io: Io,
    listener: *Io.net.Server,
    handler: Handler,
    options: Options,
) !void {
    var group: Io.Group = .init;
    defer group.await(io) catch |err| switch (err) {
        // The caller cancelled the accept loop; the group's tasks already saw it.
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
            handler,
            options,
        }) catch |err| switch (err) {
            // No worker to hand it to: serve it on this thread instead.
            error.ConcurrencyUnavailable => serveConnection(
                io,
                stream,
                handler,
                options,
            ) catch continue,
        };
    }
}

/// A connection as a group task. The task's error set is the cancelation
/// boundary, so a failed connection ends quietly the way the inline loop did.
fn serveConnectionTask(
    io: Io,
    stream: Io.net.Stream,
    handler: Handler,
    options: Options,
) Io.Cancelable!void {
    // A failed connection ends its own task, which is what the inline loop did
    // with `catch continue`. Nothing here can be cancelled, so nothing
    // propagates.
    serveConnection(io, stream, handler, options) catch return;
}

/// Serve one connection until the peer closes it.
pub fn serveConnection(
    io: Io,
    stream: Io.net.Stream,
    handler: Handler,
    options: Options,
) !void {
    defer stream.close(io);

    const read_buffer = try options.allocator.alloc(u8, 16 * 1024);
    defer options.allocator.free(read_buffer);
    const write_buffer = try options.allocator.alloc(u8, 16 * 1024);
    defer options.allocator.free(write_buffer);

    var adapter: Adapter = .init(handler);
    try exchange_mod.blocking.serve(io, stream, adapter.engine(), .{
        .allocator = options.allocator,
        .read_buffer = read_buffer,
        .write_buffer = write_buffer,
        .max_streams = options.max_streams,
        .common = .{ .body_bytes = options.body_bytes },
    });
}

/// The translation to and from a gRPC exchange, shared by the owned adapter and
/// the one `serve` builds.
fn dispatch(handler_value: Handler, exchange: *Exchange) anyerror!void {
    const content_type = exchange.request.head.headers.get("content-type") orelse {
        return answer(exchange, .{}, Errors.unsupported_media_type);
    };
    if (!std.mem.startsWith(u8, content_type, "application/grpc")) {
        return answer(exchange, .{}, Errors.unsupported_media_type);
    }

    // Every message the client sent, in wire order. The engine buffered the
    // whole body, so a client stream arrives complete; a unary handler that
    // wants exactly one checks for it itself.
    var walker = message.Iterator.init(exchange.request.body);
    var request_messages: std.ArrayList(Message) = .empty;
    while (true) {
        const next = walker.next() catch return answer(exchange, .{}, Errors.malformed);
        const found = next orelse break;
        try request_messages.append(exchange.arena, .{
            .compressed = found.compressed,
            .bytes = found.bytes,
        });
    }

    var response: Response = .{};
    handler_value.call(handler_value.ptr, .{
        .path = exchange.request.head.path orelse "",
        .messages = request_messages.items,
        .metadata = exchange.request.head.headers,
        .arena = exchange.arena,
    }, &response) catch |err| {
        return answer(exchange, .{}, .{ .code = .internal, .message = @errorName(err) });
    };
    return answer(exchange, response, response.status);
}

/// Write one answer: HTTP 200, the framed message, and the status in the
/// trailing section. Everything is allocated from the exchange arena, which
/// lives until the engine finishes the stream.
fn answer(exchange: *Exchange, response: Response, call_status: status.Status) !void {
    const arena = exchange.arena;

    var body_len: usize = 0;
    for (response.messages) |one| body_len += message.prefix_len + one.len;
    var body: []const u8 = &.{};
    if (body_len != 0) {
        const framed = try arena.alloc(u8, body_len);
        var at: usize = 0;
        for (response.messages) |one| {
            at += (try message.write(framed[at..], one, false)).len;
        }
        body = framed[0..at];
    }

    const encoded = try arena.alloc(u8, status.encodedLen(call_status.message));
    const call_message = try status.encodeMessage(encoded, call_status.message);
    const code = try arena.print("{d}", .{@backingInt(call_status.code)});

    const trailers = try arena.alloc(http.Header, 2 + response.metadata.len);
    trailers[0] = .{ .name = "grpc-status", .value = code };
    trailers[1] = .{ .name = "grpc-message", .value = call_message };
    @memcpy(trailers[2..], response.metadata);

    try exchange.respond(.{
        .status = .ok,
        .content_type = "application/grpc",
        .trailers = trailers,
    }, body);
}

const blocking = @import("blocking.zig");
const Call = @import("Call.zig");

test "our client calls our server" {
    const testing = std.testing;

    const App = struct {
        const Self = @This();

        path: [64]u8 = undefined,
        path_len: usize = 0,

        fn handle(self: *Self, request: Request, response: *Response) anyerror!void {
            const copy = @min(request.path.len, self.path.len);
            @memcpy(self.path[0..copy], request.path[0..copy]);
            self.path_len = copy;

            if (std.mem.endsWith(u8, request.path, "/Missing")) {
                response.status = .{ .code = .not_found, .message = "no such key: 100% done" };
                return;
            }
            if (request.messages.len != 1) {
                response.status = .{ .code = .internal, .message = "one message, please" };
                return;
            }
            if (std.mem.eql(u8, request.messages[0].bytes, "ping")) {
                response.messages = &.{"pong"};
                return;
            }
            var echo: std.ArrayList([]const u8) = .empty;
            for (request.messages) |one| try echo.append(request.arena, one.bytes);
            response.messages = echo.items;
        }
    };

    const Thread = struct {
        const Self = @This();

        io: Io,
        listener: *Io.net.Server,
        handler: Handler,
        result: ?anyerror = null,

        fn run(self: *Self) void {
            const stream = self.listener.accept(self.io) catch |err| {
                self.result = err;
                return;
            };
            serveConnection(self.io, stream, self.handler, .{
                .allocator = testing.allocator,
            }) catch |err| {
                self.result = err;
            };
        }
    };

    const address = try Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var listener = try address.listen(testing.io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(testing.io);

    var client_address = address;
    client_address.setPort(listener.socket.address.getPort());

    var app: App = .{};
    var thread_state: Thread = .{
        .io = testing.io,
        .listener = &listener,
        .handler = Handler.init(&app, App.handle),
    };
    const thread = try std.Thread.spawn(.{}, Thread.run, .{&thread_state});

    const socket = try client_address.connect(testing.io, .{ .mode = .stream });
    var client: blocking.Client = undefined;
    try client.init(testing.allocator, testing.io, socket);
    var joined = false;
    defer if (!joined) {
        client.deinit();
        thread.join();
    };

    var request_buffer: [64]u8 = undefined;
    const request_body = try message.write(&request_buffer, "ping", false);

    var reply: [256]u8 = undefined;
    const first = try client.unary(.{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .body = request_body,
    }, &reply);
    try testing.expectEqual(status.Code.ok, first.status.code);
    try testing.expectEqualStrings("pong", first.message);

    var missing_reply: [256]u8 = undefined;
    const second = try client.unary(.{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "Missing",
        .body = request_body,
    }, &missing_reply);
    try testing.expectEqual(status.Code.not_found, second.status.code);
    try testing.expectEqualStrings("no such key: 100% done", second.status.message);

    try testing.expectEqualStrings("/helloworld.Greeter/Missing", app.path[0..app.path_len]);

    client.deinit();
    thread.join();
    joined = true;
    try testing.expect(thread_state.result == null);
}

test "the readiness server answers our client" {
    const testing = std.testing;

    const App = struct {
        const Self = @This();

        fn handle(_: *Self, request: Request, response: *Response) anyerror!void {
            if (request.messages.len == 1 and
                std.mem.eql(u8, request.messages[0].bytes, "ping"))
            {
                response.messages = &.{"pong"};
                return;
            }
            response.status = .{ .code = .unimplemented, .message = "only ping" };
        }
    };

    const Runner = struct {
        const Self = @This();

        server: *ReadinessServer,
        result: ?anyerror = null,

        fn run(self: *Self) void {
            self.server.run() catch |err| {
                self.result = err;
            };
        }
    };

    var app: App = .{};
    var server: ReadinessServer = undefined;
    try server.init(
        testing.io,
        try Io.net.IpAddress.parseLiteral("127.0.0.1:0"),
        Handler.init(&app, App.handle),
        .{ .allocator = testing.allocator, .max_connections = 4 },
    );
    defer server.deinit();

    var runner: Runner = .{ .server = &server };
    const thread = try std.Thread.spawn(.{}, Runner.run, .{&runner});
    var joined = false;
    defer if (!joined) {
        server.requestStop();
        thread.join();
    };

    const socket = try server.address().connect(testing.io, .{ .mode = .stream });
    var client: blocking.Client = undefined;
    try client.init(testing.allocator, testing.io, socket);
    var closed = false;
    defer if (!closed) client.deinit();

    var request_buffer: [64]u8 = undefined;
    const body = try message.write(&request_buffer, "ping", false);
    var reply: [64]u8 = undefined;

    const response = try client.unary(.{
        .authority = "127.0.0.1",
        .service = "probe.Probe",
        .method = "Ping",
        .body = body,
    }, &reply);
    try testing.expectEqual(status.Code.ok, response.status.code);
    try testing.expectEqualStrings("pong", response.message);

    client.deinit();
    closed = true;
    server.requestStop();
    thread.join();
    joined = true;
    try testing.expect(runner.result == null);
}

test "the server answers a stream of messages" {
    const testing = std.testing;

    const App = struct {
        const Self = @This();

        fn handle(_: *Self, request: Request, response: *Response) anyerror!void {
            // Echo every message, which is a client stream and a server stream
            // in one call.
            const out = try request.arena.alloc([]const u8, request.messages.len);
            for (request.messages, 0..) |one, index| out[index] = one.bytes;
            response.messages = out;
        }
    };

    const Thread = struct {
        const Self = @This();

        io: Io,
        listener: *Io.net.Server,
        handler: Handler,
        result: ?anyerror = null,

        fn run(self: *Self) void {
            const stream = self.listener.accept(self.io) catch |err| {
                self.result = err;
                return;
            };
            serveConnection(self.io, stream, self.handler, .{
                .allocator = testing.allocator,
            }) catch |err| {
                self.result = err;
            };
        }
    };

    const address = try Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var listener = try address.listen(testing.io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(testing.io);

    var client_address = address;
    client_address.setPort(listener.socket.address.getPort());

    var app: App = .{};
    var thread_state: Thread = .{
        .io = testing.io,
        .listener = &listener,
        .handler = Handler.init(&app, App.handle),
    };
    const thread = try std.Thread.spawn(.{}, Thread.run, .{&thread_state});

    const socket = try client_address.connect(testing.io, .{ .mode = .stream });
    var client: blocking.Client = undefined;
    try client.init(testing.allocator, testing.io, socket);
    var joined = false;
    defer if (!joined) {
        client.deinit();
        thread.join();
    };

    // Three messages up, three back: the shape the old one-message handler
    // could not express at all.
    var calls = [_]Call{try client.open(.{
        .authority = "127.0.0.1",
        .service = "probe.Probe",
        .method = "Echo",
        .streaming = true,
    })};
    defer calls[0].deinit();

    for ([_][]const u8{ "one", "two", "three" }) |text| {
        try calls[0].writeMessage(text);
    }
    calls[0].closeSend();

    while (calls[0].result() == null) {
        if (!try client.step(&calls)) break;
    }
    try testing.expectEqual(status.Code.ok, calls[0].result().?.code);

    // The message bytes borrow the call's buffer, so compare in place rather
    // than holding slices past the next read.
    const expected = [_][]const u8{ "one", "two", "three" };
    var count: usize = 0;
    while (try calls[0].nextMessage()) |found| {
        try testing.expect(count < expected.len);
        try testing.expectEqualStrings(expected[count], found.bytes);
        count += 1;
    }
    try testing.expectEqual(@as(usize, 3), count);

    client.deinit();
    joined = true;
    thread.join();
    try testing.expect(thread_state.result == null);
}
