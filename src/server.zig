//! gRPC handlers on HTTP/2 engines.
//!
//! `Stream` reads messages and writes framed bytes as they arrive. `Handler`
//! is the buffered convenience: every request message in, every response out.
//! Both adapters use the same status trailers and support caller-owned TLS.
//!
//! The status travels in the trailing HEADERS section, never in `:status`.
//! A call that fails is still HTTP 200 with `grpc-status` set, which is the one
//! thing a gRPC server cannot share with an ordinary request/response handler.

const std = @import("std");
const Io = std.Io;
const assert = std.debug.assert;
const mem = std.mem;
const testing = std.testing;
const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const builtin = @import("builtin");

const http = @import("http");
const exchange_mod = http.h2.engine;
const Exchange = exchange_mod.Exchange;
const time = @import("stdx").time;

const blocking = @import("blocking.zig");
const Call = @import("Call.zig");
const message = @import("message.zig");
pub const Stream = @import("server/Stream.zig");
const status = @import("status.zig");

/// The readiness engine http ships for this OS. epoll and kqueue are better
/// than poll where they exist, and http exposes each only on its own platform.
const Backend = switch (builtin.os.tag) {
    .linux => exchange_mod.epoll,
    .macos => exchange_mod.kqueue,
    else => exchange_mod.poll,
};
/// A nonblocking callback on an accepted gRPC call. `Stream.context` retains
/// application state between invocations. Pending reads and writes yield to
/// the engine. The adapter owns `Exchange.context`.
pub const StreamingHandler = struct {
    ptr: *anyopaque,
    call: *const fn (ptr: *anyopaque, stream: *Stream) anyerror!void,
    /// Runs once when the call closes, including reset. The call arena remains
    /// readable during this callback. It is freed after the callback returns.
    complete: ?*const fn (ptr: *anyopaque, stream: *Stream) void = null,

    pub fn init(
        ptr: anytype,
        comptime function: fn (@TypeOf(ptr), *Stream) anyerror!void,
    ) StreamingHandler {
        const Capture = struct {
            fn invoke(raw: *anyopaque, stream: *Stream) anyerror!void {
                const typed: @TypeOf(ptr) = @ptrCast(@alignCast(raw));
                return function(typed, stream);
            }
        };
        return .{ .ptr = ptr, .call = Capture.invoke };
    }
};

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
    arena: Allocator,
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
    /// Header fields to send before the body. Borrowed until the response is
    /// complete.
    headers: []const http.Header = &.{},
    /// Trailing fields to add beside `grpc-status`. Borrowed the same way.
    metadata: []const http.Header = &.{},
    /// A serialized `google.rpc.Status` for `grpc-status-details-bin`. The
    /// server base64-encodes it; an empty value sends no such trailer.
    details: []const u8 = &.{},
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
    allocator: Allocator,
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
    const StreamingState = struct {
        stream: Stream,
        failure: ?anyerror = null,
    };

    handler: union(enum) {
        buffered: Handler,
        streaming: struct { io: Io, handler: StreamingHandler },
    },

    pub fn init(handler_value: Handler) Adapter {
        return .{ .handler = .{ .buffered = handler_value } };
    }

    pub fn initStreaming(io: Io, handler_value: StreamingHandler) Adapter {
        return .{ .handler = .{ .streaming = .{ .io = io, .handler = handler_value } } };
    }

    pub fn engine(self: *Adapter) exchange_mod.Handler {
        return .{
            .ptr = self,
            .vtable = &.{
                .handle = handle,
                .complete = complete,
                .report_error = reportError,
                .policy_response = exchange_mod.Handler.defaultPolicyResponse,
            },
        };
    }

    fn reportError(_: *anyopaque, _: anyerror) void {}

    fn handle(ptr: *anyopaque, exchange: *Exchange) anyerror!void {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        switch (self.handler) {
            .buffered => |handler| return dispatch(handler, exchange),
            .streaming => |app| {
                if (!try acceptRequest(exchange)) return;
                const state: *StreamingState = if (exchange.context) |saved|
                    @ptrCast(@alignCast(saved))
                else blk: {
                    const state = try exchange.arena.create(StreamingState);
                    state.* = .{ .stream = try Stream.init(app.io, exchange) };
                    exchange.context = state;
                    break :blk state;
                };
                const stream = &state.stream;
                stream.exchange = exchange;
                if (stream.finished) return;
                // The application gets one turn for an expired RPC deadline.
                // A later blocked head must wait for credit, not the same timer.
                if (try stream.expired()) stream.deadline_notified = true;
                if (state.failure) |err| return failStream(stream, err);
                app.handler.call(app.handler.ptr, stream) catch |err| switch (err) {
                    error.Pending, error.WouldBlock => {
                        const wake = if (stream.exchange.wake == .expired)
                            time.OptionalTimestamp.none
                        else
                            stream.exchange.deadline;
                        try stream.park(wake);
                    },
                    error.Canceled => {},
                    else => {
                        state.failure = err;
                        try failStream(stream, err);
                    },
                };
            },
        }
    }

    fn failStream(stream: *Stream, err: anyerror) !void {
        stream.finish(.{
            .code = if (err == error.TooLarge) .resource_exhausted else .internal,
            .message = @errorName(err),
        }, &.{}, &.{}) catch |write_error| switch (write_error) {
            error.WouldBlock => {
                // The error result is fixed. Wait for output credit, not an
                // expired application timer that would spin on this trailer.
                stream.exchange.setDeadline(.none);
                stream.exchange.waitForProgress();
            },
            else => return write_error,
        };
    }

    fn complete(ptr: *anyopaque, exchange: *Exchange) void {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        switch (self.handler) {
            .buffered => {},
            .streaming => |app| {
                const callback = app.handler.complete orelse return;
                const saved = exchange.context orelse return;
                const state: *StreamingState = @ptrCast(@alignCast(saved));
                state.stream.exchange = exchange;
                callback(app.handler.ptr, &state.stream);
            },
        }
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
    allocator: Allocator,
    /// Slots, and the connections they can carry at once.
    max_connections: usize = 128,
    max_streams: u16 = 64,
    /// Maximum unread request bytes in streaming mode, including prefixes.
    /// Buffered mode applies this limit to the complete request body.
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
        return self.initAdapter(io, bind, .init(handler), options);
    }

    /// Keep `self` at a stable address until `deinit`. Each callback must yield
    /// on Pending or WouldBlock and retain its unsent bytes in per-call state.
    /// `body_bytes` caps unread input. The HTTP engine reuses body buffers.
    /// `Stream.message_limit` bounds assembly. The caller controls application storage.
    pub fn initStreaming(
        self: *ReadinessServer,
        io: Io,
        bind: Io.net.IpAddress,
        handler: StreamingHandler,
        options: ReadinessOptions,
    ) !void {
        return self.initAdapter(io, bind, .initStreaming(io, handler), options);
    }

    fn initAdapter(
        self: *ReadinessServer,
        io: Io,
        bind: Io.net.IpAddress,
        adapter: Adapter,
        options: ReadinessOptions,
    ) !void {
        self.adapter = adapter;
        self.inner = try Backend.Server.init(io, bind, self.adapter.engine(), .{
            .allocator = options.allocator,
            .max_connections = options.max_connections,
            .read_buffer_bytes = options.read_buffer_bytes,
            .write_buffer_bytes = options.write_buffer_bytes,
            .max_streams = options.max_streams,
            .common = .{ .body_bytes = options.body_bytes },
            .body_mode = if (self.adapter.handler == .streaming) .stream else .buffered,
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
fn acceptRequest(exchange: *Exchange) !bool {
    if (exchange.request.head.method != .POST) {
        try exchange.respond(.{
            .status = .method_not_allowed,
            .content_type = "text/plain",
            .headers = &.{.{ .name = "allow", .value = "POST" }},
        }, "gRPC requires POST");
        return false;
    }
    const content_type = exchange.request.head.headers.get("content-type") orelse "";
    const prefix = "application/grpc";
    const supported_media = std.ascii.startsWithIgnoreCase(content_type, prefix) and
        (content_type.len == prefix.len or content_type[prefix.len] == '+' or
            content_type[prefix.len] == ';');
    if (!supported_media) {
        try exchange.respond(.{
            .status = .unsupported_media_type,
            .content_type = "text/plain",
        }, Errors.unsupported_media_type.message);
        return false;
    }
    return true;
}

fn dispatch(handler_value: Handler, exchange: *Exchange) anyerror!void {
    if (!try acceptRequest(exchange)) return;

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

    const trailers = try Stream.trailers(arena, call_status, response.metadata, response.details);

    try exchange.respond(.{
        .status = .ok,
        .content_type = "application/grpc",
        .headers = response.headers,
        .trailers = trailers,
    }, body);
}

test "our client calls our server" {
    const App = struct {
        const Self = @This();

        path: [64]u8 = undefined,
        path_len: usize = 0,

        fn handle(self: *Self, request: Request, response: *Response) anyerror!void {
            const copy = @min(request.path.len, self.path.len);
            @memcpy(self.path[0..copy], request.path[0..copy]);
            self.path_len = copy;

            if (mem.endsWith(u8, request.path, "/Missing")) {
                response.status = .{ .code = .not_found, .message = "no such key: 100% done" };
                return;
            }
            if (request.messages.len != 1) {
                response.status = .{ .code = .internal, .message = "one message, please" };
                return;
            }
            if (mem.eql(u8, request.messages[0].bytes, "ping")) {
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

    const address: Io.net.IpAddress = try .parseLiteral("127.0.0.1:0");
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

    var reply: [256]u8 = undefined;
    const first = try client.unary(.{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .message = "ping",
    }, &reply);
    try testing.expectEqual(status.Code.ok, first.status.code);
    try testing.expectEqualStrings("pong", first.message);

    var missing_reply: [256]u8 = undefined;
    const second = try client.unary(.{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "Missing",
        .message = "ping",
    }, &missing_reply);
    try testing.expectEqual(status.Code.not_found, second.status.code);
    try testing.expectEqualStrings("no such key: 100% done", second.status.message);

    try testing.expectEqualStrings("/helloworld.Greeter/Missing", app.path[0..app.path_len]);

    client.deinit();
    thread.join();
    joined = true;
    try testing.expect(thread_state.result == null);
}

test "captured unary keeps metadata and details past the call" {
    const details = "\x08\x05\x12\x04gone\x00\xff";
    const App = struct {
        const Self = @This();

        fn handle(_: *Self, request: Request, response: *Response) anyerror!void {
            if (mem.endsWith(u8, request.path, "/Normal")) {
                response.messages = &.{"pong"};
                response.headers = &.{.{ .name = "x-head", .value = "h1" }};
                response.metadata = &.{.{ .name = "x-trail", .value = "t1" }};
                response.details = details;
                return;
            }
            response.status = .{ .code = .not_found, .message = "nothing here" };
            response.headers = &.{.{ .name = "x-head", .value = "only" }};
            response.metadata = &.{.{ .name = "x-trail", .value = "only-trail" }};
            response.details = details;
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

    const address: Io.net.IpAddress = try .parseLiteral("127.0.0.1:0");
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

    var normal_reply: [64]u8 = undefined;
    var normal = try client.unaryCaptured(.{
        .authority = "127.0.0.1",
        .service = "probe.Probe",
        .method = "Normal",
        .message = "ping",
    }, &normal_reply);
    defer normal.deinit();

    // The second call frees its own Call and polls the connection again. The
    // first capture must not depend on either.
    var other_reply: [64]u8 = undefined;
    var other = try client.unaryCaptured(.{
        .authority = "127.0.0.1",
        .service = "probe.Probe",
        .method = "Other",
        .message = "ping",
    }, &other_reply);
    defer other.deinit();
    @memset(&other_reply, 0xaa);

    try testing.expectEqual(status.Code.ok, normal.status.code);
    try testing.expectEqualStrings("pong", normal.message);
    try testing.expectEqual(@as(*const u8, &normal_reply[0]), &normal.message[0]);
    try testing.expectEqualStrings("h1", normal.headers.?.get("x-head").?);
    try testing.expect(normal.headers.?.get("x-trail") == null);
    try testing.expectEqualStrings("t1", normal.trailers.?.get("x-trail").?);
    try testing.expectEqualStrings(details, normal.error_details);

    // The server's status-only answer still has a head and trailers.
    try testing.expectEqual(status.Code.not_found, other.status.code);
    try testing.expectEqual(@as(usize, 0), other.message.len);
    try testing.expectEqualStrings("only", other.headers.?.get("x-head").?);
    try testing.expectEqualStrings("only-trail", other.trailers.?.get("x-trail").?);
    try testing.expectEqualStrings(details, other.error_details);

    // A reply buffer too small for the message is an error, not a truncation,
    // and the connection serves the next call.
    var tiny: [3]u8 = undefined;
    try testing.expectError(error.MessageTooLarge, client.unaryCaptured(.{
        .authority = "127.0.0.1",
        .service = "probe.Probe",
        .method = "Normal",
        .message = "ping",
    }, &tiny));
    var exact: [4]u8 = undefined;
    var fits = try client.unaryCaptured(.{
        .authority = "127.0.0.1",
        .service = "probe.Probe",
        .method = "Normal",
        .message = "ping",
    }, &exact);
    defer fits.deinit();
    try testing.expectEqualStrings("pong", fits.message);

    client.deinit();
    thread.join();
    joined = true;
    try testing.expect(thread_state.result == null);
}

test "the readiness server answers our client" {
    const App = struct {
        const Self = @This();

        fn handle(_: *Self, request: Request, response: *Response) anyerror!void {
            if (request.messages.len == 1 and
                mem.eql(u8, request.messages[0].bytes, "ping"))
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

    var reply: [64]u8 = undefined;

    const response = try client.unary(.{
        .authority = "127.0.0.1",
        .service = "probe.Probe",
        .method = "Ping",
        .message = "ping",
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

    const address: Io.net.IpAddress = try .parseLiteral("127.0.0.1:0");
    var listener = try address.listen(testing.io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(testing.io);

    var client_address = address;
    client_address.setPort(listener.socket.address.getPort());

    var app: App = .{};
    var thread_state: Thread = .{
        .io = testing.io,
        .listener = &listener,
        .handler = .init(&app, App.handle),
    };
    const thread: std.Thread = try .spawn(.{}, Thread.run, .{&thread_state});

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

test "non-gRPC requests use HTTP errors without invoking the handler" {
    const App = struct {
        fn handle(called: *bool, _: Request, _: *Response) anyerror!void {
            called.* = true;
        }
    };
    const Case = struct {
        method: http.Method,
        content_type: ?[]const u8,
        expected: http.Status,
    };
    const cases = [_]Case{
        .{ .method = .POST, .content_type = null, .expected = .unsupported_media_type },
        .{ .method = .POST, .content_type = "text/plain", .expected = .unsupported_media_type },
        .{
            .method = .POST,
            .content_type = "application/grpc-web",
            .expected = .unsupported_media_type,
        },
        .{ .method = .DELETE, .content_type = "application/grpc", .expected = .method_not_allowed },
    };
    for (cases) |case| {
        var arena: ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        var fields: [1]http.Header = undefined;
        const headers: []const http.Header = if (case.content_type) |value| blk: {
            fields[0] = .{ .name = "content-type", .value = value };
            break :blk &fields;
        } else &.{};
        var exchange = testExchange(arena.allocator(), case.method, headers);
        var called = false;
        try dispatch(Handler.init(&called, App.handle), &exchange);
        try testing.expect(!called);
        const response = exchange.response.?;
        try testing.expectEqual(case.expected, response.options.status);
        try testing.expectEqual(@as(usize, 0), response.options.trailers.len);
        try testing.expectEqualStrings("text/plain", response.options.content_type.?);
        if (case.expected == .method_not_allowed) {
            const response_headers: http.Headers = .{ .items = response.options.headers };
            try testing.expectEqualStrings("POST", response_headers.get("allow").?);
            try testing.expectEqualStrings("gRPC requires POST", response.body);
        } else {
            try testing.expectEqualStrings(Errors.unsupported_media_type.message, response.body);
        }
    }
}

test "gRPC media types invoke the handler with suffixes parameters and mixed case" {
    const App = struct {
        fn handle(called: *bool, _: Request, _: *Response) anyerror!void {
            called.* = true;
        }
    };
    const types = [_][]const u8{
        "application/grpc",
        "application/grpc+proto",
        "application/grpc+json",
        "application/grpc; charset=utf-8",
        "application/grpc+proto; charset=utf-8",
        "Application/GRPC",
        "APPLICATION/GRPC+PROTO; charset=utf-8",
    };
    for (types) |content_type| {
        var arena: ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        var exchange = testExchange(arena.allocator(), .POST, &.{.{
            .name = "content-type",
            .value = content_type,
        }});
        var called = false;
        try dispatch(Handler.init(&called, App.handle), &exchange);
        try testing.expect(called);
        try testing.expectEqual(http.Status.ok, exchange.response.?.options.status);
    }
}

fn testExchange(
    arena: Allocator,
    method: http.Method,
    headers: []const http.Header,
) Exchange {
    const Sink = @FieldType(Exchange, "sink");
    const Noop = struct {
        var context: u8 = 0;
        fn buffered(
            _: *anyopaque,
            _: exchange_mod.ResponseOptions,
            _: []const u8,
        ) Sink.SinkError!void {}
        fn begin(_: *anyopaque, _: exchange_mod.ResponseOptions) Sink.SinkError!void {}
        fn write(_: *anyopaque, bytes: []const u8) Sink.SinkError!usize {
            return bytes.len;
        }
        fn end(_: *anyopaque, _: []const http.Header) Sink.SinkError!void {}
        fn reset(_: *anyopaque, _: http.h2.frame.ErrorCode) Sink.SinkError!void {}
    };
    return .{
        .id = 0,
        .stream_id = 1,
        .request = .{
            .head = .{
                .method = method,
                .method_token = method.token(),
                .scheme = "http",
                .authority = "localhost",
                .path = "/test.Service/Call",
                .protocol = null,
                .headers = .{ .items = headers },
            },
            .body = &.{},
            .trailers = .{ .items = &.{} },
        },
        .arena = arena,
        .request_body = null,
        .reset_code = null,
        .response_started = false,
        .sink = .{ .ptr = &Noop.context, .vtable = &.{
            .buffered = Noop.buffered,
            .begin = Noop.begin,
            .write = Noop.write,
            .end = Noop.end,
            .reset = Noop.reset,
        } },
        .parked = false,
        .context = null,
        .wake = .progress,
        .deadline = .none,
    };
}

const ByteSource = struct {
    bytes: []const u8,
    offset: u32 = 0,
    pending: bool = true,

    fn source(self: *ByteSource) http.stream.ReadStream {
        return .{ .ptr = self, .vtable = &.{ .next = next, .close = close } };
    }

    fn next(ptr: *anyopaque) http.stream.ReadError!http.stream.ReadResult {
        const self: *ByteSource = @ptrCast(@alignCast(ptr));
        if (self.pending) {
            self.pending = false;
            return error.Pending;
        }
        if (self.offset == self.bytes.len) return error.EndOfStream;
        const byte = self.bytes[self.offset..][0..1];
        self.offset += 1;
        self.pending = true;
        return .{ .data = byte };
    }

    fn close(_: *anyopaque) void {}
};

test "an incremental stream retains every prefix and body byte across Pending" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var buffer: [32]u8 = undefined;
    const first = try message.write(&buffer, "one", false);
    const second = try message.write(buffer[first.len..], "", true);
    var source: ByteSource = .{ .bytes = buffer[0 .. first.len + second.len] };
    var exchange = testExchange(arena.allocator(), .POST, &.{});
    exchange.request_body = source.source();
    var stream = try Stream.init(testing.io, &exchange);
    var count: u32 = 0;
    var pending_count: u32 = 0;
    while (true) {
        const found = stream.next() catch |err| switch (err) {
            error.Pending => {
                pending_count += 1;
                try testing.expect(pending_count <= source.bytes.len + 1);
                continue;
            },
            else => return err,
        };
        const one = found orelse break;
        try testing.expectEqual(count != 0, one.compressed);
        try testing.expectEqualStrings(if (count == 0) "one" else "", one.bytes);
        count += 1;
    }
    try testing.expectEqual(@as(u32, 2), count);
    try testing.expectEqual(source.bytes.len + 1, pending_count);
}

test "an incremental stream rejects truncation invalid flags and oversized headers" {
    const cases = [_]struct {
        bytes: []const u8,
        limit: u32 = message.default_limit,
        expected: anyerror,
    }{
        .{ .bytes = "\x00\x00\x00", .expected = error.Truncated },
        .{ .bytes = "\x00\x00\x00\x00\x02x", .expected = error.Truncated },
        .{ .bytes = "\x02\x00\x00\x00\x00", .expected = error.InvalidFlag },
        .{ .bytes = "\x00\x00\x00\x00\x02xy", .limit = 1, .expected = error.TooLarge },
    };
    for (cases) |case| {
        var arena: ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        var source: ByteSource = .{ .bytes = case.bytes };
        var exchange = testExchange(arena.allocator(), .POST, &.{});
        exchange.request_body = source.source();
        var stream = try Stream.init(testing.io, &exchange);
        stream.message_limit = case.limit;
        while (true) {
            _ = stream.next() catch |err| switch (err) {
                error.Pending => continue,
                else => {
                    try testing.expectEqual(case.expected, err);
                    try testing.expectError(case.expected, stream.next());
                    if (err == error.TooLarge) try testing.expectEqual(@as(u32, 5), source.offset);
                    break;
                },
            };
            return error.TestExpectedError;
        }
    }
}

test "a streaming adapter parks on a blocked head and resumes the same call" {
    const Sink = @FieldType(Exchange, "sink");
    const Fixture = struct {
        const Self = @This();

        calls: u32 = 0,
        heads: u32 = 0,
        ends: u32 = 0,
        completed: u32 = 0,

        fn handle(self: *Self, stream: *Stream) !void {
            self.calls += 1;
            if (stream.context) |context| {
                try testing.expect(context == @as(*anyopaque, self));
            } else stream.context = self;
            try stream.begin(&.{});
            try stream.finish(.{ .code = .ok }, &.{}, &.{});
        }

        fn begin(ptr: *anyopaque, _: exchange_mod.ResponseOptions) Sink.SinkError!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.calls == 1) return error.WouldBlock;
            self.heads += 1;
        }

        fn end(ptr: *anyopaque, trailers: []const http.Header) Sink.SinkError!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const headers: http.Headers = .{ .items = trailers };
            assert(mem.eql(u8, "0", headers.get("grpc-status").?));
            self.ends += 1;
        }

        fn complete(ptr: *anyopaque, stream: *Stream) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            assert(stream.context.? == @as(*anyopaque, self));
            self.completed += 1;
        }
    };
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var fixture: Fixture = .{};
    var handler = StreamingHandler.init(&fixture, Fixture.handle);
    handler.complete = Fixture.complete;
    var adapter = Adapter.initStreaming(testing.io, handler);
    const engine = adapter.engine();
    var exchange = testExchange(arena.allocator(), .POST, &.{.{
        .name = "content-type",
        .value = "application/grpc",
    }});
    var sink_vtable = exchange.sink.vtable.*;
    sink_vtable.begin = Fixture.begin;
    sink_vtable.end = Fixture.end;
    exchange.sink = .{ .ptr = &fixture, .vtable = &sink_vtable };

    try engine.vtable.handle(engine.ptr, &exchange);
    try testing.expect(exchange.parked);
    try testing.expect(!exchange.response_started);
    try testing.expectEqual(@as(u32, 0), fixture.heads);
    try testing.expectEqual(@as(u32, 0), fixture.ends);
    const saved = exchange.context;

    exchange.parked = false;
    try engine.vtable.handle(engine.ptr, &exchange);
    try testing.expect(exchange.context == saved);
    try testing.expectEqual(@as(u32, 1), fixture.heads);
    try testing.expectEqual(@as(u32, 1), fixture.ends);
    try engine.vtable.handle(engine.ptr, &exchange);
    try testing.expectEqual(@as(u32, 2), fixture.calls);
    engine.vtable.complete(engine.ptr, &exchange);
    try testing.expectEqual(@as(u32, 1), fixture.completed);
}

test "Pending preserves the application wake" {
    const App = struct {
        const Self = @This();

        fn wake(self: *Self, stream: *Stream) !void {
            _ = self;
            try stream.park(try .init(try stream.now()));
            return error.Pending;
        }
    };
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var exchange = testExchange(arena.allocator(), .POST, &.{
        .{ .name = "content-type", .value = "application/grpc" },
    });
    var app: App = .{};
    var adapter = Adapter.initStreaming(
        testing.io,
        StreamingHandler.init(&app, App.wake),
    );
    const engine = adapter.engine();
    try engine.vtable.handle(engine.ptr, &exchange);
    try testing.expect(exchange.parked);
    try testing.expect(exchange.deadline.get() != null);
}

test "ServerStream.next yields data before the server can send its trailers" {
    const App = struct {
        const Self = @This();
        released: std.atomic.Value(bool) = .init(false),
        sent_first: bool = false,

        fn handle(self: *Self, stream: *Stream) !void {
            if (!self.sent_first) {
                try stream.begin(&.{});
                var buffer: [16]u8 = undefined;
                const frame = try message.write(&buffer, "one", false);
                if (try stream.write(frame) != frame.len) return error.UnexpectedShortWrite;
                self.sent_first = true;
                return stream.park(try .init(try (try stream.now()).add(
                    try time.Duration.fromMilliseconds(1),
                )));
            }
            if (!self.released.load(.acquire)) {
                return stream.park(try .init(try (try stream.now()).add(
                    try time.Duration.fromMilliseconds(1),
                )));
            }
            var buffer: [16]u8 = undefined;
            const frame = try message.write(&buffer, "two", false);
            if (try stream.write(frame) != frame.len) return error.UnexpectedShortWrite;
            try stream.finish(.{ .code = .ok }, &.{}, &.{});
        }
    };
    const Runner = struct {
        fn run(server: *ReadinessServer) void {
            server.run() catch |err| std.debug.panic("server failed: {s}", .{@errorName(err)});
        }
    };
    var app: App = .{};
    var server: ReadinessServer = undefined;
    try server.initStreaming(
        testing.io,
        try .parseLiteral("127.0.0.1:0"),
        .init(&app, App.handle),
        .{ .allocator = testing.allocator, .max_connections = 1 },
    );
    defer server.deinit();
    const thread = try std.Thread.spawn(.{}, Runner.run, .{&server});
    defer {
        app.released.store(true, .release);
        server.requestStop();
        thread.join();
    }
    const socket = try server.address().connect(testing.io, .{ .mode = .stream });
    var stream: blocking.ServerStream = undefined;
    try stream.init(testing.allocator, testing.io, socket, .{
        .authority = "localhost",
        .service = "probe.Probe",
        .method = "Read",
        .timeout = try .init(.fromSecondsComptime(2)),
    });
    defer stream.deinit();
    const first = (try stream.next()) orelse return error.ExpectedFirstMessage;
    try testing.expectEqualStrings("one", first.bytes);
    try testing.expect(stream.status() == null);
    app.released.store(true, .release);
    const second = (try stream.next()) orelse return error.ExpectedSecondMessage;
    try testing.expectEqualStrings("two", second.bytes);
    try testing.expect(try stream.next() == null);
    try testing.expectEqual(status.Code.ok, stream.status().?.code);
}

test "a blocked error head or trailer retries without calling the application again" {
    const Sink = @FieldType(Exchange, "sink");
    const Fixture = struct {
        const Self = @This();
        calls: u32 = 0,
        attempts: u32 = 0,
        block_begin: bool,
        code: []const u8 = "",

        fn handle(self: *Self, _: *Stream) !void {
            self.calls += 1;
            return error.MalformedMessage;
        }

        fn block(self: *Self) Sink.SinkError!void {
            self.attempts += 1;
            if (self.attempts == 1) return error.WouldBlock;
        }

        fn begin(ptr: *anyopaque, _: exchange_mod.ResponseOptions) Sink.SinkError!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.block_begin) try self.block();
        }

        fn end(ptr: *anyopaque, trailers: []const http.Header) Sink.SinkError!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (!self.block_begin) try self.block();
            const headers: http.Headers = .{ .items = trailers };
            self.code = headers.get("grpc-status").?;
        }
    };
    for ([_]bool{ false, true }) |block_begin| {
        var arena: ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        var fixture: Fixture = .{ .block_begin = block_begin };
        var adapter = Adapter.initStreaming(
            testing.io,
            StreamingHandler.init(&fixture, Fixture.handle),
        );
        const engine = adapter.engine();
        var exchange = testExchange(arena.allocator(), .POST, &.{
            .{ .name = "content-type", .value = "application/grpc" },
            .{ .name = "grpc-timeout", .value = "0n" },
        });
        var sink_vtable = exchange.sink.vtable.*;
        sink_vtable.begin = Fixture.begin;
        sink_vtable.end = Fixture.end;
        exchange.sink = .{ .ptr = &fixture, .vtable = &sink_vtable };
        try engine.vtable.handle(engine.ptr, &exchange);
        try testing.expect(exchange.parked);
        exchange.parked = false;
        try engine.vtable.handle(engine.ptr, &exchange);
        try testing.expectEqual(@as(u32, 1), fixture.calls);
        try testing.expectEqual(@as(u32, 2), fixture.attempts);
        try testing.expectEqualStrings("13", fixture.code);
        try testing.expect(exchange.deadline.get() == null);
    }
}

test "a deadline crossed during a callback still wakes the application once" {
    const Sink = @FieldType(Exchange, "sink");
    const Fixture = struct {
        const Self = @This();
        calls: u32 = 0,

        fn handle(self: *Self, stream: *Stream) !void {
            self.calls += 1;
            if (self.calls == 1) {
                // Simulate clock expiry between callback entry and its yield.
                stream.deadline = try .init(try .fromStd(
                    (try stream.now()).toStd().subDuration(.fromNanoseconds(1)),
                ));
                return error.Pending;
            }
            try testing.expect(try stream.expired());
            try stream.begin(&.{});
        }

        fn begin(_: *anyopaque, _: exchange_mod.ResponseOptions) Sink.SinkError!void {
            return error.WouldBlock;
        }
    };
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var fixture: Fixture = .{};
    var adapter = Adapter.initStreaming(
        testing.io,
        StreamingHandler.init(&fixture, Fixture.handle),
    );
    const engine = adapter.engine();
    var exchange = testExchange(arena.allocator(), .POST, &.{
        .{ .name = "content-type", .value = "application/grpc" },
        .{ .name = "grpc-timeout", .value = "60000m" },
    });
    var sink_vtable = exchange.sink.vtable.*;
    sink_vtable.begin = Fixture.begin;
    exchange.sink = .{ .ptr = &fixture, .vtable = &sink_vtable };
    try engine.vtable.handle(engine.ptr, &exchange);
    try testing.expect(exchange.parked);
    try testing.expect(exchange.deadline.get() != null);
    exchange.wake = .expired;
    exchange.parked = false;
    try engine.vtable.handle(engine.ptr, &exchange);
    try testing.expect(exchange.parked);
    try testing.expectEqual(@as(u32, 2), fixture.calls);
    try testing.expect(exchange.deadline.get() == null);
}

/// A sink that accepts a settable number of body bytes and records the rest of
/// the exchange, so tests can stall a write at any byte.
const SlowSink = struct {
    const Sink = @FieldType(Exchange, "sink");

    allow: usize = 0,
    written: [128]u8 = undefined,
    written_len: usize = 0,
    ends: u32 = 0,
    code: []const u8 = "",
    reset_code: ?http.h2.frame.ErrorCode = null,

    fn install(self: *SlowSink, exchange: *Exchange) void {
        const vtable = exchange.sink.vtable.*;
        const Wrapped = struct {
            var table: @TypeOf(vtable) = undefined;
        };
        Wrapped.table = vtable;
        Wrapped.table.write = write;
        Wrapped.table.end = end;
        Wrapped.table.reset = reset;
        exchange.sink = .{ .ptr = self, .vtable = &Wrapped.table };
    }

    fn write(ptr: *anyopaque, bytes: []const u8) Sink.SinkError!usize {
        const self: *SlowSink = @ptrCast(@alignCast(ptr));
        const count = @min(self.allow, bytes.len);
        @memcpy(self.written[self.written_len..][0..count], bytes[0..count]);
        self.written_len += count;
        self.allow -= count;
        return count;
    }

    fn end(ptr: *anyopaque, trailers: []const http.Header) Sink.SinkError!void {
        const self: *SlowSink = @ptrCast(@alignCast(ptr));
        const headers: http.Headers = .{ .items = trailers };
        self.code = headers.get("grpc-status").?;
        self.ends += 1;
    }

    fn reset(ptr: *anyopaque, code: http.h2.frame.ErrorCode) Sink.SinkError!void {
        const self: *SlowSink = @ptrCast(@alignCast(ptr));
        self.reset_code = code;
    }

    fn output(self: *const SlowSink) []const u8 {
        return self.written[0..self.written_len];
    }
};

test "send keeps an exact cursor across partial prefixes and stalls" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var sink: SlowSink = .{};
    var exchange = testExchange(arena.allocator(), .POST, &.{});
    sink.install(&exchange);
    var stream = try Stream.init(testing.io, &exchange);

    var caller = "hello".*;
    sink.allow = 3;
    try stream.send(&caller, false);
    try testing.expectEqual(@as(usize, 7), stream.unsent());
    // The caller may reuse its bytes at once; the stream owns the frame.
    @memset(&caller, 'X');

    var second = "x".*;
    sink.allow = 0;
    try testing.expectError(error.Pending, stream.send(&second, true));
    try testing.expectEqual(@as(usize, 7), stream.unsent());
    sink.allow = 4;
    try testing.expectError(error.Pending, stream.send(&second, true));
    try testing.expectEqual(@as(usize, 3), stream.unsent());
    try testing.expectEqual(@as(usize, 7), sink.written_len);

    // Flushing the rest of the first message accepts the second and sends it whole.
    sink.allow = 100;
    try stream.send(&second, true);
    try testing.expectEqual(@as(usize, 0), stream.unsent());
    const expected = "\x00\x00\x00\x00\x05hello\x01\x00\x00\x00\x01x";
    try testing.expectEqualSlices(u8, expected, sink.output());

    // The storage is reused: a message no larger than the first allocates nothing.
    const storage = stream.queued.ptr;
    try stream.send("abc", false);
    try testing.expect(stream.queued.ptr == storage);
    try stream.flush();
    try stream.flush();
}

test "flush reports zero accepted bytes as Pending and resumes" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var sink: SlowSink = .{};
    var exchange = testExchange(arena.allocator(), .POST, &.{});
    sink.install(&exchange);
    var stream = try Stream.init(testing.io, &exchange);
    try stream.send("ab", false);
    try testing.expectEqual(@as(usize, 7), stream.unsent());
    try testing.expectError(error.Pending, stream.flush());
    sink.allow = 1;
    try testing.expectError(error.Pending, stream.flush());
    try testing.expectEqual(@as(usize, 6), stream.unsent());
    sink.allow = 6;
    try stream.flush();
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x00\x02ab", sink.output());
}

test "send rejects a body above send_limit and keeps the queued message" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var sink: SlowSink = .{};
    var exchange = testExchange(arena.allocator(), .POST, &.{});
    sink.install(&exchange);
    var stream = try Stream.init(testing.io, &exchange);
    try testing.expectEqual(message.default_limit, stream.send_limit);
    stream.send_limit = 3;
    try stream.send("abc", false);
    try testing.expectError(error.TooLarge, stream.send("abcd", false));
    try testing.expectEqual(@as(usize, 8), stream.unsent());
    sink.allow = 8;
    try stream.flush();
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x00\x03abc", sink.output());
}

test "raw write stays a plain byte sink beside send" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var sink: SlowSink = .{ .allow = 2 };
    var exchange = testExchange(arena.allocator(), .POST, &.{});
    sink.install(&exchange);
    var stream = try Stream.init(testing.io, &exchange);
    try testing.expectEqual(@as(usize, 2), try stream.write("abcd"));
    try testing.expectEqual(@as(usize, 0), try stream.write("cd"));
    try testing.expectEqual(@as(usize, 0), stream.unsent());
    try testing.expectEqual(@as(usize, 0), stream.queued.len);
}

test "finish waits for queued output, then sends trailers once" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var sink: SlowSink = .{};
    var exchange = testExchange(arena.allocator(), .POST, &.{});
    sink.install(&exchange);
    var stream = try Stream.init(testing.io, &exchange);
    try stream.send("ab", false);
    try testing.expectError(error.WouldBlock, stream.finish(.{ .code = .ok }, &.{}, &.{}));
    sink.allow = 3;
    try testing.expectError(error.WouldBlock, stream.finish(.{ .code = .ok }, &.{}, &.{}));
    try testing.expectEqual(@as(u32, 0), sink.ends);
    sink.allow = 4;
    // The later arguments are ignored once the message is out and trailers are fixed.
    try stream.finish(.{ .code = .ok }, &.{}, &.{});
    try testing.expectEqual(@as(u32, 1), sink.ends);
    try testing.expectEqualStrings("0", sink.code);
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x00\x02ab", sink.output());
    try stream.finish(.{ .code = .internal }, &.{}, &.{});
    try testing.expectEqual(@as(u32, 1), sink.ends);
    try testing.expectError(error.Finished, stream.send("x", false));
}

test "an expired finish drops an unstarted message as DEADLINE_EXCEEDED" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var sink: SlowSink = .{};
    var exchange = testExchange(arena.allocator(), .POST, &.{
        .{ .name = "grpc-timeout", .value = "0n" },
    });
    sink.install(&exchange);
    var stream = try Stream.init(testing.io, &exchange);
    try stream.send("ab", false);
    try stream.finish(.{ .code = .ok }, &.{}, &.{});
    try testing.expectEqualStrings("4", sink.code);
    try testing.expectEqual(@as(usize, 0), sink.written_len);
    try testing.expectEqual(@as(?http.h2.frame.ErrorCode, null), sink.reset_code);
}

test "an expired finish keeps an explicit error status" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var sink: SlowSink = .{};
    var exchange = testExchange(arena.allocator(), .POST, &.{
        .{ .name = "grpc-timeout", .value = "0n" },
    });
    sink.install(&exchange);
    var stream = try Stream.init(testing.io, &exchange);
    try stream.send("ab", false);
    try stream.finish(.{ .code = .not_found }, &.{}, &.{});
    try testing.expectEqualStrings("5", sink.code);
    try testing.expectEqual(@as(usize, 0), sink.written_len);
}

test "an expired finish resets after part of a message was sent" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var sink: SlowSink = .{ .allow = 2 };
    var exchange = testExchange(arena.allocator(), .POST, &.{
        .{ .name = "grpc-timeout", .value = "0n" },
    });
    sink.install(&exchange);
    var stream = try Stream.init(testing.io, &exchange);
    try stream.send("ab", false);
    try testing.expectEqual(@as(usize, 5), stream.unsent());
    try stream.finish(.{ .code = .ok }, &.{}, &.{});
    try testing.expectEqual(@as(?http.h2.frame.ErrorCode, .cancel), sink.reset_code);
    try testing.expectEqual(@as(u32, 0), sink.ends);
    try testing.expectEqual(@as(usize, 2), sink.written_len);
    try testing.expect(stream.finished);
    try stream.finish(.{ .code = .ok }, &.{}, &.{});
    try testing.expectEqual(@as(u32, 0), sink.ends);
}

test "a streaming adapter parks a stalled final message with the RPC deadline" {
    const Fixture = struct {
        const Self = @This();
        calls: u32 = 0,
        began: bool = false,
        sent: bool = false,

        fn handle(self: *Self, stream: *Stream) !void {
            self.calls += 1;
            if (!self.began) {
                try stream.begin(&.{});
                self.began = true;
            }
            if (!self.sent) {
                try stream.send("ab", false);
                self.sent = true;
            }
            try stream.finish(.{ .code = .ok }, &.{}, &.{});
        }
    };
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var fixture: Fixture = .{};
    var sink: SlowSink = .{};
    var adapter = Adapter.initStreaming(
        testing.io,
        StreamingHandler.init(&fixture, Fixture.handle),
    );
    const engine = adapter.engine();
    var exchange = testExchange(arena.allocator(), .POST, &.{
        .{ .name = "content-type", .value = "application/grpc" },
        .{ .name = "grpc-timeout", .value = "60000m" },
    });
    sink.install(&exchange);
    try engine.vtable.handle(engine.ptr, &exchange);
    try testing.expect(exchange.parked);
    try testing.expect(exchange.deadline.get() != null);
    try testing.expectEqual(@as(u32, 0), sink.ends);
    sink.allow = 100;
    exchange.parked = false;
    try engine.vtable.handle(engine.ptr, &exchange);
    try testing.expect(!exchange.parked);
    try testing.expectEqual(@as(u32, 2), fixture.calls);
    try testing.expectEqual(@as(u32, 1), sink.ends);
    try testing.expectEqualStrings("0", sink.code);
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x00\x02ab", sink.output());
    try testing.expect(exchange.deadline.get() == null);
}

test "a streaming adapter resets an expired partial message once" {
    const Fixture = struct {
        const Self = @This();
        calls: u32 = 0,

        fn handle(self: *Self, stream: *Stream) !void {
            self.calls += 1;
            if (self.calls == 1) {
                try stream.begin(&.{});
                try stream.send("ab", false);
                try stream.finish(.{ .code = .ok }, &.{}, &.{});
            }
        }
    };
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var fixture: Fixture = .{};
    var sink: SlowSink = .{ .allow = 2 };
    var adapter = Adapter.initStreaming(
        testing.io,
        StreamingHandler.init(&fixture, Fixture.handle),
    );
    const engine = adapter.engine();
    var exchange = testExchange(arena.allocator(), .POST, &.{
        .{ .name = "content-type", .value = "application/grpc" },
        .{ .name = "grpc-timeout", .value = "0n" },
    });
    sink.install(&exchange);
    try engine.vtable.handle(engine.ptr, &exchange);
    // Part of the message went out, so the expired call resets instead of parking.
    try testing.expect(!exchange.parked);
    try testing.expectEqual(@as(?http.h2.frame.ErrorCode, .cancel), sink.reset_code);
    try testing.expectEqual(@as(u32, 0), sink.ends);
    try engine.vtable.handle(engine.ptr, &exchange);
    try testing.expectEqual(@as(u32, 1), fixture.calls);
}
