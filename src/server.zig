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
const exchange_mod = http.h2.engine;
const Exchange = exchange_mod.Exchange;

pub const Request = struct {
    /// The `:path` as it arrived: `/helloworld.Greeter/SayHello`.
    path: []const u8,
    /// The request message with the length prefix removed. Borrows the request
    /// body, so it is valid for the length of the handler call.
    msg: []const u8,
    /// The regular fields of the request head.
    metadata: http.Headers,
    /// Per-stream storage that lives until the response is complete. Allocate
    /// the response message here: the server reads it after the handler
    /// returns, so a handler's stack buffer would already be gone.
    arena: std.mem.Allocator,
};

pub const Response = struct {
    /// The response message. It must outlive the handler call, so allocate it
    /// from `Request.arena` or point it at static bytes. The server adds the
    /// length prefix and frames it.
    msg: []const u8 = &.{},
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

/// Accept connections until an unrecoverable accept error.
pub fn serve(
    io: Io,
    listener: *Io.net.Server,
    handler: Handler,
    options: Options,
) !void {
    while (true) {
        const stream = listener.accept(io) catch |err| switch (err) {
            error.ConnectionAborted, error.WouldBlock => continue,
            error.Canceled => return,
            else => return err,
        };
        serveConnection(io, stream, handler, options) catch continue;
    }
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

    var adapter: Adapter = .{ .handler = handler, .allocator = options.allocator };
    try exchange_mod.blocking.serve(io, stream, adapter.vtable(), .{
        .allocator = options.allocator,
        .read_buffer = read_buffer,
        .write_buffer = write_buffer,
        .max_streams = options.max_streams,
        .common = .{ .body_bytes = options.body_bytes },
    });
}

/// The handler http's engine calls, and the translation to and from a gRPC
/// exchange.
const Adapter = struct {
    handler: Handler,
    allocator: std.mem.Allocator,

    fn vtable(self: *Adapter) exchange_mod.Handler {
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

        const content_type = exchange.request.head.headers.get("content-type") orelse {
            return answer(exchange, .{}, Errors.unsupported_media_type);
        };
        if (!std.mem.startsWith(u8, content_type, "application/grpc")) {
            return answer(exchange, .{}, Errors.unsupported_media_type);
        }

        // A unary call is exactly one message. Zero or two is the caller's
        // mistake, and the status says which.
        var walker = message.Iterator.init(exchange.request.body);
        const first = walker.next() catch return answer(exchange, .{}, Errors.malformed);
        const body = first orelse return answer(exchange, .{}, Errors.not_unary);
        const extra = walker.next() catch return answer(exchange, .{}, Errors.malformed);
        if (extra != null) return answer(exchange, .{}, Errors.not_unary);

        var response: Response = .{};
        self.handler.call(self.handler.ptr, .{
            .path = exchange.request.head.path orelse "",
            .msg = body.bytes,
            .metadata = exchange.request.head.headers,
            .arena = exchange.arena,
        }, &response) catch |err| {
            return answer(exchange, .{}, .{
                .code = .internal,
                .message = @errorName(err),
            });
        };
        return answer(exchange, response, response.status);
    }

    /// Write one answer: HTTP 200, the framed message, and the status in the
    /// trailing section. Everything is allocated from the exchange arena, which
    /// lives until the engine finishes the stream.
    fn answer(exchange: *Exchange, response: Response, call_status: status.Status) !void {
        const arena = exchange.arena;

        var body: []const u8 = &.{};
        if (response.msg.len != 0) {
            const framed = try arena.alloc(u8, message.prefix_len + response.msg.len);
            body = try message.write(framed, response.msg, false);
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
};

test "our client calls our server" {
    const testing = std.testing;
    const blocking = @import("blocking.zig");
    const grpc_message = message;

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
            if (std.mem.eql(u8, request.msg, "ping")) {
                response.msg = "pong";
                return;
            }
            response.msg = request.msg;
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
    const request_body = try grpc_message.write(&request_buffer, "ping", false);

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
