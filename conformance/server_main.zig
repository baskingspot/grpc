//! A server under test for the connectrpc conformance runner.
//!
//! In server mode the runner writes one `ServerCompatRequest` to this program's
//! stdin and then expects one length-prefixed `ServerCompatResponse` on stdout,
//! carrying the host and port it should dial. This program starts a gRPC server,
//! reports where it listens, and serves the conformance service until the runner
//! sends SIGTERM or closes stdin.
//!
//! Plaintext HTTP/2 only for now. A `use_tls` request is refused rather than
//! silently served in the clear, because a client that asked for TLS would then
//! fail verification for a reason that is not its own.

const std = @import("std");
const grpc = @import("grpc");
const http = @import("http");
const pb = @import("protobuf");
const messages = @import("messages.zig");

const Io = std.Io;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa: std.mem.Allocator = init.arena.allocator();

    var in_buffer: [16 * 1024]u8 = undefined;
    var stdin_reader = Io.File.stdin().reader(io, &in_buffer);
    const reader = &stdin_reader.interface;

    var out_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = Io.File.stdout().writer(io, &out_buffer);
    const writer = &stdout_writer.interface;

    var length_bytes: [4]u8 = undefined;
    try reader.readSliceAll(&length_bytes);
    const length = std.mem.readInt(u32, &length_bytes, .big);
    const body = try gpa.alloc(u8, length);
    try reader.readSliceAll(body);

    var request_reader: Io.Reader = .fixed(body);
    const request = try pb.decodeLeaky(messages.ServerCompatRequest, gpa, &request_reader, .{});

    if (request.protocol != messages.protocol_grpc) return error.UnsupportedProtocol;
    if (request.use_tls) return error.UnsupportedTls;
    if (request.message_receive_limit != 0 and
        request.message_receive_limit < 4 * 1024 * 1024)
    {
        // The limit is a client-side option here: a message above it is what the
        // runner will ask us to reject. The engine's body cap is the only knob
        // this server has today.
    }

    var app: App = .{ .io = io };
    var server: grpc.server.ReadinessServer = undefined;
    try server.init(
        io,
        try Io.net.IpAddress.parseLiteral("127.0.0.1:0"),
        grpc.server.Handler.init(&app, App.handle),
        .{ .allocator = gpa, .max_connections = 8 },
    );
    defer server.deinit();

    const response: messages.ServerCompatResponse = .{
        .host = "127.0.0.1",
        .port = server.address().getPort(),
    };

    var encoded: Io.Writer.Allocating = .init(gpa);
    defer encoded.deinit();
    try pb.encode(response, .{}, &encoded.writer);
    var size: [4]u8 = undefined;
    std.mem.writeInt(u32, &size, @intCast(encoded.written().len), .big);
    try writer.writeAll(&size);
    try writer.writeAll(encoded.written());
    try writer.flush();

    // The runner stops the process with SIGTERM; serving until the loop ends is
    // enough, and stdin closing is the other natural end.
    try server.run();
}

/// The conformance service. Unary and IdempotentUnary answer; everything else
/// is UNIMPLEMENTED, which the runner reports as a case rather than a hang.
const App = struct {
    io: Io,

    fn handle(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) anyerror!void {
        const slash = std.mem.findScalarLast(u8, request.path, '/');
        const method = if (slash) |at| request.path[at + 1 ..] else request.path;

        if (std.mem.eql(u8, method, "Unary") or std.mem.eql(u8, method, "IdempotentUnary")) {
            return self.unary(request, response);
        }
        if (std.mem.eql(u8, method, "Unimplemented")) {
            response.status = .{
                .code = .unimplemented,
                .message = "unimplemented on purpose",
            };
            return;
        }
        response.status = .{ .code = .unimplemented, .message = "not implemented yet" };
    }

    fn unary(self: *App, request: grpc.server.Request, response: *grpc.server.Response) !void {
        if (request.messages.len != 1) {
            response.status = .{ .code = .internal, .message = "unary takes one message" };
            return;
        }
        const arena = request.arena;
        const wire = request.messages[0].bytes;

        var reader: Io.Reader = .fixed(wire);
        const decoded = try pb.decodeLeaky(messages.UnaryRequest, arena, &reader, .{});
        const definition: messages.UnaryResponseDefinition =
            decoded.response_definition orelse .{};

        response.headers = try convert(arena, definition.response_headers);
        response.metadata = try convert(arena, definition.response_trailers);

        if (definition.err) |failure| {
            response.status = .{
                .code = @enumFromInt(@as(u8, @intCast(failure.code))),
                .message = failure.message orelse "",
            };
            return;
        }
        if (definition.response_delay_ms != 0) {
            // A blocking engine has one thread, so this sleeps the loop. That
            // is the test's intent: a slow server.
            const delay = Io.Clock.Duration.fromMilliseconds(definition.response_delay_ms);
            self.io.sleep(delay, .awake) catch |err| switch (err) {
                error.Canceled => return,
            };
        }

        const data = if (definition.response_data.len != 0)
            definition.response_data
        else
            decoded.request_data;

        const requests = try arena.alloc(messages.Any, 1);
        requests[0] = .{
            .type_url = "type.googleapis.com/connectrpc.conformance.v1.UnaryRequest",
            .value = wire,
        };

        const reply: messages.UnaryResponse = .{
            .payload = .{
                .data = data,
                .request_info = .{
                    .request_headers = try headersOf(arena, request.metadata),
                    .requests = requests,
                },
            },
        };

        var encoded: Io.Writer.Allocating = .init(arena);
        try pb.encode(reply, .{}, &encoded.writer);
        const messages_out = try arena.alloc([]const u8, 1);
        messages_out[0] = encoded.written();
        response.messages = messages_out;
    }

    fn convert(arena: std.mem.Allocator, fields: []const messages.Header) ![]const http.Header {
        var count: usize = 0;
        for (fields) |field| count += @max(field.value.len, 1);
        const out = try arena.alloc(http.Header, count);
        var index: usize = 0;
        for (fields) |field| {
            if (field.value.len == 0) {
                out[index] = .{ .name = field.name, .value = "" };
                index += 1;
                continue;
            }
            for (field.value) |value| {
                out[index] = .{ .name = field.name, .value = value };
                index += 1;
            }
        }
        return out;
    }

    fn headersOf(arena: std.mem.Allocator, fields: http.Headers) ![]const messages.Header {
        var out: std.ArrayList(messages.Header) = .empty;
        var index: usize = 0;
        while (index < fields.items.len) : (index += 1) {
            const name = fields.items[index].name;
            var values: std.ArrayList([]const u8) = .empty;
            var scan = index;
            while (scan < fields.items.len and
                std.mem.eql(u8, fields.items[scan].name, name)) : (scan += 1)
            {
                try values.append(arena, fields.items[scan].value);
            }
            try out.append(arena, .{ .name = name, .value = values.items });
            index = scan - 1;
        }
        return out.items;
    }
};
