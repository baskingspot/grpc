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

    var app: App = .{};
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

/// The conformance service. One method so far; the rest answer UNIMPLEMENTED so
/// the runner reports a number instead of a hang.
const App = struct {
    fn handle(
        _: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) anyerror!void {
        if (std.mem.endsWith(u8, request.path, "/Unimplemented")) {
            response.status = .{
                .code = .unimplemented,
                .message = "unimplemented on purpose",
            };
            return;
        }
        response.status = .{ .code = .unimplemented, .message = "not implemented yet" };
    }
};
