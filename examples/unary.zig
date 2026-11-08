//! One unary call over a plaintext socket: a client for `examples/greeter.zig`.
//!
//! `just demo-greeter` starts the server, runs this, and stops it. To call
//! another endpoint, pass one: `zig build demo-unary -- baskin 10.0.0.5:50051`.

const std = @import("std");
const grpc = @import("grpc");
const pb = @import("protobuf");

const HelloRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{ .name = pb.string(1) };
    name: []const u8 = "",
};

const HelloReply = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{ .message = pb.string(1) };
    message: []const u8 = "",
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa: std.mem.Allocator = init.arena.allocator();

    const args = try init.minimal.args.toSlice(gpa);
    const name = if (args.len > 1) args[1] else "baskingspot";
    const endpoint = if (args.len > 2) args[2] else "127.0.0.1:50051";

    var encoded: [128]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&encoded);
    try pb.encode(HelloRequest{ .name = name }, .{}, &writer);

    var frame: [128 + grpc.message.prefix_len]u8 = undefined;
    const request = try grpc.message.write(&frame, std.Io.Writer.buffered(&writer), false);

    const address = try std.Io.net.IpAddress.parseLiteral(endpoint);
    const stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var reply: [4 * 1024]u8 = undefined;
    const response = try grpc.blocking.unary(io, stream, .{
        .authority = endpoint,
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .body = request,
    }, .{ .allocator = gpa, .message = &reply });

    if (!response.status.isOk()) {
        std.debug.print("{s}: {s}\n", .{ @tagName(response.status.code), response.status.message });
        return error.CallFailed;
    }

    var reader: std.Io.Reader = .fixed(response.message);
    const hello = try pb.decodeLeaky(HelloReply, gpa, &reader, .{});
    std.debug.print("{s}\n", .{hello.message});
}
