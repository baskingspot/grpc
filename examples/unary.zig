//! One unary call over a plaintext socket.
//!
//! `just example` compiles it. Run it against a server that answers
//! `helloworld.Greeter/SayHello` on 127.0.0.1:50051, such as the greeter in
//! every gRPC tutorial.

const std = @import("std");
const grpc = @import("grpc");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa: std.mem.Allocator = init.arena.allocator();

    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:50051");
    const stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    // An empty HelloRequest is a frame with a zero length and no bytes.
    var request_buffer: [16]u8 = undefined;
    const request = try grpc.message.write(&request_buffer, &.{}, false);

    var reply: [4 * 1024]u8 = undefined;
    const response = try grpc.blocking.unary(io, stream, .{
        .authority = "127.0.0.1:50051",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .body = request,
    }, .{ .allocator = gpa, .message = &reply });

    std.debug.print("{s} ({d} bytes)\n", .{ @tagName(response.status.code), response.message.len });
}
