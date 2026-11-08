//! A greeter server: the other side of `examples/unary.zig`.
//!
//! It answers `helloworld.Greeter/SayHello` with protobuf messages written by
//! hand, and returns UNIMPLEMENTED for any other path. `just demo-greeter` runs
//! it and calls it once, then stops it.

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

const Greeter = struct {
    fn handle(
        _: *Greeter,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) anyerror!void {
        if (!std.mem.eql(u8, request.path, "/helloworld.Greeter/SayHello")) {
            response.status = .{ .code = .unimplemented, .message = "no such method" };
            return;
        }

        if (request.messages.len != 1) {
            response.status = .{ .code = .internal, .message = "SayHello takes one message" };
            return;
        }

        var reader: std.Io.Reader = .fixed(request.messages[0].bytes);
        const hello = try pb.decodeLeaky(HelloRequest, request.arena, &reader, .{});

        var encoded: [512]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&encoded);
        try pb.encode(HelloReply{
            .message = try request.arena.print("hello, {s}", .{hello.name}),
        }, .{}, &writer);

        // The server reads this after the handler returns, so it lives in the
        // exchange arena rather than on this stack.
        const encoded_reply = try request.arena.dupe(u8, std.Io.Writer.buffered(&writer));
        const replies = try request.arena.alloc([]const u8, 1);
        replies[0] = encoded_reply;
        response.messages = replies;
    }
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa: std.mem.Allocator = init.arena.allocator();

    var greeter: Greeter = .{};

    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:50051");
    var listener = try address.listen(io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(io);

    std.debug.print("greeter listening on 127.0.0.1:50051\n", .{});
    try grpc.server.serve(io, &listener, grpc.server.Handler.init(&greeter, Greeter.handle), .{
        .allocator = gpa,
    });
}
