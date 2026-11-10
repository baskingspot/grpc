//! One unary call over a plaintext socket: a client for `examples/greeter.zig`.
//!
//! `just demo-greeter` starts the server, runs this, and stops it. To call
//! another endpoint, pass one: `zig build demo-unary -- baskin 10.0.0.5:50051`.

const std = @import("std");
const Io = std.Io;
const print = std.debug.print;

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
    const gpa = init.arena.allocator();

    const args = try init.minimal.args.toSlice(gpa);
    const name = if (args.len > 1) args[1] else "baskingspot";
    const endpoint = if (args.len > 2) args[2] else "127.0.0.1:50051";

    var encoded: [128]u8 = undefined;
    var writer: Io.Writer = .fixed(&encoded);
    try pb.encode(HelloRequest{ .name = name }, .{}, &writer);

    const address: Io.net.IpAddress = try .parseLiteral(endpoint);
    const socket = try address.connect(io, .{ .mode = .stream });
    var client: grpc.blocking.Client = undefined;
    client.init(gpa, io, socket) catch |err| {
        socket.close(io);
        return err;
    };
    defer client.deinit();

    var reply: [4 * 1024]u8 = undefined;
    const response = try client.unary(.{
        .authority = endpoint,
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .message = writer.buffered(),
        .timeout = try .init(try grpc.time.Duration.fromSeconds(2)),
    }, &reply);

    if (!response.status.isOk()) {
        print("{s}: {s}\n", .{ @tagName(response.status.code), response.status.message });
        return error.CallFailed;
    }

    var reader: Io.Reader = .fixed(response.message);
    const hello = try pb.decodeLeaky(HelloReply, gpa, &reader, .{});
    print("{s}\n", .{hello.message});
}
