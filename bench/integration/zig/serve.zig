//! The Zig arm of the local integration race: `bench/integration/README.md`.
//!
//! It serves `bench.v1.Bench/Unary` with a constant reply and never looks at
//! the request, so a round measures framing, HTTP/2, and the socket, and
//! nothing else. The accept loop is `server.serve`'s with one addition:
//! TCP_NODELAY on each accepted socket, because grpc-go and tonic turn it on
//! by default and the stock loop does not set it.
//!
//! Usage: `bench-integration-zig [port] [reply-size]`, defaults 50151 and 8.

const std = @import("std");
const Io = std.Io;
const posix = std.posix;
const print = std.debug.print;

const grpc = @import("grpc");

/// The same stream limit every arm advertises (README, "Configuration
/// parity"). Above every profile in the matrix, so no arm throttles.
const max_streams: u16 = 1000;

/// The reply every call returns: a serialized `bench.v1.Payload` with
/// `reply-size` bytes, composed once at startup and borrowed by every
/// response, so the handler allocates nothing.
var reply: []const u8 = &.{};

var reply_buffer: [128 * 1024]u8 = undefined;

fn composeReply(size: usize) []const u8 {
    // Field 1, wire type 2: the `data` bytes of a bench.v1.Payload.
    reply_buffer[0] = 0x0A;
    var i: usize = 1;
    var v = size;
    while (v >= 0x80) : (v >>= 7) {
        reply_buffer[i] = @as(u8, @truncate(v)) | 0x80;
        i += 1;
    }
    reply_buffer[i] = @as(u8, @truncate(v));
    i += 1;
    @memset(reply_buffer[i .. i + size], 'a');
    return reply_buffer[0 .. i + size];
}

const Bench = struct {
    fn handle(
        _: *Bench,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) anyerror!void {
        if (!std.mem.eql(u8, request.path, "/bench.v1.Bench/Unary")) {
            response.status = .{ .code = .unimplemented, .message = "no such method" };
            return;
        }
        response.messages = (&reply)[0..1];
    }
};

/// One connection as a group task, the way `server.serve` runs it: a failed
/// connection ends its own task quietly.
fn conn(
    gpa: std.mem.Allocator,
    io: Io,
    stream: Io.net.Stream,
    handler: grpc.server.Handler,
) Io.Cancelable!void {
    grpc.server.serveConnection(io, stream, handler, .{
        .allocator = gpa,
        .max_streams = max_streams,
    }) catch return;
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.arena.allocator();

    const args = try init.minimal.args.toSlice(gpa);
    const port = if (args.len > 1) args[1] else "50151";
    const size = if (args.len > 2)
        std.fmt.parseInt(usize, args[2], 10) catch 8
    else
        8;

    reply = composeReply(size);

    var bench: Bench = .{};
    const handler = grpc.server.Handler.init(&bench, Bench.handle);

    const endpoint = try gpa.print("127.0.0.1:{s}", .{port});
    const address: Io.net.IpAddress = try .parseLiteral(endpoint);
    var listener = try address.listen(io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(io);

    print("zig arm listening on 127.0.0.1:{s}, {d} byte replies\n", .{ port, size });

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
        // grpc-go and tonic default their accepted sockets to TCP_NODELAY;
        // set it here so the network configuration matches across arms. A
        // failure here means the odd socket option is unsupported: run on
        // with Nagle, and the round still compares like with like.
        posix.setsockopt(
            stream.socket.handle,
            posix.IPPROTO.TCP,
            posix.TCP.NODELAY,
            &std.mem.toBytes(@as(c_int, 1)),
        ) catch |err| print("setsockopt TCP_NODELAY failed: {s}\n", .{@errorName(err)});
        group.concurrent(io, conn, .{ gpa, io, stream, handler }) catch |err| switch (err) {
            // No worker to hand it to: serve it on this thread instead.
            error.ConcurrencyUnavailable => grpc.server.serveConnection(io, stream, handler, .{
                .allocator = gpa,
                .max_streams = max_streams,
            }) catch continue,
        };
    }
}
