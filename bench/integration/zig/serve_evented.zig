//! The evented Zig arm of the local integration race: `bench/integration/README.md`.
//!
//! The same service, constant reply, TCP_NODELAY, and stream limit as
//! `serve.zig`, but the accept loop and every connection run on zio's
//! evented `std.Io` (github.com/lalinsky/zio) instead of the threaded one.
//! `grpc.server.serveConnection` is called unchanged; it receives zio's `Io`.
//!
//! Usage: `bench-integration-evented [port] [reply-size]`, defaults 50154 and 8.

const std = @import("std");
const Io = std.Io;
const posix = std.posix;
const print = std.debug.print;

const grpc = @import("grpc");
const zio = @import("zio");

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
    const arena = init.arena.allocator();

    const args = try init.minimal.args.toSlice(arena);
    const port = if (args.len > 1) args[1] else "50154";
    const size = if (args.len > 2)
        std.fmt.parseInt(usize, args[2], 10) catch 8
    else
        8;

    reply = composeReply(size);

    var bench: Bench = .{};
    const handler: grpc.server.Handler = .init(&bench, Bench.handle);

    // zio runs one executor (one thread) unless told otherwise.
    // BENCH_ZIO_EXECUTORS=N picks N, and 0 sizes it to the cores the process
    // may use, so the same binary measures both shapes. The scheduler
    // allocates from the process allocator, not the arena.
    const Executors = @FieldType(zio.RuntimeOptions, "executors");
    const requested = init.environ_map.get("BENCH_ZIO_EXECUTORS") orelse "1";
    const count = std.fmt.parseInt(u8, requested, 10) catch 1;
    const executors: Executors = if (count == 0) .auto else .exact(count);
    const rt: *zio.Runtime = try .init(init.gpa, .{ .executors = executors });
    defer rt.deinit();
    const io = rt.io();

    const endpoint = try arena.print("127.0.0.1:{s}", .{port});
    const address: Io.net.IpAddress = try .parseLiteral(endpoint);
    var listener = try address.listen(io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(io);

    print("evented arm listening on 127.0.0.1:{s}, {d} byte replies\n", .{ port, size });

    var group: Io.Group = .init;
    defer group.cancel(io);

    while (true) {
        const stream = listener.accept(io) catch |err| switch (err) {
            error.ConnectionAborted, error.WouldBlock => continue,
            error.Canceled => return,
            else => return err,
        };
        posix.setsockopt(
            stream.socket.handle,
            posix.IPPROTO.TCP,
            posix.TCP.NODELAY,
            &std.mem.toBytes(@as(c_int, 1)),
        ) catch |err| print("setsockopt TCP_NODELAY failed: {s}\n", .{@errorName(err)});
        group.concurrent(io, conn, .{ arena, io, stream, handler }) catch |err| switch (err) {
            error.ConcurrencyUnavailable => stream.close(io),
        };
    }
}
