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
        if (counting) |c| {
            const n = c.requests.fetchAdd(1, .monotonic);
            // Every 100,000th request reports the allocation traffic since
            // the last report: the steady-state allocs-per-request figure a
            // profiling pass wants, without a logging thread.
            if (n > 0 and n % 100_000 == 0) c.report();
        }
        response.messages = (&reply)[0..1];
    }
};

/// Opt-in allocation counting (bench/integration/README.md, "Profiling"):
/// pass `count` as the third argument. The allocator every connection gets
/// is then this wrapper, and every 100,000th request logs the traffic since
/// the previous line, so a load run reads steady-state allocations per
/// request straight off stderr. The spin lock and the atomic adds make it
/// measurably heavier than the stock path, which is why it is opt-in.
const Counting = struct {
    child: std.mem.Allocator,
    locked: std.atomic.Value(bool) = .init(false),
    live: usize = 0,
    live_bytes: usize = 0,
    peak_live: usize = 0,
    peak_live_bytes: usize = 0,
    total_allocs: std.atomic.Value(u64) = .init(0),
    total_frees: std.atomic.Value(u64) = .init(0),
    total_alloc_bytes: std.atomic.Value(u64) = .init(0),
    requests: std.atomic.Value(u64) = .init(0),
    last_mark_allocs: u64 = 0,
    last_mark_bytes: u64 = 0,

    fn allocator(self: *Counting) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// A spin lock: the critical sections are counter updates reached a few
    /// times per request at most, so contention is brief and rare, and the
    /// 0.17 blocking mutex needs an `Io` this allocator does not have.
    fn lock(self: *Counting) void {
        while (self.locked.cmpxchgStrong(false, true, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    fn unlock(self: *Counting) void {
        self.locked.store(false, .release);
    }

    /// One line of traffic on stderr. One thread reaches this: the boundary
    /// request number is handed out exactly once by `fetchAdd`.
    fn report(self: *Counting) void {
        const requests = self.requests.load(.monotonic);
        const allocs = self.total_allocs.load(.monotonic);
        const bytes = self.total_alloc_bytes.load(.monotonic);
        const delta_allocs = allocs - self.last_mark_allocs;
        const delta_bytes = bytes - self.last_mark_bytes;
        self.last_mark_allocs = allocs;
        self.last_mark_bytes = bytes;
        print(
            "count: requests={d} delta_allocs={d} allocs_per_request={d:.3} " ++
                "delta_alloc_bytes={d} live={d} peak_live={d} live_bytes={d} " ++
                "peak_live_bytes={d} total_allocs={d} total_alloc_bytes={d}\n",
            .{
                requests,
                delta_allocs,
                @as(f64, @floatFromInt(delta_allocs)) / 100_000,
                delta_bytes,
                self.live,
                self.peak_live,
                self.live_bytes,
                self.peak_live_bytes,
                allocs,
                bytes,
            },
        );
    }

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = allocFn,
        .resize = resizeFn,
        .remap = remapFn,
        .free = freeFn,
    };

    fn allocFn(
        ptr: *anyopaque,
        len: usize,
        alignment: std.mem.Alignment,
        ret_addr: usize,
    ) ?[*]u8 {
        const self: *Counting = @ptrCast(@alignCast(ptr));
        const result = self.child.rawAlloc(len, alignment, ret_addr);
        if (result != null) {
            self.lock();
            defer self.unlock();
            _ = self.total_allocs.fetchAdd(1, .monotonic);
            _ = self.total_alloc_bytes.fetchAdd(len, .monotonic);
            self.live += 1;
            self.live_bytes += len;
            self.peak_live = @max(self.peak_live, self.live);
            self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
        }
        return result;
    }

    fn freeFn(
        ptr: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        ret_addr: usize,
    ) void {
        const self: *Counting = @ptrCast(@alignCast(ptr));
        self.child.rawFree(memory, alignment, ret_addr);
        self.lock();
        defer self.unlock();
        _ = self.total_frees.fetchAdd(1, .monotonic);
        self.live -= 1;
        self.live_bytes -= memory.len;
    }

    fn resizeFn(
        ptr: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) bool {
        const self: *Counting = @ptrCast(@alignCast(ptr));
        const result = self.child.rawResize(memory, alignment, new_len, ret_addr);
        if (result) {
            self.lock();
            defer self.unlock();
            self.live_bytes += new_len - memory.len;
            self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
        }
        return result;
    }

    fn remapFn(
        ptr: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) ?[*]u8 {
        const self: *Counting = @ptrCast(@alignCast(ptr));
        const result = self.child.rawRemap(memory, alignment, new_len, ret_addr);
        if (result != null) {
            self.lock();
            defer self.unlock();
            self.live_bytes += new_len - memory.len;
            self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
        }
        return result;
    }
};

var counting: ?*Counting = null;

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
    var gpa = init.arena.allocator();

    const args = try init.minimal.args.toSlice(gpa);
    const port = if (args.len > 1) args[1] else "50151";
    const size = if (args.len > 2)
        std.fmt.parseInt(usize, args[2], 10) catch 8
    else
        8;

    reply = composeReply(size);

    var bench: Bench = .{};
    const handler = grpc.server.Handler.init(&bench, Bench.handle);

    // Third argument `count`: allocation counting (Counting above).
    if (args.len > 3 and std.mem.eql(u8, args[3], "count")) {
        const c = try gpa.create(Counting);
        c.* = .{ .child = gpa };
        counting = c;
        gpa = c.allocator();
        print("zig arm counting allocations\n", .{});
    }
    const allocator = gpa;

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
        group.concurrent(io, conn, .{ allocator, io, stream, handler }) catch |err| switch (err) {
            // No worker to hand it to: serve it on this thread instead.
            error.ConcurrencyUnavailable => grpc.server.serveConnection(io, stream, handler, .{
                .allocator = allocator,
                .max_streams = max_streams,
            }) catch continue,
        };
    }
}
