//! The load generator for the integration race's capacity passes:
//! `bench/integration/README.md`.
//!
//! One thread per connection, each carrying `streams` concurrent unary calls
//! over one `Client` in a closed loop: a call that completes is recorded and
//! immediately reopened, so the in-flight count stays constant and the server
//! is the only thing that can slow the offer down. It speaks plain gRPC, so
//! it drives the zig, go, and rust arms alike — the point is a client cheap
//! enough that the capacity number belongs to the server. The window's
//! aggregate lands on stdout as one JSON line; the race's driver parses it.
//!
//! Usage: bench-integration-loadgen [endpoint] [duration_s] [connections]
//!   [streams_per_connection] [message_bytes]
//! Defaults: 127.0.0.1:50151, 10, 16, 4, 8.

const std = @import("std");
const Io = std.Io;
const print = std.debug.print;

const grpc = @import("grpc");

/// The same stream limit every arm advertises; the generator stays under it.
const max_streams: u16 = 1000;

var stop: std.atomic.Value(bool) = .init(false);
var requests: std.atomic.Value(u64) = .init(0);
var errors: std.atomic.Value(u64) = .init(0);
var reopens: std.atomic.Value(u64) = .init(0);
/// Errors by gRPC status code, indexed by the code's enum value, so a
/// capacity pass can say which failure it saw.
var error_codes: [24]std.atomic.Value(u64) = undefined;
/// The first failing call's status message, captured once: the server's
/// INTERNAL answers name the error, so one line usually explains the rest.
var first_error: std.atomic.Value(bool) = .init(false);
var first_error_message: [128]u8 = undefined;
var first_error_len: usize = 0;

/// The latency histogram: 1 µs buckets to 1 ms, then 4 µs buckets to 65 ms,
/// then one overflow bucket. Writers only fetchAdd, so no lock is needed.
const fine_buckets = 1024;
const coarse_buckets = 16 * 1024;
const bucket_count = fine_buckets + coarse_buckets + 1;
var histogram: [bucket_count]std.atomic.Value(u64) = undefined;

fn bucketOf(latency_ns: u64) usize {
    const us = latency_ns / std.time.ns_per_us;
    if (us < fine_buckets) return @intCast(us);
    const coarse = us / 4;
    if (coarse < coarse_buckets) return @intCast(fine_buckets + coarse);
    return bucket_count - 1;
}

fn record(latency_ns: u64) void {
    _ = requests.fetchAdd(1, .monotonic);
    _ = histogram[bucketOf(latency_ns)].fetchAdd(1, .monotonic);
}

/// One connection: open `streams` calls, drive, record, reopen. A connection
/// that fails ends its thread; the servers under capacity load are expected
/// to survive, and a death shows up as the rps collapsing in the summary.
const Worker = struct {
    gpa: std.mem.Allocator,
    io: Io,
    target: Io.net.IpAddress,
    authority: []const u8,
    streams: usize,
    message: []const u8,

    fn run(self: *Worker) Io.Cancelable!void {
        self.drive() catch |err| {
            print("loadgen connection failed: {s}\n", .{@errorName(err)});
        };
    }

    fn drive(self: *Worker) !void {
        const socket = try self.target.connect(self.io, .{ .mode = .stream });
        // ghz defaults its sockets to TCP_NODELAY; match it, or Nagle turns
        // the closed loop into 40 ms stalls.
        std.posix.setsockopt(
            socket.socket.handle,
            std.posix.IPPROTO.TCP,
            std.posix.TCP.NODELAY,
            &std.mem.toBytes(@as(c_int, 1)),
        ) catch |err| print("setsockopt TCP_NODELAY failed: {s}\n", .{@errorName(err)});
        var client: grpc.Client = undefined;
        try client.init(self.gpa, self.io, socket);
        defer client.deinit();

        const calls = try self.gpa.alloc(grpc.Call, self.streams);
        const opened = try self.gpa.alloc(Io.Timestamp, self.streams);
        defer self.gpa.free(calls);
        defer self.gpa.free(opened);

        var live: usize = 0;
        defer for (calls[0..live]) |*call| call.deinit();
        for (calls, opened) |*call, *at| {
            call.* = try self.openCall(&client);
            at.* = Io.Clock.now(.awake, self.io);
            live += 1;
        }

        while (!stop.load(.monotonic)) {
            if (!try client.step(calls)) break;
            const now = Io.Clock.now(.awake, self.io);
            for (calls, opened) |*call, *at| {
                const result = call.result() orelse continue;
                record(@intCast(Io.Timestamp.durationTo(at.*, now).nanoseconds));
                if (result.code != .ok) {
                    _ = errors.fetchAdd(1, .monotonic);
                    _ = error_codes[@intCast(@backingInt(result.code))]
                        .fetchAdd(1, .monotonic);
                    if (!first_error.swap(true, .acq_rel)) {
                        first_error_len = @min(
                            result.message.len,
                            first_error_message.len,
                        );
                        @memcpy(
                            first_error_message[0..first_error_len],
                            result.message[0..first_error_len],
                        );
                    }
                }
                call.deinit();
                call.* = try self.openCall(&client);
                at.* = now;
                _ = reopens.fetchAdd(1, .monotonic);
            }
        }
    }

    fn openCall(self: *Worker, client: *grpc.Client) !grpc.Call {
        return client.open(.{
            .authority = self.authority,
            .service = "bench.v1.Bench",
            .method = "Unary",
            .body = self.message,
            .timeout = .fromSecondsComptime(10),
        });
    }
};

/// The serialized `bench.v1.Payload` with `size` bytes of data: field 1,
/// wire type 2, then the length-prefixed data, plus the five gRPC frame
/// bytes. The same request bytes the ghz passes replay.
fn composeMessage(buffer: []u8, size: usize) []const u8 {
    // The serialized bench.v1.Payload: field 1, wire type 2, a length
    // varint, and the data. The gRPC frame prefix counts exactly those bytes.
    var varint: [5]u8 = undefined;
    var varint_len: usize = 0;
    var v = size;
    while (v >= 0x80) : (v >>= 7) {
        varint[varint_len] = @as(u8, @truncate(v)) | 0x80;
        varint_len += 1;
    }
    varint[varint_len] = @as(u8, @truncate(v));
    varint_len += 1;

    const body_len = 1 + varint_len + size; // tag, varint, data
    buffer[0] = 0x00; // gRPC frame: uncompressed flag.
    buffer[1] = @truncate(body_len >> 24);
    buffer[2] = @truncate(body_len >> 16);
    buffer[3] = @truncate(body_len >> 8);
    buffer[4] = @truncate(body_len);
    buffer[5] = 0x0A; // field 1, wire type 2.
    @memcpy(buffer[6 .. 6 + varint_len], varint[0..varint_len]);
    @memset(buffer[6 + varint_len .. 6 + varint_len + size], 'a');
    return buffer[0 .. 5 + body_len];
}

fn percentile(sorted_count: u64, fraction: f64) usize {
    const target: u64 = @intFromFloat(@as(f64, @floatFromInt(sorted_count)) * fraction);
    var seen: u64 = 0;
    for (&histogram, 0..) |*bucket, index| {
        seen += bucket.load(.monotonic);
        if (seen >= target) {
            if (index < fine_buckets) return index; // 1 µs buckets
            if (index < fine_buckets + coarse_buckets) {
                return (index - fine_buckets) * 4; // 4 µs buckets
            }
            return coarse_buckets * 4; // the overflow bucket
        }
    }
    return 0;
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    const args = try init.minimal.args.toSlice(gpa);
    const endpoint = if (args.len > 1) args[1] else "127.0.0.1:50151";
    const duration = if (args.len > 2)
        std.fmt.parseInt(u64, args[2], 10) catch 10
    else
        10;
    const connections = if (args.len > 3)
        std.fmt.parseInt(usize, args[3], 10) catch 16
    else
        16;
    const streams = if (args.len > 4)
        std.fmt.parseInt(usize, args[4], 10) catch 4
    else
        4;
    const size = if (args.len > 5)
        std.fmt.parseInt(usize, args[5], 10) catch 8
    else
        8;
    if (streams > max_streams) return error.StreamLimit;

    var message_buffer: [128 * 1024]u8 = undefined;
    const message = composeMessage(&message_buffer, size);

    const target: Io.net.IpAddress = try .parseLiteral(endpoint);
    const authority = endpoint;

    for (&histogram) |*bucket| bucket.* = .init(0);
    for (&error_codes) |*code| code.* = .init(0);

    const workers = try gpa.alloc(Worker, connections);
    defer gpa.free(workers);
    for (workers) |*worker| worker.* = .{
        .gpa = gpa,
        .io = io,
        .target = target,
        .authority = authority,
        .streams = streams,
        .message = message,
    };

    var group: Io.Group = .init;
    defer group.cancel(io);
    for (workers) |*worker| {
        group.concurrent(io, Worker.run, .{worker}) catch |err| switch (err) {
            // No thread to spare: run it here instead.
            error.ConcurrencyUnavailable => try Worker.run(worker),
        };
    }

    try Io.sleep(io, Io.Duration.fromSeconds(1), .awake);
    const measured_start = Io.Clock.now(.awake, io);
    const baseline = requests.load(.monotonic);
    try Io.sleep(io, Io.Duration.fromSeconds(@intCast(duration)), .awake);
    const measured_end = Io.Clock.now(.awake, io);
    stop.store(true, .monotonic);
    try group.await(io);

    const window_ns: f64 = @floatFromInt(Io.Timestamp.durationTo(
        measured_start,
        measured_end,
    ).nanoseconds);
    const count = requests.load(.monotonic) - baseline;
    const rps: f64 = @as(f64, @floatFromInt(count)) /
        (window_ns / std.time.ns_per_s);
    const p50 = percentile(count, 0.50);
    const p90 = percentile(count, 0.90);
    const p99 = percentile(count, 0.99);

    var out: [512]u8 = undefined;
    const json = try std.mem.print(
        &out,
        "{{\"count\":{d},\"errors\":{d},\"rps\":{d:.2},\"p50_us\":{d}," ++
            "\"p90_us\":{d},\"p99_us\":{d},\"connections\":{d}," ++
            "\"streams\":{d},\"size\":{d}}}\n",
        .{
            count,
            errors.load(.monotonic),
            rps,
            p50,
            p90,
            p99,
            connections,
            streams,
            size,
        },
    );
    try Io.File.writeStreamingAll(Io.File.stdout(), io, json);

    var first = true;
    for (&error_codes, 0..) |*code, index| {
        const failures = code.load(.monotonic);
        if (failures == 0) continue;
        if (first) {
            print("loadgen error codes:\n", .{});
            first = false;
        }
        print("  {d}: {d}\n", .{ index, failures });
        if (first_error_len > 0) {
            print("  first message: \"{s}\"\n", .{first_error_message[0..first_error_len]});
            first_error_len = 0;
        }
    }
}
