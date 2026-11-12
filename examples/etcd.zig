//! An etcd client: Put, Range, and DeleteRange over gRPC.
//!
//! The message types below are the etcd server's own, written out by hand with
//! their field numbers. Nothing is generated and no `.proto` file is read;
//! `baskingspot/protobuf` writes and reads them from the struct.
//!
//! `just demo-etcd` starts a throwaway etcd, runs this, and stops it. To point
//! it somewhere else, pass an endpoint: `zig build demo-etcd -- 10.0.0.5:2379`.

const std = @import("std");
const print = std.debug.print;
const Io = std.Io;
const ArenaAllocator = std.heap.ArenaAllocator;

const grpc = @import("grpc");
const pb = @import("protobuf");

const default_endpoint = "127.0.0.1:2379";

// etcdserverpb.ResponseHeader
const ResponseHeader = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .cluster_id = pb.field(1),
        .member_id = pb.field(2),
        .revision = pb.field(3),
        .raft_term = pb.field(4),
    };

    cluster_id: u64 = 0,
    member_id: u64 = 0,
    revision: i64 = 0,
    raft_term: u64 = 0,
};

// mvccpb.KeyValue
const KeyValue = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .key = pb.bytes(1),
        .create_revision = pb.field(2),
        .mod_revision = pb.field(3),
        .version = pb.field(4),
        .value = pb.bytes(5),
        .lease = pb.field(6),
    };

    key: []const u8 = "",
    create_revision: i64 = 0,
    mod_revision: i64 = 0,
    version: i64 = 0,
    value: []const u8 = "",
    lease: i64 = 0,
};

// etcdserverpb.PutRequest and PutResponse
const PutRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .key = pb.bytes(1),
        .value = pb.bytes(2),
        .lease = pb.field(3),
        .prev_kv = pb.field(4),
    };

    key: []const u8 = "",
    value: []const u8 = "",
    lease: i64 = 0,
    prev_kv: bool = false,
};

const PutResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .prev_kv = pb.field(2),
    };

    header: ?ResponseHeader = null,
    prev_kv: ?KeyValue = null,
};

// etcdserverpb.RangeRequest and RangeResponse
const RangeRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .key = pb.bytes(1),
        .range_end = pb.bytes(2),
        .limit = pb.field(3),
        .count_only = pb.field(9),
    };

    key: []const u8 = "",
    range_end: []const u8 = "",
    limit: i64 = 0,
    count_only: bool = false,
};

const RangeResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .kvs = pb.field(2),
        .more = pb.field(3),
        .count = pb.field(4),
    };

    header: ?ResponseHeader = null,
    kvs: []const KeyValue = &.{},
    more: bool = false,
    count: i64 = 0,
};

// etcdserverpb.DeleteRangeRequest and DeleteRangeResponse
const DeleteRangeRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .key = pb.bytes(1),
        .range_end = pb.bytes(2),
    };

    key: []const u8 = "",
    range_end: []const u8 = "",
};

const DeleteRangeResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .deleted = pb.field(2),
    };

    header: ?ResponseHeader = null,
    deleted: i64 = 0,
};

// etcdserverpb.WatchRequest. The `oneof request_union` is three fields where
// at most one is set, which is all the wire carries: an optional field that
// is null writes nothing, and one that is present is written even when empty.
const WatchRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .create_request = pb.field(1),
        .cancel_request = pb.field(2),
    };

    create_request: ?WatchCreateRequest = null,
    cancel_request: ?WatchCancelRequest = null,
};

const WatchCreateRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .key = pb.bytes(1),
        .range_end = pb.bytes(2),
        .start_revision = pb.field(3),
        .prev_kv = pb.field(6),
    };

    key: []const u8 = "",
    range_end: []const u8 = "",
    /// Zero watches live changes only. To resume after a reconnect, pass the
    /// last seen revision plus one and the server replays what was missed.
    start_revision: i64 = 0,
    prev_kv: bool = false,
};

const WatchCancelRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .watch_id = pb.field(1),
    };

    watch_id: i64 = 0,
};

// mvccpb.Event
const Event = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .type = pb.field(1),
        .kv = pb.field(2),
        .prev_kv = pb.field(3),
    };

    const Type = enum(i32) {
        put = 0,
        delete = 1,
    };

    type: Type = .put,
    kv: KeyValue = .{},
    prev_kv: ?KeyValue = null,
};

// etcdserverpb.WatchResponse. Note the numbering: the flags and reason are
// fields 3-7 and the events jumped to 11, which is not the order they are
// declared in.
const WatchResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .watch_id = pb.field(2),
        .created = pb.field(3),
        .canceled = pb.field(4),
        .compact_revision = pb.field(5),
        .cancel_reason = pb.string(6),
        .events = pb.field(11),
    };

    header: ?ResponseHeader = null,
    watch_id: i64 = 0,
    created: bool = false,
    canceled: bool = false,
    compact_revision: i64 = 0,
    cancel_reason: []const u8 = "",
    events: []const Event = &.{},
};

/// The end of a prefix range. etcd reads `key < k < range_end`, so the end is
/// this prefix with its last byte incremented.
fn prefixEnd(out: []u8, prefix: []const u8) []const u8 {
    @memcpy(out[0..prefix.len], prefix);
    var last = prefix.len;
    while (last > 0) {
        last -= 1;
        if (out[last] == 0xff) continue;
        out[last] += 1;
        return out[0 .. last + 1];
    }
    // Every byte of the prefix is 0xff: the range runs to the end of the keyspace.
    return out[0..0];
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const arena = init.arena.allocator();

    const args = try init.minimal.args.toSlice(arena);
    const endpoint = if (args.len > 1) args[1] else default_endpoint;

    const address: Io.net.IpAddress = try .parseLiteral(endpoint);
    const socket = try address.connect(io, .{ .mode = .stream });

    var client: grpc.Client = undefined;
    try client.init(arena, io, socket);
    defer client.deinit();

    var session: Etcd = .{
        .client = &client,
        .arena = .init(arena),
        .endpoint = endpoint,
    };
    defer session.arena.deinit();

    const put = try session.put("demo/greeting", "hello from grpc");
    print("put demo/greeting at revision {d}\n", .{put.header.?.revision});

    _ = try session.put("demo/count", "1");

    const listed = try session.range("demo/");
    print("range demo/ returned {d} keys at revision {d}\n", .{
        listed.kvs.len,
        listed.header.?.revision,
    });
    for (listed.kvs) |kv| {
        print("  {s} = {s} (mod revision {d})\n", .{
            kv.key,
            kv.value,
            kv.mod_revision,
        });
    }

    const removed = try session.deleteRange("demo/");
    print("deleted {d} keys\n", .{removed.deleted});

    const empty = try session.range("demo/");
    print("range demo/ after the delete: {d} keys\n", .{empty.kvs.len});

    // A watch stream, the shape a real client holds open for the life of the
    // process: its own connection, a synchronous read loop, and events from other
    // writers. The unary client above plays the external writer.
    print("\nwatch demo/ on a second connection:\n", .{});
    const watch_socket = try address.connect(io, .{ .mode = .stream });
    errdefer watch_socket.close(io);

    var watch_client: grpc.Client = undefined;
    try watch_client.init(arena, io, watch_socket);
    defer watch_client.deinit();

    var watch_calls: [1]grpc.Call = .{try watch_client.open(.{
        .authority = endpoint,
        .service = "etcdserverpb.Watch",
        .method = "Watch",
        .streaming = true,
        // The demo drives the stream to a clean cancel. If etcd stalls, the
        // call ends DEADLINE_EXCEEDED here instead of hanging the example.
        .timeout = .fromSecondsComptime(10),
    })};
    var watch = watch_client.stream(&watch_calls[0], &watch_calls);
    defer watch.deinit(); // The call borrows the client: release it first.

    var watch_arena: ArenaAllocator = .init(arena);
    defer watch_arena.deinit();

    var end_buffer: [256]u8 = undefined;
    const range_end = prefixEnd(&end_buffer, "demo/");
    try sendWatch(&watch, .{ .create_request = .{
        .key = "demo/",
        .range_end = range_end,
    } });

    const created = (try nextResponse(&watch, &watch_arena)) orelse
        return error.WatchClosedBeforeCreated;
    if (!created.created) return error.WatchNotCreated;
    print("  watch {d} created at revision {d}\n", .{
        created.watch_id,
        created.header.?.revision,
    });

    _ = try session.put("demo/watched", "a watched value");
    const put_event = (try nextResponse(&watch, &watch_arena)) orelse
        return error.WatchClosedBeforePutEvent;
    if (put_event.events.len != 1 or put_event.events[0].type != .put) {
        return error.WatchPutEventMissing;
    }
    var kv = put_event.events[0].kv;
    print("  put {s} = {s} (mod revision {d})\n", .{
        kv.key,
        kv.value,
        kv.mod_revision,
    });

    _ = try session.deleteRange("demo/");
    const delete_event = (try nextResponse(&watch, &watch_arena)) orelse
        return error.WatchClosedBeforeDeleteEvent;
    if (delete_event.events.len != 1 or delete_event.events[0].type != .delete) {
        return error.WatchDeleteEventMissing;
    }
    kv = delete_event.events[0].kv;
    print("  deleted {s} (mod revision {d})\n", .{
        kv.key,
        kv.mod_revision,
    });

    // Cancel and read the confirmation, then close our side and let the call
    // end with the server's trailers.
    try sendWatch(&watch, .{ .cancel_request = .{ .watch_id = created.watch_id } });
    const canceled = (try nextResponse(&watch, &watch_arena)) orelse
        return error.WatchClosedBeforeCanceled;
    if (!canceled.canceled) return error.WatchNotCanceled;
    print("  watch canceled: \"{s}\"\n", .{canceled.cancel_reason});

    // etcd keeps a watch stream open after the client half-closes: the stream
    // is the session, and a half-close only says "no more watch requests". A
    // client ends one by dropping the call, the same teardown etcd-zig does on
    // disconnect. `deinit` below resets the stream and the client closes the
    // socket.
    print("  dropping the watch stream\n", .{});
}

/// Send one watch request. `send` refuses a second message while the first
/// is still queued, so a refusal means advance and try again.
fn sendWatch(watch: *grpc.Stream, request: WatchRequest) !void {
    var buffer: [512]u8 = undefined;
    var writer: Io.Writer = .fixed(&buffer);
    try pb.encode(WatchRequest, request, .{}, &writer);
    while (!try watch.send(writer.buffered())) {
        if (!try watch.advance()) return error.WatchSendBlocked;
    }
}

/// The next watch response, waiting until one arrives. Null when the call
/// ended. The response borrows `arena` until the next call here; scalars stay
/// valid. This is the facade-shaped core of a watch client's `next`.
fn nextResponse(watch: *grpc.Stream, arena: *ArenaAllocator) !?WatchResponse {
    while (true) {
        if (try watch.nextMessage()) |found| {
            _ = arena.reset(.retain_capacity);
            var reader: Io.Reader = .fixed(found.bytes);
            const value = try pb.decodeLeaky(
                WatchResponse,
                arena.allocator(),
                &reader,
                .{},
            );
            return value;
        }
        if (watch.result() != null) return null;
        if (!try watch.advance()) return null;
    }
}

/// The KV service as four calls. Each one encodes a request, sends it as one
/// gRPC message, and decodes the reply.
const Etcd = struct {
    client: *grpc.Client,
    /// Decoded replies are copied here and reset on the next call, so each
    /// reply is valid until the call after it.
    arena: ArenaAllocator,
    endpoint: []const u8,

    fn put(self: *Etcd, key: []const u8, value: []const u8) !PutResponse {
        return self.call(PutRequest, PutResponse, "Put", .{ .key = key, .value = value });
    }

    fn range(self: *Etcd, prefix: []const u8) !RangeResponse {
        var end_buffer: [256]u8 = undefined;
        const end = prefixEnd(&end_buffer, prefix);
        return self.call(RangeRequest, RangeResponse, "Range", .{
            .key = prefix,
            .range_end = end,
        });
    }

    fn deleteRange(self: *Etcd, prefix: []const u8) !DeleteRangeResponse {
        var end_buffer: [256]u8 = undefined;
        const end = prefixEnd(&end_buffer, prefix);
        return self.call(DeleteRangeRequest, DeleteRangeResponse, "DeleteRange", .{
            .key = prefix,
            .range_end = end,
        });
    }

    fn call(
        self: *Etcd,
        comptime Request: type,
        comptime Response: type,
        method: []const u8,
        request: Request,
    ) !Response {
        var request_body: [512]u8 = undefined;
        var request_writer: Io.Writer = .fixed(&request_body);
        try pb.encode(Request, request, .{}, &request_writer);

        var reply: [4096]u8 = undefined;
        const response = try self.client.unary(.{
            .authority = self.endpoint,
            .service = "etcdserverpb.KV",
            .method = method,
            .message = request_writer.buffered(),
            // A real client gives up: the call ends DEADLINE_EXCEEDED instead
            // of waiting on a server that stopped answering.
            .timeout = .fromSecondsComptime(2),
        }, &reply);

        if (response.status.code != .ok) {
            print("{s} failed: {s}\n", .{ method, @tagName(response.status.code) });
            return error.CallFailed;
        }

        // Copy the decoded slices into the session arena. `decodeFromSlice`
        // would borrow the reply buffer, which is a stack local here.
        _ = self.arena.reset(.retain_capacity);
        var reader: Io.Reader = .fixed(response.message);
        return pb.decodeLeaky(Response, self.arena.allocator(), &reader, .{});
    }
};
