//! The vocabulary the two kitchen-sink examples share: a hand-written protobuf
//! service, the scripted calls both clients make, and the checks both run.
//!
//! `kitchen_blocking.zig` serves it with threads and blocking handlers.
//! `kitchen_readiness.zig` serves it on one epoll thread. Everything here is
//! plain functions over `grpc` primitives, so neither program depends on the
//! other, and both are held to the same expectations.
//!
//! The service is `kitchen.Sink`:
//!
//!   Shout     unary          Note -> Note        upper-cases the text
//!   Tally     client stream  stream Note -> Note counts what it was sent
//!   Countdown server stream  Countdown -> stream Note, one tick per interval
//!   Chat      bidi           stream Note -> stream Note, "re: " each one

const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const print = std.debug.print;

const grpc = @import("grpc");
const pb = @import("protobuf");

pub const service = "kitchen.Sink";

pub const Note = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{ .text = pb.string(1), .seq = pb.field(2) };
    text: []const u8 = "",
    seq: u32 = 0,
};

pub const Countdown = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .from = pb.field(1),
        .interval_ms = pb.field(2),
    };
    from: u32 = 0,
    interval_ms: u32 = 0,
};

/// A server refuses a message longer than this, so a reply built from one
/// ("re: " plus the text) always fits a fixed buffer.
pub const message_limit = 128;
pub const flood_count = 3000;
const flood_text: *const [100]u8 = &@splat('x');
pub const reply_capacity = message_limit + 64;

/// An optional field section, as `Call` and `blocking.Captured` return it.
const Fields = @FieldType(grpc.blocking.Captured, "headers");

/// The type of a header field. `grpc` does not re-export `http.Header`, so this
/// names it through the one public field that carries it.
pub const Header = @typeInfo(@FieldType(grpc.server.Response, "metadata")).pointer.child;

/// Trailing metadata both servers attach: who served the call, and the
/// caller's `x-request-id` echoed back. `request_id` is copied into `arena`.
// ziglint-ignore: Z012 Header is public; the linter cannot see through @typeInfo
pub fn trailerFields(
    arena: Allocator,
    served_by: []const u8,
    request_id: ?[]const u8,
) Allocator.Error![]const Header {
    const fields = try arena.alloc(Header, 2);
    fields[0] = .{ .name = "x-served-by", .value = served_by };
    fields[1] = .{ .name = "x-echo-request-id", .value = try arena.dupe(u8, request_id orelse "") };
    return fields;
}

pub const Method = enum { Shout, Tally, Countdown, Chat };

pub fn route(path: []const u8) ?Method {
    const prefix = "/" ++ service ++ "/";
    if (!mem.startsWith(u8, path, prefix)) return null;
    return std.meta.stringToEnum(Method, path[prefix.len..]);
}

pub fn encode(buffer: []u8, value: anytype) ![]const u8 {
    var writer: Io.Writer = .fixed(buffer);
    try pb.encode(value, .{}, &writer);
    return writer.buffered();
}

/// Decode a message whose strings are copied into `arena`.
pub fn decode(comptime T: type, arena: Allocator, bytes: []const u8) !T {
    var reader: Io.Reader = .fixed(bytes);
    return pb.decodeLeaky(T, arena, &reader, .{});
}

/// Decode a message whose strings borrow `bytes`, with no allocation. It is
/// valid only while `bytes` is: for a server stream that is until the next
/// `Stream.next`, so use it before reading again.
pub fn decodeBorrowed(comptime T: type, bytes: []const u8) !T {
    var none: std.heap.FixedBufferAllocator = .init(&.{});
    return pb.decodeFromSliceLeaky(T, none.allocator(), bytes, .{});
}

// The business logic, as pure functions, so each server only decides when to
// call them.

/// The reply to a Shout, or null for an empty note.
pub fn shout(note: Note, out: []u8) ?Note {
    if (note.text.len == 0) return null;
    return .{ .text = std.ascii.upperString(out, note.text), .seq = note.seq };
}

pub fn chatReply(note: Note, out: []u8) !Note {
    return .{ .text = try mem.print(out, "re: {s}", .{note.text}), .seq = note.seq };
}

pub fn tallyReply(count: u32, bytes: usize, out: []u8) !Note {
    return .{
        .text = try mem.print(out, "tally: {d} notes, {d} bytes", .{ count, bytes }),
        .seq = count,
    };
}

pub fn tick(remaining: u32) Note {
    return .{ .text = "tick", .seq = remaining };
}

// What each client does. A script is an RPC plus the messages it sends.

pub const Rpc = enum {
    /// Unary: succeeds.
    shout,
    /// Unary: the server answers INVALID_ARGUMENT.
    shout_empty,
    /// Client streaming: five notes up, one tally back.
    tally,
    /// Server streaming: three ticks, ten milliseconds apart.
    countdown,
    /// Bidirectional: four notes up, four replies back, sent as they arrive.
    chat,
    /// Server streaming that outlasts a 150 ms timeout.
    too_slow,
    /// Server streaming that the client cancels after three ticks.
    abort,
    /// Bidirectional: three thousand notes each way, far past one flow-control
    /// window, so both sides must stop and wait for credit. Streaming servers only.
    flood,

    pub fn method(self: Rpc) Method {
        return switch (self) {
            .shout, .shout_empty => .Shout,
            .tally => .Tally,
            .countdown, .too_slow, .abort => .Countdown,
            .chat, .flood => .Chat,
        };
    }

    /// A call that sends exactly one message.
    fn first(index: usize, buffer: []u8, value: anytype) !?[]const u8 {
        return if (index == 0) try encode(buffer, value) else null;
    }

    /// The nth request message, encoded into `buffer`, or null past the last.
    pub fn request(self: Rpc, index: usize, buffer: []u8) !?[]const u8 {
        const words = [_][]const u8{ "hello", "brave", "new", "kitchen", "sink" };
        switch (self) {
            .shout => return first(index, buffer, Note{ .text = "hello kitchen", .seq = 1 }),
            .shout_empty => return first(index, buffer, Note{}),
            .tally => return if (index < 5) try encode(buffer, Note{
                .text = words[index],
                .seq = @intCast(index),
            }) else null,
            .chat => return if (index < 4) try encode(buffer, Note{
                .text = words[index],
                .seq = @intCast(index),
            }) else null,
            .countdown => return first(index, buffer, Countdown{ .from = 3, .interval_ms = 10 }),
            .too_slow => return first(index, buffer, Countdown{ .from = 50, .interval_ms = 20 }),
            .abort => return first(index, buffer, Countdown{ .from = 1000, .interval_ms = 10 }),
            .flood => return if (index < flood_count) try encode(buffer, Note{
                .text = flood_text,
                .seq = @intCast(index),
            }) else null,
        }
    }

    /// The request id for this call. `inline else` makes each `&.{...}` a
    /// distinct comptime constant: a slice of a runtime value would point at this
    /// function's stack frame and dangle in the returned request.
    fn metadata(self: Rpc) []const Header {
        return switch (self) {
            inline else => |rpc| &.{.{ .name = "x-request-id", .value = @tagName(rpc) }},
        };
    }

    /// The call the client opens. Every call is opened `streaming`, so
    /// `Script` writes each message with `Call.writeMessage`; the unary
    /// shape is a stream that sends one message and closes.
    pub fn open(self: Rpc, authority: []const u8) grpc.blocking.Request {
        return .{
            .authority = authority,
            .service = service,
            .method = @tagName(self.method()),
            .streaming = true,
            .metadata = self.metadata(),
            .timeout = if (self == .too_slow)
                .fromMillisecondsComptime(150)
            else
                .fromSecondsComptime(5),
        };
    }

    pub fn expectedCode(self: Rpc) grpc.status.Code {
        return switch (self) {
            .shout_empty => .invalid_argument,
            .too_slow => .deadline_exceeded,
            .abort => .cancelled,
            else => .ok,
        };
    }
};

/// Feeds one stream its request messages and decides when to hang up.
///
/// `step` writes at most one message. `Stream.send` refuses to lend another
/// message to the writer until its previous frame has drained, so this never
/// queues more than one message of memory. The shared `drive` loop below does
/// each transport turn; that is the client's flow control.
pub const Script = struct {
    rpc: Rpc,
    sent: usize = 0,
    send_done: bool = false,
    cancelled: bool = false,

    pub fn step(self: *Script, stream: *grpc.Stream, outcome: *const Outcome) !void {
        if (stream.result() != null) return;
        if (self.rpc == .abort and !self.cancelled and outcome.notes.items.len >= 3) {
            self.cancelled = true;
            try stream.cancel();
        }
        if (self.send_done) return;
        var buffer: [reply_capacity]u8 = undefined;
        if (try self.rpc.request(self.sent, &buffer)) |bytes| {
            if (try stream.send(bytes)) self.sent += 1;
        } else {
            // Even closing waits for the one frame this script lent the call.
            if (stream.pendingBytes() != 0) return;
            stream.closeSend();
            self.send_done = true;
        }
    }
};

/// Drive byte streams on one client or session, independent of its transport.
/// Blocking streams advance with `Client.step`; readiness streams advance with
/// `Session.wait`. Both stop after progress and return false when every call
/// has a status, so the scheduling choice stays in the stream construction.
pub fn drive(
    streams: []grpc.Stream,
    scripts: []Script,
    outcomes: []Outcome,
) !void {
    while (true) {
        for (streams, scripts, outcomes) |*stream, *script, *outcome| {
            try script.step(stream, outcome);
        }
        // Either operation advances every call on the shared connection.
        const progressed = streams.len != 0 and try streams[0].advance();
        for (streams, outcomes) |*stream, *outcome| try outcome.take(stream.raw());
        if (!progressed) return;
    }
}

/// What one call produced, copied out of the call so it outlives `deinit`.
pub const Outcome = struct {
    rpc: Rpc,
    arena: ArenaAllocator,
    notes: std.ArrayList(Note) = .empty,
    status: ?grpc.status.Status = null,
    served_by: []const u8 = "",
    echoed_id: []const u8 = "",
    /// A failure of the client machinery, as opposed to a gRPC status.
    failure: ?anyerror = null,

    pub fn init(gpa: Allocator, rpc: Rpc) Outcome {
        return .{ .rpc = rpc, .arena = .init(gpa) };
    }

    pub fn deinit(self: *Outcome) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Move everything the call has so far into this outcome. Message bytes
    /// borrow the call until the next read, so each one is decoded, and its
    /// strings copied, before the loop asks for the next.
    pub fn take(self: *Outcome, call: *grpc.Call) !void {
        const a = self.arena.allocator();
        while (try call.nextMessage()) |found| {
            try self.notes.append(a, try decode(Note, a, found.bytes));
        }
        if (self.status != null) return;
        const result = call.result() orelse return;
        self.status = .{ .code = result.code, .message = try a.dupe(u8, result.message) };
        // A call that fails before its first message answers in the head
        // alone, so the status fields may be in either section.
        self.served_by = try copyField(a, call, "x-served-by");
        self.echoed_id = try copyField(a, call, "x-echo-request-id");
    }

    fn copyField(a: Allocator, call: *const grpc.Call, name: []const u8) ![]const u8 {
        return copyFrom(a, call.responseHeaders(), call.responseTrailers(), name);
    }

    /// Copy a field out of a captured unary response, which has the same two
    /// sections as a call.
    pub fn takeCaptured(
        self: *Outcome,
        captured: *const grpc.blocking.Captured,
    ) !void {
        const a = self.arena.allocator();
        self.status = .{
            .code = captured.status.code,
            .message = try a.dupe(u8, captured.status.message),
        };
        self.served_by = try copyFrom(a, captured.headers, captured.trailers, "x-served-by");
        self.echoed_id = try copyFrom(a, captured.headers, captured.trailers, "x-echo-request-id");
    }

    fn copyFrom(
        a: Allocator,
        headers: Fields,
        trailers: Fields,
        name: []const u8,
    ) ![]const u8 {
        if (trailers) |fields| {
            if (fields.get(name)) |value| return a.dupe(u8, value);
        }
        if (headers) |fields| {
            if (fields.get(name)) |value| return a.dupe(u8, value);
        }
        return "";
    }

    /// Check the outcome against what the script promises.
    pub fn verify(self: *const Outcome) !void {
        if (self.failure) |err| return err;
        const result = self.status orelse return error.NoStatus;
        if (result.code != self.rpc.expectedCode()) return error.WrongStatus;
        const got = self.notes.items;
        switch (self.rpc) {
            .shout => {
                if (got.len != 1 or !mem.eql(u8, got[0].text, "HELLO KITCHEN")) {
                    return error.WrongReply;
                }
                if (!mem.eql(u8, self.echoed_id, "shout")) {
                    return error.MetadataLost;
                }
            },
            .shout_empty => if (got.len != 0) return error.WrongReply,
            .tally => {
                if (got.len != 1 or got[0].seq != 5) return error.WrongReply;
            },
            .countdown => {
                if (got.len != 3) return error.WrongReply;
                for (got, 0..) |note, i| if (note.seq != 3 - i) return error.WrongReply;
            },
            .chat => {
                if (got.len != 4) return error.WrongReply;
                for (got, 0..) |note, i| {
                    if (note.seq != i or !mem.startsWith(u8, note.text, "re: ")) {
                        return error.WrongReply;
                    }
                }
            },
            // A buffered server cannot send ticks before it finishes.
            .too_slow => if (got.len >= 50) return error.WrongReply,
            .abort => if (got.len < 3) return error.WrongReply,
            .flood => {
                if (got.len != flood_count) return error.WrongReply;
                for (got, 0..) |note, i| if (note.seq != i) return error.WrongReply;
            },
        }
    }

    fn clip(text: []const u8) []const u8 {
        return text[0..@min(text.len, 24)];
    }

    pub fn report(self: *const Outcome) void {
        const result = self.status orelse grpc.status.Status{
            .code = .unknown,
            .message = "no status",
        };
        print("  {s:<12} {s:<18} notes={d}", .{
            @tagName(self.rpc),
            @tagName(result.code),
            self.notes.items.len,
        });
        if (self.notes.items.len != 0) print(" first=\"{s}\" last=\"{s}\"", .{
            clip(self.notes.items[0].text),
            clip(self.notes.items[self.notes.items.len - 1].text),
        });
        if (self.served_by.len != 0) print(" served-by={s}", .{self.served_by});
        if (self.echoed_id.len != 0) print(" id={s}", .{self.echoed_id});
        if (result.message.len != 0) print(" message=\"{s}\"", .{result.message});
        print("\n", .{});
    }
};

pub fn verifyAll(outcomes: []const Outcome) !void {
    var failed = false;
    for (outcomes) |*outcome| {
        outcome.verify() catch |err| {
            print("  FAILED {s}: {s}\n", .{ @tagName(outcome.rpc), @errorName(err) });
            failed = true;
        };
    }
    if (failed) return error.ExpectationFailed;
}
