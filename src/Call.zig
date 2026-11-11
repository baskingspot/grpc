//! One gRPC call over one HTTP/2 stream.
//!
//! The call owns the request bytes going out, the reassembly of the message
//! frames coming back, and the status the stream ends with. It does not own the
//! connection: `h2.client.Stream` is the write handle, and the driver hands
//! every event to `handle`. Events for another stream are ignored, so a driver
//! may hand every event to every live call.
//!
//! A call is finished when `result` returns a status. The status is the answer,
//! whatever the HTTP layer did, so a reset, a GOAWAY, and a clean trailer all
//! end the same way.

const std = @import("std");
const mem = std.mem;
const time = @import("stdx").time;
const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const testing = std.testing;

const http = @import("http");
const h2 = http.h2;
const Header = http.Header;
const Headers = http.Headers;

const message = @import("message.zig");
const metadata = @import("head.zig");
const sim = @import("sim.zig");
const status = @import("status.zig");
/// A finished call's answer. `message` borrows the call's arena and is valid
/// until `deinit`.
pub const Result = status.Status;

const Call = @This();

pub const Error = error{
    /// `writeMessage` after `closeSend`.
    SendClosed,
} || Allocator.Error;

pub const Options = struct {
    /// Return a compressed message with its flag set instead of ending the call
    /// with INTERNAL. The layer above owns the codec, so this is how it takes
    /// delivery.
    allow_compressed: bool = false,
    /// The encodings named in `grpc-accept-encoding`, comma separated. A response
    /// that names one outside this list cannot be read here.
    accepted_encodings: []const u8 = "identity",
    /// Close the send side once `request` is on the wire. A streaming caller
    /// sets this false and closes with `closeSend`.
    send_closed: bool = true,
    /// The largest message this call accepts, excluding the five prefix bytes.
    ///
    /// Under compression this is the plaintext budget, not the length on the
    /// wire: with the flag set the prefix counts gzip bytes, and a small body
    /// can decode past any multiple of itself. A decoder target is sized from
    /// this number, and a decode that overflows it is RESOURCE_EXHAUSTED.
    message_limit: u32 = message.default_limit,
};

allocator: Allocator,
stream: h2.client.Stream,
/// Framed request bytes not yet written, from `sent` onward.
out: std.ArrayList(u8) = .empty,
sent: usize = 0,
/// The caller has finished the send side.
closing: bool = false,
send_closed: bool = false,
/// RST_STREAM is queued, so this call never queues another.
reset_sent: bool = false,
allow_compressed: bool = false,
/// Response bytes waiting to be walked, from `consumed` onward.
body: std.ArrayList(u8) = .empty,
consumed: usize = 0,
/// End of the latest frame checked against the per-message receive limit.
validated: usize = 0,
arena: ArenaAllocator,
resolved: ?Result = null,
/// Absolute monotonic deadline from the driver's clock, or `.none`. The driver
/// sets it once and expires the call when it passes.
deadline: time.OptionalTimestamp = .none,
/// The head's field section, borrowed from the connection. Valid until the
/// connection polls again, so copy it before the next call.
head_fields: ?Headers = null,
/// The trailing field section, borrowed the same way.
trailer_fields: ?Headers = null,
/// The decoded `grpc-status-details-bin` value: a serialized `google.rpc.Status`
/// in this call's arena, or empty when the peer sent none. The framing layer
/// does not parse it; the codec above owns that type.
details: []const u8 = &.{},
/// The encodings this call can read, from `Options.accepted_encodings`.
accepted_encodings: []const u8 = "identity",
/// Set when the body cannot be read further, so a driver that keeps calling
/// `nextMessage` stops instead of re-reading the same broken frame.
body_broken: bool = false,
http_status: u16 = 0,
limit: u32,

/// A call starts with `request` as its first bytes, and closes the send side
/// after them. A streaming caller appends with `writeMessage` and closes with
/// `closeSend` instead.
pub fn init(
    allocator: Allocator,
    stream: h2.client.Stream,
    request: []const u8,
    options: Options,
) Error!Call {
    var call: Call = .{
        .allocator = allocator,
        .stream = stream,
        .arena = .init(allocator),
        .limit = options.message_limit,
        .allow_compressed = options.allow_compressed,
        .accepted_encodings = options.accepted_encodings,
    };
    if (request.len != 0) {
        try call.out.appendSlice(allocator, request);
    }
    call.closing = options.send_closed;
    return call;
}

/// Free the call. A call that never got a status resets its stream first, so
/// the peer stops working for a caller that has gone. The reset is best effort:
/// nothing is left to report an error to.
pub fn deinit(self: *Call) void {
    if (self.resolved == null) self.bestEffortReset();
    self.out.deinit(self.allocator);
    self.body.deinit(self.allocator);
    self.arena.deinit();
    self.* = undefined;
}

/// The answer, or null while the call is still running.
pub fn result(self: *const Call) ?Result {
    return self.resolved;
}

/// The raw error details the peer attached, decoded from base64. Empty when
/// there are none, or when the value was not base64.
pub fn errorDetails(self: *const Call) []const u8 {
    return self.details;
}

/// The response header fields, or null before a head arrived. The view is a
/// copy in this call's arena, valid until `deinit`.
pub fn responseHeaders(self: *const Call) ?Headers {
    return self.head_fields;
}

/// The trailing fields, or null when the peer sent none. A copy in this call's
/// arena, like `responseHeaders`.
pub fn responseTrailers(self: *const Call) ?Headers {
    return self.trailer_fields;
}

/// Finish the call locally when the transport ends before the peer answers.
/// A truncation is not the peer's status, so it gets the code the specification
/// gives an interrupted call.
pub fn abandon(self: *Call, code: status.Code, text: []const u8) void {
    if (self.resolved != null) return;
    self.body_broken = true;
    self.resolve(code, text);
}

/// The HTTP status the response head carried, or 0 before a head arrived.
pub fn httpStatus(self: *const Call) u16 {
    return self.http_status;
}

pub fn streamId(self: *const Call) u31 {
    return self.stream.id;
}

/// True when every request byte is on the wire and END_STREAM was sent.
pub fn requestSent(self: *const Call) bool {
    return self.send_closed;
}

/// Framed request bytes in this call's queue that HTTP/2 has not accepted yet.
/// This excludes bytes already accepted by the connection, including any that
/// its own output buffers still hold. Callers can bound their lending to this
/// queue without inspecting its fields.
pub fn pendingBytes(self: *const Call) usize {
    return self.out.items.len - self.sent;
}

/// Frame one message and queue it behind whatever is still unsent.
pub fn writeMessage(self: *Call, msg: []const u8) Error!void {
    return self.writeMessageFlagged(msg, false);
}

/// The same, with the compressed flag the caller's codec decided.
pub fn writeMessageFlagged(self: *Call, bytes: []const u8, compressed: bool) Error!void {
    if (self.closing) return error.SendClosed;
    const frame = try self.out.addManyAsSlice(self.allocator, message.prefix_len + bytes.len);
    _ = message.write(frame, bytes, compressed) catch |err| switch (err) {
        error.NoSpace, error.TooLarge, error.Truncated, error.InvalidFlag => unreachable,
    };
}

/// Finish the request: the next pump sends END_STREAM behind the queued bytes.
pub fn closeSend(self: *Call) void {
    self.closing = true;
}

/// The largest message this call accepts, before decompression.
pub fn messageLimit(self: *const Call) u32 {
    return self.limit;
}

/// Write the request until h2 takes no more. False means backpressure: call
/// again after a `.writable` event or after the connection drains.
pub fn pump(self: *Call) !bool {
    if (self.send_closed) return true;
    if (self.resolved != null) return true;

    while (self.sent < self.out.items.len) {
        const pending = self.out.items[self.sent..];
        const accepted = self.stream.write(pending, false) catch |err| switch (err) {
            // The peer closed the stream under us. Nothing more can be sent;
            // the status that ends the call says why.
            error.StreamClosed, error.UnknownStream => {
                self.send_closed = true;
                return true;
            },
            else => return err,
        };
        if (accepted == 0) return false;
        self.sent += accepted;
    }

    if (self.sent != 0) {
        self.out.clearRetainingCapacity();
        self.sent = 0;
    }
    if (!self.closing) return false;

    self.stream.finish() catch |err| switch (err) {
        // The stream is already gone, as in the write path above: the end of
        // the call reports why, and a finish cannot succeed later.
        error.StreamClosed, error.UnknownStream => {},
        else => return err,
    };
    self.send_closed = true;
    return true;
}

/// Drop read bytes from the front of the reassembly buffer. A drained buffer is
/// cleared; otherwise the unread tail moves forward once the read prefix is at
/// least half the buffer, so the copy is at most the bytes already read and
/// the buffer stays bounded by the unread bytes instead of growing with the
/// stream. It runs only where the borrowed slice from the last `nextMessage`
/// may already be invalid.
fn compact(self: *Call) void {
    if (self.consumed == 0) return;
    const len = self.body.items.len;
    if (self.consumed == len) {
        self.body.clearRetainingCapacity();
        self.consumed = 0;
        self.validated = 0;
    } else if (self.consumed >= len - self.consumed) {
        const tail = len - self.consumed;
        @memmove(self.body.items[0..tail], self.body.items[self.consumed..]);
        self.body.shrinkRetainingCapacity(tail);
        self.validated -|= self.consumed;
        self.consumed = 0;
    }
}

/// The next complete response message, or null when more bytes are needed.
///
/// The returned bytes borrow the reassembly buffer and are valid until the next
/// `handle` or `nextMessage`. A compressed message resolves the call as
/// UNIMPLEMENTED, because this layer accepts `identity` and nothing else.
pub fn nextMessage(self: *Call) Error!?message.Message {
    if (self.body_broken) return null;
    self.compact();

    var walker: message.Iterator = .{
        .bytes = self.body.items[self.consumed..],
        .limit = self.limit,
    };
    const walked = walker.next() catch |err| switch (err) {
        // Bytes that do not make a whole message are only a problem once the
        // stream has ended: until then they are a message still arriving.
        error.Truncated => {
            if (self.resolved != null) {
                self.body_broken = true;
                self.override(.internal, "truncated message");
            }
            return null;
        },
        error.TooLarge => {
            self.body_broken = true;
            self.override(.resource_exhausted, "response message too large");
            return null;
        },
        // A flag byte that is not 0 or 1. The server broke the framing, and the
        // call reports that rather than handing the caller an error to route.
        else => {
            self.body_broken = true;
            self.override(.internal, "malformed message frame");
            return null;
        },
    };
    const found = walked orelse return null;

    self.consumed += walker.offset;

    if (found.compressed and !self.allow_compressed) {
        self.body_broken = true;
        self.override(.internal, "message is compressed and this call accepted identity");
        return null;
    }
    return found;
}

/// Abort the call. The peer sees `RST_STREAM` with CANCEL, and the call ends
/// CANCELLED. Calling this on a finished call does nothing, and a second call
/// queues no second reset. The send side is finished with it: the reset is
/// already queued, so a later `closeSend` has nothing to send. A failed reset
/// returns its error and leaves the call as it was, so it may be tried again.
pub fn cancel(self: *Call) !void {
    if (self.resolved != null) return;
    try self.reset();
    self.resolve(.cancelled, "");
}

/// Finish the call locally because its deadline passed, and tell the peer.
/// The first status wins: a call that already ended keeps its answer and
/// queues nothing.
pub fn expire(self: *Call) void {
    if (self.resolved != null) return;
    self.abandon(.deadline_exceeded, "");
    self.bestEffortReset();
}

/// `reset` for a caller with nowhere to report a failure. A failed reset
/// leaves the call as it was.
fn bestEffortReset(self: *Call) void {
    self.reset() catch return;
}

/// Queue RST_STREAM(CANCEL) once and close the send side. The stream is
/// already gone when the peer finished it first, and then there is nothing to
/// send.
fn reset(self: *Call) !void {
    if (self.reset_sent) return;
    try self.stream.cancel(.cancel);
    self.reset_sent = true;
    self.closing = true;
    self.send_closed = true;
    self.out.clearRetainingCapacity();
    self.sent = 0;
}

/// Take one event. An event for another stream is ignored, so a driver that
/// owns several calls may hand every event to every call.
pub fn handle(self: *Call, event: h2.client.Event) Error!void {
    switch (event) {
        .head => |head| {
            if (head.stream != self.stream.id) return;
            try self.onHead(head.head, head.end_stream);
        },
        .data => |data| {
            if (data.stream != self.stream.id) return;
            try self.onData(data.bytes, data.lease);
        },
        .trailers => |trailers| {
            if (trailers.stream != self.stream.id) return;
            self.trailer_fields = try self.copyFields(trailers.headers);
            try self.statusFrom(trailers.headers);
        },
        .closed => |closed| {
            if (closed.stream != self.stream.id) return;
            try self.onClosed(closed.reason);
        },
        .writable, .goaway => {},
    }
}

fn onHead(self: *Call, response: http.client.ResponseHead, end_stream: bool) Error!void {
    if (self.resolved != null) return;
    self.head_fields = try self.copyFields(response.headers);
    self.http_status = response.status.code();
    if (self.http_status != 200) {
        self.resolve(.fromHttp(self.http_status), "");
        return;
    }

    // A gRPC answer carries the gRPC media type, and a response that names no
    // encoding means identity. Anything else is a server this client cannot
    // read, and the specification says to report that as unknown.
    const content_type = response.headers.get("content-type") orelse "";
    if (!validContentType(content_type)) {
        self.resolve(.unknown, "response content-type is not application/grpc");
        return;
    }
    if (response.headers.get("grpc-encoding")) |encoding| {
        if (!self.accepts(encoding)) {
            self.resolve(.internal, "response used an encoding this client did not accept");
            return;
        }
    }
    // The trailers-only answer puts the status in the head's own field section.
    if (end_stream) try self.statusFrom(response.headers);
}

fn onData(self: *Call, bytes: []const u8, lease: h2.client.DataLease) Error!void {
    defer lease.release() catch {};

    if (self.resolved != null) return;

    try self.body.appendSlice(self.allocator, bytes);
    self.validateFrames();
}

/// An incoming DATA chunk can carry several messages. Check the declared length
/// of each one instead of treating the total unread bytes as one message.
fn validateFrames(self: *Call) void {
    while (true) {
        const remaining = self.body.items.len - self.validated;
        if (remaining < message.prefix_len) return;
        const header = message.parseHeader(
            self.body.items[self.validated..],
        ) catch |err| switch (err) {
            error.TooLarge => unreachable,
            error.Truncated => return,
            else => {
                self.override(.internal, "malformed message frame");
                return;
            },
        };
        if (header.length > self.limit) {
            self.resolve(.resource_exhausted, "response message too large");
            return;
        }
        const next = @as(u64, self.validated) + message.prefix_len + header.length;
        if (next > self.body.items.len) return;
        self.validated = @intCast(next);
    }
}

fn onClosed(self: *Call, reason: h2.client.Close) Error!void {
    if (self.resolved != null) return;
    switch (reason) {
        .end_stream => {
            // A clean end that never carried a status. The specification says a
            // client reports a response it cannot interpret as unknown.
            self.resolve(.unknown, "missing grpc-status");
        },
        .reset => |code| self.resolve(resetToGrpc(code), ""),
        .goaway => self.resolve(.unavailable, "connection went away"),
        .local => self.resolve(.cancelled, ""),
    }
}

/// The connection owns the field section it hands over and reclaims it when the
/// stream retires, which can happen before the caller reads anything. So the
/// call keeps its own copy in the arena the status text already lives in.
fn copyFields(self: *Call, headers: Headers) Error!Headers {
    const items = try self.arena.allocator().alloc(Header, headers.items.len);
    for (headers.items, 0..) |field, index| {
        items[index] = .{
            .name = try self.arena.allocator().dupe(u8, field.name),
            .value = try self.arena.allocator().dupe(u8, field.value),
        };
    }
    return .{ .items = items };
}

/// The `grpc-status-details-bin` value is base64 of a serialized
/// `google.rpc.Status`. A value that is not base64 is dropped rather than
/// failing the call: the code already told the caller what happened.
fn decodeDetails(self: *Call, encoded: []const u8) Error![]const u8 {
    const len = metadata.binaryDecodedLen(encoded) catch return &.{};
    if (len == 0) return &.{};
    const buffer = try self.arena.allocator().alloc(u8, len);
    return metadata.decodeBinary(buffer, encoded) catch &.{};
}

fn statusFrom(self: *Call, headers: Headers) Error!void {
    var count: usize = 0;
    var iterator = headers.values("grpc-status");
    var raw: ?[]const u8 = null;
    while (iterator.next()) |value| {
        count += 1;
        raw = value;
    }
    if (count > 1) {
        self.resolve(.internal, "duplicate grpc-status");
        return;
    }

    const field = raw orelse {
        self.resolve(.unknown, "missing grpc-status");
        return;
    };
    const code = status.parse(field) catch {
        self.resolve(.unknown, "invalid grpc-status");
        return;
    };

    if (headers.get("grpc-status-details-bin")) |encoded| {
        self.details = try self.decodeDetails(encoded);
    }

    const text = headers.get("grpc-message") orelse {
        self.resolve(code, "");
        return;
    };
    try self.resolveDecoded(code, text);
}

fn resolveDecoded(self: *Call, code: status.Code, wire: []const u8) Error!void {
    if (self.resolved != null) return;
    const buffer = try self.arena.allocator().alloc(u8, wire.len);
    const decoded = status.decode(buffer, wire) catch {
        self.resolve(code, "invalid grpc-message");
        return;
    };
    self.resolved = .{ .code = code, .message = decoded };
}

/// The first answer wins. A head that rejects the response, or a trailer that
/// carries the status, must not be overwritten by a later event about the same
/// call.
fn resolve(self: *Call, code: status.Code, text: []const u8) void {
    if (self.resolved != null) return;
    self.resolved = .{ .code = code, .message = text };
}

/// The same, for a body that turns out to be unreadable after the trailers
/// already said the call succeeded.
fn override(self: *Call, code: status.Code, text: []const u8) void {
    self.resolved = .{ .code = code, .message = text };
}

/// Whether the response encoding is one this call can read.
fn accepts(self: *const Call, encoding: []const u8) bool {
    var rest = self.accepted_encodings;
    while (rest.len != 0) {
        const comma = mem.findScalar(u8, rest, ',') orelse rest.len;
        const item = mem.trim(u8, rest[0..comma], " \t");
        if (mem.eql(u8, item, encoding)) return true;
        rest = rest[@min(comma + 1, rest.len)..];
    }
    return false;
}

/// `application/grpc` or `application/grpc+<subformat>`, and nothing else.
fn validContentType(value: []const u8) bool {
    const prefix = "application/grpc";
    if (!mem.startsWith(u8, value, prefix)) return false;
    if (value.len == prefix.len) return true;
    if (value[prefix.len] == ';') return true;
    return mem.eql(u8, value, "application/grpc+proto");
}

/// The gRPC code for a peer `RST_STREAM`, from the mapping in the gRPC
/// specification.
fn resetToGrpc(raw: u32) status.Code {
    const code: h2.frame.ErrorCode = @fromBackingInt(@intCast(raw));
    return switch (code) {
        .refused_stream => .unavailable,
        .cancel => .cancelled,
        .enhance_your_calm => .resource_exhausted,
        .inadequate_security => .permission_denied,
        else => .internal,
    };
}

test {
    _ = @import("sim.zig");
}

fn openCall(t: *sim.Pair, target: []const u8, framed: []const u8) !Call {
    const stream = try t.client.open(.{
        .method_token = "POST",
        .scheme = .http,
        .authority = "example.com",
        .target = target,
        .headers = &.{
            .{ .name = "content-type", .value = "application/grpc+proto" },
            .{ .name = "te", .value = "trailers" },
        },
    }, false);
    return Call.init(testing.allocator, stream, framed, .{});
}

/// Poll the connection into the call until it finishes or runs dry.
fn drive(t: *sim.Pair, call: *Call) !void {
    while (call.result() == null) {
        var handled = false;
        while (try t.client.poll()) |event| {
            try call.handle(event);
            handled = true;
        }
        if (!handled) return error.NoEvent;
    }
}

/// Send the `grpc-status: 0` trailer section the way a server ends a call.
fn okTrailers(t: *sim.Pair, id: u31) !void {
    const fields = [_]Header{.{ .name = "grpc-status", .value = "0" }};
    try t.server.submitTrailers(testing.allocator, id, &fields);
}

fn greet(t: *sim.Pair, framed: []const u8) !Call {
    try t.handshake();
    return openCall(t, "/helloworld.Greeter/SayHello", framed);
}

test "a unary call frames one request and reads one message" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    try pair.server.submitResponse(testing.allocator, id, .{
        .status = .ok,
        .content_type = "application/grpc",
    }, false);
    var out: [64]u8 = undefined;
    const world = try message.write(&out, "world", false);
    _ = try pair.server.submitData(testing.allocator, id, world, false);
    try okTrailers(&pair, id);
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.ok, call.result().?.code);
    try testing.expectEqual(@as(u16, 200), call.httpStatus());

    const reply = (try call.nextMessage()).?;
    try testing.expectEqualStrings("world", reply.bytes);
    try testing.expect((try call.nextMessage()) == null);
}

test "pendingBytes counts framed request bytes until h2 accepts them" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    const framed = try message.write(&request, "pending", false);
    var call = try greet(&pair, framed);
    defer call.deinit();

    try testing.expectEqual(framed.len, call.pendingBytes());
    try testing.expect(try call.pump());
    try testing.expectEqual(@as(usize, 0), call.pendingBytes());
    try testing.expect(call.requestSent());
}

test "a trailers-only answer carries the status in the head" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    try pair.server.submitResponse(testing.allocator, id, .{
        .status = .ok,
        .content_type = "application/grpc",
        .headers = &.{
            .{ .name = "grpc-status", .value = "5" },
            .{ .name = "grpc-message", .value = "no%20such%20thing" },
        },
    }, true);
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.not_found, call.result().?.code);
    try testing.expectEqualStrings("no such thing", call.result().?.message);
}

test "an HTTP status that is not a gRPC answer maps to a code" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    try pair.server.submitResponse(testing.allocator, id, .{ .status = .not_found }, true);
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.unimplemented, call.result().?.code);
}

test "a clean response with no grpc-status is a protocol failure" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    try pair.server.submitResponse(testing.allocator, id, .{
        .status = .ok,
        .content_type = "application/grpc",
    }, true);
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.unknown, call.result().?.code);
    try testing.expectEqualStrings("missing grpc-status", call.result().?.message);
}

test "each message has its own limit when DATA carries more than one" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "", false));
    defer call.deinit();
    call.limit = 8;
    _ = try call.pump();
    try pair.toServer();
    const id = try pair.nextRequest();

    try pair.server.submitResponse(testing.allocator, id, .{
        .status = .ok,
        .content_type = "application/grpc",
    }, false);
    var frames: [16]u8 = undefined;
    const first = try message.write(frames[0..8], "one", false);
    const second = try message.write(frames[8..], "two", false);
    try testing.expectEqual(@as(usize, 8), first.len);
    try testing.expectEqual(@as(usize, 8), second.len);
    _ = try pair.server.submitData(testing.allocator, id, &frames, false);
    try okTrailers(&pair, id);
    try pair.toClient();
    try drive(&pair, &call);

    try testing.expectEqual(status.Code.ok, call.result().?.code);
    try testing.expectEqualStrings("one", (try call.nextMessage()).?.bytes);
    try testing.expectEqualStrings("two", (try call.nextMessage()).?.bytes);
}

test "reading every message as it arrives keeps the reassembly buffer bounded" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "", false));
    defer call.deinit();
    _ = try call.pump();
    try pair.toServer();
    const id = try pair.nextRequest();
    try pair.server.submitResponse(testing.allocator, id, .{
        .status = .ok,
        .content_type = "application/grpc",
    }, false);
    try pair.toClient();
    while (try pair.client.poll()) |event| try call.handle(event);

    // Each 1005-byte message straddles the 4000-byte chunk boundaries, so a
    // partial message is always waiting when the whole ones are read.
    var payload: [1000]u8 = undefined;
    for (&payload, 0..) |*byte, i| byte.* = @intCast(i % 251);
    var stream: [1005]u8 = undefined;
    const framed = try message.write(&stream, &payload, false);
    var chunk: [4000]u8 = undefined;
    var fill: usize = 0;
    var cap_after_warmup: usize = 0;
    var cap_max: usize = 0;
    var received: usize = 0;
    var sent: usize = 0;
    for (0..200) |round| {
        // The client's window updates reach the server before the next chunk.
        try pair.toServer();
        for (&chunk, 0..) |*byte, i| byte.* = framed[(fill + i) % framed.len];
        const accepted = try pair.server.submitData(testing.allocator, id, &chunk, false);
        fill = (fill + accepted) % framed.len;
        try pair.toClient();
        while (try pair.client.poll()) |event| try call.handle(event);
        while (try call.nextMessage()) |got| {
            try testing.expectEqualSlices(u8, &payload, got.bytes);
            received += 1;
        }
        sent += accepted;
        // The partial message and prefix stay exactly as they arrived.
        try testing.expectEqual(sent / framed.len, received);
        try testing.expectEqual(sent % framed.len, call.body.items.len - call.consumed);
        cap_max = @max(cap_max, call.body.capacity);
        if (round == 9) cap_after_warmup = call.body.capacity;
    }
    // Allocator growth rounds capacity up, so compare with the warmed-up
    // plateau rather than an invented byte count. Without compaction the
    // capacity keeps growing with the stream.
    try testing.expectEqual(cap_after_warmup, cap_max);
}

test "one message split across DATA frames is reassembled" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    var out: [64]u8 = undefined;
    const framed = try message.write(&out, "split-me", false);
    try pair.server.submitResponse(testing.allocator, id, .{
        .status = .ok,
        .content_type = "application/grpc",
    }, false);
    _ = try pair.server.submitData(testing.allocator, id, framed[0..4], false);
    _ = try pair.server.submitData(testing.allocator, id, framed[4..], false);
    try okTrailers(&pair, id);
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.ok, call.result().?.code);
    try testing.expectEqualStrings("split-me", (try call.nextMessage()).?.bytes);
}

test "a reset maps to the code the specification gives it" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    try pair.server.submitReset(testing.allocator, id, .cancel);
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.cancelled, call.result().?.code);
}

test "a refused stream is retriable and says so" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    try pair.server.submitReset(testing.allocator, id, .refused_stream);
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.unavailable, call.result().?.code);
}

test "a compressed message is refused rather than handed on" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    var out: [64]u8 = undefined;
    const framed = try message.write(&out, "gzipped", true);
    try pair.server.submitResponse(testing.allocator, id, .{
        .status = .ok,
        .content_type = "application/grpc",
    }, false);
    _ = try pair.server.submitData(testing.allocator, id, framed, false);
    try okTrailers(&pair, id);
    try pair.toClient();

    while (call.result() == null) {
        const event = (try pair.client.poll()) orelse break;
        try call.handle(event);
        _ = try call.nextMessage();
    }
    try testing.expectEqual(status.Code.internal, call.result().?.code);
}

test "a head that rejects the response is not overwritten by trailers" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    // The wrong content type ends the call at the head. The trailers that follow
    // say the call succeeded, and they must not reopen it.
    try pair.server.submitResponse(testing.allocator, id, .{
        .status = .ok,
        .content_type = "text/plain",
    }, false);
    try okTrailers(&pair, id);
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.unknown, call.result().?.code);
}

test "a duplicated grpc-status resolves only its own call as internal" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    const framed = try message.write(&request, "hello", false);
    var call = try greet(&pair, framed);
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    try pair.server.submitResponse(testing.allocator, id, .{
        .status = .ok,
        .content_type = "application/grpc",
    }, false);
    const fields = [_]Header{
        .{ .name = "grpc-status", .value = "0" },
        .{ .name = "grpc-status", .value = "5" },
    };
    try pair.server.submitTrailers(testing.allocator, id, &fields);
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.internal, call.result().?.code);
    try testing.expectEqualStrings("duplicate grpc-status", call.result().?.message);
}

test "a duplicated grpc-status does not replace an earlier status" {
    var pair: sim.Pair = try .init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();
    call.abandon(.unavailable, "first");

    const fields = [_]Header{
        .{ .name = "grpc-status", .value = "0" },
        .{ .name = "grpc-status", .value = "5" },
    };
    try call.statusFrom(.{ .items = &fields });
    try testing.expectEqual(status.Code.unavailable, call.result().?.code);
    try testing.expectEqualStrings("first", call.result().?.message);
}
