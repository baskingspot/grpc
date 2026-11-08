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
const http = @import("http");
const message = @import("message.zig");
const status = @import("status.zig");
const h2 = http.h2;

const Headers = http.Headers;

/// A finished call's answer. `message` borrows the call's arena and is valid
/// until `deinit`.
pub const Result = status.Status;

const Call = @This();

pub const Error = error{
    /// A `grpc-status` field appeared twice in one field section.
    DuplicateStatus,
} || std.mem.Allocator.Error;

pub const Options = struct {
    /// The largest message this call accepts, excluding the five prefix bytes.
    message_limit: u32 = message.default_limit,
};

allocator: std.mem.Allocator,
stream: h2.client.Stream,
/// Framed request messages, borrowed from the caller.
request: []const u8,
sent: usize = 0,
request_done: bool = false,
/// Response bytes waiting to be walked, from `consumed` onward.
body: std.ArrayList(u8) = .empty,
consumed: usize = 0,
arena: std.heap.ArenaAllocator,
resolved: ?Result = null,
/// Set when the body cannot be read further, so a driver that keeps calling
/// `nextMessage` stops instead of re-reading the same broken frame.
body_broken: bool = false,
http_status: u16 = 0,
limit: u32,

pub fn init(
    allocator: std.mem.Allocator,
    stream: h2.client.Stream,
    request: []const u8,
    options: Options,
) Call {
    return .{
        .allocator = allocator,
        .stream = stream,
        .request = request,
        .arena = .init(allocator),
        .limit = options.message_limit,
    };
}

pub fn deinit(self: *Call) void {
    self.body.deinit(self.allocator);
    self.arena.deinit();
    self.* = undefined;
}

/// The answer, or null while the call is still running.
pub fn result(self: *const Call) ?Result {
    return self.resolved;
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
    return self.request_done;
}

/// Write the request until h2 takes no more. False means backpressure: call
/// again after a `.writable` event or after the connection drains.
pub fn pump(self: *Call) !bool {
    if (self.request_done) return true;
    if (self.resolved != null) return true;

    if (self.request.len == 0) {
        try self.stream.finish();
        self.request_done = true;
        return true;
    }

    while (self.sent < self.request.len) {
        const accepted = try self.stream.write(self.request[self.sent..], true);
        if (accepted == 0) return false;
        self.sent += accepted;
    }
    self.request_done = true;
    return true;
}

/// The next complete response message, or null when more bytes are needed.
///
/// The returned bytes borrow the reassembly buffer and are valid until the next
/// `handle` or `nextMessage`. A compressed message resolves the call as
/// UNIMPLEMENTED, because this layer accepts `identity` and nothing else.
pub fn nextMessage(self: *Call) Error!?message.Message {
    if (self.body_broken) return null;
    if (self.consumed > 0 and self.consumed == self.body.items.len) {
        self.body.clearRetainingCapacity();
        self.consumed = 0;
    }

    var walker: message.Iterator = .{
        .bytes = self.body.items[self.consumed..],
        .limit = self.limit,
    };
    const walked = walker.next() catch |err| switch (err) {
        error.Truncated => return null,
        error.TooLarge => {
            self.body_broken = true;
            self.resolve(.resource_exhausted, "response message too large");
            return null;
        },
        // A flag byte that is not 0 or 1. The server broke the framing, and the
        // call reports that rather than handing the caller an error to route.
        else => {
            self.body_broken = true;
            self.resolve(.internal, "malformed message frame");
            return null;
        },
    };
    const found = walked orelse return null;

    self.consumed += walker.offset;

    if (found.compressed) {
        self.body_broken = true;
        self.resolve(.unimplemented, "compressed message with identity accepted");
        return null;
    }
    return found;
}

/// Take the next event from the connection. False means the connection has
/// nothing queued.
///
/// This takes the whole connection rather than one event because h2 does not
/// name its client event union outside the module. It also means one call owns
/// a connection: an event for another stream is dropped, so a multiplexed
/// driver needs the router that is not here yet.
pub fn step(self: *Call, conn: *h2.client.Connection) !bool {
    const event = (try conn.poll()) orelse return false;
    switch (event) {
        .head => |head| if (head.stream == self.stream.id) try self.onHead(head.head, head.end_stream),
        .data => |data| if (data.stream == self.stream.id) try self.onData(data.bytes, data.lease),
        .trailers => |trailers| if (trailers.stream == self.stream.id) try self.onTrailers(trailers.headers),
        .closed => |closed| if (closed.stream == self.stream.id) {
            switch (closed.reason) {
                .end_stream => self.onEndStream(),
                .reset => |code| self.resolve(resetToGrpc(code), ""),
                .goaway => self.resolve(.unavailable, "connection went away"),
                .local => self.resolve(.cancelled, ""),
            }
        },
        .writable, .goaway => {},
    }
    return true;
}

fn onHead(self: *Call, head: http.client.ResponseHead, end_stream: bool) Error!void {
    if (self.resolved != null) return;
    self.http_status = head.status.code();
    if (self.http_status != 200) {
        self.resolve(httpToGrpc(self.http_status), "");
        return;
    }
    // The trailers-only answer puts the status in the head's own field section.
    if (end_stream) try self.statusFrom(head.headers);
}

fn onData(self: *Call, bytes: []const u8, lease: h2.client.DataLease) Error!void {
    defer lease.release() catch {};

    if (self.resolved != null) return;

    const buffered = self.body.items.len - self.consumed;
    if (buffered + bytes.len > self.limit + message.prefix_len) {
        self.resolve(.resource_exhausted, "response message too large");
        return;
    }
    try self.body.appendSlice(self.allocator, bytes);
}

fn onTrailers(self: *Call, headers: Headers) Error!void {
    if (self.resolved != null) return;
    try self.statusFrom(headers);
}

/// A clean end of stream that never carried a status. A server that ends a
/// response without `grpc-status` broke the protocol, and the difference
/// between that and a half-written message is worth naming.
fn onEndStream(self: *Call) void {
    if (self.resolved != null) return;
    if (self.body.items.len != self.consumed) {
        self.resolve(.internal, "truncated message");
        return;
    }
    self.resolve(.internal, "missing grpc-status");
}

fn statusFrom(self: *Call, headers: Headers) Error!void {
    var count: usize = 0;
    var iterator = headers.values("grpc-status");
    var raw: ?[]const u8 = null;
    while (iterator.next()) |value| {
        count += 1;
        raw = value;
    }
    if (count > 1) return error.DuplicateStatus;

    const field = raw orelse {
        self.resolve(.internal, "missing grpc-status");
        return;
    };
    const code = status.parse(field) catch {
        self.resolve(.internal, "invalid grpc-status");
        return;
    };

    const text = headers.get("grpc-message") orelse {
        self.resolve(code, "");
        return;
    };
    try self.resolveDecoded(code, text);
}

fn resolveDecoded(self: *Call, code: status.Code, wire: []const u8) Error!void {
    const buffer = try self.arena.allocator().alloc(u8, wire.len);
    const decoded = status.decode(buffer, wire) catch {
        self.resolve(code, "invalid grpc-message");
        return;
    };
    self.resolved = .{ .code = code, .message = decoded };
}

fn resolve(self: *Call, code: status.Code, text: []const u8) void {
    self.resolved = .{ .code = code, .message = text };
}

/// The gRPC code for an HTTP status on a response that is not a gRPC answer.
fn httpToGrpc(code: u16) status.Code {
    return switch (code) {
        400 => .internal,
        401 => .unauthenticated,
        403 => .permission_denied,
        404 => .unimplemented,
        429 => .unavailable,
        502, 503, 504 => .unavailable,
        else => .unknown,
    };
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

const testing = std.testing;
const sim = @import("sim.zig");

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

/// Step the call until it finishes or the connection runs dry.
fn drive(t: *sim.Pair, call: *Call) !void {
    while (call.result() == null) {
        if (!try call.step(&t.client)) return error.NoEvent;
    }
}

fn greet(t: *sim.Pair, framed: []const u8) !Call {
    try t.handshake();
    return openCall(t, "/helloworld.Greeter/SayHello", framed);
}

test "a unary call frames one request and reads one message" {
    var pair = try sim.Pair.init(testing.allocator, 4);
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
    _ = try pair.server.submitData(testing.allocator, id, try message.write(&out, "world", false), false);
    try pair.server.submitTrailers(testing.allocator, id, &.{.{ .name = "grpc-status", .value = "0" }});
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.ok, call.result().?.code);
    try testing.expectEqual(@as(u16, 200), call.httpStatus());

    const reply = (try call.nextMessage()).?;
    try testing.expectEqualStrings("world", reply.bytes);
    try testing.expect((try call.nextMessage()) == null);
}

test "a trailers-only answer carries the status in the head" {
    var pair = try sim.Pair.init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    try pair.server.submitResponse(testing.allocator, id, .{
        .status = .ok,
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
    var pair = try sim.Pair.init(testing.allocator, 4);
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
    var pair = try sim.Pair.init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    try pair.server.submitResponse(testing.allocator, id, .{ .status = .ok }, true);
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.internal, call.result().?.code);
    try testing.expectEqualStrings("missing grpc-status", call.result().?.message);
}

test "one message split across DATA frames is reassembled" {
    var pair = try sim.Pair.init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    var out: [64]u8 = undefined;
    const framed = try message.write(&out, "split-me", false);
    try pair.server.submitResponse(testing.allocator, id, .{ .status = .ok }, false);
    _ = try pair.server.submitData(testing.allocator, id, framed[0..4], false);
    _ = try pair.server.submitData(testing.allocator, id, framed[4..], false);
    try pair.server.submitTrailers(testing.allocator, id, &.{.{ .name = "grpc-status", .value = "0" }});
    try pair.toClient();

    try drive(&pair, &call);
    try testing.expectEqual(status.Code.ok, call.result().?.code);
    try testing.expectEqualStrings("split-me", (try call.nextMessage()).?.bytes);
}

test "a reset maps to the code the specification gives it" {
    var pair = try sim.Pair.init(testing.allocator, 4);
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
    var pair = try sim.Pair.init(testing.allocator, 4);
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
    var pair = try sim.Pair.init(testing.allocator, 4);
    defer pair.deinit();

    var request: [64]u8 = undefined;
    var call = try greet(&pair, try message.write(&request, "hello", false));
    defer call.deinit();

    try testing.expect(try call.pump());
    try pair.toServer();
    const id = try pair.nextRequest();

    var out: [64]u8 = undefined;
    const framed = try message.write(&out, "gzipped", true);
    try pair.server.submitResponse(testing.allocator, id, .{ .status = .ok }, false);
    _ = try pair.server.submitData(testing.allocator, id, framed, false);
    try pair.server.submitTrailers(testing.allocator, id, &.{.{ .name = "grpc-status", .value = "0" }});
    try pair.toClient();

    while (call.result() == null) {
        if (!try call.step(&pair.client)) break;
        _ = try call.nextMessage();
    }
    try testing.expectEqual(status.Code.unimplemented, call.result().?.code);
}
