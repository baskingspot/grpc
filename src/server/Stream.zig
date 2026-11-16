//! A gRPC stream on an HTTP/2 exchange, with no socket or proto codec.
//!
//! `next` reassembles one message across body chunks and decodes it when the
//! request head named a compression codec from the protocol's registry.
//! `write` accepts framed bytes and returns the accepted prefix. The caller
//! retains the unsent tail. `send` frames one message into stream-owned
//! storage and `flush` drives it through `write`. `begin` and `finish` write
//! the response head and the gRPC status trailers.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Base64Encoder = std.base64.standard_no_pad.Encoder;

const http = @import("http");
const Exchange = http.h2.engine.Exchange;

const message = @import("../message.zig");
const compression = @import("../compression.zig");
const status = @import("../status.zig");
const timeout = @import("../timeout.zig");
const time = @import("stdx").time;

const mem = std.mem;

const Stream = @This();

/// The engine replaces this pointer on each invocation. Do not retain it.
exchange: *Exchange,
io: Io,
/// Application state that survives a pending read or write. Allocate it from
/// `exchange.arena` for the call lifetime. The adapter owns `Exchange.context`.
context: ?*anyopaque = null,
/// The request-head deadline on the awake clock, or `.none`.
deadline: time.OptionalTimestamp = .none,
/// Maximum framed message bytes. Zero permits empty messages only. For a
/// compressed frame this bounds the wire bytes; the plaintext a decode may
/// expand to is bounded by the same number, which is the message the peer
/// meant to send.
message_limit: u32 = message.default_limit,
/// The codec the request head named, or null for identity. Resolved once
/// at init from the `grpc-encoding` field.
codec: ?*const compression.Codec = null,
/// The head named an encoding outside the registry. `next` fails with
/// `error.UnsupportedEncoding`; the adapter answers UNIMPLEMENTED with the
/// accept list, exactly as the buffered path does.
encoding_unknown: bool = false,
/// Maximum message body bytes `send` accepts, excluding the five-byte prefix.
send_limit: u32 = message.default_limit,
input: []const u8 = &.{},
prefix: [message.prefix_len]u8 = undefined,
prefix_used: u8 = 0,
header: ?message.Header = null,
read_error: ?anyerror = null,
body: std.ArrayList(u8) = .empty,
ready: bool = false,
deadline_notified: bool = false,
finished: bool = false,
/// One framed message accepted by `send`: `queued[queued_sent..queued_len]` is
/// unsent. The storage comes from `exchange.arena`, grows only when a larger
/// message arrives, and is reused by every later message.
queued: []u8 = &.{},
queued_len: usize = 0,
queued_sent: usize = 0,
/// The trailers `finish` committed to. Retries send these, not new arguments.
final: ?[]const http.Header = null,

pub fn init(io: Io, exchange: *Exchange) time.Timestamp.Error!Stream {
    var self: Stream = .{ .io = io, .exchange = exchange };
    try self.setup(exchange);
    return self;
}

/// The same, with the message limit the server was configured for. The
/// streaming adapter builds its streams this way so a `serveStreaming`
/// options limit reaches the framing and decode checks.
pub fn initLimited(io: Io, exchange: *Exchange, message_limit: u32) time.Timestamp.Error!Stream {
    var self: Stream = .{ .io = io, .exchange = exchange, .message_limit = message_limit };
    try self.setup(exchange);
    return self;
}

fn setup(self: *Stream, exchange: *Exchange) time.Timestamp.Error!void {
    self.deadline = try timeout.deadline(
        exchange.request.head.headers.get("grpc-timeout"),
        try self.now(),
    );
    // The request names its encoding once, in the head. `identity` or no
    // field at all is the absence of a codec. A name in the registry decodes
    // every compressed message in `next`. A name outside it is the spec's
    // UNIMPLEMENTED, which surfaces as `error.UnsupportedEncoding` on the
    // first read and repeats, like any terminal error.
    const encoding = exchange.request.head.headers.get("grpc-encoding") orelse "identity";
    if (mem.eql(u8, encoding, "identity")) return;
    if (compression.byName(encoding)) |codec| {
        self.codec = codec;
    } else {
        self.encoding_unknown = true;
    }
}

/// A message borrows this stream until the next `next`. A decoded message
/// borrows the exchange arena the same way. Pending reads retain incomplete
/// prefixes and bodies. A clean end returns null. A framing or compression
/// error is terminal and repeats on later reads.
pub fn next(self: *Stream) !?message.Message {
    if (self.read_error) |err| return err;
    if (self.ready) {
        self.body.clearRetainingCapacity();
        self.prefix_used = 0;
        self.header = null;
        self.ready = false;
    }
    while (true) {
        if (self.header) |header| {
            if (self.body.items.len == header.length) {
                self.ready = true;
                const found = try self.deliver(header);
                return found;
            }
        }
        if (self.input.len == 0) {
            const source = self.exchange.request_body orelse return error.NotStreaming;
            const chunk = source.next() catch |err| switch (err) {
                error.EndOfStream => {
                    if (self.prefix_used != 0) {
                        self.read_error = error.Truncated;
                        return error.Truncated;
                    }
                    return null;
                },
                else => return err,
            };
            self.input = chunk.data;
        }
        if (self.prefix_used < message.prefix_len) {
            const count = @min(self.input.len, message.prefix_len - self.prefix_used);
            @memcpy(self.prefix[self.prefix_used..][0..count], self.input[0..count]);
            self.prefix_used += @intCast(count);
            self.input = self.input[count..];
            if (self.prefix_used != message.prefix_len) continue;
            const header = message.parseHeader(&self.prefix) catch |err| {
                self.read_error = err;
                return err;
            };
            if (header.length > self.message_limit) {
                self.read_error = error.TooLarge;
                return error.TooLarge;
            }
            self.header = header;
            continue;
        }
        const count = @min(self.input.len, self.header.?.length - self.body.items.len);
        try self.body.appendSlice(self.exchange.arena, self.input[0..count]);
        self.input = self.input[count..];
    }
}

/// One assembled message, decoded when the head named a codec and the frame
/// says it is compressed. An uncompressed frame under a named codec passes
/// through untouched. The wire length is bounded by the framing check in
/// `next`; the plaintext a decode expands to is bounded by `message_limit`,
/// which is what the peer meant to send.
fn deliver(self: *Stream, header: message.Header) !message.Message {
    if (self.encoding_unknown) {
        self.read_error = error.UnsupportedEncoding;
        return error.UnsupportedEncoding;
    }
    if (!header.compressed) return .{ .compressed = false, .bytes = self.body.items };
    const active = self.codec orelse {
        self.read_error = error.CompressedUnderIdentity;
        return error.CompressedUnderIdentity;
    };
    const plain = try self.exchange.arena.alloc(u8, self.message_limit);
    const decoded = active.decode(self.body.items, plain) catch |err| {
        self.read_error = switch (err) {
            error.BufferTooSmall => error.TooLarge,
            error.DecodeFailed => error.DecodeFailed,
        };
        return self.read_error.?;
    };
    return .{ .compressed = false, .bytes = plain[0..decoded] };
}

pub fn begin(self: *Stream, headers: []const http.Header) !void {
    try self.exchange.beginResponse(.{
        .status = .ok,
        .content_type = "application/grpc",
        .headers = headers,
    });
}

/// Write already-framed gRPC bytes. Zero means backpressure, not completion.
/// This is the raw byte sink: the caller keeps the unaccepted tail and no
/// message state is involved. Do not interleave it with a message that
/// `unsent` reports as incomplete, or the frames corrupt each other.
pub fn write(self: *Stream, bytes: []const u8) !usize {
    return self.exchange.write(bytes);
}

/// Frame `bytes` and queue the message, then try to write it. The framed copy
/// lives in stream-owned storage, so `bytes` may change once this returns.
///
/// A previous message that is still unsent is flushed first. If it cannot
/// finish, this returns `error.Pending` and `bytes` is not accepted: return
/// the error from the handler and call `send` again on the next turn. After a
/// successful return a rest that the transport has not taken stays queued; see
/// `unsent` and `flush`. `error.TooLarge` rejects a body above `send_limit`.
/// Any other error is a transport failure and ends the stream.
pub fn send(self: *Stream, bytes: []const u8, compressed: bool) !void {
    if (self.finished) return error.Finished;
    if (bytes.len > self.send_limit) return error.TooLarge;
    try self.flush();
    const total = message.prefix_len + bytes.len;
    if (self.queued.len < total) {
        const limit = @as(usize, self.send_limit) + message.prefix_len;
        const grown = @min(@max(total, self.queued.len * 2), limit);
        self.queued = try self.exchange.arena.alloc(u8, grown);
    }
    message.writeHeader(self.queued[0..message.prefix_len], compressed, @intCast(bytes.len));
    @memcpy(self.queued[message.prefix_len..total], bytes);
    self.queued_len = total;
    self.queued_sent = 0;
    self.flush() catch |err| switch (err) {
        error.Pending => {},
        else => return err,
    };
}

/// Write the queued message through `write` until it is complete. Zero accepted
/// bytes returns `error.Pending`; the cursor keeps every accepted byte, and the
/// next call continues from it. With nothing queued this succeeds.
pub fn flush(self: *Stream) !void {
    while (self.queued_sent < self.queued_len) {
        const accepted = try self.write(self.queued[self.queued_sent..self.queued_len]);
        if (accepted == 0) return error.Pending;
        self.queued_sent += accepted;
    }
    self.queued_len = 0;
    self.queued_sent = 0;
}

/// Bytes of the queued message the transport has not accepted.
pub fn unsent(self: *const Stream) usize {
    return self.queued_len - self.queued_sent;
}

/// Send the final status, once. A call after completion returns without effect.
/// Retry on WouldBlock, which also reports a queued message that is still
/// unsent: the trailers follow only after its last byte. The first call that
/// reaches the trailers fixes them, and a retry sends those exactly.
///
/// An expired RPC deadline ends a message that cannot finish. If no byte of it
/// was sent, it is dropped and the status becomes DEADLINE_EXCEEDED unless
/// `result` already is an error. If part was sent, the stream is reset with
/// CANCEL and no trailers follow a truncated message. Before expiry the
/// message is waited for, so a clean finish never drops accepted output.
/// A blocked, expired final status waits for output credit instead of another
/// immediate deadline wake.
pub fn finish(
    self: *Stream,
    result: status.Status,
    metadata: []const http.Header,
    details: []const u8,
) !void {
    if (self.finished) return;
    if (try self.expired()) self.deadline_notified = true;
    if (self.final == null) {
        var outcome = result;
        var kept_details = details;
        if (self.unsent() != 0) {
            self.flush() catch |err| switch (err) {
                error.Pending => {
                    if (!try self.expired()) return error.WouldBlock;
                    if (self.queued_sent != 0) {
                        try self.exchange.resetStream(.cancel);
                        self.queued_len = 0;
                        self.queued_sent = 0;
                        self.exchange.setDeadline(.none);
                        self.finished = true;
                        return;
                    }
                    self.queued_len = 0;
                    if (outcome.code == .ok) {
                        outcome = .{ .code = .deadline_exceeded, .message = "deadline exceeded" };
                        kept_details = &.{};
                    }
                },
                else => return err,
            };
        }
        if (!self.exchange.response_started) try self.begin(&.{});
        self.final = try trailers(self.exchange.arena, outcome, metadata, kept_details);
    }
    try self.exchange.endResponse(self.final.?);
    self.exchange.setDeadline(.none);
    self.finished = true;
}

/// Yield until input, output credit, or the earlier of this wake and the RPC
/// deadline. A null wake waits only for progress or the RPC deadline.
pub fn park(self: *Stream, wake: time.OptionalTimestamp) time.Timestamp.Error!void {
    var at = wake;
    if (self.deadline.get()) |deadline| {
        // An expired status write waits for credit. The timer already fired.
        if (!self.deadline_notified or deadline.compare(try self.now()) == .gt) {
            if (at.get()) |existing| {
                if (deadline.compare(existing) == .lt) at = try .init(deadline);
            } else {
                at = try .init(deadline);
            }
        }
    }
    self.exchange.setDeadline(at);
    self.exchange.waitForProgress();
}

pub fn now(self: *const Stream) time.Timestamp.Error!time.Timestamp {
    return .fromStd(Io.Clock.now(.awake, self.io));
}

pub fn expired(self: *const Stream) time.Timestamp.Error!bool {
    const deadline = self.deadline.get() orelse return false;
    return (try self.now()).compare(deadline) != .lt;
}

/// Serialize the status identically for buffered and incremental responses.
pub fn trailers(
    arena: Allocator,
    result: status.Status,
    metadata: []const http.Header,
    details: []const u8,
) ![]const http.Header {
    const encoded = try arena.alloc(u8, status.encodedLen(result.message));
    const text = try status.encodeMessage(encoded, result.message);
    const code = try arena.print("{d}", .{@backingInt(result.code)});
    const with_message: u32 = @intFromBool(text.len != 0);
    const with_details: u32 = @intFromBool(details.len != 0);
    const fields = try arena.alloc(http.Header, 1 + with_message + with_details + metadata.len);
    fields[0] = .{ .name = "grpc-status", .value = code };
    var at: u32 = 1;
    if (with_message != 0) {
        fields[at] = .{ .name = "grpc-message", .value = text };
        at += 1;
    }
    if (with_details != 0) {
        const buffer = try arena.alloc(
            u8,
            Base64Encoder.calcSize(details.len),
        );
        fields[at] = .{
            .name = "grpc-status-details-bin",
            .value = Base64Encoder.encode(buffer, details),
        };
        at += 1;
    }
    @memcpy(fields[at..], metadata);
    return fields;
}
