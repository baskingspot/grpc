//! The gRPC message frame: a compressed flag, a big-endian length, and a body.
//!
//! One message is five header bytes and a body: a flag byte that is 0 or 1, a
//! big-endian u32 length, then that many bytes of encoded message. Nothing here
//! reads the body. The codec above decides what the bytes mean; grpc counts
//! them and hands them over unchanged.

const std = @import("std");
const testing = std.testing;
const mem = std.mem;

pub const prefix_len = 5;

/// The receive limit every gRPC implementation starts from: 4 MiB.
pub const default_limit: u32 = 4 * 1024 * 1024;

/// The largest value a length can hold.
pub const max_length: u32 = std.math.maxInt(u32);

pub const Error = error{
    /// The buffer ends before the header, or before the body the header promised.
    Truncated,
    /// The length is above the limit the caller set.
    TooLarge,
    /// The flag byte was neither 0 nor 1. The flag is a boolean on the wire.
    InvalidFlag,
    /// The destination slice cannot hold the frame.
    NoSpace,
};

pub const Header = struct {
    compressed: bool,
    length: u32,
};

/// One decoded message. `bytes` borrows from the buffer it was read out of.
pub const Message = struct {
    compressed: bool,
    bytes: []const u8,
};

/// Read the frame header at the start of `bytes`. Five bytes is enough.
pub fn parseHeader(bytes: []const u8) Error!Header {
    if (bytes.len < prefix_len) return error.Truncated;
    const compressed, const length = switch (bytes[0]) {
        0 => .{ false, mem.readInt(u32, bytes[1..5], .big) },
        1 => .{ true, mem.readInt(u32, bytes[1..5], .big) },
        else => return error.InvalidFlag,
    };
    return .{ .compressed = compressed, .length = length };
}

/// Write the five header bytes for a body of `length`.
pub fn writeHeader(out: *[prefix_len]u8, compressed: bool, length: u32) void {
    out[0] = @intFromBool(compressed);
    mem.writeInt(u32, out[1..5], length, .big);
}

/// A whole frame in one slice. A message above 4 GiB has no gRPC encoding.
pub fn write(out: []u8, body: []const u8, compressed: bool) Error![]u8 {
    const length: u32 = @intCast(body.len);
    if (out.len < prefix_len) return error.NoSpace;
    const total = @as(usize, prefix_len) + body.len;
    if (out.len < total) return error.NoSpace;
    writeHeader(out[0..prefix_len], compressed, length);
    @memcpy(out[prefix_len..total], body);
    return out[0..total];
}

/// Walk complete messages out of one contiguous buffer.
///
/// The buffer has to hold a whole message: gRPC messages outlive the DATA frame
/// they arrived in, so the caller concatenates first and reads second. The
/// iterator reports `Truncated` rather than buffering, because buffering is a
/// policy and this layer holds no allocator.
pub const Iterator = struct {
    bytes: []const u8,
    offset: usize = 0,
    limit: u32 = default_limit,

    pub fn init(bytes: []const u8) Iterator {
        return .{ .bytes = bytes };
    }

    /// The next complete message, or null at the end of the buffer.
    pub fn next(self: *Iterator) Error!?Message {
        if (self.offset == self.bytes.len) return null;
        const header = try parseHeader(self.bytes[self.offset..]);
        if (header.length > self.limit) return error.TooLarge;
        const start = self.offset + prefix_len;
        const end = start + header.length;
        if (end > self.bytes.len) return error.Truncated;
        self.offset = end;
        return .{ .compressed = header.compressed, .bytes = self.bytes[start..end] };
    }
};

test "a frame round trips through the codec" {
    var buffer: [64]u8 = undefined;
    const body = "hello, tour";
    const frame = try write(&buffer, body, false);

    try testing.expectEqual(body.len + prefix_len, frame.len);
    const header = try parseHeader(frame);
    try testing.expect(!header.compressed);
    try testing.expectEqual(@as(u32, body.len), header.length);

    var iterator = Iterator.init(frame);
    const message = (try iterator.next()).?;
    try testing.expectEqualStrings(body, message.bytes);
    try testing.expect(!message.compressed);
    try testing.expect((try iterator.next()) == null);
}

test "the compressed flag is the byte the wire carries" {
    var buffer: [16]u8 = undefined;
    const frame = try write(&buffer, "x", true);
    try testing.expectEqual(@as(u8, 1), frame[0]);
    try testing.expect((try parseHeader(frame)).compressed);
}

test "a flag other than 0 or 1 is refused" {
    const bad = [_]u8{ 2, 0, 0, 0, 1, 0xFF };
    try testing.expectError(error.InvalidFlag, parseHeader(&bad));
}

test "a short header is truncated, not an error in disguise" {
    try testing.expectError(error.Truncated, parseHeader(&[_]u8{ 0, 0 }));
    try testing.expectError(error.Truncated, parseHeader(&[_]u8{}));
}

test "a body cut short by the buffer is truncated" {
    var buffer: [32]u8 = undefined;
    const frame = try write(&buffer, "twelve bytes", false);
    var iterator = Iterator.init(frame[0 .. frame.len - 1]);
    try testing.expectError(error.Truncated, iterator.next());
}

test "a message over the limit is refused before it is read" {
    var buffer: [64]u8 = undefined;
    const frame = try write(&buffer, "0123456789", false);
    var iterator: Iterator = .{ .bytes = frame, .limit = 9 };
    try testing.expectError(error.TooLarge, iterator.next());

    var exact: Iterator = .{ .bytes = frame, .limit = 10 };
    const message = (try exact.next()).?;
    try testing.expectEqualStrings("0123456789", message.bytes);
}

test "two frames in one buffer are walked in order" {
    var buffer: [64]u8 = undefined;
    const first = try write(&buffer, "one", false);
    const second = try write(buffer[first.len..], "two", false);
    const both = buffer[0 .. first.len + second.len];

    var iterator = Iterator.init(both);
    try testing.expectEqualStrings("one", (try iterator.next()).?.bytes);
    try testing.expectEqualStrings("two", (try iterator.next()).?.bytes);
    try testing.expect((try iterator.next()) == null);
}

test "an empty message is a valid frame" {
    var buffer: [8]u8 = undefined;
    const frame = try write(&buffer, "", false);
    try testing.expectEqual(@as(usize, prefix_len), frame.len);
    var iterator = Iterator.init(frame);
    const message = (try iterator.next()).?;
    try testing.expectEqual(@as(usize, 0), message.bytes.len);
}

test "a destination too small is refused rather than overrun" {
    var small: [4]u8 = undefined;
    try testing.expectError(error.NoSpace, write(&small, "", false));

    var body_only: [5]u8 = undefined;
    try testing.expectError(error.NoSpace, write(&body_only, "x", false));
}
