//! gRPC status codes and the trailer field that carries them.
//!
//! A call ends with one code. The code travels in `grpc-status` when the server
//! answered, and in the HTTP/2 error when it did not, so this module is a plain
//! mapping from the wire to a value and back.

const std = @import("std");
const testing = std.testing;

/// The codes gRPC defines. The wire is a decimal number, not a name.
pub const Code = enum(u8) {
    ok = 0,
    cancelled = 1,
    unknown = 2,
    invalid_argument = 3,
    deadline_exceeded = 4,
    not_found = 5,
    already_exists = 6,
    permission_denied = 7,
    resource_exhausted = 8,
    failed_precondition = 9,
    aborted = 10,
    out_of_range = 11,
    unimplemented = 12,
    internal = 13,
    unavailable = 14,
    data_loss = 15,
    unauthenticated = 16,

    /// A decimal `grpc-status` value. A code outside the defined range reads as
    /// `unknown`, which is what the specification says to report.
    pub fn fromInt(raw: u8) Code {
        return std.enums.fromInt(Code, raw) orelse .unknown;
    }
};

pub const Error = error{
    /// `grpc-status` was empty.
    Missing,
    /// A digit was not decimal, or there was more than one field's worth.
    InvalidCode,
    /// A `%` was not followed by two hexadecimal digits.
    InvalidEscape,
    /// The destination slice cannot hold the decoded message.
    NoSpace,
};

/// A finished call: the code and whatever text the server attached.
pub const Status = struct {
    code: Code,
    message: []const u8 = "",

    pub fn isOk(self: Status) bool {
        return self.code == .ok;
    }
};

/// Parse the `grpc-status` field. Leading zeros are legal, more than three
/// digits is not.
pub fn parse(bytes: []const u8) Error!Code {
    if (bytes.len == 0) return error.Missing;
    if (bytes.len > 3) return error.InvalidCode;
    var value: u32 = 0;
    for (bytes) |byte| {
        if (byte < '0' or byte > '9') return error.InvalidCode;
        value = value * 10 + (byte - '0');
    }
    return .fromInt(@intCast(value));
}

/// Decode a `grpc-message` field. Every byte stands for itself except `%`,
/// which introduces two hexadecimal digits naming one byte.
///
/// The result is UTF-8 in practice and this does not check it: a malformed
/// message is the server's defect, and dropping the text would hide the status.
pub fn decode(out: []u8, wire: []const u8) Error![]const u8 {
    var written: usize = 0;
    var index: usize = 0;
    while (index < wire.len) {
        if (wire[index] != '%') {
            if (written == out.len) return error.NoSpace;
            out[written] = wire[index];
            written += 1;
            index += 1;
            continue;
        }
        if (index + 3 > wire.len) return error.InvalidEscape;
        const high = hexValue(wire[index + 1]) orelse return error.InvalidEscape;
        const low = hexValue(wire[index + 2]) orelse return error.InvalidEscape;
        if (written == out.len) return error.NoSpace;
        out[written] = high << 4 | low;
        written += 1;
        index += 3;
    }
    return out[0..written];
}

/// The number of bytes `decode` writes for this field: never more than the
/// wire length, since an escape is three bytes for one.
pub fn decodedLen(wire: []const u8) usize {
    return wire.len;
}

/// The number of bytes `encodeMessage` writes: three for each byte that needs an
/// escape, one for the rest.
pub fn encodedLen(text: []const u8) usize {
    var total: usize = 0;
    for (text) |byte| total += if (needsEscape(byte)) 3 else 1;
    return total;
}

/// Encode text for the `grpc-message` field. A byte outside visible ASCII, or
/// `%` itself, becomes `%XX`.
pub fn encodeMessage(out: []u8, text: []const u8) Error![]const u8 {
    if (out.len < encodedLen(text)) return error.NoSpace;

    const digits = "0123456789ABCDEF";
    var written: usize = 0;
    for (text) |byte| {
        if (!needsEscape(byte)) {
            out[written] = byte;
            written += 1;
            continue;
        }
        out[written] = '%';
        out[written + 1] = digits[byte >> 4];
        out[written + 2] = digits[byte & 0x0f];
        written += 3;
    }
    return out[0..written];
}

fn needsEscape(byte: u8) bool {
    return byte < 0x20 or byte > 0x7e or byte == '%';
}

fn hexValue(byte: u8) ?u8 {
    return switch (byte) {
        '0'...'9' => byte - '0',
        'a'...'f' => byte - 'a' + 10,
        'A'...'F' => byte - 'A' + 10,
        else => null,
    };
}

test "every defined code is its wire number" {
    try testing.expectEqual(@as(u8, 0), @backingInt(Code.ok));
    try testing.expectEqual(@as(u8, 16), @backingInt(Code.unauthenticated));
    try testing.expectEqual(Code.unavailable, Code.fromInt(14));
}

test "an undefined number reads as unknown" {
    try testing.expectEqual(Code.unknown, Code.fromInt(17));
    try testing.expectEqual(Code.unknown, Code.fromInt(255));
    try testing.expectEqual(Code.unknown, try parse("99"));
}

test "grpc-status parses decimal and nothing else" {
    try testing.expectEqual(Code.ok, try parse("0"));
    try testing.expectEqual(Code.resource_exhausted, try parse("8"));
    try testing.expectEqual(Code.unauthenticated, try parse("016"));
    try testing.expectEqual(Code.internal, try parse("13"));

    try testing.expectError(error.Missing, parse(""));
    try testing.expectError(error.InvalidCode, parse("-1"));
    try testing.expectError(error.InvalidCode, parse("1x"));
    try testing.expectError(error.InvalidCode, parse("1234"));
}

test "a message decodes its escapes and keeps plain text" {
    var out: [64]u8 = undefined;
    const decoded = try decode(&out, "no space%20here");
    try testing.expectEqualStrings("no space here", decoded);

    const mixed = try decode(&out, "%41%42C");
    try testing.expectEqualStrings("ABC", mixed);
}

test "a broken escape is refused" {
    var out: [8]u8 = undefined;
    try testing.expectError(error.InvalidEscape, decode(&out, "100%"));
    try testing.expectError(error.InvalidEscape, decode(&out, "100%2"));
    try testing.expectError(error.InvalidEscape, decode(&out, "100%zz"));
}

test "a message encodes what decode escapes" {
    var out: [64]u8 = undefined;
    // A space is visible ASCII, so it goes through unchanged; `%` never does.
    try testing.expectEqualStrings("no space", try encodeMessage(&out, "no space"));
    try testing.expectEqualStrings("bell%07", try encodeMessage(&out, "bell\x07"));
    try testing.expectEqualStrings("100%25", try encodeMessage(&out, "100%"));
    try testing.expectEqualStrings("caf%C3%A9", try encodeMessage(&out, "caf\xc3\xa9"));

    var round: [64]u8 = undefined;
    const encoded = try encodeMessage(&out, "key not found: caf\xc3\xa9 100%");
    const decoded = try decode(&round, encoded);
    try testing.expectEqualStrings("key not found: caf\xc3\xa9 100%", decoded);
}

test "an encoding destination too small is refused" {
    var out: [4]u8 = undefined;
    try testing.expectError(error.NoSpace, encodeMessage(&out, "caf\xc3\xa9"));
    try testing.expectEqualStrings("abcd", try encodeMessage(&out, "abcd"));
}

test "a destination too small is refused" {
    var out: [2]u8 = undefined;
    try testing.expectError(error.NoSpace, decode(&out, "abc"));
    const short = try decode(&out, "%41%42");
    try testing.expectEqualStrings("AB", short);
}
