//! gRPC status codes and the trailer field that carries them.
//!
//! A call ends with one code. The code travels in `grpc-status` when the server
//! answered, and in the HTTP/2 error when it did not, so this module is a plain
//! mapping from the wire to a value and back.

const std = @import("std");

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

/// A decimal `grpc-status` value. A code outside the defined range reads as
/// `unknown`, which is what the specification says to report.
pub fn codeFromInt(raw: u8) Code {
    return std.enums.fromInt(Code, raw) orelse .unknown;
}

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
    return codeFromInt(@intCast(value));
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

fn hexValue(byte: u8) ?u8 {
    return switch (byte) {
        '0'...'9' => byte - '0',
        'a'...'f' => byte - 'a' + 10,
        'A'...'F' => byte - 'A' + 10,
        else => null,
    };
}

test "every defined code is its wire number" {
    try std.testing.expectEqual(@as(u8, 0), @backingInt(Code.ok));
    try std.testing.expectEqual(@as(u8, 16), @backingInt(Code.unauthenticated));
    try std.testing.expectEqual(Code.unavailable, codeFromInt(14));
}

test "an undefined number reads as unknown" {
    try std.testing.expectEqual(Code.unknown, codeFromInt(17));
    try std.testing.expectEqual(Code.unknown, codeFromInt(255));
    try std.testing.expectEqual(Code.unknown, try parse("99"));
}

test "grpc-status parses decimal and nothing else" {
    try std.testing.expectEqual(Code.ok, try parse("0"));
    try std.testing.expectEqual(Code.resource_exhausted, try parse("8"));
    try std.testing.expectEqual(Code.unauthenticated, try parse("016"));
    try std.testing.expectEqual(Code.internal, try parse("13"));

    try std.testing.expectError(error.Missing, parse(""));
    try std.testing.expectError(error.InvalidCode, parse("-1"));
    try std.testing.expectError(error.InvalidCode, parse("1x"));
    try std.testing.expectError(error.InvalidCode, parse("1234"));
}

test "a message decodes its escapes and keeps plain text" {
    var out: [64]u8 = undefined;
    const decoded = try decode(&out, "no space%20here");
    try std.testing.expectEqualStrings("no space here", decoded);

    const mixed = try decode(&out, "%41%42C");
    try std.testing.expectEqualStrings("ABC", mixed);
}

test "a broken escape is refused" {
    var out: [8]u8 = undefined;
    try std.testing.expectError(error.InvalidEscape, decode(&out, "100%"));
    try std.testing.expectError(error.InvalidEscape, decode(&out, "100%2"));
    try std.testing.expectError(error.InvalidEscape, decode(&out, "100%zz"));
}

test "a destination too small is refused" {
    var out: [2]u8 = undefined;
    try std.testing.expectError(error.NoSpace, decode(&out, "abc"));
    const short = try decode(&out, "%41%42");
    try std.testing.expectEqualStrings("AB", short);
}
