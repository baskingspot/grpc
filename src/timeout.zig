//! The `grpc-timeout` request field: at most eight digits and one unit.
//!
//! A client sends the time it will wait. The server reports
//! `DEADLINE_EXCEEDED` when that time runs out, so the field is advisory in
//! wording and a deadline in practice.

const std = @import("std");

pub const Unit = enum(u8) {
    hours = 'H',
    minutes = 'M',
    seconds = 'S',
    milliseconds = 'm',
    microseconds = 'u',
    nanoseconds = 'n',

    pub fn nanos(self: Unit) u64 {
        return switch (self) {
            .hours => 3_600_000_000_000,
            .minutes => 60_000_000_000,
            .seconds => 1_000_000_000,
            .milliseconds => 1_000_000,
            .microseconds => 1_000,
            .nanoseconds => 1,
        };
    }
};

/// The most digits the field may hold.
pub const max_digits = 8;

pub const Timeout = struct {
    value: u32,
    unit: Unit,

    /// The same deadline in nanoseconds. Saturates rather than wraps, because a
    /// wrapped deadline is a shorter one than the caller asked for.
    pub fn toNanos(self: Timeout) u64 {
        const value: u64 = self.value;
        return value *| self.unit.nanos();
    }
};

pub const Error = error{
    /// Fewer than one digit and one unit.
    Truncated,
    /// More than `max_digits` digits.
    TooManyDigits,
    /// The last byte was not one of `HMSmun`.
    InvalidUnit,
    /// A byte before the unit was not a decimal digit.
    InvalidDigit,
};

pub fn parse(bytes: []const u8) Error!Timeout {
    if (bytes.len < 2) return error.Truncated;
    const unit = parseUnit(bytes[bytes.len - 1]) orelse return error.InvalidUnit;
    const digits = bytes[0 .. bytes.len - 1];
    if (digits.len > max_digits) return error.TooManyDigits;
    var value: u32 = 0;
    for (digits) |digit| {
        if (digit < '0' or digit > '9') return error.InvalidDigit;
        value = value * 10 + (digit - '0');
    }
    return .{ .value = value, .unit = unit };
}

/// Write a deadline as eight characters or fewer.
///
/// The unit is the coarsest one that represents the value exactly, so a whole
/// second goes out as `1S` and a millisecond and a half as `1500m`. When no
/// unit is exact in eight digits, the unit is the coarsest whose rounded-up
/// count fits, and the count rounds up so the encoded deadline is never shorter
/// than the one the caller asked for.
pub fn encode(out: *[max_digits + 1]u8, nanos: u64) []const u8 {
    if (nanos == 0) return format(out, 0, .nanoseconds);

    for (coarsest_first) |unit| {
        const unit_nanos = unit.nanos();
        if (nanos % unit_nanos != 0) continue;
        const count = nanos / unit_nanos;
        if (count == 0 or count > max_value) continue;
        return format(out, @intCast(count), unit);
    }

    for (finer) |unit| {
        const unit_nanos = unit.nanos();
        const count = nanos / unit_nanos + @intFromBool(nanos % unit_nanos != 0);
        if (count == 0 or count > max_value) continue;
        return format(out, @intCast(count), unit);
    }

    // Hours always fit: eight hours-digits reach 11,415 years and a u64 of
    // nanoseconds reaches 584.
    const hours_nanos = Unit.hours.nanos();
    const hours = nanos / hours_nanos + @intFromBool(nanos % hours_nanos != 0);
    return format(out, @intCast(hours), .hours);
}

const max_value = 99_999_999;

const coarsest_first = [_]Unit{
    .hours,
    .minutes,
    .seconds,
    .milliseconds,
    .microseconds,
    .nanoseconds,
};

/// The same units without hours, which the caller adds last because it cannot
/// fail.
const finer = [_]Unit{ .minutes, .seconds, .milliseconds, .microseconds, .nanoseconds };

fn format(out: *[max_digits + 1]u8, value: u32, unit: Unit) []const u8 {
    var digits: [max_digits]u8 = undefined;
    var length: usize = 0;
    var rest = value;
    while (rest >= 10) : (rest /= 10) {
        digits[length] = '0' + @as(u8, @intCast(rest % 10));
        length += 1;
    }
    digits[length] = '0' + @as(u8, @intCast(rest));
    length += 1;

    var index: usize = 0;
    while (index < length) : (index += 1) out[index] = digits[length - 1 - index];
    out[length] = @backingInt(unit);
    return out[0 .. length + 1];
}

fn parseUnit(byte: u8) ?Unit {
    return std.enums.fromInt(Unit, byte);
}

test "each unit is one byte and one count of nanoseconds" {
    try std.testing.expectEqual(@as(u64, 1), Unit.nanoseconds.nanos());
    try std.testing.expectEqual(@as(u64, 1_000), Unit.microseconds.nanos());
    try std.testing.expectEqual(@as(u64, 3_600_000_000_000), Unit.hours.nanos());
}

test "the wire form parses" {
    const seconds = try parse("1S");
    try std.testing.expectEqual(@as(u32, 1), seconds.value);
    try std.testing.expectEqual(Unit.seconds, seconds.unit);
    try std.testing.expectEqual(@as(u64, 1_000_000_000), seconds.toNanos());

    const millis = try parse("1500m");
    try std.testing.expectEqual(@as(u64, 1_500_000_000), millis.toNanos());

    const zero = try parse("0n");
    try std.testing.expectEqual(@as(u64, 0), zero.toNanos());

    const widest = try parse("99999999H");
    try std.testing.expectEqual(max_value, widest.value);
}

test "a malformed field is refused" {
    try std.testing.expectError(error.Truncated, parse(""));
    try std.testing.expectError(error.Truncated, parse("1"));
    try std.testing.expectError(error.InvalidUnit, parse("1x"));
    try std.testing.expectError(error.InvalidDigit, parse("1.5S"));
    try std.testing.expectError(error.TooManyDigits, parse("123456789S"));
    try std.testing.expectError(error.InvalidDigit, parse("-1S"));
}

test "encoding prefers the coarsest exact unit" {
    var out: [max_digits + 1]u8 = undefined;
    try std.testing.expectEqualStrings("1S", encode(&out, 1_000_000_000));
    try std.testing.expectEqualStrings("1500m", encode(&out, 1_500_000_000));
    try std.testing.expectEqualStrings("90M", encode(&out, 5_400_000_000_000));
    try std.testing.expectEqualStrings("1500u", encode(&out, 1_500_000));
    try std.testing.expectEqualStrings("1001n", encode(&out, 1_001));
    try std.testing.expectEqualStrings("0n", encode(&out, 0));
    try std.testing.expectEqualStrings("8760H", encode(&out, 31_536_000_000_000_000));
}

test "a value with no exact unit rounds up at a coarser one" {
    var out: [max_digits + 1]u8 = undefined;
    const encoded = encode(&out, 1_000_000_001);
    const parsed = try parse(encoded);
    try std.testing.expect(parsed.toNanos() >= 1_000_000_001);
}

test "a deadline past eight hours-digits uses hours" {
    var out: [max_digits + 1]u8 = undefined;
    try std.testing.expectEqualStrings("5124096H", encode(&out, std.math.maxInt(u64)));
    const parsed = try parse(encode(&out, std.math.maxInt(u64)));
    try std.testing.expectEqual(Unit.hours, parsed.unit);
    try std.testing.expectEqual(@as(u32, 5_124_096), parsed.value);
}
