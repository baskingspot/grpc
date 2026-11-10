//! The request head for one call: the `:path`, the gRPC fields, and the
//! caller's metadata.
//!
//! h2 writes the pseudo-fields from its own request struct, so this module
//! produces regular fields only, in wire order. A key or value that cannot go
//! on the wire is refused here rather than becoming a connection error three
//! layers down.

const std = @import("std");
const testing = std.testing;
const base64 = std.base64.standard;
const Base64Decoder = std.base64.Base64Decoder;
const mem = std.mem;
const time = @import("stdx").time;

const http = @import("http");
const Header = http.Header;

const timeout = @import("timeout.zig");

pub const content_type = "application/grpc+proto";

/// The one encoding this layer reads. A server that answers with anything else
/// gets UNIMPLEMENTED until a codec is wired above the call.
pub const accept_encoding = "identity";

pub const Request = struct {
    /// A dotted service name, as the generated stubs spell it:
    /// `helloworld.Greeter`.
    service: []const u8,
    /// The RPC name: `SayHello`.
    method: []const u8,
    /// The encoding this call sends messages in. `identity` writes no
    /// `grpc-encoding` field, which is the same thing on the wire.
    encoding: []const u8 = "identity",
    /// The encodings this client can read, sent as `grpc-accept-encoding`.
    accept_encoding: []const u8 = "identity",
    /// Sent as `grpc-timeout` when set, and enforced as a deadline when the
    /// blocking client runs.
    timeout: time.OptionalDuration = .none,
    /// Regular fields the caller adds. Names are lowercase.
    metadata: []const Header = &.{},
};

pub const Error = error{
    /// The destination cannot hold the path or the field list.
    NoSpace,
    InvalidService,
    InvalidMethod,
    InvalidName,
    InvalidValue,
    /// A key beginning with `grpc-`, which the specification reserves.
    ReservedName,
};

const leading = [_]Header{
    .{ .name = "content-type", .value = content_type },
    .{ .name = "te", .value = "trailers" },
};

/// Write `/<service>/<method>` and return the slice used.
pub fn path(out: []u8, request: Request) Error![]const u8 {
    try validateComponent(request.service, error.InvalidService);
    try validateComponent(request.method, error.InvalidMethod);

    const total = request.service.len + request.method.len + 2;
    if (out.len < total) return error.NoSpace;

    out[0] = '/';
    @memcpy(out[1..][0..request.service.len], request.service);
    out[request.service.len + 1] = '/';
    @memcpy(out[request.service.len + 2 ..][0..request.method.len], request.method);
    return out[0..total];
}

/// A metadata key whose value is raw bytes on the wire. gRPC marks those with a
/// `-bin` suffix and base64-encodes the value.
pub fn isBinary(name: []const u8) bool {
    return mem.endsWith(u8, name, "-bin");
}

/// The base64 length of `raw`, padding included.
pub fn binaryEncodedLen(raw: []const u8) usize {
    return base64.Encoder.calcSize(raw.len);
}

/// Base64-encode a binary metadata value into `out`.
pub fn encodeBinary(out: []u8, raw: []const u8) Error![]const u8 {
    if (out.len < binaryEncodedLen(raw)) return error.NoSpace;
    return base64.Encoder.encode(out, raw);
}

/// The decoders a `-bin` value can need. A gRPC peer may pad the base64 or
/// leave it bare: the Go implementation writes it without padding, so the
/// decoder is chosen from the length.
fn decoder(encoded: []const u8) *const Base64Decoder {
    return if (encoded.len % 4 == 0)
        &base64.Decoder
    else
        &std.base64.standard_no_pad.Decoder;
}

/// The length `decodeBinary` writes for this value.
pub fn binaryDecodedLen(encoded: []const u8) Error!usize {
    return decoder(encoded).calcSizeForSlice(encoded) catch error.InvalidValue;
}

/// Base64-decode a binary metadata value into `out`.
pub fn decodeBinary(out: []u8, encoded: []const u8) Error![]const u8 {
    const len = try binaryDecodedLen(encoded);
    if (out.len < len) return error.NoSpace;
    decoder(encoded).decode(out[0..len], encoded) catch return error.InvalidValue;
    return out[0..len];
}

/// How many regular fields `fields` writes for this request.
pub fn fieldCount(request: Request) usize {
    return leading.len +
        @intFromBool(request.timeout.get() != null) +
        @intFromBool(!mem.eql(u8, request.encoding, "identity")) +
        1 + // grpc-accept-encoding
        request.metadata.len;
}

/// Fill `out` with every regular field, in wire order, and return the slice
/// used.
///
/// `timeout_buf` holds the encoded deadline, and `scratch` holds each lowered
/// key and the base64 of every binary value, because the fields borrow both.
/// Size `scratch` as the sum of every key length and `binaryEncodedLen` over the
/// `-bin` values.
pub fn fields(
    out: []Header,
    scratch: []u8,
    timeout_buf: *[timeout.max_digits + 1]u8,
    request: Request,
) Error![]Header {
    var count: usize = 0;
    for (leading) |field| {
        if (count == out.len) return error.NoSpace;
        out[count] = field;
        count += 1;
    }
    if (request.timeout.get()) |duration| {
        if (count == out.len) return error.NoSpace;
        out[count] = .{
            .name = "grpc-timeout",
            .value = timeout.encode(timeout_buf, duration.nanoseconds),
        };
        count += 1;
    }
    if (!mem.eql(u8, request.encoding, "identity")) {
        if (count == out.len) return error.NoSpace;
        out[count] = .{ .name = "grpc-encoding", .value = request.encoding };
        count += 1;
    }
    if (count == out.len) return error.NoSpace;
    out[count] = .{ .name = "grpc-accept-encoding", .value = request.accept_encoding };
    count += 1;
    var left = scratch;
    for (request.metadata) |field| {
        try validateName(field.name);
        if (count == out.len) return error.NoSpace;
        if (left.len < field.name.len) return error.NoSpace;

        // HTTP/2 field names are lower case, and a caller should not have to
        // know that. The copy lands in the same scratch as the base64 values.
        const name = left[0..field.name.len];
        for (field.name, 0..) |byte, index| name[index] = std.ascii.toLower(byte);
        left = left[field.name.len..];

        if (isBinary(field.name)) {
            const encoded = try encodeBinary(left, field.value);
            out[count] = .{ .name = name, .value = encoded };
            left = left[encoded.len..];
        } else {
            try validateValue(field.value);
            out[count] = .{ .name = name, .value = field.value };
        }
        count += 1;
    }
    return out[0..count];
}

/// A metadata key: ASCII letters, digits, `-`, `_`, and `.`. An upper-case key
/// is legal input and goes on the wire lower-cased, which `fields` does.
pub fn validateName(name: []const u8) Error!void {
    if (name.len == 0) return error.InvalidName;
    if (mem.startsWith(u8, name, "grpc-") or mem.startsWith(u8, name, "GRPC-")) {
        return error.ReservedName;
    }
    for (name) |byte| {
        switch (byte) {
            'a'...'z', 'A'...'Z', '0'...'9', '-', '_', '.' => {},
            else => return error.InvalidName,
        }
    }
}

/// A metadata value: printable ASCII and spaces, with no control bytes.
/// Binary metadata arrives base64-encoded, so it passes this check too.
pub fn validateValue(value: []const u8) Error!void {
    for (value) |byte| {
        if (byte < 0x20 or byte > 0x7e) return error.InvalidValue;
    }
}

fn validateComponent(value: []const u8, failure: Error) Error!void {
    if (value.len == 0) return failure;
    for (value) |byte| {
        switch (byte) {
            'a'...'z', 'A'...'Z', '0'...'9', '.', '_', '-' => {},
            else => return failure,
        }
    }
}

test "the path is the service and the method" {
    var buffer: [64]u8 = undefined;
    const request: Request = .{ .service = "helloworld.Greeter", .method = "SayHello" };
    try testing.expectEqualStrings("/helloworld.Greeter/SayHello", try path(&buffer, request));
}

test "a path that does not fit is refused, not truncated" {
    var buffer: [8]u8 = undefined;
    const request: Request = .{ .service = "helloworld.Greeter", .method = "SayHello" };
    try testing.expectError(error.NoSpace, path(&buffer, request));
}

test "a service or method that cannot be on the wire is refused" {
    var buffer: [64]u8 = undefined;
    const empty: Request = .{ .service = "", .method = "SayHello" };
    try testing.expectError(error.InvalidService, path(&buffer, empty));

    const slash: Request = .{ .service = "a/b", .method = "SayHello" };
    try testing.expectError(error.InvalidService, path(&buffer, slash));

    const spaced: Request = .{ .service = "a.B", .method = "Say Hello" };
    try testing.expectError(error.InvalidMethod, path(&buffer, spaced));

    const no_method: Request = .{ .service = "a.B", .method = "" };
    try testing.expectError(error.InvalidMethod, path(&buffer, no_method));
}

test "the head leads with the three gRPC fields" {
    var buffer: [8]Header = undefined;
    var scratch: [64]u8 = undefined;
    var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
    const request: Request = .{ .service = "a.B", .method = "C" };
    const written = try fields(&buffer, &scratch, &timeout_buf, request);

    try testing.expectEqual(@as(usize, 3), written.len);
    try testing.expectEqualStrings("content-type", written[0].name);
    try testing.expectEqualStrings("application/grpc+proto", written[0].value);
    try testing.expectEqualStrings("te", written[1].name);
    try testing.expectEqualStrings("trailers", written[1].value);
    try testing.expectEqualStrings("grpc-accept-encoding", written[2].name);
    try testing.expectEqualStrings("identity", written[2].value);
}

test "a deadline becomes the grpc-timeout field and the caller's fields follow" {
    var buffer: [8]Header = undefined;
    var scratch: [64]u8 = undefined;
    var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
    const metadata = [_]Header{.{ .name = "x-request-id", .value = "7f3a" }};
    const request: Request = .{
        .service = "a.B",
        .method = "C",
        .timeout = try .init(.fromNanoseconds(1_500_000_000)),
        .metadata = &metadata,
    };

    try testing.expectEqual(@as(usize, 5), fieldCount(request));
    const written = try fields(&buffer, &scratch, &timeout_buf, request);
    try testing.expectEqual(@as(usize, 5), written.len);
    try testing.expectEqualStrings("grpc-timeout", written[2].name);
    try testing.expectEqualStrings("1500m", written[2].value);
    try testing.expectEqualStrings("grpc-accept-encoding", written[3].name);
    try testing.expectEqualStrings("identity", written[3].value);
    try testing.expectEqualStrings("x-request-id", written[4].name);
    try testing.expectEqualStrings("7f3a", written[4].value);
}

test "a field list too small is refused" {
    var buffer: [2]Header = undefined;
    var scratch: [64]u8 = undefined;
    var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
    const request: Request = .{ .service = "a.B", .method = "C" };
    try testing.expectError(error.NoSpace, fields(&buffer, &scratch, &timeout_buf, request));
}

test "a binary metadata value is base64 on the wire" {
    var buffer: [8]Header = undefined;
    var scratch: [32]u8 = undefined;
    var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
    const metadata = [_]Header{
        .{ .name = "trace-bin", .value = "\x00\x01\xff" },
        .{ .name = "x-plain", .value = "text" },
    };
    const request: Request = .{ .service = "a.B", .method = "C", .metadata = &metadata };

    const written = try fields(&buffer, &scratch, &timeout_buf, request);
    try testing.expectEqual(@as(usize, 5), written.len);
    try testing.expectEqualStrings("trace-bin", written[3].name);
    try testing.expectEqualStrings("AAH/", written[3].value);
    try testing.expectEqualStrings("text", written[4].value);

    var decoded: [8]u8 = undefined;
    const expected = "\x00\x01\xff";
    try testing.expectEqualSlices(u8, expected, try decodeBinary(&decoded, written[3].value));
    // A peer may leave the padding off, which the Go implementation does:
    // two bytes encode as "AAE=" padded and "AAE" bare.
    try testing.expectEqualSlices(u8, "\x00\x01", try decodeBinary(&decoded, "AAE="));
    try testing.expectEqualSlices(u8, "\x00\x01", try decodeBinary(&decoded, "AAE"));
}

test "a plain metadata value with control bytes is still refused" {
    var buffer: [8]Header = undefined;
    var scratch: [32]u8 = undefined;
    var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
    const metadata = [_]Header{.{ .name = "x-binary-looking", .value = "\x07" }};
    const request: Request = .{
        .service = "a.B",
        .method = "C",
        .metadata = &metadata,
    };
    try testing.expectError(
        error.InvalidValue,
        fields(&buffer, &scratch, &timeout_buf, request),
    );
}

test "a reserved or malformed metadata key is refused" {
    try testing.expectError(error.ReservedName, validateName("grpc-trace-bin"));
    try testing.expectError(error.InvalidName, validateName(""));
    try validateName("X-Request-Id");
    try testing.expectError(error.InvalidName, validateName("x request"));
    try testing.expectError(error.InvalidValue, validateValue("bell\x07"));
    try validateName("x-request-id");
    try validateName("x.bin_2");
    try validateValue("plain ascii and spaces");
}
