//! The request head for one call: the `:path`, the gRPC fields, and the
//! caller's metadata.
//!
//! h2 writes the pseudo-fields from its own request struct, so this module
//! produces regular fields only, in wire order. A key or value that cannot go
//! on the wire is refused here rather than becoming a connection error three
//! layers down.

const std = @import("std");
const http = @import("http");
const timeout = @import("timeout.zig");

const Header = http.Header;

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
    /// Sent as `grpc-timeout` when set, and enforced as a deadline when the
    /// blocking client runs.
    deadline_ns: ?u64 = null,
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
    .{ .name = "grpc-accept-encoding", .value = accept_encoding },
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

/// How many regular fields `fields` writes for this request.
pub fn fieldCount(request: Request) usize {
    return leading.len + @intFromBool(request.deadline_ns != null) + request.metadata.len;
}

/// Fill `out` with every regular field, in wire order, and return the slice
/// used. `timeout_buf` holds the encoded deadline, because the field borrows it.
pub fn fields(out: []Header, timeout_buf: *[timeout.max_digits + 1]u8, request: Request) Error![]Header {
    var count: usize = 0;
    for (leading) |field| {
        if (count == out.len) return error.NoSpace;
        out[count] = field;
        count += 1;
    }
    if (request.deadline_ns) |deadline| {
        if (count == out.len) return error.NoSpace;
        out[count] = .{ .name = "grpc-timeout", .value = timeout.encode(timeout_buf, deadline) };
        count += 1;
    }
    for (request.metadata) |field| {
        try validateField(field);
        if (count == out.len) return error.NoSpace;
        out[count] = field;
        count += 1;
    }
    return out[0..count];
}

/// A metadata key: lowercase ASCII, digits, `-`, `_`, and `.`.
pub fn validateName(name: []const u8) Error!void {
    if (name.len == 0) return error.InvalidName;
    if (std.mem.startsWith(u8, name, "grpc-")) return error.ReservedName;
    for (name) |byte| {
        switch (byte) {
            'a'...'z', '0'...'9', '-', '_', '.' => {},
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

fn validateField(field: Header) Error!void {
    try validateName(field.name);
    try validateValue(field.value);
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
    try std.testing.expectEqualStrings("/helloworld.Greeter/SayHello", try path(&buffer, request));
}

test "a path that does not fit is refused, not truncated" {
    var buffer: [8]u8 = undefined;
    const request: Request = .{ .service = "helloworld.Greeter", .method = "SayHello" };
    try std.testing.expectError(error.NoSpace, path(&buffer, request));
}

test "a service or method that cannot be on the wire is refused" {
    var buffer: [64]u8 = undefined;
    try std.testing.expectError(error.InvalidService, path(&buffer, .{ .service = "", .method = "SayHello" }));
    try std.testing.expectError(error.InvalidService, path(&buffer, .{ .service = "a/b", .method = "SayHello" }));
    try std.testing.expectError(error.InvalidMethod, path(&buffer, .{ .service = "a.B", .method = "Say Hello" }));
    try std.testing.expectError(error.InvalidMethod, path(&buffer, .{ .service = "a.B", .method = "" }));
}

test "the head leads with the three gRPC fields" {
    var buffer: [8]Header = undefined;
    var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
    const request: Request = .{ .service = "a.B", .method = "C" };
    const written = try fields(&buffer, &timeout_buf, request);

    try std.testing.expectEqual(@as(usize, 3), written.len);
    try std.testing.expectEqualStrings("content-type", written[0].name);
    try std.testing.expectEqualStrings("application/grpc+proto", written[0].value);
    try std.testing.expectEqualStrings("te", written[1].name);
    try std.testing.expectEqualStrings("trailers", written[1].value);
    try std.testing.expectEqualStrings("grpc-accept-encoding", written[2].name);
    try std.testing.expectEqualStrings("identity", written[2].value);
}

test "a deadline becomes the grpc-timeout field and the caller's fields follow" {
    var buffer: [8]Header = undefined;
    var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
    const metadata = [_]Header{.{ .name = "x-request-id", .value = "7f3a" }};
    const request: Request = .{
        .service = "a.B",
        .method = "C",
        .deadline_ns = 1_500_000_000,
        .metadata = &metadata,
    };

    try std.testing.expectEqual(@as(usize, 5), fieldCount(request));
    const written = try fields(&buffer, &timeout_buf, request);
    try std.testing.expectEqual(@as(usize, 5), written.len);
    try std.testing.expectEqualStrings("grpc-timeout", written[3].name);
    try std.testing.expectEqualStrings("1500m", written[3].value);
    try std.testing.expectEqualStrings("x-request-id", written[4].name);
    try std.testing.expectEqualStrings("7f3a", written[4].value);
}

test "a field list too small is refused" {
    var buffer: [2]Header = undefined;
    var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
    const request: Request = .{ .service = "a.B", .method = "C" };
    try std.testing.expectError(error.NoSpace, fields(&buffer, &timeout_buf, request));
}

test "a reserved or malformed metadata key is refused" {
    try std.testing.expectError(error.ReservedName, validateName("grpc-trace-bin"));
    try std.testing.expectError(error.InvalidName, validateName(""));
    try std.testing.expectError(error.InvalidName, validateName("X-Request-Id"));
    try std.testing.expectError(error.InvalidName, validateName("x request"));
    try std.testing.expectError(error.InvalidValue, validateValue("bell\x07"));
    try validateName("x-request-id");
    try validateName("x.bin_2");
    try validateValue("plain ascii and spaces");
}
