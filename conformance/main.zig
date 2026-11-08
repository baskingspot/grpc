//! A conformance client under test.
//!
//! The connectrpc conformance runner writes length-prefixed
//! `ClientCompatRequest` messages to this program's stdin and reads
//! length-prefixed `ClientCompatResponse` messages from its stdout. Each request
//! describes one RPC; this program issues it and reports what the server said.
//!
//! `just conformance` runs the suite. Unary calls are the covered stream type
//! today; anything else answers with an error result, which the runner reports
//! as a failed case rather than a crash.

const std = @import("std");
const grpc = @import("grpc");
const http = @import("http");
const pb = @import("protobuf");
const messages = @import("messages.zig");

const Io = std.Io;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa: std.mem.Allocator = init.arena.allocator();

    var in_buffer: [16 * 1024]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().reader(io, &in_buffer);
    const reader = &stdin_reader.interface;

    var out_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &out_buffer);
    const writer = &stdout_writer.interface;

    while (true) {
        var length_bytes: [4]u8 = undefined;
        reader.readSliceAll(&length_bytes) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        const length = std.mem.readInt(u32, &length_bytes, .big);

        const body = try gpa.alloc(u8, length);
        defer gpa.free(body);
        try reader.readSliceAll(body);

        var arena: std.heap.ArenaAllocator = .init(gpa);
        defer arena.deinit();
        const allocator = arena.allocator();

        var request_reader: Io.Reader = .fixed(body);
        const request = try pb.decodeLeaky(
            messages.ClientCompatRequest,
            allocator,
            &request_reader,
            .{},
        );

        const response = dispatch(io, allocator, request) catch |err| blk: {
            if (err == error.InvalidName or err == error.InvalidValue) {
                for (request.request_headers) |header| {
                    std.debug.print("HEADER [{s}] {d} values\n", .{ header.name, header.value.len });
                }
            }
            break :blk messages.ClientCompatResponse{
                .test_name = request.test_name,
                .err = .{ .message = @errorName(err) },
            };
        };

        var encoded: std.Io.Writer.Allocating = .init(gpa);
        defer encoded.deinit();
        try pb.encode(response, .{}, &encoded.writer);

        const bytes = encoded.written();
        var size: [4]u8 = undefined;
        std.mem.writeInt(u32, &size, @intCast(bytes.len), .big);
        try writer.writeAll(&size);
        try writer.writeAll(bytes);
        try writer.flush();
    }
}

fn dispatch(
    io: Io,
    allocator: std.mem.Allocator,
    request: messages.ClientCompatRequest,
) !messages.ClientCompatResponse {
    if (request.protocol != messages.protocol_grpc) return error.UnsupportedProtocol;
    if (request.codec != messages.codec_proto) return error.UnsupportedCodec;
    if (request.compression != messages.compression_identity) return error.UnsupportedCompression;
    if (request.stream_type != messages.stream_type_unary) return error.UnsupportedStreamType;
    if (request.server_tls_cert.len != 0) return error.UnsupportedTls;

    return .{
        .test_name = request.test_name,
        .response = try unary(io, allocator, request),
    };
}

fn unary(
    io: Io,
    allocator: std.mem.Allocator,
    request: messages.ClientCompatRequest,
) !messages.ClientResponseResult {
    if (request.request_messages.len != 1) return error.NotOneRequestMessage;

    var endpoint_buffer: [256]u8 = undefined;
    const endpoint = try std.fmt.bufPrint(&endpoint_buffer, "{s}:{d}", .{
        request.host,
        request.port,
    });

    const metadata = try convertHeaders(allocator, request.request_headers);

    var frame: [64 * 1024]u8 = undefined;
    const body = try grpc.message.write(&frame, request.request_messages[0].value, false);

    const address = try Io.net.IpAddress.parseLiteral(endpoint);
    const socket = try address.connect(io, .{ .mode = .stream });
    // The client owns the socket from here and closes it in `deinit`.
    var client: grpc.blocking.Client = undefined;
    try client.init(allocator, io, socket);
    defer client.deinit();

    var calls = [_]grpc.Call{try client.open(.{
        .authority = endpoint,
        .service = request.service orelse "connectrpc.conformance.v1.ConformanceService",
        .method = request.method orelse "Unary",
        .body = body,
        .deadline_ns = if (request.timeout_ms) |ms| @as(u64, ms) * std.time.ns_per_ms else null,
        .metadata = metadata,
    })};
    defer calls[0].deinit();

    while (calls[0].result() == null) {
        if (!try client.step(&calls)) return error.Stalled;
    }

    var result: messages.ClientResponseResult = .{};
    result.response_headers = try convertView(allocator, calls[0].responseHeaders());
    result.response_trailers = try convertView(allocator, calls[0].responseTrailers());

    // Walk the messages first: a message this client cannot read resolves the
    // call from inside `nextMessage`, so the status is only final after this.
    var payloads: std.ArrayList(messages.ConformancePayload) = .empty;
    while (try calls[0].nextMessage()) |found| {
        var holder_reader: Io.Reader = .fixed(found.bytes);
        const holder = pb.decodeLeaky(
            messages.PayloadHolder,
            allocator,
            &holder_reader,
            .{},
        ) catch {
            return error.MalformedResponse;
        };
        try payloads.append(allocator, holder.payload orelse .{});
    }

    const call_status = calls[0].result().?;
    if (!call_status.isOk()) {
        // The text borrows the call's arena, and the call is freed when this
        // function returns; the response is encoded after that.
        var call_error: messages.Error = .{
            .code = @intFromEnum(call_status.code),
            .message = try allocator.dupe(u8, call_status.message),
        };
        // `grpc-status-details-bin` is a serialized `google.rpc.Status`; the
        // suite compares the details it carries.
        const details = calls[0].errorDetails();
        std.debug.print("DETAILS {d} bytes\n", .{details.len});
        if (details.len != 0) {
            var status_reader: Io.Reader = .fixed(details);
            const call_details = pb.decodeLeaky(
                messages.RpcStatus,
                allocator,
                &status_reader,
                .{},
            ) catch messages.RpcStatus{};
            std.debug.print("DECODED {d} details\n", .{call_details.details.len});
            call_error.details = call_details.details;
        }
        result.err = call_error;
    }

    result.payloads = payloads.items;

    // A unary call carries exactly one response message. Zero or two means the
    // server answered something else.
    if (result.err == null and payloads.items.len != 1) {
        result.err = .{
            .code = 12,
            .message = "a unary response carries exactly one message",
        };
    }
    // A failing unary call delivers no message, whatever arrived on the wire.
    if (result.err != null) result.payloads = &.{};

    return result;
}

/// The suite models one header entry with repeated values; HTTP has one line per
/// value, so one entry becomes several fields.
fn convertHeaders(
    allocator: std.mem.Allocator,
    headers: []const messages.Header,
) ![]const http.Header {
    var count: usize = 0;
    for (headers) |header| count += @max(header.value.len, 1);

    const fields = try allocator.alloc(http.Header, count);
    var index: usize = 0;
    for (headers) |header| {
        if (header.value.len == 0) {
            fields[index] = .{ .name = header.name, .value = "" };
            index += 1;
            continue;
        }
        for (header.value) |value| {
            fields[index] = .{ .name = header.name, .value = value };
            index += 1;
        }
    }
    return fields;
}

/// The reverse: one entry per name, with its values in wire order. The view
/// borrows the connection's arena, so every string is copied before the next
/// poll can reclaim it.
fn convertView(
    allocator: std.mem.Allocator,
    view: ?http.Headers,
) ![]const messages.Header {
    const headers = view orelse return &.{};

    var converted: std.ArrayList(messages.Header) = .empty;
    var index: usize = 0;
    while (index < headers.items.len) : (index += 1) {
        const name = headers.items[index].name;

        var values: std.ArrayList([]const u8) = .empty;
        var scan = index;
        while (scan < headers.items.len and std.mem.eql(u8, headers.items[scan].name, name)) : (scan += 1) {
            try values.append(allocator, try allocator.dupe(u8, headers.items[scan].value));
        }
        try converted.append(allocator, .{
            .name = try allocator.dupe(u8, name),
            .value = values.items,
        });
        index = scan - 1;
    }
    return converted.items;
}
