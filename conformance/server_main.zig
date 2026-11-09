//! A server under test for the connectrpc conformance runner.
//!
//! In server mode the runner writes one `ServerCompatRequest` to this program's
//! stdin and then expects one length-prefixed `ServerCompatResponse` on stdout,
//! carrying the host and port it should dial. This program starts a gRPC server,
//! reports where it listens, and serves the conformance service until the
//! runner terminates the process. TLS credentials come from the runner.

const std = @import("std");
const grpc = @import("grpc");
const grpc_tls = @import("grpc_tls");
const tls = @import("tls");
const http = @import("http");
const pb = @import("protobuf");
const compress = @import("compress");
const messages = @import("messages.zig");

const Io = std.Io;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa: std.mem.Allocator = init.arena.allocator();

    var in_buffer: [16 * 1024]u8 = undefined;
    var stdin_reader = Io.File.stdin().reader(io, &in_buffer);
    const reader = &stdin_reader.interface;

    var out_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = Io.File.stdout().writer(io, &out_buffer);
    const writer = &stdout_writer.interface;

    var length_bytes: [4]u8 = undefined;
    try reader.readSliceAll(&length_bytes);
    const length = std.mem.readInt(u32, &length_bytes, .big);
    const body = try gpa.alloc(u8, length);
    try reader.readSliceAll(body);

    var request_reader: Io.Reader = .fixed(body);
    const request = try pb.decodeLeaky(messages.ServerCompatRequest, gpa, &request_reader, .{});

    if (request.protocol != messages.protocol_grpc) return error.UnsupportedProtocol;
    var app: App = .{ .io = io, .limit = request.message_receive_limit };
    const handler = grpc.server.Handler.init(&app, App.handle);
    const bind_address: Io.net.IpAddress = try .parseLiteral("127.0.0.1:0");

    // Several legal messages can share one HTTP body. The handler enforces
    // the per-message limit independently of this aggregate body budget.
    const body_bytes = 1024 * 1024;
    if (request.use_tls) {
        if (request.client_tls_cert.len != 0) return error.UnsupportedClientCertificate;
        const creds = request.server_creds orelse return error.MissingTlsCredentials;
        const der = try gpa.alloc(u8, creds.cert.len);
        // The harness accepts at most 16 certificates, including the leaf.
        var chain_storage: [16][]const u8 = undefined;
        const chain = try tls.CertificateChain.fromPem(creds.cert, der, &chain_storage);
        var key = try tls.signature.PrivateKey.fromPemAuto(creds.key);
        defer key.deinit();
        try key.pairsWith(chain.slice[0]);

        var secure_server: grpc_tls.ReadinessServer = undefined;
        try secure_server.init(io, bind_address, handler, .{
            .allocator = init.gpa,
            .max_connections = 1024,
            .common = .{ .body_bytes = body_bytes },
            .transport = .{
                .handshake = .{ .alpn_protocols = &.{"h2"} },
                .cert_chain = chain.slice,
                .signer = key.signer(),
                .alpn = "h2",
            },
        });
        defer secure_server.deinit();
        try reportAndRun(gpa, writer, &secure_server, "localhost", creds.cert);
        return;
    }

    var server: grpc.server.ReadinessServer = undefined;
    try server.init(io, bind_address, handler, .{
        .allocator = init.gpa,
        .max_connections = 1024,
        .body_bytes = body_bytes,
    });
    defer server.deinit();
    try reportAndRun(gpa, writer, &server, "127.0.0.1", "");
}

fn reportAndRun(
    allocator: std.mem.Allocator,
    writer: *Io.Writer,
    server: anytype,
    host: []const u8,
    pem_cert: []const u8,
) !void {
    const response: messages.ServerCompatResponse = .{
        .host = host,
        .port = server.address().getPort(),
        .pem_cert = pem_cert,
    };
    var encoded: Io.Writer.Allocating = .init(allocator);
    defer encoded.deinit();
    try pb.encode(response, .{}, &encoded.writer);
    var size: [4]u8 = undefined;
    std.mem.writeInt(u32, &size, @intCast(encoded.written().len), .big);
    try writer.writeAll(&size);
    try writer.writeAll(encoded.written());
    try writer.flush();
    try server.run();
}

/// The conformance service on the buffered server adapter.
const App = struct {
    io: Io,
    /// The largest message the runner says it will send. A bigger one must be
    /// rejected with RESOURCE_EXHAUSTED, which several cases check.
    limit: usize = 0,

    fn handle(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) anyerror!void {
        const content_type = request.metadata.get("content-type") orelse "";
        const end = std.mem.findScalar(u8, content_type, ';') orelse content_type.len;
        const media = std.mem.trim(u8, content_type[0..end], " \t");
        if (!std.ascii.eqlIgnoreCase(media, "application/grpc") and
            !std.ascii.eqlIgnoreCase(media, "application/grpc+proto"))
        {
            response.status = .{ .code = .unimplemented, .message = "unsupported codec" };
            return;
        }
        const encoding = request.metadata.get("grpc-encoding") orelse "identity";
        if (!std.mem.eql(u8, encoding, "identity") and !std.mem.eql(u8, encoding, "gzip")) {
            response.status = .{ .code = .unimplemented, .message = "unsupported encoding" };
            return;
        }
        const slash = std.mem.findScalarLast(u8, request.path, '/');
        const method = if (slash) |at| request.path[at + 1 ..] else request.path;

        if (std.mem.eql(u8, method, "Unary") or std.mem.eql(u8, method, "IdempotentUnary")) {
            return self.unary(request, response);
        }
        if (std.mem.eql(u8, method, "ClientStream")) {
            return self.clientStream(request, response);
        }
        if (std.mem.eql(u8, method, "ServerStream")) {
            return self.serverStream(request, response);
        }
        if (std.mem.eql(u8, method, "BidiStream")) {
            return self.bidiStream(request, response);
        }
        if (std.mem.eql(u8, method, "Unimplemented")) {
            response.status = .{
                .code = .unimplemented,
                .message = "unimplemented on purpose",
            };
            return;
        }
        response.status = .{ .code = .unimplemented, .message = "not implemented yet" };
    }

    fn unary(self: *App, request: grpc.server.Request, response: *grpc.server.Response) !void {
        const kind = "UnaryRequest";
        const deadline = self.expiresAt(request);
        if (request.messages.len != 1) {
            response.status = .{ .code = .unimplemented, .message = "unary takes one message" };
            return;
        }
        const arena = request.arena;
        if (try self.overLimit(arena, request, response)) return;

        // A message the client compressed arrives compressed. The call layer
        // flags it and leaves the bytes alone, so the codec above it decodes.
        const maybe_wire = try self.plain(arena, request.messages[0], response);
        const wire = maybe_wire orelse return;

        var reader: Io.Reader = .fixed(wire);
        const decoded = try pb.decodeLeaky(messages.UnaryRequest, arena, &reader, .{});
        const definition: messages.UnaryResponseDefinition =
            decoded.response_definition orelse .{};

        response.headers = try convert(arena, definition.response_headers);
        response.metadata = try convert(arena, definition.response_trailers);

        if (definition.err) |failure| {
            return self.failWith(
                kind,
                arena,
                request,
                response,
                @fromBackingInt(@intCast(@as(u8, @intCast(failure.code)))),
                failure.message orelse "",
                failure.details,
            );
        }
        if (!self.wait(deadline, definition.response_delay_ms, response)) return;

        const requests = try arena.alloc(messages.Any, 1);
        requests[0] = .{
            .type_url = "type.googleapis.com/connectrpc.conformance.v1.UnaryRequest",
            .value = wire,
        };

        const reply: messages.UnaryResponse = .{
            .payload = .{
                .data = definition.response_data,
                .request_info = .{
                    .request_headers = try headersOf(arena, request.metadata),
                    .timeout_ms = timeoutOf(request),
                    .requests = requests,
                },
            },
        };

        var encoded: Io.Writer.Allocating = .init(arena);
        try pb.encode(reply, .{}, &encoded.writer);
        const messages_out = try arena.alloc([]const u8, 1);
        messages_out[0] = encoded.written();
        response.messages = messages_out;
    }

    /// Every request message has arrived by the time a buffered engine calls
    /// the handler, so a client stream is one answer built from the first
    /// request's definition.
    fn clientStream(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) !void {
        const kind = "ClientStreamRequest";
        const deadline = self.expiresAt(request);
        const arena = request.arena;
        if (try self.overLimit(arena, request, response)) return;
        const definition: messages.UnaryResponseDefinition = blk: {
            if (request.messages.len == 0) break :blk .{};
            const wire = (try self.plain(arena, request.messages[0], response)) orelse return;
            var reader: Io.Reader = .fixed(wire);
            const decoded = try pb.decodeLeaky(messages.ClientStreamRequest, arena, &reader, .{});
            break :blk decoded.response_definition orelse .{};
        };

        response.headers = try convert(arena, definition.response_headers);
        response.metadata = try convert(arena, definition.response_trailers);
        if (definition.err) |failure| {
            return self.failWith(
                kind,
                arena,
                request,
                response,
                @fromBackingInt(@intCast(@as(u8, @intCast(failure.code)))),
                failure.message orelse "",
                failure.details,
            );
        }

        const requests = try arena.alloc(messages.Any, request.messages.len);
        for (request.messages, 0..) |one, at| {
            const body = (try self.plain(arena, one, response)) orelse return;
            requests[at] = .{
                .type_url = "type.googleapis.com/connectrpc.conformance.v1.ClientStreamRequest",
                .value = body,
            };
        }

        if (!self.wait(deadline, definition.response_delay_ms, response)) return;

        const payload: messages.ConformancePayload = .{
            .data = definition.response_data,
            .request_info = .{
                .request_headers = try headersOf(arena, request.metadata),
                .timeout_ms = timeoutOf(request),
                .requests = requests,
            },
        };
        const reply: messages.UnaryResponse = .{ .payload = payload };
        var encoded: Io.Writer.Allocating = .init(arena);
        try pb.encode(reply, .{}, &encoded.writer);
        const out = try arena.alloc([]const u8, 1);
        out[0] = encoded.written();
        response.messages = out;
    }

    /// One request, many replies: every entry in the definition becomes a
    /// response message.
    fn serverStream(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) !void {
        const kind = "ServerStreamRequest";
        const deadline = self.expiresAt(request);
        const arena = request.arena;
        if (try self.overLimit(arena, request, response)) return;
        if (request.messages.len != 1) {
            response.status = .{
                .code = .unimplemented,
                .message = "server stream takes one message",
            };
            return;
        }
        const wire = (try self.plain(arena, request.messages[0], response)) orelse return;

        var reader: Io.Reader = .fixed(wire);
        const decoded = try pb.decodeLeaky(messages.ServerStreamRequest, arena, &reader, .{});
        const definition: messages.StreamResponseDefinition =
            decoded.response_definition orelse .{};

        response.headers = try convert(arena, definition.response_headers);
        response.metadata = try convert(arena, definition.response_trailers);

        // No entries means no response messages. The suite has a case for it.
        const datas = definition.response_data;
        const out = try arena.alloc([]const u8, datas.len);
        for (datas, 0..) |data, at| {
            if (!self.wait(deadline, definition.response_delay_ms, response)) return;
            var encoded: Io.Writer.Allocating = .init(arena);
            const reply: messages.UnaryResponse = .{ .payload = .{
                .data = data,
                .request_info = if (at == 0)
                    .{
                        .request_headers = try headersOf(arena, request.metadata),
                        .timeout_ms = timeoutOf(request),
                        .requests = try oneAny("ServerStreamRequest", arena, wire),
                    }
                else
                    .{},
            } };
            try pb.encode(reply, .{}, &encoded.writer);
            out[at] = try arena.dupe(u8, encoded.written());
        }
        response.messages = out;

        // An error and its messages travel together: the definition can ask for
        // both, and the suite checks the count of each.
        if (definition.err) |failure| {
            return self.failWith(
                kind,
                arena,
                request,
                response,
                @fromBackingInt(@intCast(@as(u8, @intCast(failure.code)))),
                failure.message orelse "",
                failure.details,
            );
        }
    }

    /// Every request in, every reply out, in order. The buffered engine means a
    /// full-duplex call cannot answer mid-stream; it arrives complete.
    fn bidiStream(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) !void {
        const kind = "BidiStreamRequest";
        const deadline = self.expiresAt(request);
        const arena = request.arena;
        if (try self.overLimit(arena, request, response)) return;
        if (request.messages.len == 0) return;
        const bodies = try arena.alloc([]const u8, request.messages.len);
        for (request.messages, 0..) |one, at| {
            bodies[at] = (try self.plain(arena, one, response)) orelse return;
        }

        var first_reader: Io.Reader = .fixed(bodies[0]);
        const first = try pb.decodeLeaky(messages.BidiStreamRequest, arena, &first_reader, .{});
        const definition: messages.StreamResponseDefinition =
            first.response_definition orelse .{};

        response.headers = try convert(arena, definition.response_headers);
        response.metadata = try convert(arena, definition.response_trailers);

        // No entries means no response messages, the same rule the server
        // stream follows.
        const datas = definition.response_data;

        const described = try arena.alloc(messages.Any, request.messages.len);
        for (bodies, 0..) |body, at| {
            described[at] = .{
                .type_url = "type.googleapis.com/connectrpc.conformance.v1.BidiStreamRequest",
                .value = body,
            };
        }

        const out = try arena.alloc([]const u8, datas.len);
        for (datas, 0..) |data, at| {
            if (!self.wait(deadline, definition.response_delay_ms, response)) return;
            var encoded: Io.Writer.Allocating = .init(arena);
            const reply: messages.UnaryResponse = .{ .payload = .{
                .data = data,
                .request_info = if (at == 0)
                    .{
                        .request_headers = try headersOf(arena, request.metadata),
                        .timeout_ms = timeoutOf(request),
                        .requests = described,
                    }
                else
                    .{},
            } };
            try pb.encode(reply, .{}, &encoded.writer);
            out[at] = try arena.dupe(u8, encoded.written());
        }
        response.messages = out;

        // An error and its messages travel together: the definition can ask for
        // both, and the suite checks the count of each.
        if (definition.err) |failure| {
            return self.failWith(
                kind,
                arena,
                request,
                response,
                @fromBackingInt(@intCast(@as(u8, @intCast(failure.code)))),
                failure.message orelse "",
                failure.details,
            );
        }
    }

    /// A deadline for the handler's cooperative waits. The buffered adapter
    /// cannot measure time spent before the handler starts.
    fn expiresAt(self: *App, request: grpc.server.Request) ?i96 {
        const raw = request.metadata.get("grpc-timeout") orelse return null;
        const duration = grpc.timeout.parse(raw) catch return null;
        return Io.Clock.awake.now(self.io).nanoseconds + @as(i96, duration.toNanos());
    }

    /// Respect the same deadline across every response delay in one call.
    fn wait(self: *App, expires: ?i96, delay_ms: u32, response: *grpc.server.Response) bool {
        const now = Io.Clock.awake.now(self.io).nanoseconds;
        const requested: i96 = @as(i96, delay_ms) * std.time.ns_per_ms;
        const duration = if (expires) |at| @min(requested, @max(0, at - now)) else requested;
        self.io.sleep(.fromNanoseconds(duration), .awake) catch |err| switch (err) {
            error.Canceled => return false,
        };
        if (expires) |at| {
            if (Io.Clock.awake.now(self.io).nanoseconds >= at) {
                response.status = .{ .code = .deadline_exceeded, .message = "deadline exceeded" };
                response.messages = &.{};
                response.details = &.{};
                return false;
            }
        }
        return true;
    }

    /// The wrapped request bytes the suite expects in `request_info`.
    fn oneAny(
        comptime kind: []const u8,
        arena: std.mem.Allocator,
        wire: []const u8,
    ) ![]const messages.Any {
        const requests = try arena.alloc(messages.Any, 1);
        requests[0] = .{
            .type_url = "type.googleapis.com/connectrpc.conformance.v1." ++ kind,
            .value = wire,
        };
        return requests;
    }

    /// Enforce the limit on both the wire message and its decoded bytes.
    /// A transport rejection has no application error details.
    fn overLimit(
        self: *App,
        arena: std.mem.Allocator,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) !bool {
        if (self.limit == 0) return false;
        for (request.messages) |one| {
            if (one.bytes.len > self.limit) {
                response.status = .{
                    .code = .resource_exhausted,
                    .message = "message is larger than the server limit",
                };
                return true;
            }
            if (one.compressed) {
                if (try self.plain(arena, one, response) == null) return true;
            }
        }
        return false;
    }

    fn failWith(
        self: *App,
        comptime kind: []const u8,
        arena: std.mem.Allocator,
        request: grpc.server.Request,
        response: *grpc.server.Response,
        code: grpc.status.Code,
        message: []const u8,
        given: []const messages.Any,
    ) !void {
        response.status = .{ .code = code, .message = message };

        var details = given;
        // service.proto requires request info in an error only when no payload
        // carried it. Preserve the requested details and append the info.
        if (response.messages.len == 0) {
            const requests = try arena.alloc(messages.Any, request.messages.len);
            for (request.messages, 0..) |one, at| {
                requests[at] = .{
                    .type_url = "type.googleapis.com/connectrpc.conformance.v1." ++ kind,
                    .value = one.bytes,
                };
                if (one.compressed) {
                    var scratch: grpc.server.Response = .{};
                    if (try self.plain(arena, one, &scratch)) |body| requests[at].value = body;
                }
            }
            const info = try oneAnyPayload(arena, .{
                .request_headers = try headersOf(arena, request.metadata),
                .timeout_ms = timeoutOf(request),
                .requests = requests,
            });
            const combined = try arena.alloc(messages.Any, given.len + info.len);
            @memcpy(combined[0..given.len], given);
            @memcpy(combined[given.len..], info);
            details = combined;
        }
        const status_message: messages.RpcStatus = .{
            .code = @intCast(@backingInt(code)),
            .message = message,
            .details = details,
        };
        var encoded: Io.Writer.Allocating = .init(arena);
        try pb.encode(status_message, .{}, &encoded.writer);
        response.details = try arena.dupe(u8, encoded.written());
    }

    /// The deadline the client asked for, in milliseconds, as the suite wants it
    /// echoed back.
    fn timeoutOf(request: grpc.server.Request) ?i64 {
        const raw = request.metadata.get("grpc-timeout") orelse return null;
        const parsed = grpc.timeout.parse(raw) catch return null;
        return @intCast(parsed.toNanos() / std.time.ns_per_ms);
    }

    /// `request_info` wrapped as the single detail the suite looks for.
    fn oneAnyPayload(
        arena: std.mem.Allocator,
        request_info: messages.ConformancePayload.RequestInfo,
    ) ![]const messages.Any {
        var encoded: Io.Writer.Allocating = .init(arena);
        try pb.encode(request_info, .{}, &encoded.writer);
        const out = try arena.alloc(messages.Any, 1);
        out[0] = .{
            .type_url = "type.googleapis.com/connectrpc.conformance.v1." ++
                "ConformancePayload.RequestInfo",
            .value = try arena.dupe(u8, encoded.written()),
        };
        return out;
    }

    /// The request message with any compression removed, or null when the answer
    /// is already an error status.
    fn plain(
        self: *App,
        arena: std.mem.Allocator,
        one: grpc.server.Message,
        response: *grpc.server.Response,
    ) !?[]const u8 {
        if (!one.compressed) return one.bytes;
        var input: Io.Reader = .fixed(one.bytes);
        var buffer: compress.gzip.Reader.Buffer = undefined;
        var decoder: compress.gzip.Reader = .init(&input, &buffer);
        const limit: Io.Limit = if (self.limit == 0) .unlimited else .limited(self.limit);
        return decoder.reader.allocRemaining(arena, limit) catch |err| switch (err) {
            error.OutOfMemory => return err,
            error.StreamTooLong => {
                response.status = .{
                    .code = .resource_exhausted,
                    .message = "decoded message is larger than the server limit",
                };
                return null;
            },
            error.ReadFailed => {
                response.status = .{ .code = .internal, .message = "cannot decode gzip" };
                return null;
            },
        };
    }

    fn convert(arena: std.mem.Allocator, fields: []const messages.Header) ![]const http.Header {
        var count: usize = 0;
        for (fields) |field| count += @max(field.value.len, 1);
        const out = try arena.alloc(http.Header, count);
        var index: usize = 0;
        for (fields) |field| {
            if (field.value.len == 0) {
                out[index] = .{ .name = field.name, .value = "" };
                index += 1;
                continue;
            }
            for (field.value) |value| {
                out[index] = .{ .name = field.name, .value = value };
                index += 1;
            }
        }
        return out;
    }

    fn headersOf(arena: std.mem.Allocator, fields: http.Headers) ![]const messages.Header {
        var out: std.ArrayList(messages.Header) = .empty;
        var index: usize = 0;
        while (index < fields.items.len) : (index += 1) {
            const name = fields.items[index].name;
            var values: std.ArrayList([]const u8) = .empty;
            var scan = index;
            while (scan < fields.items.len and
                std.mem.eql(u8, fields.items[scan].name, name)) : (scan += 1)
            {
                try values.append(arena, fields.items[scan].value);
            }
            try out.append(arena, .{ .name = name, .value = values.items });
            index = scan - 1;
        }
        return out.items;
    }
};

fn testRequest(
    arena: std.mem.Allocator,
    path: []const u8,
    bodies: []const grpc.server.Message,
) grpc.server.Request {
    return .{
        .path = path,
        .messages = bodies,
        .metadata = .{ .items = &.{.{
            .name = "content-type",
            .value = "application/grpc+proto",
        }} },
        .arena = arena,
    };
}

fn encodeTest(arena: std.mem.Allocator, value: anytype) ![]const u8 {
    var out: Io.Writer.Allocating = .init(arena);
    try pb.encode(value, .{}, &out.writer);
    return out.written();
}

test "a PEM without a certificate returns an error before key access" {
    var der: [128]u8 = undefined;
    var chain: [16][]const u8 = undefined;
    for ([_][]const u8{ "", "not pem", "-----BEGIN PRIVATE KEY-----" }) |pem| {
        try std.testing.expectError(
            error.NoCertificate,
            tls.CertificateChain.fromPem(pem, &der, &chain),
        );
    }
}

test "the conformance codec accepts media type parameters and mixed case" {
    const supported = [_][]const u8{
        "application/grpc",
        "application/grpc+proto",
        "application/grpc; charset=utf-8",
        "application/grpc+proto; charset=utf-8",
        "Application/GRPC",
        "APPLICATION/GRPC+PROTO; charset=utf-8",
    };
    for (supported) |content_type| {
        var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
        defer arena.deinit();
        var app: App = .{ .io = std.testing.io };
        var request = testRequest(arena.allocator(), "/test/Unary", &.{.{
            .compressed = false,
            .bytes = "",
        }});
        request.metadata = .{ .items = &.{.{ .name = "content-type", .value = content_type }} };
        var response: grpc.server.Response = .{};
        try app.handle(request, &response);
        try std.testing.expectEqual(grpc.status.Code.ok, response.status.code);
        try std.testing.expectEqual(@as(usize, 1), response.messages.len);
    }
}

test "an unsupported codec stays an application error rather than HTTP 415" {
    for ([_][]const u8{ "application/grpc+json", "application/grpc+foo; charset=utf-8" }) |media| {
        var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
        defer arena.deinit();
        var app: App = .{ .io = std.testing.io };
        var request = testRequest(arena.allocator(), "/test/Unary", &.{.{
            .compressed = false,
            .bytes = "",
        }});
        request.metadata = .{ .items = &.{.{ .name = "content-type", .value = media }} };
        var response: grpc.server.Response = .{};
        try app.handle(request, &response);
        try std.testing.expectEqual(grpc.status.Code.unimplemented, response.status.code);
        try std.testing.expectEqualStrings("unsupported codec", response.status.message);
        try std.testing.expectEqual(@as(usize, 0), response.messages.len);
        try std.testing.expectEqual(@as(usize, 0), response.details.len);
    }
}

test "an empty client stream returns request info without an error" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    var app: App = .{ .io = std.testing.io };
    var response: grpc.server.Response = .{};
    try app.handle(testRequest(arena.allocator(), "/test/ClientStream", &.{}), &response);
    try std.testing.expectEqual(grpc.status.Code.ok, response.status.code);
    try std.testing.expectEqual(@as(usize, 1), response.messages.len);
    var reader: Io.Reader = .fixed(response.messages[0]);
    const reply = try pb.decodeLeaky(messages.PayloadHolder, arena.allocator(), &reader, .{});
    try std.testing.expectEqual(@as(usize, 0), reply.payload.?.request_info.?.requests.len);
    try std.testing.expectEqual(@as(usize, 0), reply.payload.?.data.len);
    try std.testing.expectEqual(@as(usize, 0), response.details.len);
}

test "an empty bidi stream returns no messages or error details" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    var app: App = .{ .io = std.testing.io };
    var response: grpc.server.Response = .{};
    try app.handle(testRequest(arena.allocator(), "/test/BidiStream", &.{}), &response);
    try std.testing.expectEqual(grpc.status.Code.ok, response.status.code);
    try std.testing.expectEqual(@as(usize, 0), response.messages.len);
    try std.testing.expectEqual(@as(usize, 0), response.details.len);
}

test "request data is not an implicit unary response definition" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const Case = struct { path: []const u8, body: []const u8 };
    const cases = [_]Case{
        .{
            .path = "/test/Unary",
            .body = try encodeTest(allocator, messages.UnaryRequest{ .request_data = "input" }),
        },
        .{
            .path = "/test/ClientStream",
            .body = try encodeTest(
                allocator,
                messages.ClientStreamRequest{ .request_data = "input" },
            ),
        },
    };
    for (cases) |case| {
        var app: App = .{ .io = std.testing.io };
        var response: grpc.server.Response = .{};
        try app.handle(testRequest(allocator, case.path, &.{.{
            .compressed = false,
            .bytes = case.body,
        }}), &response);
        try std.testing.expectEqual(grpc.status.Code.ok, response.status.code);
        try std.testing.expectEqual(@as(usize, 1), response.messages.len);
        var reader: Io.Reader = .fixed(response.messages[0]);
        const reply = try pb.decodeLeaky(messages.PayloadHolder, allocator, &reader, .{});
        try std.testing.expectEqual(@as(usize, 0), reply.payload.?.data.len);
        try std.testing.expectEqual(@as(usize, 1), reply.payload.?.request_info.?.requests.len);
    }
}

test "timeout echo and enforcement accept the same timeout grammar" {
    const Case = struct { raw: []const u8, ms: ?i64 };
    const cases = [_]Case{
        .{ .raw = "200m", .ms = 200 },
        .{ .raw = "1S", .ms = 1000 },
        .{ .raw = "1M", .ms = 60_000 },
        .{ .raw = "1H", .ms = 3_600_000 },
        .{ .raw = "500000u", .ms = 500 },
        .{ .raw = "1n", .ms = 0 },
        .{ .raw = "+1S", .ms = null },
        .{ .raw = "-1S", .ms = null },
        .{ .raw = "123456789m", .ms = null },
    };
    for (cases) |case| {
        var request = testRequest(std.testing.allocator, "/test/Unary", &.{});
        request.metadata = .{ .items = &.{.{ .name = "grpc-timeout", .value = case.raw }} };
        try std.testing.expectEqual(case.ms, App.timeoutOf(request));
    }
}

test "definition details precede request info only before response messages" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const given: messages.Any = .{ .type_url = "type.example/detail", .value = "given" };
    const failure: messages.Error = .{ .code = 10, .message = "planned", .details = &.{given} };
    const unary: messages.UnaryResponseDefinition = .{ .err = failure };
    const stream: messages.StreamResponseDefinition = .{ .err = failure };
    const stream_data: messages.StreamResponseDefinition = .{
        .err = failure,
        .response_data = &.{"one"},
    };
    const Case = struct { path: []const u8, body: []const u8, responses: u32, details: u32 };
    const cases = [_]Case{
        .{ .path = "/test/Unary", .body = try encodeTest(allocator, messages.UnaryRequest{
            .response_definition = unary,
        }), .responses = 0, .details = 2 },
        .{ .path = "/test/ClientStream", .body = try encodeTest(
            allocator,
            messages.ClientStreamRequest{ .response_definition = unary },
        ), .responses = 0, .details = 2 },
        .{ .path = "/test/ServerStream", .body = try encodeTest(
            allocator,
            messages.ServerStreamRequest{ .response_definition = stream },
        ), .responses = 0, .details = 2 },
        .{ .path = "/test/ServerStream", .body = try encodeTest(
            allocator,
            messages.ServerStreamRequest{ .response_definition = stream_data },
        ), .responses = 1, .details = 1 },
        .{ .path = "/test/BidiStream", .body = try encodeTest(allocator, messages.BidiStreamRequest{
            .response_definition = stream,
        }), .responses = 0, .details = 2 },
        .{ .path = "/test/BidiStream", .body = try encodeTest(allocator, messages.BidiStreamRequest{
            .response_definition = stream_data,
        }), .responses = 1, .details = 1 },
    };
    for (cases) |case| {
        var app: App = .{ .io = std.testing.io };
        var response: grpc.server.Response = .{};
        try app.handle(testRequest(allocator, case.path, &.{.{
            .compressed = false,
            .bytes = case.body,
        }}), &response);
        try std.testing.expectEqual(grpc.status.Code.aborted, response.status.code);
        try std.testing.expectEqual(case.responses, response.messages.len);
        var reader: Io.Reader = .fixed(response.details);
        const result = try pb.decodeLeaky(messages.RpcStatus, allocator, &reader, .{});
        try std.testing.expectEqual(case.details, result.details.len);
        try std.testing.expectEqualStrings(given.type_url, result.details[0].type_url);
        try std.testing.expectEqualStrings(given.value, result.details[0].value);
        if (case.details == 2) {
            try std.testing.expectEqualStrings(
                "type.googleapis.com/connectrpc.conformance.v1.ConformancePayload.RequestInfo",
                result.details[1].type_url,
            );
        }
    }
}
