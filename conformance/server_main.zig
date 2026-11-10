//! A server under test for the connectrpc conformance runner.
//!
//! In server mode the runner writes one `ServerCompatRequest` to this program's
//! stdin and then expects one length-prefixed `ServerCompatResponse` on stdout,
//! carrying the host and port it should dial. This program starts a gRPC server,
//! reports where it listens, and serves the conformance service until the
//! runner terminates the process. TLS credentials come from the runner.

const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const ascii = std.ascii;
const Allocator = mem.Allocator;
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const compress = @import("compress");
const grpc = @import("grpc");
const grpc_tls = @import("grpc_tls");
const http = @import("http");
const pb = @import("protobuf");
const tls = @import("tls");

const messages = @import("messages.zig");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.arena.allocator();

    var in_buffer: [16 * 1024]u8 = undefined;
    var stdin_reader = Io.File.stdin().reader(io, &in_buffer);
    const reader = &stdin_reader.interface;

    var out_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = Io.File.stdout().writer(io, &out_buffer);
    const writer = &stdout_writer.interface;

    var length_bytes: [4]u8 = undefined;
    try reader.readSliceAll(&length_bytes);
    const length = mem.readInt(u32, &length_bytes, .big);
    const body = try gpa.alloc(u8, length);
    try reader.readSliceAll(body);

    var request_reader: Io.Reader = .fixed(body);
    const request = try pb.decodeLeaky(messages.ServerCompatRequest, gpa, &request_reader, .{});

    if (request.protocol != messages.protocol_grpc) return error.UnsupportedProtocol;
    var app: StreamingApp = .{ .app = .{ .limit = request.message_receive_limit } };
    const handler: grpc.server.StreamingHandler = .init(&app, StreamingApp.handle);
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
        const chain: tls.CertificateChain = try .fromPem(creds.cert, der, &chain_storage);
        var key: tls.signature.PrivateKey = try .fromPemAuto(creds.key);
        defer key.deinit();
        try key.pairsWith(chain.slice[0]);

        var secure_server: grpc_tls.ReadinessServer = undefined;
        try secure_server.initStreaming(io, bind_address, handler, .{
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
    try server.initStreaming(io, bind_address, handler, .{
        .allocator = init.gpa,
        .max_connections = 1024,
        .body_bytes = body_bytes,
    });
    defer server.deinit();
    try reportAndRun(gpa, writer, &server, "127.0.0.1", "");
}

fn reportAndRun(
    allocator: Allocator,
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
    mem.writeInt(u32, &size, @intCast(encoded.written().len), .big);
    try writer.writeAll(&size);
    try writer.writeAll(encoded.written());
    try writer.flush();
    try server.run();
}

/// The response payloads and errors, independent of transport scheduling.
const App = struct {
    /// The largest message the runner says it will send. A bigger one must be
    /// rejected with RESOURCE_EXHAUSTED, which several cases check.
    limit: usize = 0,

    fn handle(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) anyerror!void {
        if (!accept(request, response)) return;
        return self.route(request, response);
    }

    fn accept(request: grpc.server.Request, response: *grpc.server.Response) bool {
        const content_type = request.metadata.get("content-type") orelse "";
        const end = mem.findScalar(u8, content_type, ';') orelse content_type.len;
        const media = mem.trim(u8, content_type[0..end], " \t");
        if (!ascii.eqlIgnoreCase(media, "application/grpc") and
            !ascii.eqlIgnoreCase(media, "application/grpc+proto"))
        {
            response.status = .{ .code = .unimplemented, .message = "unsupported codec" };
            return false;
        }
        const encoding = request.metadata.get("grpc-encoding") orelse "identity";
        if (!mem.eql(u8, encoding, "identity") and !mem.eql(u8, encoding, "gzip")) {
            response.status = .{ .code = .unimplemented, .message = "unsupported encoding" };
            return false;
        }
        return true;
    }

    fn route(self: *App, request: grpc.server.Request, response: *grpc.server.Response) !void {
        const slash = mem.findScalarLast(u8, request.path, '/');
        const method = if (slash) |at| request.path[at + 1 ..] else request.path;

        if (mem.eql(u8, method, "Unary") or mem.eql(u8, method, "IdempotentUnary")) {
            return self.unary(request, response);
        }
        if (mem.eql(u8, method, "ClientStream")) {
            return self.clientStream(request, response);
        }
        if (mem.eql(u8, method, "ServerStream")) {
            return self.serverStream(request, response);
        }
        if (mem.eql(u8, method, "BidiStream")) {
            return self.bidiStream(request, response);
        }
        if (mem.eql(u8, method, "Unimplemented")) {
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

    /// Build one answer from the first request's definition after upload completion.
    fn clientStream(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) !void {
        const kind = "ClientStreamRequest";

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

    /// Build half-duplex replies after upload completion. StreamingApp owns full-duplex.
    fn bidiStream(
        self: *App,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) !void {
        const kind = "BidiStreamRequest";

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

    /// The wrapped request bytes the suite expects in `request_info`.
    fn oneAny(
        comptime kind: []const u8,
        arena: Allocator,
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
        arena: Allocator,
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
        arena: Allocator,
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
        arena: Allocator,
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
        arena: Allocator,
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

    fn convert(arena: Allocator, fields: []const messages.Header) ![]const http.Header {
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

    fn headersOf(arena: Allocator, fields: http.Headers) ![]const messages.Header {
        var out: std.ArrayList(messages.Header) = .empty;
        var index: usize = 0;
        while (index < fields.items.len) : (index += 1) {
            const name = fields.items[index].name;
            var values: std.ArrayList([]const u8) = .empty;
            var scan = index;
            while (scan < fields.items.len and
                mem.eql(u8, fields.items[scan].name, name)) : (scan += 1)
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
    arena: Allocator,
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

fn encodeTest(arena: Allocator, value: anytype) ![]const u8 {
    var out: Io.Writer.Allocating = .init(arena);
    try pb.encode(value, .{}, &out.writer);
    return out.written();
}

test "a PEM without a certificate returns an error before key access" {
    var der: [128]u8 = undefined;
    var chain: [16][]const u8 = undefined;
    for ([_][]const u8{ "", "not pem", "-----BEGIN PRIVATE KEY-----" }) |pem| {
        try testing.expectError(
            error.NoCertificate,
            tls.CertificateChain.fromPem(pem, &der, &chain),
        );
    }
}

test "the TLS client public methods compile" {
    testing.refAllDecls(grpc_tls.Connection);
}

test "a failed TLS client initializer leaves its socket with the caller" {
    const address: Io.net.IpAddress = try .parse("127.0.0.1", 0);
    var listener = try address.listen(testing.io, .{ .mode = .stream });
    defer listener.deinit(testing.io);
    const socket = try listener.socket.address.connect(testing.io, .{ .mode = .stream });
    defer socket.close(testing.io);
    var connection: grpc_tls.Connection = undefined;
    try testing.expectError(error.MissingEndCertificateMarker, connection.init(
        testing.allocator,
        testing.io,
        socket,
        .{
            .ca_pem = "-----BEGIN CERTIFICATE-----\n",
            .host_name = "localhost",
            .now_sec = 0,
            .deadline_ns = 0,
        },
    ));
    const peer = try listener.accept(testing.io);
    defer peer.close(testing.io);
    var writer = socket.writer(testing.io, &.{});
    try writer.interface.writeAll("kept");
    try writer.interface.flush();
    var buffer: [4]u8 = undefined;
    var reader = peer.reader(testing.io, &buffer);
    try testing.expectEqualStrings("kept", try reader.interface.take(4));
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
        var arena: ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        var app: App = .{};
        var request = testRequest(arena.allocator(), "/test/Unary", &.{.{
            .compressed = false,
            .bytes = "",
        }});
        request.metadata = .{ .items = &.{.{ .name = "content-type", .value = content_type }} };
        var response: grpc.server.Response = .{};
        try app.handle(request, &response);
        try testing.expectEqual(grpc.status.Code.ok, response.status.code);
        try testing.expectEqual(@as(usize, 1), response.messages.len);
    }
}

test "an unsupported codec stays an application error rather than HTTP 415" {
    for ([_][]const u8{ "application/grpc+json", "application/grpc+foo; charset=utf-8" }) |media| {
        var arena: ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        var app: App = .{};
        var request = testRequest(arena.allocator(), "/test/Unary", &.{.{
            .compressed = false,
            .bytes = "",
        }});
        request.metadata = .{ .items = &.{.{ .name = "content-type", .value = media }} };
        var response: grpc.server.Response = .{};
        try app.handle(request, &response);
        try testing.expectEqual(grpc.status.Code.unimplemented, response.status.code);
        try testing.expectEqualStrings("unsupported codec", response.status.message);
        try testing.expectEqual(@as(usize, 0), response.messages.len);
        try testing.expectEqual(@as(usize, 0), response.details.len);
    }
}

test "an empty client stream returns request info without an error" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var app: App = .{};
    var response: grpc.server.Response = .{};
    try app.handle(testRequest(arena.allocator(), "/test/ClientStream", &.{}), &response);
    try testing.expectEqual(grpc.status.Code.ok, response.status.code);
    try testing.expectEqual(@as(usize, 1), response.messages.len);
    var reader: Io.Reader = .fixed(response.messages[0]);
    const reply = try pb.decodeLeaky(messages.PayloadHolder, arena.allocator(), &reader, .{});
    try testing.expectEqual(@as(usize, 0), reply.payload.?.request_info.?.requests.len);
    try testing.expectEqual(@as(usize, 0), reply.payload.?.data.len);
    try testing.expectEqual(@as(usize, 0), response.details.len);
}

test "an empty bidi stream returns no messages or error details" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var app: App = .{};
    var response: grpc.server.Response = .{};
    try app.handle(testRequest(arena.allocator(), "/test/BidiStream", &.{}), &response);
    try testing.expectEqual(grpc.status.Code.ok, response.status.code);
    try testing.expectEqual(@as(usize, 0), response.messages.len);
    try testing.expectEqual(@as(usize, 0), response.details.len);
}

test "request data is not an implicit unary response definition" {
    var arena: ArenaAllocator = .init(testing.allocator);
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
        var app: App = .{};
        var response: grpc.server.Response = .{};
        try app.handle(testRequest(allocator, case.path, &.{.{
            .compressed = false,
            .bytes = case.body,
        }}), &response);
        try testing.expectEqual(grpc.status.Code.ok, response.status.code);
        try testing.expectEqual(@as(usize, 1), response.messages.len);
        var reader: Io.Reader = .fixed(response.messages[0]);
        const reply = try pb.decodeLeaky(messages.PayloadHolder, allocator, &reader, .{});
        try testing.expectEqual(@as(usize, 0), reply.payload.?.data.len);
        try testing.expectEqual(@as(usize, 1), reply.payload.?.request_info.?.requests.len);
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
        var request = testRequest(testing.allocator, "/test/Unary", &.{});
        request.metadata = .{ .items = &.{.{ .name = "grpc-timeout", .value = case.raw }} };
        try testing.expectEqual(case.ms, App.timeoutOf(request));
    }
}

test "definition details precede request info only before response messages" {
    var arena: ArenaAllocator = .init(testing.allocator);
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
        var app: App = .{};
        var response: grpc.server.Response = .{};
        try app.handle(testRequest(allocator, case.path, &.{.{
            .compressed = false,
            .bytes = case.body,
        }}), &response);
        try testing.expectEqual(grpc.status.Code.aborted, response.status.code);
        try testing.expectEqual(case.responses, response.messages.len);
        var reader: Io.Reader = .fixed(response.details);
        const result = try pb.decodeLeaky(messages.RpcStatus, allocator, &reader, .{});
        try testing.expectEqual(case.details, result.details.len);
        try testing.expectEqualStrings(given.type_url, result.details[0].type_url);
        try testing.expectEqualStrings(given.value, result.details[0].value);
        if (case.details == 2) {
            try testing.expectEqualStrings(
                "type.googleapis.com/connectrpc.conformance.v1.ConformancePayload.RequestInfo",
                result.details[1].type_url,
            );
        }
    }
}

/// Scheduling and upload state for each independent call.
const StreamingApp = struct {
    app: App,

    fn handle(self: *StreamingApp, stream: *grpc.server.Stream) !void {
        stream.message_limit = if (self.app.limit == 0)
            std.math.maxInt(u32)
        else
            @intCast(@min(self.app.limit, std.math.maxInt(u32)));
        const state: *State = if (stream.context) |saved|
            @ptrCast(@alignCast(saved))
        else blk: {
            const state = try stream.exchange.arena.create(State);
            state.* = .{};
            stream.context = state;
            if (!App.accept(state.request(stream), &state.response)) {
                state.input_complete = true;
                state.final_ready = true;
            }
            break :blk state;
        };
        return state.advance(&self.app, stream);
    }

    const State = struct {
        received: std.ArrayList(grpc.server.Message) = .empty,
        since_response: std.ArrayList(messages.Any) = .empty,
        response: grpc.server.Response = .{},
        definition: messages.StreamResponseDefinition = .{},
        first: bool = true,
        full_duplex: bool = false,
        input_complete: bool = false,
        final_ready: bool = false,
        delay_ms: u32 = 0,
        sent_count: u32 = 0,
        queued: []const []const u8 = &.{},
        queue_index: u32 = 0,
        pending: []const u8 = &.{},
        due_ns: ?i96 = null,

        fn request(self: *const State, stream: *grpc.server.Stream) grpc.server.Request {
            return .{
                .path = stream.exchange.request.head.path orelse "",
                .messages = self.received.items,
                .metadata = stream.exchange.request.head.headers,
                .arena = stream.exchange.arena,
            };
        }

        fn advance(self: *State, app: *App, stream: *grpc.server.Stream) !void {
            while (true) {
                if (stream.expired()) {
                    if (self.pending.len != 0 and self.pending.len <
                        grpc.message.prefix_len + self.queued[self.queue_index].len)
                    {
                        // Trailers after a partial frame turn a timeout into a
                        // framing error. Abort the stream instead.
                        try stream.exchange.resetStream(.cancel);
                        stream.exchange.setDeadline(null);
                        stream.finished = true;
                        return;
                    }
                    self.pending = &.{};
                    self.response.status = .{
                        .code = .deadline_exceeded,
                        .message = "deadline exceeded",
                    };
                    self.response.details = &.{};
                    return self.finish(stream);
                }
                if (!try self.flush(stream)) return;
                if (self.input_complete) {
                    if (self.full_duplex) {
                        if (self.sent_count < self.definition.response_data.len) {
                            try self.queueFull(stream, false);
                            continue;
                        }
                        try self.finishFull(app, stream);
                    } else if (!self.final_ready) {
                        try app.handle(self.request(stream), &self.response);
                        self.queued = self.response.messages;
                        self.final_ready = true;
                        if (self.queued.len != 0) continue;
                    }
                    return self.finish(stream);
                }
                const found = stream.next() catch |err| switch (err) {
                    error.Pending => return stream.park(null),
                    else => return err,
                };
                if (found) |one| {
                    try self.receive(app, stream, one);
                } else self.input_complete = true;
            }
        }

        fn receive(
            self: *State,
            app: *App,
            stream: *grpc.server.Stream,
            one: grpc.message.Message,
        ) !void {
            const arena = stream.exchange.arena;
            const raw: grpc.server.Message = .{
                .compressed = one.compressed,
                .bytes = try arena.dupe(u8, one.bytes),
            };
            var check = self.request(stream);
            check.messages = &.{raw};
            if (try app.overLimit(arena, check, &self.response)) {
                self.input_complete = true;
                self.final_ready = true;
                self.full_duplex = false;
                return;
            }
            const plain = (try app.plain(arena, raw, &self.response)) orelse {
                self.input_complete = true;
                self.final_ready = true;
                self.full_duplex = false;
                return;
            };
            try self.received.append(arena, raw);
            if (self.first) {
                try self.firstMessage(stream, plain);
                self.first = false;
            }
            if (!self.full_duplex) return;
            try self.since_response.append(arena, .{
                .type_url = "type.googleapis.com/connectrpc.conformance.v1.BidiStreamRequest",
                .value = plain,
            });
            if (self.sent_count >= self.definition.response_data.len) {
                self.input_complete = true;
                return;
            }
            try self.queueFull(stream, true);
        }

        fn firstMessage(self: *State, stream: *grpc.server.Stream, plain: []const u8) !void {
            const request_value = self.request(stream);
            const arena = request_value.arena;
            const path = request_value.path;
            var reader: Io.Reader = .fixed(plain);
            if (mem.endsWith(u8, path, "/Unary") or
                mem.endsWith(u8, path, "/IdempotentUnary"))
            {
                const decoded = try pb.decodeLeaky(messages.UnaryRequest, arena, &reader, .{});
                self.delay_ms = if (decoded.response_definition) |d| d.response_delay_ms else 0;
            } else if (mem.endsWith(u8, path, "/ClientStream")) {
                const decoded = try pb.decodeLeaky(
                    messages.ClientStreamRequest,
                    arena,
                    &reader,
                    .{},
                );
                self.delay_ms = if (decoded.response_definition) |d| d.response_delay_ms else 0;
            } else if (mem.endsWith(u8, path, "/ServerStream")) {
                const decoded = try pb.decodeLeaky(
                    messages.ServerStreamRequest,
                    arena,
                    &reader,
                    .{},
                );
                self.delay_ms = if (decoded.response_definition) |d| d.response_delay_ms else 0;
            } else if (mem.endsWith(u8, path, "/BidiStream")) {
                const decoded = try pb.decodeLeaky(messages.BidiStreamRequest, arena, &reader, .{});
                self.definition = decoded.response_definition orelse .{};
                self.full_duplex = decoded.full_duplex;
                self.delay_ms = self.definition.response_delay_ms;
                if (self.full_duplex) {
                    self.response.headers = try App.convert(
                        arena,
                        self.definition.response_headers,
                    );
                    self.response.metadata = try App.convert(
                        arena,
                        self.definition.response_trailers,
                    );
                }
            }
        }

        fn queueFull(self: *State, stream: *grpc.server.Stream, from_request: bool) !void {
            const request_value = self.request(stream);
            const first = self.sent_count == 0;
            const info: ?messages.ConformancePayload.RequestInfo = if (first or from_request) .{
                .request_headers = if (first)
                    try App.headersOf(request_value.arena, request_value.metadata)
                else
                    &.{},
                .timeout_ms = if (first) App.timeoutOf(request_value) else null,
                .requests = self.since_response.items,
            } else null;
            var encoded: Io.Writer.Allocating = .init(request_value.arena);
            try pb.encode(messages.UnaryResponse{ .payload = .{
                .data = self.definition.response_data[self.sent_count],
                .request_info = info,
            } }, .{}, &encoded.writer);
            const output = try request_value.arena.alloc([]const u8, 1);
            output[0] = encoded.written();
            self.queued = output;
            self.since_response.clearRetainingCapacity();
        }

        /// The pending cursor survives both a delayed response and zero credit.
        fn flush(self: *State, stream: *grpc.server.Stream) !bool {
            if (self.queued.len != 0 and !stream.exchange.response_started) {
                try stream.begin(self.response.headers);
            }
            while (self.queue_index < self.queued.len) {
                if (self.pending.len == 0) {
                    if (self.due_ns == null) {
                        self.due_ns = stream.now() + @as(i96, self.delay_ms) * std.time.ns_per_ms;
                    }
                    if (stream.now() < self.due_ns.?) {
                        stream.park(self.due_ns);
                        return false;
                    }
                    const data = self.queued[self.queue_index];
                    const buffer = try stream.exchange.arena.alloc(
                        u8,
                        grpc.message.prefix_len + data.len,
                    );
                    self.pending = try grpc.message.write(buffer, data, false);
                }
                while (self.pending.len != 0) {
                    const count = try stream.write(self.pending);
                    if (count == 0) {
                        stream.park(null);
                        return false;
                    }
                    self.pending = self.pending[count..];
                }
                self.queue_index += 1;
                self.sent_count += 1;
                self.due_ns = null;
            }
            self.queued = &.{};
            self.queue_index = 0;
            return true;
        }

        fn finishFull(self: *State, app: *App, stream: *grpc.server.Stream) !void {
            if (self.definition.err) |failure| {
                // The payload already carried RequestInfo after the first send.
                self.response.messages = if (self.sent_count != 0) &.{""} else &.{};
                try app.failWith(
                    "BidiStreamRequest",
                    stream.exchange.arena,
                    self.request(stream),
                    &self.response,
                    @fromBackingInt(@intCast(@as(u8, @intCast(failure.code)))),
                    failure.message orelse "",
                    failure.details,
                );
            }
        }

        fn finish(self: *State, stream: *grpc.server.Stream) !void {
            if (!stream.exchange.response_started) try stream.begin(self.response.headers);
            try stream.finish(self.response.status, self.response.metadata, self.response.details);
        }
    };
};

const TestOutput = struct {
    const Sink = @FieldType(http.h2.engine.Exchange, "sink");

    body: Io.Writer.Allocating,
    credit: u32 = std.math.maxInt(u32),
    heads: u32 = 0,
    trailers: []const http.Header = &.{},
    block_end: bool = false,
    block_begin: bool = false,
    resets: u32 = 0,

    fn exchange(self: *TestOutput, arena: Allocator) http.h2.engine.Exchange {
        return .{
            .id = 0,
            .stream_id = 1,
            .request = .{
                .head = .{
                    .method = .POST,
                    .method_token = "POST",
                    .scheme = "http",
                    .authority = "localhost",
                    .path = "/test/ServerStream",
                    .protocol = null,
                    .headers = .{ .items = &.{} },
                },
                .body = &.{},
                .trailers = .{ .items = &.{} },
            },
            .arena = arena,
            .request_body = null,
            .reset_code = null,
            .response_started = false,
            .sink = .{ .ptr = self, .vtable = &.{
                .buffered = buffered,
                .begin = begin,
                .write = write,
                .end = end,
                .reset = reset,
            } },
            .parked = false,
            .context = null,
            .wake = .progress,
            .deadline_ns = null,
        };
    }

    fn buffered(
        _: *anyopaque,
        _: http.h2.engine.ResponseOptions,
        _: []const u8,
    ) Sink.SinkError!void {
        unreachable;
    }

    fn begin(ptr: *anyopaque, _: http.h2.engine.ResponseOptions) Sink.SinkError!void {
        const self: *TestOutput = @ptrCast(@alignCast(ptr));
        if (self.block_begin) return error.WouldBlock;
        self.heads += 1;
    }

    fn write(ptr: *anyopaque, bytes: []const u8) Sink.SinkError!usize {
        const self: *TestOutput = @ptrCast(@alignCast(ptr));
        const count = @min(self.credit, bytes.len);
        self.body.writer.writeAll(bytes[0..count]) catch return error.OutOfMemory;
        self.credit -= @intCast(count);
        return count;
    }

    fn end(ptr: *anyopaque, trailers: []const http.Header) Sink.SinkError!void {
        const self: *TestOutput = @ptrCast(@alignCast(ptr));
        if (self.block_end) return error.WouldBlock;
        self.trailers = trailers;
    }

    fn reset(ptr: *anyopaque, code: http.h2.frame.ErrorCode) Sink.SinkError!void {
        const self: *TestOutput = @ptrCast(@alignCast(ptr));
        std.debug.assert(code == .cancel);
        self.resets += 1;
    }
};

test "a later deadline preserves sent messages and a blocked write retains its tail" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var output: TestOutput = .{ .body = .init(arena.allocator()), .credit = 2 };
    var exchange = output.exchange(arena.allocator());
    var stream = grpc.server.Stream.init(testing.io, &exchange);
    var app: App = .{};
    var state: StreamingApp.State = .{
        .input_complete = true,
        .final_ready = true,
        .queued = &.{ "one", "two" },
        .delay_ms = 60_000,
        .due_ns = stream.now() - 1,
    };

    try state.advance(&app, &stream);
    try testing.expect(exchange.parked);
    try testing.expectEqual(@as(usize, 2), output.body.written().len);
    try testing.expectEqual(@as(usize, 6), state.pending.len);
    try testing.expectEqual(@as(u32, 0), state.sent_count);

    output.credit = 1024;
    exchange.parked = false;
    try state.advance(&app, &stream);
    try testing.expect(exchange.parked);
    try testing.expectEqual(@as(u32, 1), state.sent_count);
    var expected_buffer: [16]u8 = undefined;
    const expected = try grpc.message.write(&expected_buffer, "one", false);
    try testing.expectEqualSlices(u8, expected, output.body.written());

    // The next message is still delayed. Expiry sends status 4 after the
    // complete first message, without a second head or a second message.
    stream.deadline_ns = stream.now() - 1;
    try state.advance(&app, &stream);
    try testing.expect(stream.finished);
    try testing.expectEqual(@as(u32, 1), output.heads);
    try testing.expectEqualSlices(u8, expected, output.body.written());
    const trailers: http.Headers = .{ .items = output.trailers };
    try testing.expectEqualStrings("4", trailers.get("grpc-status").?);
    try testing.expect(trailers.get("grpc-status-details-bin") == null);
    try testing.expect(exchange.deadline_ns == null);
}

test "a blocked deadline trailer waits for credit without another expired timer" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var output: TestOutput = .{ .body = .init(arena.allocator()), .block_end = true };
    var exchange = output.exchange(arena.allocator());
    exchange.request.head.headers = .{ .items = &.{.{ .name = "grpc-timeout", .value = "0n" }} };
    var stream = grpc.server.Stream.init(testing.io, &exchange);
    var app: App = .{};
    var state: StreamingApp.State = .{};
    try testing.expect(stream.deadline_ns != null);
    try testing.expectError(error.WouldBlock, state.advance(&app, &stream));
    stream.park(null);
    try testing.expect(exchange.parked);
    try testing.expect(exchange.deadline_ns == null);
    try testing.expect(!stream.finished);
    try testing.expectEqual(@as(usize, 0), output.body.written().len);

    output.block_end = false;
    try state.advance(&app, &stream);
    try testing.expect(stream.finished);
    try testing.expectEqual(@as(u32, 1), output.heads);
    const trailers: http.Headers = .{ .items = output.trailers };
    try testing.expectEqualStrings("4", trailers.get("grpc-status").?);
    try testing.expect(trailers.get("grpc-status-details-bin") == null);
}

test "configured unlimited and large limits replace the stream default before its first read" {
    const Source = struct {
        const Self = @This();
        sent: bool = false,

        fn next(ptr: *anyopaque) http.stream.ReadError!http.stream.ReadResult {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.sent) return error.Pending;
            self.sent = true;
            // An envelope for 4 MiB + 1, without the body allocation.
            return .{ .data = "\x00\x00\x40\x00\x01" };
        }

        fn close(_: *anyopaque) void {}
    };
    for ([_]usize{ 0, 4 * 1024 * 1024 + 1, 64 }) |limit| {
        var arena: ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        var output: TestOutput = .{ .body = .init(arena.allocator()) };
        var exchange = output.exchange(arena.allocator());
        exchange.request.head.headers = .{ .items = &.{
            .{ .name = "content-type", .value = "application/grpc" },
            .{ .name = "grpc-timeout", .value = "60000m" },
        } };
        var source: Source = .{};
        exchange.request_body = .{ .ptr = &source, .vtable = &.{
            .next = Source.next,
            .close = Source.close,
        } };
        var stream = grpc.server.Stream.init(testing.io, &exchange);
        const deadline = stream.deadline_ns;
        var app: StreamingApp = .{ .app = .{ .limit = limit } };
        if (limit == 64) {
            try testing.expectError(error.TooLarge, app.handle(&stream));
            continue;
        }
        try app.handle(&stream);
        try testing.expect(exchange.parked);
        try testing.expect(!stream.finished);
        try testing.expectEqual(@as(u32, 4 * 1024 * 1024 + 1), stream.header.?.length);
        try app.handle(&stream);
        try testing.expectEqual(deadline, stream.deadline_ns);
        try testing.expectEqual(deadline, exchange.deadline_ns);
    }
}

test "a blocked final response head cannot rearm an observed RPC deadline" {
    var arena: ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var output: TestOutput = .{ .body = .init(arena.allocator()), .block_begin = true };
    var exchange = output.exchange(arena.allocator());
    exchange.request.head.headers = .{ .items = &.{
        .{ .name = "content-type", .value = "application/grpc" },
        .{ .name = "grpc-timeout", .value = "0n" },
    } };
    var app: StreamingApp = .{ .app = .{} };
    var adapter = grpc.server.Adapter.initStreaming(
        testing.io,
        grpc.server.StreamingHandler.init(&app, StreamingApp.handle),
    );
    const engine = adapter.engine();
    for (0..3) |_| {
        exchange.parked = false;
        try engine.vtable.handle(engine.ptr, &exchange);
        try testing.expect(exchange.parked);
        try testing.expect(exchange.deadline_ns == null);
        try testing.expect(!exchange.response_started);
        exchange.wake = .expired;
    }
    output.block_begin = false;
    try engine.vtable.handle(engine.ptr, &exchange);
    try testing.expectEqual(@as(u32, 1), output.heads);
    const trailers: http.Headers = .{ .items = output.trailers };
    try testing.expectEqualStrings("4", trailers.get("grpc-status").?);
}

test "expiry resets a partial frame but finishes cleanly before or after whole frames" {
    for ([_]u32{ 0, 2, 6, 8 }) |credit| {
        var arena: ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        var output: TestOutput = .{ .body = .init(arena.allocator()), .credit = credit };
        var exchange = output.exchange(arena.allocator());
        var stream = grpc.server.Stream.init(testing.io, &exchange);
        var app: App = .{};
        var state: StreamingApp.State = .{
            .input_complete = true,
            .final_ready = true,
            .queued = &.{ "one", "two" },
        };
        try state.advance(&app, &stream);
        try testing.expect(exchange.parked);
        try testing.expectEqual(credit, output.body.written().len);
        stream.deadline_ns = stream.now() - 1;
        try state.advance(&app, &stream);
        try testing.expect(stream.finished);
        try testing.expect(exchange.deadline_ns == null);
        if (credit == 2 or credit == 6) {
            try testing.expectEqual(@as(u32, 1), output.resets);
            try testing.expectEqual(@as(usize, 0), output.trailers.len);
        } else {
            try testing.expectEqual(@as(u32, 0), output.resets);
            const trailers: http.Headers = .{ .items = output.trailers };
            try testing.expectEqualStrings("4", trailers.get("grpc-status").?);
        }
    }
}
