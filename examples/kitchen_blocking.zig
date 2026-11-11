//! The kitchen sink, blocking edition: a threaded server and blocking clients
//! over real loopback sockets, in one process.
//!
//! Server: `grpc.server.serve` accepts on the `Io` executor and runs each
//! connection as its own concurrent task. The handler is buffered: it sees every
//! request message once the client has closed its side, and returns every
//! response message at once. That makes client streaming natural, and server
//! streaming and bidi possible only as "all in, then all out".
//!
//! Clients: each shows a different blocking shape.
//!   - `Client.unary`, the one-call convenience, for Shout.
//!   - `ServerStream`, pulling one message at a time, for Countdown.
//!   - `Client.open` plus the shared `Stream` facade, for Tally and Chat.
//!   - one connection multiplexing four `Stream` calls at once. Both kitchens
//!     use the same scripted drive loop; a blocking stream advances `step`.
//!
//! `zig build kitchen-blocking` runs it. It exits non-zero if any expectation
//! fails.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const print = std.debug.print;

const grpc = @import("grpc");

const kitchen = @import("kitchen.zig");

const Sink = struct {
    io: Io,
    /// Handlers run on several threads at once, so shared state is atomic.
    handled: std.atomic.Value(u32) = .init(0),

    fn handle(
        self: *Sink,
        request: grpc.server.Request,
        response: *grpc.server.Response,
    ) anyerror!void {
        _ = self.handled.fetchAdd(1, .monotonic);
        const method = kitchen.route(request.path) orelse
            return reject(response, .unimplemented, "no such method");
        const arena = request.arena;
        response.metadata = try kitchen.trailerFields(
            arena,
            "blocking",
            request.metadata.get("x-request-id"),
        );

        for (request.messages) |one| {
            if (one.compressed or one.bytes.len > kitchen.message_limit) {
                return reject(response, .resource_exhausted, "note too large");
            }
        }

        var out: std.ArrayList([]const u8) = .empty;
        var scratch: [kitchen.reply_capacity]u8 = undefined;
        switch (method) {
            .Shout => {
                if (request.messages.len != 1) {
                    return reject(response, .invalid_argument, "Shout takes one note");
                }
                const note = try kitchen.decode(kitchen.Note, arena, request.messages[0].bytes);
                const reply = kitchen.shout(note, &scratch) orelse
                    return reject(response, .invalid_argument, "note text is empty");
                try emit(arena, &out, reply);
            },
            .Tally => {
                var bytes: usize = 0;
                for (request.messages) |one| {
                    bytes += (try kitchen.decode(kitchen.Note, arena, one.bytes)).text.len;
                }
                const count: u32 = @intCast(request.messages.len);
                try emit(arena, &out, try kitchen.tallyReply(count, bytes, &scratch));
            },
            .Chat => for (request.messages) |one| {
                const note = try kitchen.decode(kitchen.Note, arena, one.bytes);
                try emit(arena, &out, try kitchen.chatReply(note, &scratch));
            },
            .Countdown => {
                if (request.messages.len != 1) {
                    return reject(response, .invalid_argument, "Countdown takes one request");
                }
                const ask = try kitchen.decode(kitchen.Countdown, arena, request.messages[0].bytes);
                // The blocking engine cannot interrupt a callback, so the
                // handler checks the client's deadline between sleeps and stops
                // instead of finishing a call nobody waits for.
                var remaining = ask.from;
                while (remaining > 0) : (remaining -= 1) {
                    if (remaining != ask.from) {
                        if (try request.expired(self.io)) {
                            return reject(response, .deadline_exceeded, "countdown too slow");
                        }
                        try self.io.sleep(.fromMilliseconds(ask.interval_ms), .awake);
                    }
                    try emit(arena, &out, kitchen.tick(remaining));
                }
            },
        }
        // The server reads these after this function returns, which is why
        // each message was copied into the exchange arena.
        response.messages = out.items;
    }

    fn reject(response: *grpc.server.Response, code: grpc.status.Code, text: []const u8) void {
        response.status = .{ .code = code, .message = text };
    }

    /// Encode one reply into the exchange arena, where the server can still
    /// read it after the handler returns.
    fn emit(arena: Allocator, out: *std.ArrayList([]const u8), value: anytype) !void {
        var wire: [kitchen.reply_capacity]u8 = undefined;
        try out.append(arena, try arena.dupe(u8, try kitchen.encode(&wire, value)));
    }
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    var sink: Sink = .{ .io = io };
    const address: Io.net.IpAddress = try .parseLiteral("127.0.0.1:0");
    var listener = try address.listen(io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(io);

    var target = address;
    target.setPort(listener.socket.address.getPort());
    var authority_buffer: [32]u8 = undefined;
    const authority = try std.mem.print(&authority_buffer, "127.0.0.1:{d}", .{target.getPort()});
    print("blocking server on {s}\n", .{authority});

    // `serve` runs until it is cancelled. It needs real concurrency, so ask for
    // it and fail loudly instead of serving connections one at a time.
    var server = try io.concurrent(grpc.server.serve, .{
        io,
        &listener,
        grpc.server.Handler.init(&sink, Sink.handle),
        .{ .allocator = gpa },
    });
    var stopped = false;
    defer if (!stopped) stop(io, &server);

    // Part A: one connection per call, all concurrent, each using the blocking
    // shape that fits it.
    print("one connection per call:\n", .{});
    const separate = [_]kitchen.Rpc{ .shout, .shout_empty, .tally, .countdown, .chat };
    var alone: [separate.len]kitchen.Outcome = undefined;
    for (&alone, separate) |*outcome, rpc| outcome.* = .init(gpa, rpc);
    defer for (&alone) |*outcome| outcome.deinit();
    {
        var group: Io.Group = .init;
        defer group.cancel(io);
        for (&alone) |*outcome| {
            const task = .{ gpa, io, target, authority, outcome };
            group.concurrent(io, runAlone, task) catch |err| switch (err) {
                // No thread to spare: run it here instead.
                error.ConcurrencyUnavailable => try @call(.auto, runAlone, task),
            };
        }
        try group.await(io);
    }
    for (&alone) |*outcome| outcome.report();

    // Part B: one connection carrying four calls at once.
    print("one connection, four calls multiplexed:\n", .{});
    const shared = [_]kitchen.Rpc{ .tally, .countdown, .chat, .too_slow };
    var together: [shared.len]kitchen.Outcome = undefined;
    for (&together, shared) |*outcome, rpc| outcome.* = .init(gpa, rpc);
    defer for (&together) |*outcome| outcome.deinit();
    multiplexed(gpa, io, target, authority, &together) catch |err| {
        for (&together) |*outcome| outcome.failure = err;
    };
    for (&together) |*outcome| outcome.report();

    // Stop accepting. Cancelling `serve` waits for the connection tasks, and
    // every client closed its socket above, so this returns promptly.
    stop(io, &server);
    stopped = true;
    print("server handled {d} calls\n", .{sink.handled.load(.monotonic)});

    try kitchen.verifyAll(&alone);
    try kitchen.verifyAll(&together);
    print("kitchen-blocking: ok\n", .{});
}

/// Cancel `serve` and wait for it. Its result is `Canceled` or a listener
/// error, and either way the demo is over.
fn stop(io: Io, server: anytype) void {
    server.cancel(io) catch |err| print("server stopped: {s}\n", .{@errorName(err)});
}

/// A group task: connect, make one call, record the result in `outcome`. A
/// failure is data for the report. Only cancellation propagates.
fn runAlone(
    gpa: Allocator,
    io: Io,
    address: Io.net.IpAddress,
    authority: []const u8,
    outcome: *kitchen.Outcome,
) Io.Cancelable!void {
    callAlone(gpa, io, address, authority, outcome) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => outcome.failure = err,
    };
}

fn callAlone(
    gpa: Allocator,
    io: Io,
    address: Io.net.IpAddress,
    authority: []const u8,
    outcome: *kitchen.Outcome,
) !void {
    const rpc = outcome.rpc;
    const socket = try address.connect(io, .{ .mode = .stream });
    var buffer: [kitchen.reply_capacity]u8 = undefined;
    const first = (try rpc.request(0, &buffer)).?;

    switch (rpc) {
        .shout, .shout_empty => {
            var client: grpc.blocking.Client = undefined;
            client.init(gpa, io, socket) catch |err| {
                socket.close(io); // On failure the caller keeps the socket.
                return err;
            };
            defer client.deinit(); // On success the client closes it.

            const open = rpc.open(authority);
            var reply: [kitchen.reply_capacity]u8 = undefined;
            var response = try client.unaryCaptured(.{
                .authority = authority,
                .service = open.service,
                .method = open.method,
                .message = first,
                .metadata = open.metadata,
                .timeout = open.timeout,
            }, &reply);
            defer response.deinit();
            // The status text borrows `reply` and the metadata borrows
            // `response`, so copy both into the outcome.
            const a = outcome.arena.allocator();
            try outcome.takeCaptured(&response);
            if (response.message.len != 0) {
                try outcome.notes.append(a, try kitchen.decode(kitchen.Note, a, response.message));
            }
        },
        .countdown => {
            var framed: [kitchen.reply_capacity + grpc.message.prefix_len]u8 = undefined;
            var open = rpc.open(authority);
            // ServerStream wants the whole request framed up front.
            open.streaming = false;
            open.body = try grpc.message.write(&framed, first, false);
            var stream: grpc.blocking.ServerStream = undefined;
            stream.init(gpa, io, socket, open) catch |err| {
                socket.close(io);
                return err;
            };
            defer stream.deinit();

            const a = outcome.arena.allocator();
            while (try stream.next()) |found| {
                try outcome.notes.append(a, try kitchen.decode(kitchen.Note, a, found.bytes));
            }
            try outcome.take(&stream.call); // The status and the trailers.
        },
        .tally, .chat => {
            var client: grpc.blocking.Client = undefined;
            client.init(gpa, io, socket) catch |err| {
                socket.close(io);
                return err;
            };
            defer client.deinit();
            var calls = [_]grpc.Call{try client.open(rpc.open(authority))};
            var streams = [_]grpc.Stream{client.stream(&calls[0], &calls)};
            defer streams[0].deinit(); // Calls borrow the client: drop them first.
            var scripts = [_]kitchen.Script{.{ .rpc = rpc }};
            try kitchen.drive(&streams, &scripts, outcome[0..1]);
        },
        .too_slow, .abort, .flood => unreachable,
    }
}

fn multiplexed(
    gpa: Allocator,
    io: Io,
    address: Io.net.IpAddress,
    authority: []const u8,
    outcomes: []kitchen.Outcome,
) !void {
    const socket = try address.connect(io, .{ .mode = .stream });
    var client: grpc.blocking.Client = undefined;
    client.init(gpa, io, socket) catch |err| {
        socket.close(io);
        return err;
    };
    defer client.deinit();

    var calls: [4]grpc.Call = undefined;
    var streams: [4]grpc.Stream = undefined;
    var scripts: [4]kitchen.Script = undefined;
    var opened: usize = 0;
    defer for (streams[0..opened]) |*stream| stream.deinit();
    for (outcomes) |outcome| {
        calls[opened] = try client.open(outcome.rpc.open(authority));
        // The complete slice is initialized before the shared loop advances.
        streams[opened] = client.stream(&calls[opened], calls[0..outcomes.len]);
        scripts[opened] = .{ .rpc = outcome.rpc };
        opened += 1;
    }
    try kitchen.drive(streams[0..opened], scripts[0..opened], outcomes);
}
