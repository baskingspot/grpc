//! The kitchen sink, readiness edition: one nonblocking server thread and one
//! nonblocking client connection, no thread per call, over real loopback.
//!
//! Server: `ReadinessServer.initStreaming` (epoll on Linux, kqueue on macOS,
//! poll elsewhere) calls `Sink.handle` each time a call has input, output
//! credit, or a timer. A turn must never block. It does what it can, then
//! returns `error.Pending` or parks, and comes back with the same
//! `Stream.context`. Everything a turn must remember lives in `State`, which is
//! allocated once from the exchange arena and reached through that context.
//!
//! Client: one `readiness.Session` carries every call over a single connection.
//! The blocking edition uses the same `Stream` drive loop; here each stream
//! advances the session with `Session.wait`.
//!
//! `zig build kitchen-readiness` runs it. It exits non-zero if any
//! expectation fails.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const print = std.debug.print;

const grpc = @import("grpc");
const Stream = grpc.server.Stream;

const kitchen = @import("kitchen.zig");

/// Everything one call remembers between turns. It lives in the exchange
/// arena, which the engine frees after `complete`.
const State = struct {
    method: ?kitchen.Method,
    began: bool = false,
    replied: bool = false,
    /// How many times the engine took no bytes at all.
    stalls: u32 = 0,
    // Tally.
    count: u32 = 0,
    bytes: usize = 0,
    // Countdown.
    started: bool = false,
    remaining: u32 = 0,
    interval_ms: u32 = 0,
    next_at: grpc.OptionalTimestamp = .none,
};

const Sink = struct {
    /// Touched only on the server thread. The main thread reads them after it
    /// has joined that thread.
    calls: u32 = 0,
    completed: u32 = 0,
    aborted: u32 = 0,
    stalls: u32 = 0,

    fn handle(self: *Sink, stream: *Stream) anyerror!void {
        const state = try self.stateOf(stream);
        if (try stream.expired()) return finish(stream, .deadline_exceeded, "deadline exceeded");
        const method = state.method orelse return finish(stream, .unimplemented, "no such method");
        // The response head goes first, on every method and every turn, so a
        // later turn that consumed input never has to wait for it.
        try begin(state, stream);
        switch (method) {
            .Shout => try shout(state, stream),
            .Tally => try tally(state, stream),
            .Countdown => try countdown(state, stream),
            .Chat => try chat(state, stream),
        }
    }

    /// The first turn creates the call's state. Later turns find it again.
    fn stateOf(self: *Sink, stream: *Stream) !*State {
        if (stream.context) |saved| return @ptrCast(@alignCast(saved));
        const state = try stream.exchange.arena.create(State);
        state.* = .{ .method = kitchen.route(stream.exchange.request.head.path orelse "") };
        stream.context = state;
        stream.message_limit = kitchen.message_limit;
        stream.send_limit = kitchen.message_limit;
        self.calls += 1;
        return state;
    }

    /// Runs once per call when it closes, including a reset by the peer. The
    /// arena is still readable here and is freed right after.
    fn complete(ptr: *anyopaque, stream: *Stream) void {
        const self: *Sink = @ptrCast(@alignCast(ptr));
        self.completed += 1;
        const state: *State = @ptrCast(@alignCast(stream.context orelse return));
        self.stalls += state.stalls;
        if (!stream.finished) self.aborted += 1;
    }
};

fn begin(state: *State, stream: *Stream) !void {
    if (state.began) return;
    try stream.begin(&.{.{ .name = "x-served-by", .value = "readiness" }});
    state.began = true;
}

fn finish(stream: *Stream, code: grpc.status.Code, text: []const u8) !void {
    const trailers = try kitchen.trailerFields(
        stream.exchange.arena,
        "readiness",
        stream.exchange.request.head.headers.get("x-request-id"),
    );
    try stream.finish(.{ .code = code, .message = text }, trailers, &.{});
}

/// Frame one reply into the stream. The previous one must be fully written, so
/// a slow reader makes the server stop producing instead of buffering.
fn queue(stream: *Stream, value: anytype) !void {
    var wire: [kitchen.reply_capacity]u8 = undefined;
    try stream.send(try kitchen.encode(&wire, value), false);
}

/// Hand the engine as much of the reply as it will take. Zero accepted is
/// backpressure, not failure: `Pending` ends the turn and the engine calls
/// again when output has drained.
fn drain(state: *State, stream: *Stream) !void {
    stream.flush() catch |err| {
        if (err == error.Pending) state.stalls += 1;
        return err;
    };
}

/// Give other connections a turn: come back as soon as the engine has run them.
fn yield(stream: *Stream) !void {
    return stream.park(try .init(try stream.now()));
}

fn shout(state: *State, stream: *Stream) !void {
    if (!state.replied) {
        const found = (try stream.next()) orelse
            return finish(stream, .invalid_argument, "Shout takes one note");
        // The bytes borrow the stream until the next read, so the reply is
        // built from them right now.
        const note = try kitchen.decodeBorrowed(kitchen.Note, found.bytes);
        var scratch: [kitchen.reply_capacity]u8 = undefined;
        const reply = kitchen.shout(note, &scratch) orelse
            return finish(stream, .invalid_argument, "note text is empty");
        try queue(stream, reply);
        state.replied = true;
    }
    try drain(state, stream);
    try finish(stream, .ok, "");
}

fn tally(state: *State, stream: *Stream) !void {
    if (!state.replied) {
        var turn: u8 = 0;
        while (try stream.next()) |found| {
            const note = try kitchen.decodeBorrowed(kitchen.Note, found.bytes);
            state.count += 1;
            state.bytes += note.text.len;
            turn += 1;
            if (turn == 8) return yield(stream);
        }
        var scratch: [kitchen.reply_capacity]u8 = undefined;
        try queue(stream, try kitchen.tallyReply(state.count, state.bytes, &scratch));
        state.replied = true;
    }
    try drain(state, stream);
    try finish(stream, .ok, "");
}

fn countdown(state: *State, stream: *Stream) !void {
    try drain(state, stream);
    if (!state.started) {
        const found = (try stream.next()) orelse
            return finish(stream, .invalid_argument, "Countdown takes one request");
        const ask = try kitchen.decodeBorrowed(kitchen.Countdown, found.bytes);
        state.remaining = ask.from;
        state.interval_ms = ask.interval_ms;
        state.started = true;
    }
    while (state.remaining > 0) {
        // The engine also calls this on output credit. Only the timer may
        // release the next tick.
        const now = try stream.now();
        if (state.next_at.get()) |at| {
            if (now.compare(at) == .lt) return stream.park(state.next_at);
        }
        try queue(stream, kitchen.tick(state.remaining));
        state.remaining -= 1;
        state.next_at = try .init(try now.add(try .fromMilliseconds(state.interval_ms)));
        try drain(state, stream);
    }
    try finish(stream, .ok, "");
}

fn chat(state: *State, stream: *Stream) !void {
    var turn: u8 = 0;
    while (true) {
        // Write before reading: a client that stops reading stops being read.
        try drain(state, stream);
        const found = (try stream.next()) orelse break;
        const note = try kitchen.decodeBorrowed(kitchen.Note, found.bytes);
        var scratch: [kitchen.reply_capacity]u8 = undefined;
        try queue(stream, try kitchen.chatReply(note, &scratch));
        turn += 1;
        if (turn == 8) {
            try drain(state, stream);
            return yield(stream);
        }
    }
    try finish(stream, .ok, "");
}

const ServerThread = struct {
    server: *grpc.server.ReadinessServer,
    failure: ?anyerror = null,

    fn run(self: *ServerThread) void {
        self.server.run() catch |err| {
            self.failure = err;
        };
    }
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    var sink: Sink = .{};
    var handler: grpc.server.StreamingHandler = .init(&sink, Sink.handle);
    handler.complete = Sink.complete;

    var server: grpc.server.ReadinessServer = undefined;
    try server.initStreaming(io, try .parseLiteral("127.0.0.1:0"), handler, .{
        .allocator = gpa,
        .max_connections = 4,
        .head_timeout = .fromSeconds(5),
        .idle_timeout = .fromSeconds(30),
    });
    defer server.deinit();

    var authority_buffer: [32]u8 = undefined;
    const authority = try std.mem.print(&authority_buffer, "127.0.0.1:{d}", .{
        server.address().getPort(),
    });
    print("readiness server on {s} (one thread)\n", .{authority});

    var runner: ServerThread = .{ .server = &server };
    const thread = try std.Thread.spawn(.{}, ServerThread.run, .{&runner});
    var joined = false;
    defer if (!joined) {
        server.requestStop();
        thread.join();
    };

    const rpcs = [_]kitchen.Rpc{
        .shout, .shout_empty, .tally, .countdown, .chat, .too_slow, .abort, .flood,
    };
    var outcomes: [rpcs.len]kitchen.Outcome = undefined;
    for (&outcomes, rpcs) |*outcome, rpc| outcome.* = .init(gpa, rpc);
    defer for (&outcomes) |*outcome| outcome.deinit();

    var again: [1]kitchen.Outcome = .{.init(gpa, .shout)};
    defer again[0].deinit();

    const socket = try server.address().connect(io, .{ .mode = .stream });
    var session: grpc.readiness.Session = undefined;
    session.init(.{ .allocator = gpa }, io, socket) catch |err| {
        socket.close(io); // On failure the caller keeps the socket.
        return err;
    };
    defer session.deinit(); // On success it closes the socket and every open call.

    // Everything at once on one connection, then another call on the same
    // connection once every slot has been handed back.
    round("eight calls, one connection:", &session, authority, &outcomes);
    round("same connection, once more:", &session, authority, &again);

    // Stop accepting, drain the open connections, and let `run` return.
    server.requestStop();
    thread.join();
    joined = true;
    if (runner.failure) |err| return err;
    print("server saw {d} calls: {d} completed, {d} aborted by the peer\n", .{
        sink.calls, sink.completed, sink.aborted,
    });
    print("server waited for output credit {d} times\n", .{sink.stalls});

    try kitchen.verifyAll(&outcomes);
    try kitchen.verifyAll(&again);
    print("kitchen-readiness: ok\n", .{});
}

fn round(
    title: []const u8,
    session: *grpc.readiness.Session,
    authority: []const u8,
    outcomes: []kitchen.Outcome,
) void {
    print("{s}\n", .{title});
    converse(session, authority, outcomes) catch |err| {
        for (outcomes) |*outcome| outcome.failure = outcome.failure orelse err;
    };
    for (outcomes) |*outcome| outcome.report();
}

/// Open every outcome's call on one session and drive them together. The slots
/// are handed back on the way out.
fn converse(
    session: *grpc.readiness.Session,
    authority: []const u8,
    outcomes: []kitchen.Outcome,
) !void {
    var streams: [8]grpc.Stream = undefined;
    var scripts: [8]kitchen.Script = undefined;
    var opened: usize = 0;
    defer for (streams[0..opened]) |*stream| stream.deinit();
    for (outcomes) |outcome| {
        const call = try session.open(outcome.rpc.open(authority));
        streams[opened] = session.stream(call);
        scripts[opened] = .{ .rpc = outcome.rpc };
        opened += 1;
    }

    try kitchen.drive(streams[0..opened], scripts[0..opened], outcomes);
}
