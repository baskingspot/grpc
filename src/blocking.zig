//! A blocking gRPC call over `std.Io`.
//!
//! This is the convenience over `Call`: it runs the socket-facing loop, and the
//! caller owns the stream. `unary` frames one serialized request and returns
//! one response message. `ServerStream` pulls one message at a time.
//! A streaming caller uses `Client.open`, `Call`, and `Client.step`.
//!
//! Socket calls enforce one absolute deadline across all reads and response
//! messages. `unaryIo` has no clock or socket, so its caller enforces deadlines.

const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const math = std.math;
const testing = std.testing;
const posix = std.posix;
const Allocator = mem.Allocator;

const http = @import("http");
const h2 = http.h2;
const Driver = h2.client.Driver;
const OptionalFd = @import("stdx").posix.OptionalFd;
const time = @import("stdx").time;

const Call = @import("Call.zig");
const head = @import("head.zig");
const message = @import("message.zig");
const sim = @import("sim.zig");
const status = @import("status.zig");
/// The module is called `status`, so a method of that name needs a type alias
/// to stay unambiguous.
const Status = status.Status;
const timeout = @import("timeout.zig");

/// One transport read. A DATA frame carries at most 16 KiB, so most reads end
/// on a frame boundary.
const read_capacity = 16 * 1024;
/// The transport writer's buffer.
const write_capacity = 4 * 1024;
/// h2 caps an outbound frame at 16 KiB, so one frame and its nine-byte header
/// fit. A buffer smaller than a queued frame makes `nextFrame` fail rather than
/// overrun.
const frame_capacity = 16 * 1024 + 9;

pub const Request = struct {
    /// `:authority`, as in `host:port`.
    authority: []const u8,
    service: []const u8,
    method: []const u8,
    /// Framed request messages, from `message.write`.
    body: []const u8 = &.{},
    /// `.https` when the transport under the reader and writer is TLS.
    scheme: http.client.Scheme = .http,
    /// The caller writes the request messages itself with `Call.writeMessage`
    /// and finishes with `Call.closeSend`, so the send side stays open.
    /// When false, `body` is the complete framed request.
    streaming: bool = false,
    /// `grpc-encoding`, when this call compresses its messages.
    encoding: []const u8 = "identity",
    /// `grpc-accept-encoding`: every encoding this client can read.
    accept_encoding: []const u8 = "identity",
    /// The largest response message this call accepts, before decompression.
    /// Null uses the message frame's default.
    message_limit: ?u32 = null,
    /// Relative call timeout. `.none` leaves the call without a deadline.
    timeout: time.OptionalDuration = .none,
    metadata: []const http.Header = &.{},
};

/// One request message for the unary helpers. The codec supplies the bytes.
pub const UnaryRequest = struct {
    authority: []const u8,
    service: []const u8,
    method: []const u8,
    /// Serialized message bytes without the five-byte gRPC prefix.
    /// The helper copies these bytes before its first transport write.
    message: []const u8 = &.{},
    /// The message already uses `encoding`. The helper does not compress it.
    compressed: bool = false,
    scheme: http.client.Scheme = .http,
    encoding: []const u8 = "identity",
    accept_encoding: []const u8 = "identity",
    /// The largest response message this call accepts, before decompression.
    message_limit: ?u32 = null,
    /// Relative call timeout. `.none` leaves the call without a deadline.
    timeout: time.OptionalDuration = .none,
    metadata: []const http.Header = &.{},

    fn validate(self: UnaryRequest) !void {
        if (self.message.len > message.max_length) return error.MessageTooLarge;
        _ = math.add(usize, message.prefix_len, self.message.len) catch
            return error.MessageTooLarge;
    }

    fn callRequest(self: UnaryRequest) Request {
        return .{
            .authority = self.authority,
            .service = self.service,
            .method = self.method,
            .scheme = self.scheme,
            .streaming = true,
            .encoding = self.encoding,
            .accept_encoding = self.accept_encoding,
            .message_limit = self.message_limit,
            .timeout = self.timeout,
            .metadata = self.metadata,
        };
    }

    fn writeTo(self: UnaryRequest, call: *Call) !void {
        try call.writeMessageFlagged(self.message, self.compressed);
        call.closeSend();
    }
};

pub const Config = struct {
    allocator: Allocator,
    /// Destination for the response: the message first, then the status text.
    /// Both `Response.message` and `Response.status.message` borrow it, so size
    /// it for the two together. A response that does not fit is
    /// `error.MessageTooLarge` rather than a truncated one.
    message: []u8,
};

pub const Response = struct {
    status: Status,
    /// The response message with the length prefix removed, a prefix of
    /// `Config.message`. Empty when the call carried no message.
    message: []const u8,
};

pub const Error = error{
    /// A unary call got a second message. The RPC is not unary.
    TooManyMessages,
    /// The request exceeds the wire length, or the response does not fit its destination.
    MessageTooLarge,
    /// The connection stopped accepting input.
    InputFull,
};

/// Send one request and read one response over a caller-owned stream.
pub fn unary(io: Io, stream: Io.net.Stream, request: UnaryRequest, config: Config) !Response {
    var write_buffer: [write_capacity]u8 = undefined;
    var stream_reader = stream.reader(io, &.{});
    var stream_writer = stream.writer(io, &write_buffer);
    return runUnary(&stream_reader.interface, &stream_writer.interface, request, config, .{
        .socket = .{ .io = io, .fd = stream.socket.handle },
    });
}

/// The same call over plaintext reader and writer interfaces. A TLS adapter
/// calls this after its handshake, which is why this takes no stream.
pub fn unaryIo(
    reader: *Io.Reader,
    writer: *Io.Writer,
    request: UnaryRequest,
    config: Config,
) !Response {
    return runUnary(reader, writer, request, config, .reader);
}

fn runUnary(
    reader: *Io.Reader,
    writer: *Io.Writer,
    request: UnaryRequest,
    config: Config,
    wait: Driver.Wait,
) !Response {
    const io: ?Io = switch (wait) {
        .socket => |socket| socket.io,
        .reader => null,
    };
    const deadline = try absoluteDeadline(io, request.timeout);
    try request.validate();
    var read_buffer: [read_capacity]u8 = undefined;
    const frame_buffer = try config.allocator.alloc(u8, frame_capacity);
    defer config.allocator.free(frame_buffer);

    var conn: h2.client.Connection = undefined;
    try conn.init(.{ .allocator = config.allocator });
    defer conn.deinit();

    const raw_request = request.callRequest();
    const stream = try openCall(&conn, config.allocator, raw_request);
    var call = try Call.init(config.allocator, stream.call, stream.body, callOptions(raw_request));
    defer call.deinit();
    call.deadline = deadline;
    try request.writeTo(&call);

    var driver: Driver = undefined;
    driver.init(&conn, reader, writer, .{
        .read_buffer = &read_buffer,
        .frame_buffer = frame_buffer,
        .wait = wait,
    });
    var pump: Pump = .{ .io = io, .driver = &driver, .call = &call };
    try pump.run(.status);
    return collect(&call, config.message);
}

/// A connection that stays open across calls.
///
/// `unary` completes one call before it returns. `open` creates concurrent
/// calls, and `step` drives them through the shared HTTP connection driver.
///
/// `init` takes a pointer because the h2 stream inside a call points at the
/// connection, and a moved struct would break that pointer.
pub const Client = struct {
    io: Io,
    allocator: Allocator,
    /// Owned and closed by `deinit` when this client opened it.
    socket: ?Io.net.Stream = null,
    transport_reader: Io.net.Stream.Reader = undefined,
    transport_writer: Io.net.Stream.Writer = undefined,
    /// The bytes this client reads and writes. A socket client points these at
    /// its own transport pair; a caller with a TLS transport points them at
    /// whatever that transport exposes as plaintext.
    reader: *Io.Reader = undefined,
    writer: *Io.Writer = undefined,
    /// The socket behind `reader` and `writer`, when there is one to poll for a
    /// deadline. A transport that decrypts in memory has none.
    fd: OptionalFd = .none,
    /// Whether the deadline is enforced by polling the fd or by the reader.
    deadline_by: DeadlineBy = .fd,
    read_deadline: ?Driver.ReadDeadline = null,
    read_buffer: [read_capacity]u8 = undefined,
    write_buffer: [write_capacity]u8 = undefined,
    frame: []u8 = &.{},
    conn: h2.client.Connection,
    driver: Driver,

    /// On success, the client owns the socket and `deinit` closes it.
    /// On failure, the caller retains the socket.
    pub fn init(
        self: *Client,
        allocator: Allocator,
        io: Io,
        socket: Io.net.Stream,
    ) !void {
        self.* = .{
            .io = io,
            .allocator = allocator,
            .socket = socket,
            .transport_reader = socket.reader(io, &.{}),
            .transport_writer = socket.writer(io, &self.write_buffer),
            .reader = &self.transport_reader.interface,
            .writer = &self.transport_writer.interface,
            .fd = .init(socket.socket.handle),
            .conn = undefined,
            .driver = undefined,
        };
        self.frame = try allocator.alloc(u8, frame_capacity);
        errdefer allocator.free(self.frame);
        try self.conn.init(.{ .allocator = allocator });
        self.initDriver();
    }

    /// A client over any byte pair, such as a TLS transport's plaintext side.
    /// The caller owns the reader and the writer, and nothing here closes them.
    ///
    pub const IoOptions = struct {
        /// The raw socket for every read in `.fd` mode.
        /// The driver consumes prefetched bytes before the next socket read.
        /// A decrypting reader requires `.reader` mode.
        fd: OptionalFd = .none,
        /// Who enforces a deadline. `fd` is right when the reader hands over
        /// the socket's own bytes. `reader` is required when it decrypts,
        /// because reading the fd would feed ciphertext to the connection and
        /// the parser reads the first three bytes as a frame length.
        deadline_by: DeadlineBy = .fd,
        /// The plaintext reader owner's deadline operations. The context must
        /// outlive this client. Required for reader-enforced call deadlines.
        read_deadline: ?Driver.ReadDeadline = null,
    };

    pub const DeadlineBy = enum { fd, reader };

    pub fn initIo(
        self: *Client,
        allocator: Allocator,
        io: Io,
        reader: *Io.Reader,
        writer: *Io.Writer,
        options: IoOptions,
    ) !void {
        self.* = .{
            .io = io,
            .allocator = allocator,
            .reader = reader,
            .writer = writer,
            .fd = options.fd,
            .deadline_by = options.deadline_by,
            .read_deadline = options.read_deadline,
            .conn = undefined,
            .driver = undefined,
        };
        self.frame = try allocator.alloc(u8, frame_capacity);
        errdefer allocator.free(self.frame);
        try self.conn.init(.{ .allocator = allocator });
        self.initDriver();
    }

    pub fn deinit(self: *Client) void {
        self.conn.deinit();
        self.allocator.free(self.frame);
        if (self.socket) |socket| socket.close(self.io);
        self.* = undefined;
    }

    /// The HTTP/2 error code the session reported for this connection, if it
    /// failed. Null means the connection has not failed.
    pub fn connectionError(self: *const Client) ?h2.frame.ErrorCode {
        return self.conn.connectionErrorCode();
    }

    pub fn isPoisoned(self: *const Client) bool {
        return self.driver.isClosed() or self.conn.isPoisoned();
    }

    /// Open a call without waiting for its status. The call borrows this
    /// client's connection, so it must be dropped before the client.
    pub fn open(self: *Client, request: Request) !Call {
        if (self.driver.isClosed()) return error.ConnectionClosed;
        if (request.timeout.get() != null and !self.driver.supportsDeadline()) {
            return error.DeadlineUnsupported;
        }
        const deadline = try absoluteDeadline(self.io, request.timeout);
        const opened = try openCall(&self.conn, self.allocator, request);
        var call = try Call.init(
            self.allocator,
            opened.call,
            opened.body,
            callOptions(request),
        );
        call.deadline = deadline;
        return call;
    }

    /// Drive every call one step: flush output, pump each request, hand each
    /// queued event to each call, and read when the connection wants input.
    ///
    /// False means no call can make progress right now. An event for a stream
    /// that is not in `calls` is dropped, so the slice must hold every call the
    /// connection carries.
    pub fn step(self: *Client, calls: []Call) !bool {
        if (self.driver.isClosed()) {
            try self.endCalls(calls);
            return false;
        }
        // The wait ends at the earliest deadline among the calls, so the one
        // that runs out first is expired instead of waiting behind another.
        var earliest: time.OptionalTimestamp = .none;
        var handled = false;
        var active_count: u32 = 0;
        const now: time.Timestamp = try .fromStd(Io.Clock.now(.awake, self.io));
        for (calls) |*call| {
            handled = expireCall(call, now) or handled;
            if (call.result() != null) continue;
            active_count += 1;
            const deadline = call.deadline.get() orelse continue;
            if (earliest.get()) |at| {
                if (deadline.compare(at) == .lt) earliest = try .init(deadline);
            } else {
                earliest = try .init(deadline);
            }
        }

        const driver = &self.driver;

        var wrote = self.conn.hasPendingOutput();
        try driver.flush();
        for (calls) |*call| _ = try call.pump();
        wrote = self.conn.hasPendingOutput() or wrote;
        try driver.flush();

        while (try self.conn.poll()) |event| {
            const observed: time.Timestamp = try .fromStd(Io.Clock.now(.awake, self.io));
            var matched = false;
            for (calls) |*call| {
                _ = expireCall(call, observed);
                if (event == .data and event.data.stream == call.stream.id) matched = true;
                try call.handle(event);
            }
            if (event == .data and !matched) try event.data.lease.release();
            handled = true;
        }
        try driver.flush();
        if (handled) return true;
        if (active_count == 0) return false;
        if (wrote) return true;
        if (!self.conn.wantsInput() and !driver.hasPendingInput()) {
            try self.endCalls(calls);
            return false;
        }

        if (earliest.get() != null and !driver.supportsDeadline()) {
            return error.DeadlineUnsupported;
        }
        const read = driver.read(earliest) catch |err| {
            if (!driver.isClosed()) return err;
            try self.endCalls(calls);
            return false;
        };
        return switch (read) {
            .ready => true,
            .timeout => result: {
                const expired_at: time.Timestamp = try .fromStd(
                    Io.Clock.now(.awake, self.io),
                );
                for (calls) |*call| _ = expireCall(call, expired_at);
                break :result true;
            },
            .eof => result: {
                try self.endCalls(calls);
                break :result false;
            },
        };
    }

    fn endCalls(self: *Client, calls: []Call) !void {
        const now = Io.Clock.now(.awake, self.io).nanoseconds;
        for (calls) |*call| {
            const current: time.Timestamp = try .fromStd(.fromNanoseconds(now));
            _ = expireCall(call, current);
            call.abandon(.unavailable, "connection closed");
        }
    }

    /// Write everything queued, and each call's request, without reading. It is
    /// the send half of `step`, for a caller that has to do something between
    /// sending and waiting, such as cancelling after a delay.
    pub fn send(self: *Client, calls: []Call) !void {
        const driver = &self.driver;
        const now = Io.Clock.now(.awake, self.io).nanoseconds;
        const current: time.Timestamp = try .fromStd(.fromNanoseconds(now));
        for (calls) |*call| _ = expireCall(call, current);
        try driver.flush();
        for (calls) |*call| _ = try call.pump();
        try driver.flush();
    }

    /// Send one serialized message and return one response on this connection.
    /// No other call can remain active. Use `open` and `step` for concurrent calls.
    pub fn unary(self: *Client, request: UnaryRequest, reply: []u8) !Response {
        try request.validate();
        var call = try self.open(request.callRequest());
        defer call.deinit();
        try request.writeTo(&call);
        var pump: Pump = .{
            .io = self.io,
            .driver = &self.driver,
            .call = &call,
        };
        try pump.run(.status);
        return collect(&call, reply);
    }

    fn initDriver(self: *Client) void {
        const wait: Driver.Wait = if (self.deadline_by == .fd and self.fd.get() != null)
            .{ .socket = .{ .io = self.io, .fd = self.fd.get().? } }
        else
            .reader;
        self.driver.init(&self.conn, self.reader, self.writer, .{
            .read_buffer = &self.read_buffer,
            .frame_buffer = self.frame,
            .wait = wait,
            .read_deadline = self.read_deadline,
        });
    }
};

/// A server-streaming call in progress: the connection, the call, and the
/// socket, so the caller pulls one message at a time.
///
/// `init` takes a pointer because the h2 stream inside the call points at the
/// connection, and a moved struct would break that pointer. The caller keeps it
/// in place and calls `deinit`.
pub const ServerStream = struct {
    io: Io,
    allocator: Allocator,
    socket: Io.net.Stream,
    transport_reader: Io.net.Stream.Reader,
    transport_writer: Io.net.Stream.Writer,
    read_buffer: [read_capacity]u8 = undefined,
    write_buffer: [write_capacity]u8 = undefined,
    frame_buffer: []u8 = &.{},
    conn: h2.client.Connection,
    driver: Driver,
    call: Call,

    /// On success, the stream owns the socket and `deinit` closes it.
    /// If initialization fails, the caller retains the socket.
    pub fn init(
        self: *ServerStream,
        allocator: Allocator,
        io: Io,
        socket: Io.net.Stream,
        request: Request,
    ) !void {
        const deadline = try absoluteDeadline(io, request.timeout);
        self.* = .{
            .io = io,
            .allocator = allocator,
            .socket = socket,
            .transport_reader = socket.reader(io, &.{}),
            .transport_writer = socket.writer(io, &self.write_buffer),
            .frame_buffer = &.{},
            .conn = undefined,
            .driver = undefined,
            .call = undefined,
        };
        self.frame_buffer = try allocator.alloc(u8, frame_capacity);
        errdefer allocator.free(self.frame_buffer);

        try self.conn.init(.{ .allocator = allocator });
        errdefer self.conn.deinit();
        self.driver.init(
            &self.conn,
            &self.transport_reader.interface,
            &self.transport_writer.interface,
            .{
                .read_buffer = &self.read_buffer,
                .frame_buffer = self.frame_buffer,
                .wait = .{ .socket = .{ .io = io, .fd = socket.socket.handle } },
            },
        );

        const opened = try openCall(&self.conn, allocator, request);
        self.call = try Call.init(allocator, opened.call, opened.body, callOptions(request));
        self.call.deadline = deadline;
    }

    pub fn deinit(self: *ServerStream) void {
        self.call.deinit();
        self.conn.deinit();
        self.allocator.free(self.frame_buffer);
        self.socket.close(self.io);
        self.* = undefined;
    }

    /// The next message, or null when the stream ended. Read `status` after
    /// null: it is the answer, and it is UNIMPLEMENTED when the peer sent a
    /// compressed message this layer cannot read.
    ///
    /// The message borrows the call's buffer and is valid until the next
    /// `next` call.
    pub fn next(self: *ServerStream) !?message.Message {
        while (true) {
            if (try self.call.nextMessage()) |found| return found;
            if (self.call.result() != null) return null;

            var pump: Pump = .{
                .io = self.io,
                .driver = &self.driver,
                .call = &self.call,
            };
            try pump.run(.event);
        }
    }

    /// The answer, or null while messages are still arriving.
    pub fn status(self: *const ServerStream) ?Status {
        return self.call.result();
    }
};

/// Internal: shared with `readiness.zig`. A request head waiting for its call.
pub const Opened = struct {
    call: h2.client.Stream,
    body: []const u8,
};

/// Internal: shared with `readiness.zig`.
/// The policy a call needs from the request it belongs to.
pub fn callOptions(request: Request) Call.Options {
    const accept = request.accept_encoding;
    return .{
        .send_closed = !request.streaming,
        .accepted_encodings = accept,
        .allow_compressed = !mem.eql(u8, accept, "identity"),
        .message_limit = request.message_limit orelse message.default_limit,
    };
}

/// Internal: shared with `readiness.zig`.
pub fn openCall(
    conn: *h2.client.Connection,
    allocator: Allocator,
    request: Request,
) !Opened {
    const path_len = try math.add(
        usize,
        try math.add(usize, request.service.len, request.method.len),
        2,
    );
    // One buffer for the `:path` and the base64 of every binary metadata value.
    var binary_len: usize = 0;
    for (request.metadata) |field| {
        binary_len += field.name.len;
        if (head.isBinary(field.name)) binary_len += head.binaryEncodedLen(field.value);
    }
    const scratch = try allocator.alloc(u8, path_len + binary_len);
    defer allocator.free(scratch);

    const target = try head.path(scratch[0..path_len], .{
        .service = request.service,
        .method = request.method,
    });

    const field_count = head.fieldCount(.{
        .service = request.service,
        .method = request.method,
        .encoding = request.encoding,
        .accept_encoding = request.accept_encoding,
        .timeout = request.timeout,
        .metadata = request.metadata,
    });
    const fields = try allocator.alloc(http.Header, field_count);
    defer allocator.free(fields);

    var timeout_buf: [timeout.max_digits + 1]u8 = undefined;
    const written = try head.fields(fields, scratch[path_len..][0..binary_len], &timeout_buf, .{
        .service = request.service,
        .method = request.method,
        .encoding = request.encoding,
        .accept_encoding = request.accept_encoding,
        .timeout = request.timeout,
        .metadata = request.metadata,
    });

    const stream = try conn.open(.{
        .method_token = "POST",
        .scheme = request.scheme,
        .authority = request.authority,
        .target = target,
        .headers = written,
    }, false);

    return .{ .call = stream, .body = request.body };
}

/// Internal: shared with `readiness.zig`.
/// When the call must give up, on the clock the wait uses.
pub fn absoluteDeadline(
    io: ?Io,
    budget: time.OptionalDuration,
) time.Timestamp.Error!time.OptionalTimestamp {
    const duration = budget.get() orelse return .none;
    const handle = io orelse return .none;
    const started: time.Timestamp = try .fromStd(Io.Clock.now(.awake, handle));
    return .init(try started.add(duration));
}

/// Internal: shared with `readiness.zig`. Check expiry before request writes
/// and event dispatch. A completed call keeps its first status.
pub fn expireCall(call: *Call, now: time.Timestamp) bool {
    if (call.result() != null) return false;
    const deadline = call.deadline.get() orelse return false;
    if (now.compare(deadline) == .lt) return false;
    call.abandon(.deadline_exceeded, "");
    return true;
}

/// One gRPC call over the shared HTTP connection driver.
const Pump = struct {
    io: ?Io,
    driver: *Driver,
    call: *Call,

    /// Drive until the status or the next event batch. Read only when the
    /// connection wants input. A null poll means no event is queued.
    fn run(self: *Pump, until: enum { status, event }) !void {
        while (self.call.result() == null) {
            if (try self.advance()) {
                if (until == .event) return;
                continue;
            }
            if (!self.call.stream.conn.wantsInput() and !self.driver.hasPendingInput()) {
                try self.endCall();
                return;
            }
            if (self.call.deadline.get() != null and !self.driver.supportsDeadline()) {
                return error.DeadlineUnsupported;
            }
            const read = self.driver.read(self.call.deadline) catch |err| {
                if (!self.driver.isClosed()) return err;
                try self.endCall();
                return;
            };
            switch (read) {
                .ready => {},
                .timeout => {
                    self.call.abandon(.deadline_exceeded, "");
                    return;
                },
                .eof => {
                    try self.endCall();
                    return;
                },
            }
        }
    }

    fn endCall(self: *Pump) !void {
        if (self.io) |io| {
            _ = expireCall(self.call, try .fromStd(Io.Clock.now(.awake, io)));
        }
        self.call.abandon(.unavailable, "connection closed");
    }

    /// Push queued bytes out and take queued events in. True when an event was
    /// handled: the caller must re-check messages and status before reading.
    fn advance(self: *Pump) !bool {
        var handled = false;
        if (self.io) |io| {
            handled = expireCall(
                self.call,
                try .fromStd(Io.Clock.now(.awake, io)),
            );
        }
        try self.driver.flush();
        _ = try self.call.pump();
        try self.driver.flush();

        while (try self.call.stream.conn.poll()) |event| {
            if (self.io) |io| {
                _ = expireCall(
                    self.call,
                    try .fromStd(Io.Clock.now(.awake, io)),
                );
            }
            if (event == .data and event.data.stream != self.call.stream.id) {
                try event.data.lease.release();
            } else {
                try self.call.handle(event);
            }
            handled = true;
        }
        try self.driver.flush();
        return handled;
    }
};

/// Internal: shared with `readiness.zig`.
pub fn collect(call: *Call, reply: []u8) !Response {
    const closed: Status = .{ .code = .unavailable, .message = "connection closed" };

    const first = (try call.nextMessage()) orelse {
        var result: Response = .{ .status = call.result() orelse closed, .message = &.{} };
        try copyStatusText(&result.status, reply, 0);
        return result;
    };
    if (first.bytes.len > reply.len) return error.MessageTooLarge;
    // Copy before asking for a second message: the next walk may clear the
    // buffer this slice points into.
    @memcpy(reply[0..first.bytes.len], first.bytes);
    const body = reply[0..first.bytes.len];
    if ((try call.nextMessage()) != null) return error.TooManyMessages;

    var result: Response = .{ .status = call.result() orelse closed, .message = body };
    try copyStatusText(&result.status, reply, body.len);
    return result;
}

/// Move the status text into caller storage, after the message. The call's own
/// arena is freed when it returns, so a status that borrowed it would leave the
/// caller holding a dangling slice.
fn copyStatusText(call_status: *Status, reply: []u8, offset: usize) !void {
    const text = call_status.message;
    if (text.len == 0) return;
    if (offset + text.len > reply.len) return error.MessageTooLarge;
    @memcpy(reply[offset..][0..text.len], text);
    call_status.message = reply[offset..][0..text.len];
}

test "a unary call over a loopback socket" {
    const address: Io.net.IpAddress = try .parseLiteral("127.0.0.1:0");
    var listener = try address.listen(testing.io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(testing.io);

    var client_address = address;
    client_address.setPort(listener.socket.address.getPort());

    var reply_buffer: [64]u8 = undefined;
    const reply_body = try message.write(&reply_buffer, "world", false);

    var server: sim.Server = .{
        .io = testing.io,
        .listener = &listener,
        .allocator = testing.allocator,
        .reply = .{ .body = reply_body, .code = "0" },
    };
    const thread = try std.Thread.spawn(.{}, sim.Server.run, .{&server});

    const stream = try client_address.connect(testing.io, .{ .mode = .stream });
    var joined = false;
    defer if (!joined) {
        stream.close(testing.io);
        thread.join();
    };

    var request_buffer: [64]u8 = undefined;
    const request_body = try message.write(&request_buffer, "hello", false);
    var response_message: [256]u8 = undefined;

    const response = try unary(testing.io, stream, .{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .message = "hello",
    }, .{ .allocator = testing.allocator, .message = &response_message });

    try testing.expectEqual(status.Code.ok, response.status.code);
    try testing.expectEqualStrings("world", response.message);
    try testing.expectEqualStrings("/helloworld.Greeter/SayHello", server.received.targetPath());
    try testing.expectEqualSlices(u8, request_body, server.received.bodyBytes());

    stream.close(testing.io);
    thread.join();
    joined = true;
    try testing.expect(server.result == null);
}

test "unary helpers emit the same bytes as a hand-framed call" {
    for ([_][]const u8{ "", "hello", "\x00\xff\x03" }) |payload| {
        for ([_]bool{ false, true }) |compressed| {
            var expected: [4096]u8 = undefined;
            const framed = try unaryRequestWire(&expected, payload, compressed, .framed);
            var client_wire: [4096]u8 = undefined;
            const client = try unaryRequestWire(&client_wire, payload, compressed, .client);
            try testing.expectEqualSlices(u8, framed, client);
            var interface_wire: [4096]u8 = undefined;
            const interfaces = try unaryRequestWire(
                &interface_wire,
                payload,
                compressed,
                .interfaces,
            );
            try testing.expectEqualSlices(u8, framed, interfaces);
        }
    }
}

fn unaryRequestWire(
    output: []u8,
    payload: []const u8,
    compressed: bool,
    mode: enum { framed, client, interfaces },
) ![]const u8 {
    var reader = Io.Reader.fixed("");
    var writer = Io.Writer.fixed(output);
    const request: UnaryRequest = .{
        .authority = "example.com",
        .service = "test.Service",
        .method = "Unary",
        .message = payload,
        .compressed = compressed,
        .scheme = .https,
        .encoding = if (compressed) "gzip" else "identity",
        .accept_encoding = "gzip,identity",
        .metadata = &.{
            .{ .name = "x-test", .value = "unary" },
            .{ .name = "value-bin", .value = "\x00\xff" },
        },
    };
    var reply: [64]u8 = undefined;
    if (mode == .interfaces) {
        const response = try unaryIo(&reader, &writer, request, .{
            .allocator = testing.allocator,
            .message = &reply,
        });
        try testing.expectEqual(status.Code.unavailable, response.status.code);
        return writer.buffered();
    }
    var client: Client = undefined;
    try client.initIo(testing.allocator, testing.io, &reader, &writer, .{});
    defer client.deinit();
    if (mode == .client) {
        const response = try client.unary(request, &reply);
        try testing.expectEqual(status.Code.unavailable, response.status.code);
    } else {
        var frame_buffer: [128]u8 = undefined;
        const frame = try message.write(&frame_buffer, payload, compressed);
        var calls: [1]Call = .{try client.open(.{
            .authority = request.authority,
            .service = request.service,
            .method = request.method,
            .body = frame,
            .scheme = request.scheme,
            .encoding = request.encoding,
            .accept_encoding = request.accept_encoding,
            .metadata = request.metadata,
        })};
        defer calls[0].deinit();
        try client.send(&calls);
    }
    return writer.buffered();
}

test "unary keeps its response limit when it opens the raw call" {
    const request: UnaryRequest = .{
        .authority = "localhost",
        .service = "test.Service",
        .method = "Unary",
        .message_limit = 7,
    };
    try testing.expectEqual(@as(u32, 7), callOptions(request.callRequest()).message_limit);
}

test "a server stream yields each message and then the status" {
    const address: Io.net.IpAddress = try .parseLiteral("127.0.0.1:0");
    var listener = try address.listen(testing.io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(testing.io);

    var client_address = address;
    client_address.setPort(listener.socket.address.getPort());

    var reply_buffer: [128]u8 = undefined;
    var reply_len: usize = 0;
    for ([_][]const u8{ "one", "two" }) |text| {
        const frame = try message.write(reply_buffer[reply_len..], text, false);
        reply_len += frame.len;
    }

    var server: sim.Server = .{
        .io = testing.io,
        .listener = &listener,
        .allocator = testing.allocator,
        .reply = .{ .body = reply_buffer[0..reply_len], .code = "0" },
    };
    const thread = try std.Thread.spawn(.{}, sim.Server.run, .{&server});

    const socket = try client_address.connect(testing.io, .{ .mode = .stream });

    var request_buffer: [64]u8 = undefined;
    const request_body = try message.write(&request_buffer, "hello", false);

    var streaming: ServerStream = undefined;
    try streaming.init(testing.allocator, testing.io, socket, .{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .body = request_body,
        .timeout = try .init(try time.Duration.fromSeconds(2)),
    });
    const deadline = streaming.call.deadline.get().?;

    const first = (try streaming.next()).?;
    try testing.expectEqualStrings("one", first.bytes);
    const second = (try streaming.next()).?;
    try testing.expectEqualStrings("two", second.bytes);
    try testing.expectEqual(deadline, streaming.call.deadline.get().?);
    try testing.expect((try streaming.next()) == null);
    try testing.expectEqual(status.Code.ok, streaming.status().?.code);

    streaming.deinit();
    thread.join();
    try testing.expect(server.result == null);
}

test "Client.step retains unaccepted input across calls" {
    var reader = Io.Reader.fixed("");
    var sent: [4096]u8 = undefined;
    var writer = Io.Writer.fixed(&sent);
    var client: Client = undefined;
    try client.initIo(testing.allocator, testing.io, &reader, &writer, .{});
    defer client.deinit();
    var calls: [1]Call = .{try client.open(.{
        .authority = "example.com",
        .service = "test.Service",
        .method = "Large",
    })};
    defer calls[0].deinit();
    try client.send(&calls);

    var peer = try h2.Session.init(testing.allocator, .{ .role = .server });
    defer peer.deinit(testing.allocator);
    try testing.expectEqual(writer.buffered().len, peer.receive(writer.buffered()));
    while (try peer.nextEvent(testing.allocator)) |_| {}
    try peer.submitResponse(testing.allocator, calls[0].stream.id, .{
        .status = .ok,
        .content_type = "application/grpc",
    }, false);
    const payload: [32 * 1024]u8 = @splat(0xa7);
    var framed: [payload.len + message.prefix_len]u8 = undefined;
    const body = try message.write(&framed, &payload, false);
    var body_offset: usize = 0;
    while (body_offset < body.len) {
        const accepted = try peer.submitData(
            testing.allocator,
            calls[0].stream.id,
            body[body_offset..],
            false,
        );
        try testing.expect(accepted != 0);
        body_offset += accepted;
    }
    const trailers = [_]http.Header{.{ .name = "grpc-status", .value = "0" }};
    try peer.submitTrailers(testing.allocator, calls[0].stream.id, &trailers);

    var wire: [40 * 1024]u8 = undefined;
    var wire_len: usize = 0;
    while (true) {
        const n = peer.send(wire[wire_len..]);
        if (n == 0) break;
        wire_len += n;
    }
    reader = .fixed(wire[0..wire_len]);

    // Full-size DATA frames cross the driver's reads. One read must retain
    // the next frame while the connection waits for its caller to poll.
    var saw_pending = false;
    var turns: usize = 0;
    while (calls[0].result() == null and turns < 20) : (turns += 1) {
        _ = try client.step(&calls);
        saw_pending = saw_pending or client.driver.hasPendingInput();
    }
    try testing.expect(calls[0].result() != null);
    try testing.expectEqual(status.Code.ok, calls[0].result().?.code);
    const found = (try calls[0].nextMessage()).?;
    try testing.expectEqualSlices(u8, &payload, found.bytes);
    try testing.expect(saw_pending);
}

test "one connection serves two calls in sequence" {
    const address: Io.net.IpAddress = try .parseLiteral("127.0.0.1:0");
    var listener = try address.listen(testing.io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(testing.io);

    var client_address = address;
    client_address.setPort(listener.socket.address.getPort());

    var reply_buffer: [64]u8 = undefined;
    const reply_body = try message.write(&reply_buffer, "world", false);

    var server: sim.Server = .{
        .io = testing.io,
        .listener = &listener,
        .allocator = testing.allocator,
        .reply = .{ .body = reply_body, .code = "0" },
    };
    const thread = try std.Thread.spawn(.{}, sim.Server.run, .{&server});

    const socket = try client_address.connect(testing.io, .{ .mode = .stream });

    var client: Client = undefined;
    try client.init(testing.allocator, testing.io, socket);

    var first_reply: [256]u8 = undefined;
    var second_reply: [256]u8 = undefined;

    const first = try client.unary(.{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .message = "hello",
    }, &first_reply);
    const second = try client.unary(.{
        .authority = "127.0.0.1",
        .service = "helloworld.Greeter",
        .method = "SayHello",
        .message = "hello",
    }, &second_reply);

    try testing.expectEqual(status.Code.ok, first.status.code);
    try testing.expectEqualStrings("world", first.message);
    try testing.expectEqual(status.Code.ok, second.status.code);
    try testing.expectEqualStrings("world", second.message);

    client.deinit();
    thread.join();
    try testing.expect(server.result == null);
}

test "two calls in flight on one connection" {
    const address: Io.net.IpAddress = try .parseLiteral("127.0.0.1:0");
    var listener = try address.listen(testing.io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(testing.io);

    var client_address = address;
    client_address.setPort(listener.socket.address.getPort());

    var reply_buffer: [64]u8 = undefined;
    const reply_body = try message.write(&reply_buffer, "world", false);

    var server: sim.Server = .{
        .io = testing.io,
        .listener = &listener,
        .allocator = testing.allocator,
        .reply = .{ .body = reply_body, .code = "0" },
    };
    const thread = try std.Thread.spawn(.{}, sim.Server.run, .{&server});

    const socket = try client_address.connect(testing.io, .{ .mode = .stream });

    var client: Client = undefined;
    try client.init(testing.allocator, testing.io, socket);

    var request_buffer: [64]u8 = undefined;
    const request_body = try message.write(&request_buffer, "hello", false);
    var calls: [2]Call = .{
        try client.open(.{
            .authority = "127.0.0.1",
            .service = "helloworld.Greeter",
            .method = "First",
            .body = request_body,
        }),
        try client.open(.{
            .authority = "127.0.0.1",
            .service = "helloworld.Greeter",
            .method = "Second",
            .body = request_body,
        }),
    };
    defer for (&calls) |*call| call.deinit();

    // Both calls are open before either finishes: one poll feeds both.
    var rounds: usize = 0;
    while (true) {
        rounds += 1;
        try testing.expect(rounds < 1000);
        if (try client.step(&calls)) continue;

        var pending = false;
        for (&calls) |*call| {
            if (call.result() == null) pending = true;
        }
        if (!pending) break;
        // No progress and a call still waiting means the driver lost an event.
        return error.NoProgress;
    }

    for (&calls) |*call| {
        try testing.expectEqual(status.Code.ok, call.result().?.code);
        const found = (try call.nextMessage()).?;
        try testing.expectEqualStrings("world", found.bytes);
    }

    client.deinit();
    thread.join();
    try testing.expect(server.result == null);
}

test "a client over an injected reader and writer pair" {
    const address: Io.net.IpAddress = try .parseLiteral("127.0.0.1:0");
    var listener = try address.listen(testing.io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(testing.io);

    var client_address = address;
    client_address.setPort(listener.socket.address.getPort());

    var reply_buffer: [64]u8 = undefined;
    const reply_body = try message.write(&reply_buffer, "world", false);

    var server: sim.Server = .{
        .io = testing.io,
        .listener = &listener,
        .allocator = testing.allocator,
        .reply = .{ .body = reply_body, .code = "0" },
    };
    const thread = try std.Thread.spawn(.{}, sim.Server.run, .{&server});

    const socket = try client_address.connect(testing.io, .{ .mode = .stream });
    var socket_closed = false;
    defer if (!socket_closed) socket.close(testing.io);
    var write_buffer: [4 * 1024]u8 = undefined;
    var stream_reader = socket.reader(testing.io, &.{});
    var stream_writer = socket.writer(testing.io, &write_buffer);

    // The pair is the caller's, which is the shape a TLS transport presents.
    var client: Client = undefined;
    try client.initIo(
        testing.allocator,
        testing.io,
        &stream_reader.interface,
        &stream_writer.interface,
        .{ .fd = .init(socket.socket.handle) },
    );
    var closed = false;
    defer if (!closed) client.deinit();

    var response_buffer: [64]u8 = undefined;

    const response = try client.unary(.{
        .authority = "127.0.0.1",
        .service = "probe.Probe",
        .method = "Echo",
        .message = "hello",
    }, &response_buffer);
    try testing.expectEqual(status.Code.ok, response.status.code);
    try testing.expectEqualStrings("world", response.message);

    client.deinit();
    closed = true;
    // The client does not own this socket, so this test closes it and the
    // server's read loop sees EOF.
    socket.close(testing.io);
    socket_closed = true;
    thread.join();
    try testing.expect(server.result == null);
}

test "queued resets lose only to deadlines that actually expired" {
    const cases = [_]struct {
        remaining: Io.Duration,
        existing: ?status.Code = null,
        expected: status.Code,
    }{
        .{ .remaining = .fromNanoseconds(-1), .expected = .deadline_exceeded },
        .{ .remaining = .fromSeconds(2), .expected = .cancelled },
        .{ .remaining = .fromNanoseconds(-1), .existing = .internal, .expected = .internal },
    };
    for (cases) |case| {
        var reader: Io.Reader = .fixed("");
        var output: [4096]u8 = undefined;
        var writer: Io.Writer = .fixed(&output);
        var client: Client = undefined;
        try client.initIo(testing.allocator, testing.io, &reader, &writer, .{});
        defer client.deinit();
        var calls: [1]Call = .{try client.open(.{
            .authority = "localhost",
            .service = "probe.Probe",
            .method = "Echo",
        })};
        defer calls[0].deinit();
        try client.send(&calls);

        // A peer SETTINGS frame followed by RST_STREAM(CANCEL) on stream 1.
        const incoming = "\x00\x00\x00\x04\x00\x00\x00\x00\x00" ++
            "\x00\x00\x04\x03\x00\x00\x00\x00\x01\x00\x00\x00\x08";
        try testing.expectEqual(incoming.len, client.conn.receive(incoming));
        calls[0].deadline = try .init(try .fromStd(
            Io.Clock.now(.awake, testing.io).addDuration(case.remaining),
        ));
        if (case.existing) |code| calls[0].abandon(code, "original status");
        try testing.expect(try client.step(&calls));
        try testing.expectEqual(case.expected, calls[0].result().?.code);
        if (case.existing != null) {
            try testing.expectEqualStrings("original status", calls[0].result().?.message);
        }
    }
}

test "expired calls need no input and finished deadlines do not govern other calls" {
    var reader: Io.Reader = .fixed("");
    var output: [4096]u8 = undefined;
    var writer: Io.Writer = .fixed(&output);
    var client: Client = undefined;
    try client.initIo(testing.allocator, testing.io, &reader, &writer, .{});
    defer client.deinit();
    var calls: [2]Call = .{
        try client.open(.{
            .authority = "localhost",
            .service = "probe.Probe",
            .method = "First",
        }),
        try client.open(.{
            .authority = "localhost",
            .service = "probe.Probe",
            .method = "Second",
        }),
    };
    defer for (&calls) |*call| call.deinit();
    calls[0].deadline = try .init(try .fromStd(
        Io.Clock.now(.awake, testing.io).subDuration(.fromNanoseconds(1)),
    ));
    try testing.expect(try client.step(calls[0..1]));
    try testing.expectEqual(status.Code.deadline_exceeded, calls[0].result().?.code);
    // The reader reports EOF. A completed deadline must not select a timed
    // socket read or spin instead of letting the other call read its input.
    try client.send(&calls);
    try testing.expect(!try client.step(&calls));
    try testing.expectEqual(status.Code.unavailable, calls[1].result().?.code);
    try testing.expectEqual(status.Code.deadline_exceeded, calls[0].result().?.code);
}

test "step yields after outbound progress before it reads EOF" {
    var reader: Io.Reader = .fixed("\x00\x00\x00\x04\x00\x00\x00\x00\x00");
    var wire: [4096]u8 = undefined;
    var writer: Io.Writer = .fixed(&wire);
    var client: Client = undefined;
    try client.initIo(testing.allocator, testing.io, &reader, &writer, .{});
    defer client.deinit();
    _ = try client.driver.read(.none);
    while (try client.conn.poll()) |_| {}
    try client.driver.flush();
    var calls: [1]Call = .{try client.open(.{
        .authority = "localhost",
        .service = "test.Service",
        .method = "Echo",
        .streaming = true,
    })};
    defer calls[0].deinit();
    try calls[0].writeMessage("ping");
    try testing.expect(try client.step(&calls));
    try testing.expect(calls[0].result() == null);
    calls[0].closeSend();
    try testing.expect(try client.step(&calls));
    try testing.expect(calls[0].result() == null);
    try testing.expect(!try client.step(&calls));
    try testing.expectEqual(status.Code.unavailable, calls[0].result().?.code);
}

test "unsupported deadlines are rejected before a call opens" {
    const options = [_]Client.IoOptions{ .{}, .{ .deadline_by = .reader, .fd = .none } };
    for (options) |io_options| {
        var reader = Io.Reader.failing;
        var output: [4096]u8 = undefined;
        var writer = Io.Writer.fixed(&output);
        var client: Client = undefined;
        try client.initIo(testing.allocator, testing.io, &reader, &writer, io_options);
        defer client.deinit();
        if (client.open(.{
            .authority = "localhost",
            .service = "test.Service",
            .method = "Rejected",
            .timeout = try .init(try time.Duration.fromSeconds(1)),
        })) |opened| {
            var call = opened;
            call.deinit();
            return error.ExpectedDeadlineUnsupported;
        } else |err| try testing.expectEqual(error.DeadlineUnsupported, err);
        try testing.expectEqual(@as(usize, 0), writer.buffered().len);
        var call = try client.open(.{
            .authority = "localhost",
            .service = "test.Service",
            .method = "First",
        });
        defer call.deinit();
        try testing.expectEqual(@as(u31, 1), call.stream.id);
    }
}

test "a terminal reader failure resolves unfinished calls and forbids reuse" {
    var reader = Io.Reader.failing;
    var output: [4096]u8 = undefined;
    var writer = Io.Writer.fixed(&output);
    var client: Client = undefined;
    try client.initIo(testing.allocator, testing.io, &reader, &writer, .{});
    defer client.deinit();
    var calls: [2]Call = .{
        try client.open(.{ .authority = "localhost", .service = "test.Service", .method = "Live" }),
        try client.open(.{ .authority = "localhost", .service = "test.Service", .method = "Done" }),
    };
    defer for (&calls) |*call| call.deinit();
    calls[1].abandon(.internal, "already resolved");
    try client.send(&calls);
    try testing.expect(!try client.step(&calls));
    try testing.expectEqual(status.Code.unavailable, calls[0].result().?.code);
    try testing.expectEqual(status.Code.internal, calls[1].result().?.code);
    try testing.expect(client.isPoisoned());
    try testing.expectError(error.ConnectionClosed, client.open(.{
        .authority = "localhost",
        .service = "test.Service",
        .method = "AfterFailure",
    }));
}

test "unary reports a terminal reader failure as unavailable" {
    var reader = Io.Reader.failing;
    var output: [4096]u8 = undefined;
    var writer = Io.Writer.fixed(&output);
    var client: Client = undefined;
    try client.initIo(testing.allocator, testing.io, &reader, &writer, .{});
    defer client.deinit();
    var reply: [64]u8 = undefined;
    const response = try client.unary(.{
        .authority = "localhost",
        .service = "test.Service",
        .method = "Unary",
    }, &reply);
    try testing.expectEqual(status.Code.unavailable, response.status.code);
    try testing.expect(client.isPoisoned());
}

const ReadDeadlineProbe = struct {
    now_ns: u64 = 1000,
    now_count: usize = 0,
    observed: time.OptionalTimestamp = .none,
    expired_flag: bool = false,
    set_count: usize = 0,

    fn now(context: ?*anyopaque, clock: Io.Clock) Io.Timestamp {
        const self: *ReadDeadlineProbe = @ptrCast(@alignCast(context.?));
        const assert = std.debug.assert;
        assert(clock == .awake);
        self.now_count += 1;
        return .fromNanoseconds(self.now_ns);
    }

    fn set(context: *anyopaque, deadline: time.OptionalTimestamp) void {
        const self: *ReadDeadlineProbe = @ptrCast(@alignCast(context));
        self.observed = deadline;
        self.set_count += 1;
        if (deadline.get()) |at| self.now_ns = at.nanoseconds;
        self.expired_flag = deadline.get() != null;
    }

    fn cause(context: *const anyopaque) ?anyerror {
        const self: *const ReadDeadlineProbe = @ptrCast(@alignCast(context));
        return if (self.expired_flag) error.TimedOut else null;
    }

    fn hooks(self: *ReadDeadlineProbe) Driver.ReadDeadline {
        return .{ .context = self, .set = set, .cause = cause };
    }
};

test "one-shot unary starts its deadline before its first allocation" {
    var probe: ReadDeadlineProbe = .{};
    var vtable = testing.io.vtable.*;
    vtable.now = ReadDeadlineProbe.now;
    const io: Io = .{ .userdata = &probe, .vtable = &vtable };
    var reader = Io.Reader.failing;
    var writer = Io.Writer.failing;
    try testing.expectError(error.OutOfMemory, runUnary(&reader, &writer, .{
        .authority = "localhost",
        .service = "test.Service",
        .method = "Unary",
        .timeout = try .init(.fromNanoseconds(100)),
    }, .{
        .allocator = testing.failing_allocator,
        .message = &.{},
    }, .{ .socket = .{ .io = io, .fd = -1 } }));
    try testing.expectEqual(@as(usize, 1), probe.now_count);
}

test "ServerStream starts its deadline before its first allocation" {
    const address: Io.net.IpAddress = try .parse("127.0.0.1", 0);
    var listener = try address.listen(testing.io, .{ .mode = .stream });
    defer listener.deinit(testing.io);
    const socket = try listener.socket.address.connect(testing.io, .{ .mode = .stream });
    defer socket.close(testing.io);
    var probe: ReadDeadlineProbe = .{};
    var vtable = testing.io.vtable.*;
    vtable.now = ReadDeadlineProbe.now;
    const io: Io = .{ .userdata = &probe, .vtable = &vtable };
    var stream: ServerStream = undefined;
    try testing.expectError(error.OutOfMemory, stream.init(testing.failing_allocator, io, socket, .{
        .authority = "localhost",
        .service = "test.Service",
        .method = "Watch",
        .timeout = try .init(.fromNanoseconds(100)),
    }));
    try testing.expectEqual(@as(usize, 1), probe.now_count);
}

test "reader waits share the call deadline across delayed steps and skip finished calls" {
    var probe: ReadDeadlineProbe = .{};
    var vtable = testing.io.vtable.*;
    vtable.now = ReadDeadlineProbe.now;
    const io: Io = .{ .userdata = &probe, .vtable = &vtable };
    var reader = Io.Reader.failing;
    var output: [4096]u8 = undefined;
    var writer = Io.Writer.fixed(&output);
    var client: Client = undefined;
    try client.initIo(testing.allocator, io, &reader, &writer, .{
        .deadline_by = .reader,
        .read_deadline = probe.hooks(),
    });
    defer client.deinit();
    var calls: [2]Call = .{
        try client.open(.{
            .authority = "localhost",
            .service = "test.Service",
            .method = "First",
            .timeout = try .init(.fromNanoseconds(100)),
        }),
        try client.open(.{
            .authority = "localhost",
            .service = "test.Service",
            .method = "Second",
            .timeout = try .init(.fromNanoseconds(300)),
        }),
    };
    defer for (&calls) |*call| call.deinit();
    try testing.expectEqual(@as(u64, 1100), calls[0].deadline.get().?.nanoseconds);
    try testing.expectEqual(@as(u64, 1300), calls[1].deadline.get().?.nanoseconds);

    // Advance the clock between open and read. The transport must not start
    // another relative budget or use the finished call on the second wait.
    probe.now_ns = 1050;
    try client.send(&calls);
    try testing.expect(try client.step(&calls));
    try testing.expectEqual(calls[0].deadline, probe.observed);
    try testing.expectEqual(status.Code.deadline_exceeded, calls[0].result().?.code);
    try testing.expect(calls[1].result() == null);
    try client.send(&calls);
    try testing.expect(try client.step(&calls));
    try testing.expectEqual(calls[1].deadline, probe.observed);
    try testing.expectEqual(status.Code.deadline_exceeded, calls[1].result().?.code);
    try testing.expectEqual(@as(usize, 2), probe.set_count);
}

test "direct Client.unary passes its one absolute deadline to the reader owner" {
    var probe: ReadDeadlineProbe = .{};
    var vtable = testing.io.vtable.*;
    vtable.now = ReadDeadlineProbe.now;
    const io: Io = .{ .userdata = &probe, .vtable = &vtable };
    var reader = Io.Reader.failing;
    var output: [4096]u8 = undefined;
    var writer = Io.Writer.fixed(&output);
    var client: Client = undefined;
    try client.initIo(testing.allocator, io, &reader, &writer, .{
        .deadline_by = .reader,
        .read_deadline = probe.hooks(),
    });
    defer client.deinit();
    var reply: [64]u8 = undefined;
    const response = try client.unary(.{
        .authority = "localhost",
        .service = "test.Service",
        .method = "Unary",
        .timeout = try .init(.fromNanoseconds(100)),
    }, &reply);
    try testing.expectEqual(status.Code.deadline_exceeded, response.status.code);
    try testing.expectEqual(@as(u64, 1100), probe.observed.get().?.nanoseconds);
    try testing.expectEqual(@as(usize, 1), probe.set_count);
}

test "late DATA releases leases with an omitted or retained expired call" {
    try lateDataAfterExpiry(false);
    try lateDataAfterExpiry(true);
}

fn lateDataAfterExpiry(retain_expired: bool) !void {
    var reader = Io.Reader.fixed("");
    var output: [4096]u8 = undefined;
    var writer = Io.Writer.fixed(&output);
    var client: Client = undefined;
    try client.initIo(testing.allocator, testing.io, &reader, &writer, .{});
    defer client.deinit();
    var calls: [2]Call = .{
        try client.open(.{
            .authority = "localhost",
            .service = "test.Service",
            .method = "Expired",
        }),
        try client.open(.{
            .authority = "localhost",
            .service = "test.Service",
            .method = "Next",
        }),
    };
    defer for (&calls) |*call| call.deinit();
    try client.send(&calls);
    calls[0].abandon(.deadline_exceeded, "");
    var peer = try h2.Session.init(testing.allocator, .{ .role = .server });
    defer peer.deinit(testing.allocator);
    try testing.expectEqual(writer.buffered().len, peer.receive(writer.buffered()));
    while (try peer.nextEvent(testing.allocator)) |_| {}
    var framed: [64]u8 = undefined;
    const body = try message.write(&framed, "response", false);
    const trailers = [_]http.Header{.{ .name = "grpc-status", .value = "0" }};
    for (&calls) |*call| {
        try peer.submitResponse(testing.allocator, call.stream.id, .{
            .status = .ok,
            .content_type = "application/grpc",
        }, false);
        try testing.expectEqual(
            body.len,
            try peer.submitData(testing.allocator, call.stream.id, body, false),
        );
        try peer.submitTrailers(testing.allocator, call.stream.id, &trailers);
    }
    var wire: [4096]u8 = undefined;
    var used: usize = 0;
    while (true) {
        const n = peer.send(wire[used..]);
        if (n == 0) break;
        used += n;
    }
    reader = .fixed(wire[0..used]);
    for (0..16) |_| {
        if (calls[1].result() != null) break;
        _ = try client.step(if (retain_expired) &calls else calls[1..]);
    }
    try testing.expectEqual(status.Code.deadline_exceeded, calls[0].result().?.code);
    try testing.expectEqual(status.Code.ok, calls[1].result().?.code);
    try testing.expectEqualStrings("response", (try calls[1].nextMessage()).?.bytes);
}

test "Client.step resolves unfinished calls at EOF and forbids connection reuse" {
    var reader = Io.Reader.fixed("");
    var output: [4096]u8 = undefined;
    var writer = Io.Writer.fixed(&output);
    var client: Client = undefined;
    try client.initIo(testing.allocator, testing.io, &reader, &writer, .{});
    defer client.deinit();
    const request: Request = .{
        .authority = "example.com",
        .service = "test.Service",
        .method = "Eof",
    };
    var calls: [2]Call = .{ try client.open(request), try client.open(request) };
    defer calls[0].deinit();
    defer calls[1].deinit();
    calls[1].abandon(.internal, "first status");
    try client.send(&calls);
    try testing.expect(!try client.step(&calls));
    try testing.expect(calls[0].result() != null);
    try testing.expectEqual(status.Code.unavailable, calls[0].result().?.code);
    try testing.expectEqual(status.Code.internal, calls[1].result().?.code);
    try testing.expect(client.isPoisoned());
    try testing.expect(!try client.step(&calls));
    try testing.expectError(error.ConnectionClosed, client.open(request));
}
