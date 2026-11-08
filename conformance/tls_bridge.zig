//! The TLS transport as a plaintext byte pair.
//!
//! `http_tls` hands a driver a transport with `write`/`send`/`receive`/`next`/
//! `consume`, not a reader and a writer. This bridges it to `Io.Reader` and
//! `Io.Writer`, which is what `grpc.blocking.Client.initIo` takes, so the
//! conformance client drives TLS through the same code as cleartext.
//!
//! The four rules from `docs/slop/TLS-SEAM.md` live here:
//!
//! 1. A frame does not fit one TLS record. `write` takes a prefix, so the
//!    writer loop tracks its own offset and never moves on with bytes queued.
//! 2. One attempt per step. `send` is non-blocking, so a step that made no
//!    progress sleeps a tick instead of spinning.
//! 3. `send` before every blocking read. During the handshake the transport owes
//!    the peer bytes, and a server waiting for our ClientHello will not answer.
//! 4. An empty `receiveBuffer` is backpressure, not EOF. Only this bridge's own
//!    socket read returning zero is EOF.

const std = @import("std");
const Io = std.Io;
const http = @import("http");
const http_tls = @import("http_tls");

const Transport = http_tls.h2.transport.tls_client.Transport;
const builtin = @import("builtin");

/// The platform syscall module the transport's `send` and `receive` want.
const Platform = switch (builtin.os.tag) {
    .linux => http.h2.engine.wait_poll.Wait.Platform,
    .macos => http.h2.engine.wait_kqueue.Wait.Platform,
    else => http.h2.engine.wait_poll.Wait.Platform,
};

const read_capacity = 16 * 1024;

pub const Error = error{
    /// The handshake finished without the ALPN protocol this client requires.
    WrongAlpn,
    /// The peer closed before the handshake completed.
    HandshakeClosed,
};

pub const Bridge = struct {
    io: Io,
    allocator: std.mem.Allocator,
    socket: Io.net.Stream,
    slot: Transport.Slot = undefined,
    bundle: http_tls.Bundle = undefined,
    reader: Io.Reader = undefined,
    writer: Io.Writer = undefined,
    read_buffer: [read_capacity]u8 = undefined,
    /// Plaintext the last `application_data` handed over and this reader has not
    /// returned yet. It stays pinned in that slice, so it is copied here.
    pending: [read_capacity]u8 = undefined,
    pending_len: usize = 0,
    pending_off: usize = 0,
    /// Absolute monotonic nanoseconds for the next socket wait.
    deadline_ns: ?i96 = null,
    /// Set when a wait ended because the deadline passed. `Io.Reader.Error` has
    /// no timeout to report, so the caller reads this after a failed call.
    expired: bool = false,
    open: bool = false,

    pub fn init(
        self: *Bridge,
        allocator: std.mem.Allocator,
        io: Io,
        socket: Io.net.Stream,
        pem: []const u8,
        host_name: []const u8,
    ) !void {
        self.* = .{
            .io = io,
            .allocator = allocator,
            .socket = socket,
        };

        self.bundle = try http_tls.anchors.build(allocator, pem, nowSeconds(io));
        errdefer self.bundle.deinit(allocator);

        self.slot = try Transport.init(
            allocator,
            io,
            socket.socket.handle,
            .{
                .ca = &self.bundle,
                .host_name = host_name,
                .now_sec = nowSeconds(io),
                .alpn = "h2",
            },
            0,
            0,
        );
        self.open = true;

        self.reader = .{
            .vtable = &.{
                .stream = stream,
                .readVec = readVec,
            },
            .buffer = &.{},
            .seek = 0,
            .end = 0,
        };
        self.writer = .{
            .vtable = &.{
                .drain = drain,
                .flush = flush,
                .rebase = rebase,
            },
            .buffer = &.{},
            .end = 0,
        };

        try self.handshake();
    }

    pub fn deinit(self: *Bridge) void {
        if (self.open) {
            self.open = false;
            self.slot.deinit();
        }
        self.bundle.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn readerInterface(self: *Bridge) *Io.Reader {
        return &self.reader;
    }

    pub fn writerInterface(self: *Bridge) *Io.Writer {
        return &self.writer;
    }

    /// The call's deadline, so a socket wait can end when the call does.
    pub fn setDeadline(self: *Bridge, absolute_ns: ?i96) void {
        self.deadline_ns = absolute_ns;
    }

    fn handshake(self: *Bridge) !void {
        while (true) {
            switch (try self.slot.next()) {
                .handshake_complete => {
                    const alpn = self.slot.selectedAlpn() orelse return error.WrongAlpn;
                    if (!std.mem.eql(u8, alpn, "h2")) return error.WrongAlpn;
                    return;
                },
                .need_input => {
                    self.slot.send(Platform) catch return error.HandshakeClosed;
                    const ready = self.readSocket() catch return error.HandshakeClosed;
                    if (!ready) return error.HandshakeClosed;
                },
                .need_output => self.slot.send(Platform) catch return error.HandshakeClosed,
                .progress => {},
                .application_data => |bytes| self.slot.consume(@intCast(bytes.len)),
                .closed => return error.HandshakeClosed,
            }
        }
    }

    fn stream(
        erased: *Io.Reader,
        w: *Io.Writer,
        limit: Io.Limit,
    ) Io.Reader.StreamError!usize {
        _ = erased;
        _ = w;
        _ = limit;
        return error.EndOfStream;
    }

    fn readVec(erased: *Io.Reader, iovecs: [][]u8) Io.Reader.Error!usize {
        return bridgeOf(erased, "reader").fill(iovecs);
    }

    fn drain(erased: *Io.Writer, data: []const []const u8, splat: usize) Io.Writer.Error!usize {
        return pushAll(bridgeOf(erased, "writer"), erased, data, splat);
    }

    fn flush(erased: *Io.Writer) Io.Writer.Error!void {
        const self = bridgeOf(erased, "writer");
        self.slot.send(Platform) catch return error.WriteFailed;
    }

    fn rebase(erased: *Io.Writer, preserve: usize, capacity: usize) Io.Writer.Error!void {
        // The bridge writes straight through, so it never rebases a buffer.
        _ = erased;
        _ = preserve;
        _ = capacity;
    }

    /// Hand plaintext to the transport, taking a prefix at a time and sending
    /// between attempts. Never returns with bytes still queued.
    fn push(
        self: *Bridge,
        w: *Io.Writer,
        data: []const []const u8,
        splat: usize,
    ) Io.Writer.Error!usize {
        var total: usize = 0;

        if (w.end != 0) {
            const buffered = w.buffer[0..w.end];
            w.end = 0;
            total += try self.pushSlice(buffered);
        }

        for (data, 0..) |slice, index| {
            const repeat = if (index + 1 == data.len) splat else 1;
            var copy: usize = 0;
            while (copy < repeat) : (copy += 1) total += try self.pushSlice(slice);
        }
        return total;
    }

    fn pushSlice(self: *Bridge, bytes: []const u8) Io.Writer.Error!usize {
        var offset: usize = 0;
        while (offset < bytes.len) {
            const accepted = self.slot.write(bytes[offset..]) catch return error.WriteFailed;
            offset += accepted;
            self.slot.send(Platform) catch return error.WriteFailed;
            if (accepted == 0) {
                // The transport still owes output. Yield rather than spin.
                self.io.sleep(.fromMilliseconds(1), .awake) catch return error.WriteFailed;
            }
        }
        return offset;
    }

    /// Return plaintext to the caller, stepping the transport as needed.
    fn fill(self: *Bridge, iovecs: [][]u8) Io.Reader.Error!usize {
        if (self.pending_off < self.pending_len) return self.handPending(iovecs);

        while (true) {
            switch (self.slot.next() catch return error.ReadFailed) {
                .application_data => |bytes| {
                    // Copy out at once: the slice is pinned only until consume.
                    const copied = copyInto(iovecs, bytes);
                    if (copied < bytes.len) {
                        const rest = bytes[copied..];
                        const kept = @min(rest.len, self.pending.len);
                        @memcpy(self.pending[0..kept], rest[0..kept]);
                        self.pending_len = kept;
                        self.pending_off = 0;
                    }
                    self.slot.consume(@intCast(bytes.len));
                    return copied;
                },
                .need_input => {
                    // Rule 3: send before any blocking read.
                    self.slot.send(Platform) catch return error.ReadFailed;
                    if (!(self.readSocket() catch return error.ReadFailed)) return 0;
                },
                .need_output => self.slot.send(Platform) catch return error.ReadFailed,
                .progress, .handshake_complete => {},
                .closed => return 0,
            }
        }
    }

    fn handPending(self: *Bridge, iovecs: [][]u8) usize {
        const rest = self.pending[self.pending_off..self.pending_len];
        const copied = copyInto(iovecs, rest);
        self.pending_off += copied;
        if (self.pending_off == self.pending_len) {
            self.pending_off = 0;
            self.pending_len = 0;
        }
        return copied;
    }

    /// Read the socket once into the transport's own buffer and commit it.
    /// False on EOF.
    ///
    /// The bytes must land in `receiveBuffer`, because that is the storage the
    /// TLS layer decrypts from; reading into a private buffer and reporting a
    /// count produces `RecordTooLarge` on the first record. An empty buffer
    /// means the transport still owes output, not that the peer closed.
    fn readSocket(self: *Bridge) !bool {
        const buffer = self.slot.receiveBuffer();
        if (buffer.len == 0) {
            self.slot.send(Platform) catch return error.WriteFailed;
            return true;
        }

        if (self.deadline_ns) |deadline| {
            const now = Io.Clock.awake.now(self.io).nanoseconds;
            if (now >= deadline) {
                self.expired = true;
                return error.TimedOut;
            }
            const give_up: Io.Clock.Timestamp = Io.Timestamp
                .fromNanoseconds(deadline)
                .withClock(Io.Clock.awake);

            var storage: [1]Io.Operation.Storage = undefined;
            var batch: Io.Batch = .init(&storage);
            var iovecs: [1][]u8 = .{buffer};
            _ = batch.add(.{ .net_read = .{
                .socket_handle = self.socket.socket.handle,
                .data = &iovecs,
            } });
            batch.awaitConcurrent(self.io, .{ .deadline = give_up }) catch |err| switch (err) {
                error.Timeout, error.Canceled => {
                    self.expired = true;
                    return error.TimedOut;
                },
                error.ConcurrencyUnavailable => return error.DeadlineUnsupported,
            };
            while (batch.next()) |completion| {
                switch (completion.result) {
                    .net_read => |result| {
                        const ready = try result;
                        if (ready.data_len == 0) return false;
                        self.slot.received(ready.data_len);
                        return true;
                    },
                    else => {},
                }
            }
            self.expired = true;
            return error.TimedOut;
        }

        var iovecs: [1][]u8 = .{buffer};
        var transport_reader = self.socket.reader(self.io, &.{});
        const n = transport_reader.interface.readVec(&iovecs) catch |err| switch (err) {
            error.EndOfStream => return false,
            else => return err,
        };
        if (n == 0) return false;
        self.slot.received(n);
        return true;
    }
};

fn bridgeOf(erased: anytype, comptime field: []const u8) *Bridge {
    const base: [*]u8 = @ptrCast(erased);
    const address = @intFromPtr(base) - @offsetOf(Bridge, field);
    const pointer: *Bridge = @ptrFromInt(address);
    return @alignCast(pointer);
}

fn pushAll(
    self: *Bridge,
    w: *Io.Writer,
    data: []const []const u8,
    splat: usize,
) Io.Writer.Error!usize {
    return self.push(w, data, splat);
}

fn copyInto(iovecs: [][]u8, bytes: []const u8) usize {
    var copied: usize = 0;
    for (iovecs) |iov| {
        if (copied == bytes.len) break;
        const take = @min(iov.len, bytes.len - copied);
        @memcpy(iov[0..take], bytes[copied..][0..take]);
        copied += take;
    }
    return copied;
}

fn nowSeconds(io: Io) i64 {
    const ns = Io.Clock.real.now(io).nanoseconds;
    return @intCast(@divTrunc(ns, std.time.ns_per_s));
}
