//! The gRPC compression codecs, over `baskingspot/compress`.
//!
//! gRPC compression is wire format, not caller policy: the protocol's
//! registry is closed (`identity`, `gzip`, `deflate`, `snappy`, `zstd`), the
//! message frame carries one flag bit, and the `grpc-encoding` and
//! `grpc-accept-encoding` fields negotiate which side may use what. So the
//! library ships the registry, backed by the org's codecs, and a caller who
//! wants different bytes still has the raw primitive underneath:
//! `Call.writeMessageFlagged` writes a compressed-flagged frame untouched,
//! and `allow_compressed` hands a compressed response message back whole.
//!
//! The layering rule holds: every convenience here writes through the
//! primitives, and the test pins its bytes to the hand-composed form.

const std = @import("std");
const Io = std.Io;
const testing = std.testing;

const compress = @import("compress");

/// One codec from the protocol's registry. The functions are pointers so a
/// `Codec` value can be a vtable a caller replaces or extends; the built-ins
/// below are the registry's members.
pub const Codec = struct {
    /// The `grpc-encoding` token this codec answers to.
    name: []const u8,
    /// The worst-case encoded size of `input_len` plaintext bytes: the size
    /// to allocate before `encode`.
    maxEncodedLen: *const fn (input_len: usize) usize,
    /// Encode `source` into `target`, which holds at least
    /// `maxEncodedLen(source.len)`. Returns the encoded byte count.
    encode: *const fn (source: []const u8, target: []u8) EncodeError!usize,
    /// Decode `source` into `target`, which is bounded by the message
    /// receive limit, because that limit applies to the plaintext. Returns
    /// the plaintext byte count.
    decode: *const fn (source: []const u8, target: []u8) DecodeError!usize,
};

pub const EncodeError = error{BufferTooSmall};

/// A decode ran out of target space, or the bytes were not what the codec's
/// name promised. The gRPC mapping is the caller's: an unknown
/// `grpc-encoding` is UNIMPLEMENTED, and bytes that fail their named codec
/// are INTERNAL.
pub const DecodeError = error{ BufferTooSmall, DecodeFailed };

fn encodeGzip(source: []const u8, target: []u8) EncodeError!usize {
    return compress.gzip.encode.compress(source, target, .{});
}

fn decodeGzip(source: []const u8, target: []u8) DecodeError!usize {
    return compress.gzip.decode.decompress(source, target) catch |err| switch (err) {
        error.BufferTooSmall => error.BufferTooSmall,
        else => error.DecodeFailed,
    };
}

/// The wire's `deflate` is the zlib container (RFC 1950), the form the
/// established runtimes mean by the token.
fn encodeDeflate(source: []const u8, target: []u8) EncodeError!usize {
    return compress.zlib.encode.compress(source, target, .{});
}

fn decodeDeflate(source: []const u8, target: []u8) DecodeError!usize {
    return compress.zlib.decode.decompress(source, target, null) catch |err| switch (err) {
        error.BufferTooSmall => error.BufferTooSmall,
        else => error.DecodeFailed,
    };
}

/// The wire's `snappy` is Google's framed stream format: the identifier
/// chunk, then length-prefixed, CRC-protected 64 KiB blocks. That is the form
/// the Java runtimes and the conformance suite speak; grpc-go's bare-block
/// snappy cannot interoperate with either, and nothing here imitates it.
fn encodeSnappy(source: []const u8, target: []u8) EncodeError!usize {
    var in: Io.Reader = .fixed(source);
    var out: Io.Writer = .fixed(target);
    // `streamAll` reports the uncompressed bytes moved; the encoded count is
    // where the fixed writer stopped.
    _ = compress.snappy.Writer.streamAll(&in, &out) catch
        return error.BufferTooSmall;
    return out.end;
}

fn decodeSnappy(source: []const u8, target: []u8) DecodeError!usize {
    var in: Io.Reader = .fixed(source);
    var out: Io.Writer = .fixed(target);
    return compress.snappy.Reader.streamAll(&in, &out) catch |err| switch (err) {
        // The fixed source never fails, so a read error is the framing's:
        // a bad identifier, chunk, or checksum.
        error.ReadFailed => error.DecodeFailed,
        // The fixed target's only failure is the plaintext budget.
        error.WriteFailed => error.BufferTooSmall,
    };
}

/// The framed worst case: the identifier chunk, then per 64 KiB block a
/// chunk header (4), a masked CRC (4), and the block's own varint preamble
/// (at most 5) with tag rounding (1), plus the block expansion the block
/// bound already carries. Loose by a few bytes per chunk, and never short.
fn maxEncodedLenSnappy(input_len: usize) usize {
    const chunk_overhead = 15;
    const chunks = (input_len + compress.snappy.encode.max_block_size - 1) /
        compress.snappy.encode.max_block_size;
    return compress.snappy.Writer.identifier.len +
        chunks * chunk_overhead +
        compress.snappy.encode.maxCompressedLength(input_len);
}

fn encodeZstd(source: []const u8, target: []u8) EncodeError!usize {
    return compress.zstd.encode.compress(source, target, .{}) catch |err| switch (err) {
        // The buffer holds maxCompressedLength, so only sizing errors can
        // arrive, and they mean the same thing here.
        else => error.BufferTooSmall,
    };
}

fn decodeZstd(source: []const u8, target: []u8) DecodeError!usize {
    return compress.zstd.decode.decompress(source, target, null) catch |err| switch (err) {
        error.BufferTooSmall => error.BufferTooSmall,
        else => error.DecodeFailed,
    };
}

/// The registry's built-in members, in the order the accept-encoding field
/// lists them.
pub const gzip: Codec = .{
    .name = "gzip",
    .maxEncodedLen = compress.gzip.encode.maxCompressedLength,
    .encode = encodeGzip,
    .decode = decodeGzip,
};

pub const deflate: Codec = .{
    .name = "deflate",
    .maxEncodedLen = compress.zlib.encode.maxCompressedLength,
    .encode = encodeDeflate,
    .decode = decodeDeflate,
};

pub const snappy: Codec = .{
    .name = "snappy",
    .maxEncodedLen = maxEncodedLenSnappy,
    .encode = encodeSnappy,
    .decode = decodeSnappy,
};

pub const zstd: Codec = .{
    .name = "zstd",
    .maxEncodedLen = compress.zstd.encode.maxCompressedLength,
    .encode = encodeZstd,
    .decode = decodeZstd,
};

/// Every built-in codec, `identity` excluded: identity is not a codec here,
/// it is the absence of one, and the call paths treat it that way.
pub const all = [_]*const Codec{ &gzip, &deflate, &snappy, &zstd };

/// The codec a `grpc-encoding` token names, or null for anything else —
/// including `identity`, which no codec answers to.
pub fn byName(name: []const u8) ?*const Codec {
    for (&all) |codec| {
        if (std.mem.eql(u8, codec.name, name)) return codec;
    }
    return null;
}

/// The `grpc-accept-encoding` value that names every built-in: what a side
/// configured with all of them advertises.
pub const accept_all = "gzip,deflate,snappy,zstd";

test "each codec round-trips a message" {
    const sample = "hello, compression, this is a message with some bytes";
    for (&all) |codec| {
        var encoded = try testing.allocator.alloc(u8, codec.maxEncodedLen(sample.len));
        defer testing.allocator.free(encoded);
        const n = try codec.encode(sample, encoded);
        var decoded = try testing.allocator.alloc(u8, sample.len + 16);
        defer testing.allocator.free(decoded);
        const plain = try codec.decode(encoded[0..n], decoded);
        try testing.expectEqualSlices(u8, sample, decoded[0..plain]);
    }
}

test "corrupt bytes fail their named codec" {
    // Either decode error is a corruption verdict: some decoders read a
    // declared length from the garbage header and report the target as too
    // small before they ever parse a bad instruction.
    for (&all) |codec| {
        var decoded: [64]u8 = undefined;
        if (codec.decode("not really", &decoded)) |_| {
            return error.TestExpectedError;
        } else |err| {
            try testing.expect(err == error.DecodeFailed or err == error.BufferTooSmall);
        }
    }
}

test "byName resolves the registry and nothing else" {
    try testing.expectEqual(&gzip, byName("gzip").?);
    try testing.expectEqual(&deflate, byName("deflate").?);
    try testing.expectEqual(&snappy, byName("snappy").?);
    try testing.expectEqual(&zstd, byName("zstd").?);
    try testing.expect(byName("identity") == null);
    try testing.expect(byName("br") == null);
}
