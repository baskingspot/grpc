//! `message` benchmarks: the five-byte header and the walk over whole frames.

const testingx = @import("testingx");
const grpc = @import("grpc");

pub fn benchmarkHeaderParse(b: *testingx.B) !void {
    var buffer: [8]u8 = undefined;
    grpc.message.writeHeader(buffer[0..grpc.message.prefix_len], false, 1024);
    const bytes = b.blackBox(buffer[0..grpc.message.prefix_len]);

    var sink: u32 = 0;
    while (try b.loop()) {
        const header = try grpc.message.parseHeader(bytes);
        sink +%= header.length;
    }
    b.keepAlive(sink);
}

pub fn benchmarkWalkOneMessage(b: *testingx.B) !void {
    var buffer: [1024 + grpc.message.prefix_len]u8 = undefined;
    @memset(buffer[grpc.message.prefix_len..], 'x');
    const body = buffer[grpc.message.prefix_len..];
    const framed = grpc.message.write(&buffer, body, false) catch unreachable;
    const bytes = b.blackBox(framed);

    var sink: u32 = 0;
    while (try b.loop()) {
        var walker = grpc.message.Iterator.init(bytes);
        while (try walker.next()) |found| sink +%= @intCast(found.bytes.len);
    }
    b.keepAlive(sink);
}

pub fn benchmarkWalkSixteenMessages(b: *testingx.B) !void {
    var body: [256]u8 = undefined;
    @memset(&body, 'y');
    var buffer: [16 * (256 + grpc.message.prefix_len)]u8 = undefined;

    var used: usize = 0;
    var index: usize = 0;
    while (index < 16) : (index += 1) {
        const framed = grpc.message.write(buffer[used..], &body, false) catch unreachable;
        used += framed.len;
    }
    const bytes = b.blackBox(buffer[0..used]);

    var sink: u32 = 0;
    while (try b.loop()) {
        var walker = grpc.message.Iterator.init(bytes);
        while (try walker.next()) |found| sink +%= @intCast(found.bytes.len);
    }
    b.keepAlive(sink);
}
