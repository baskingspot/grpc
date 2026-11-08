//! `status` benchmarks: the two trailer fields a call ends with.

const testingx = @import("testingx");
const grpc = @import("grpc");

pub fn benchmarkStatusParse(b: *testingx.B) !void {
    const wire = b.blackBox("14");

    var sink: u8 = 0;
    while (try b.loop()) {
        const code = try grpc.status.parse(wire);
        sink +%= @backingInt(code);
    }
    b.keepAlive(sink);
}

pub fn benchmarkMessageDecode(b: *testingx.B) !void {
    const wire = b.blackBox("no%20such%20service%3A%20helloworld.Greeter");
    var out: [64]u8 = undefined;

    var sink: usize = 0;
    while (try b.loop()) {
        const decoded = try grpc.status.decode(&out, wire);
        sink +%= decoded.len;
    }
    b.keepAlive(sink);
}

pub fn benchmarkTimeoutEncode(b: *testingx.B) !void {
    const deadline = b.blackBox(@as(u64, 1_500_000_000) * 5);
    var out: [9]u8 = undefined;

    var sink: usize = 0;
    while (try b.loop()) {
        sink +%= grpc.timeout.encode(&out, deadline).len;
    }
    b.keepAlive(sink);
}
