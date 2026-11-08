//! The bench suite: the frame walk and the two field parsers every call pays
//! for on the way through.

const testingx = @import("testingx");

const frame = @import("frame_bench.zig");
const fields = @import("fields_bench.zig");

pub fn benchmarkMain(b: *testingx.B) !void {
    try b.addMod(frame);
    try b.addMod(fields);
}
