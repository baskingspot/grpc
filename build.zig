const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // The HTTP/2 client this sits on. `http` is untagged while incubating, so
    // the pin is a commit; `update-deps.py` moves it.
    const http_dep = b.dependency("http", .{ .target = target, .optimize = optimize });

    // The one exported module. A consumer imports `grpc` and gets this, and
    // nothing else: there is no binary, and no dev-only graph yet.
    const mod = b.addModule("grpc", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "http", .module = http_dep.module("http") }},
    });

    const tests = b.addTest(.{ .root_module = mod });
    const run_tests = b.addRunArtifact(tests);
    run_tests.addPassthruArgs();
    const test_step = b.step("test", "Run the unit tests");
    test_step.dependOn(&run_tests.step);
}
