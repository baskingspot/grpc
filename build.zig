const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // The HTTP/2 client this sits on. `http` is untagged while incubating, so
    // the pin is a commit; `update-deps.py` moves it.
    const http_dep = b.dependency("http", .{
        .target = target,
        .optimize = optimize,
    });

    // The one exported module. A consumer imports `grpc` and gets this, and
    // nothing else: there is no binary.
    const mod = b.addModule("grpc", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "http", .module = http_dep.module("http") }},
    });

    if (b.pkg_hash.len != 0) return;

    // The runner and the bench engine. Lazy because only the root build uses
    // them; a consumer never fetches testingx. On the first configure pass it
    // is not fetched yet, `lazyDependency` returns null, and the build runner
    // fetches it and reruns this script.
    const testingx_dep = b.lazyDependency("testingx", .{ .target = target });
    const test_runner: ?std.Build.Step.Compile.TestRunner = if (testingx_dep) |dep|
        .{ .path = dep.path("src/runner.zig"), .mode = .simple }
    else
        null;

    const tests = b.addTest(.{
        .root_module = mod,
        .test_runner = test_runner,
    });
    const run_tests = b.addRunArtifact(tests);
    passthruArgs(b, run_tests);

    const test_step = b.step("test", "Run the unit tests");
    test_step.dependOn(&run_tests.step);

    if (b.lazyImport(@This(), "testingx")) |testingx| {
        const dep = testingx_dep orelse unreachable;
        const bench_optimize: std.builtin.OptimizeMode = .ReleaseFast;
        const bench_http = b.dependency("http", .{ .target = target, .optimize = bench_optimize });
        const bench_mod = b.addModule("grpc-bench", .{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = bench_optimize,
            .imports = &.{.{ .name = "http", .module = bench_http.module("http") }},
        });

        const run_bench = testingx.addBench(b, .{
            .module = dep.module("testingx"),
            .root_source_file = b.path("bench/bench.zig"),
            .imports = &.{.{ .name = "grpc", .module = bench_mod }},
            .target = target,
        });
        passthruArgs(b, run_bench);

        b.step("bench", "Run benchmarks").dependOn(&run_bench.step);
    }
}

/// `b.args` (removed in Zig 0.17) and `Run.addPassthruArgs` are the same
/// feature on either side of the split. Probe the build API, not the version.
fn passthruArgs(b: *std.Build, run: *std.Build.Step.Run) void {
    if (comptime @hasDecl(std.Build.Step.Run, "addPassthruArgs")) {
        run.addPassthruArgs();
    } else if (b.args) |args| {
        run.addArgs(args);
    }
}
