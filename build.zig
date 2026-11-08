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

    // The README example, compiled on every run so it cannot rot. It is not run:
    // it wants a server that speaks helloworld.Greeter.
    // The examples speak protobuf, so they need the codec. It is lazy: a
    // consumer of grpc never fetches it, and a fresh tree pulls it once.
    const protobuf_dep = b.lazyDependency("protobuf", .{
        .target = target,
        .optimize = optimize,
    });
    const compress_dep = b.lazyDependency("compress", .{
        .target = target,
        .optimize = optimize,
    });
    if (protobuf_dep) |pb_dep| {
        const example_step = b.step("example", "Build the examples");

        const unary_example = b.addExecutable(.{
            .name = "unary-example",
            .root_module = b.createModule(.{
                .root_source_file = b.path("examples/unary.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "grpc", .module = mod },
                    .{ .name = "protobuf", .module = pb_dep.module("protobuf") },
                },
            }),
        });
        example_step.dependOn(&unary_example.step);

        const run_unary = b.addRunArtifact(unary_example);
        passthruArgs(b, run_unary);
        b.step("demo-unary", "Call the greeter example").dependOn(&run_unary.step);

        const greeter_example = b.addExecutable(.{
            .name = "greeter-example",
            .root_module = b.createModule(.{
                .root_source_file = b.path("examples/greeter.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "grpc", .module = mod },
                    .{ .name = "protobuf", .module = pb_dep.module("protobuf") },
                },
            }),
        });
        example_step.dependOn(&greeter_example.step);

        // The conformance client the connectrpc runner drives over stdin and
        // stdout. Dev-only, like the examples.
        if (compress_dep) |gzip_dep| {
            const conformance_client = b.addExecutable(.{
                .name = "conformance-client",
                .root_module = b.createModule(.{
                    .root_source_file = b.path("conformance/main.zig"),
                    .target = target,
                    .optimize = optimize,
                    .imports = &.{
                        .{ .name = "grpc", .module = mod },
                        .{ .name = "http", .module = http_dep.module("http") },
                        .{ .name = "protobuf", .module = pb_dep.module("protobuf") },
                        .{ .name = "compress", .module = gzip_dep.module("compress") },
                    },
                }),
            });
            conformance_client.step.dependOn(b.getInstallStep());
            b.step("conformance-client", "Build the conformance client under test")
                .dependOn(&b.addInstallArtifact(conformance_client, .{}).step);
        }

        const run_greeter = b.addRunArtifact(greeter_example);
        passthruArgs(b, run_greeter);
        b.step("serve-greeter", "Serve the greeter example on 127.0.0.1:50051")
            .dependOn(&run_greeter.step);

        const etcd_example = b.addExecutable(.{
            .name = "etcd-example",
            .root_module = b.createModule(.{
                .root_source_file = b.path("examples/etcd.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "grpc", .module = mod },
                    .{ .name = "protobuf", .module = pb_dep.module("protobuf") },
                },
            }),
        });
        const etcd_step = b.step("example-etcd", "Build the etcd example");
        etcd_step.dependOn(&etcd_example.step);

        // Runs an etcd demo against whatever endpoint is passed through. The
        // `demo-etcd` recipe starts a throwaway etcd first.
        const run_etcd = b.addRunArtifact(etcd_example);
        passthruArgs(b, run_etcd);
        b.step("demo-etcd", "Run the etcd example against a running etcd")
            .dependOn(&run_etcd.step);
    }

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
