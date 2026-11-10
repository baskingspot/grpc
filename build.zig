const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const expose_tls = b.option(bool, "tls", "Enable grpc_tls and its TLS dependencies") orelse
        (b.pkg_hash.len == 0);

    const http_dep = b.dependency("http", .{
        .target = target,
        .optimize = optimize,
        .tls = expose_tls,
    });
    const stdx_dep = b.dependency("stdx", .{ .target = target, .optimize = optimize });

    // Core consumers configure only HTTP. TLS and the developer graph are
    // separate opt-ins, so neither enters a plain consumer's fetch path.
    const mod = b.addModule("grpc", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "http", .module = http_dep.module("http") },
            .{ .name = "stdx", .module = stdx_dep.module("stdx") },
        },
    });

    const tls_mod: ?*std.Build.Module = if (expose_tls) tls: {
        const module = b.addModule("grpc_tls", .{
            .root_source_file = b.path("src/tls.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "grpc", .module = mod },
                .{ .name = "http_tls", .module = http_dep.module("http_tls") },
            },
        });
        module.link_libc = true;
        module.linkSystemLibrary("crypto", .{});
        break :tls module;
    } else null;

    if (b.pkg_hash.len != 0) return;

    const testingx_dep = b.lazyDependency("testingx", .{ .target = target });
    const test_runner: ?std.Build.Step.Compile.TestRunner = if (testingx_dep) |dep|
        .{ .path = dep.path("src/runner.zig"), .mode = .simple }
    else
        null;
    const conformance_test_step = b.step(
        "test-conformance",
        "Run the conformance harness unit tests",
    );

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
    const tls_dep = if (expose_tls) b.lazyDependency("tls", .{
        .target = target,
        .optimize = optimize,
    }) else null;
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
        if (compress_dep) |gzip_dep| if (tls_mod) |tls_module| {
            const conformance_client = b.addExecutable(.{
                .name = "conformance-client",
                .root_module = b.createModule(.{
                    .root_source_file = b.path("conformance/main.zig"),
                    .target = target,
                    .optimize = optimize,
                    .imports = &.{
                        .{ .name = "grpc", .module = mod },
                        .{ .name = "http", .module = http_dep.module("http") },
                        .{ .name = "grpc_tls", .module = tls_module },
                        .{ .name = "protobuf", .module = pb_dep.module("protobuf") },
                        .{ .name = "compress", .module = gzip_dep.module("compress") },
                    },
                }),
            });
            // The TLS transport brings libcrypto, exactly as the server engine
            // does; the library itself still links no crypto.
            conformance_client.root_module.link_libc = true;
            conformance_client.root_module.linkSystemLibrary("crypto", .{});
            conformance_client.step.dependOn(b.getInstallStep());
            b.step("conformance-client", "Build the conformance client under test")
                .dependOn(&b.addInstallArtifact(conformance_client, .{}).step);

            if (tls_dep) |crypto_dep| {
                const conformance_server = b.addExecutable(.{
                    .name = "conformance-server",
                    .root_module = b.createModule(.{
                        .root_source_file = b.path("conformance/server_main.zig"),
                        .target = target,
                        .optimize = optimize,
                        .imports = &.{
                            .{ .name = "grpc", .module = mod },
                            .{ .name = "grpc_tls", .module = tls_module },
                            .{ .name = "http", .module = http_dep.module("http") },
                            .{ .name = "tls", .module = crypto_dep.module("tls") },
                            .{ .name = "protobuf", .module = pb_dep.module("protobuf") },
                            .{ .name = "compress", .module = gzip_dep.module("compress") },
                        },
                    }),
                });
                conformance_server.step.dependOn(b.getInstallStep());
                b.step("conformance-server", "Build the conformance server under test")
                    .dependOn(&b.addInstallArtifact(conformance_server, .{}).step);

                const server_tests = b.addTest(.{
                    .root_module = conformance_server.root_module,
                    .test_runner = test_runner,
                });
                const run_server_tests = b.addRunArtifact(server_tests);
                passthruArgs(b, run_server_tests);
                conformance_test_step.dependOn(&run_server_tests.step);
            }
        };

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
        const bench_http = b.dependency("http", .{
            .target = target,
            .optimize = bench_optimize,
            .tls = false,
        });
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
