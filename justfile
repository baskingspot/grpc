# grpc: gRPC in Zig. Message framing, status codes, and calls over HTTP/2, with no code generation. `just --list` shows the recipes.
# AGENTS.md holds the working conventions.

[private]
default:
    @just --list

[doc("Unit tests, through the testingx runner, with a hard timeout so a hung socket test fails instead of waiting")]
[group("test")]
test:
    zig build test --summary all -- --timeout=2m

[doc("Run the greeter server, call it with the client example, and stop it")]
[group("test")]
demo-greeter NAME="baskin":
    #!/usr/bin/env bash
    set -euo pipefail
    log=$(mktemp)
    zig build serve-greeter >"$log" 2>&1 &
    pid=$!
    trap 'kill "$pid" 2>/dev/null || true; pkill -f greeter-example 2>/dev/null || true; rm -f "$log"' EXIT

    for _ in $(seq 1 100); do
        if (exec 3<>/dev/tcp/127.0.0.1/50051) 2>/dev/null; then
            exec 3>&-
            break
        fi
        sleep 0.1
    done
    zig build demo-unary -- "{{ NAME }}"

[doc("Run the etcd example against a throwaway etcd on 127.0.0.1:22379, then stop it. Set ETCD_PORT to move it")]
[group("test")]
demo-etcd:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v etcd >/dev/null || { echo "etcd is not on PATH; run inside nix develop" >&2; exit 1; }
    port="${ETCD_PORT:-22379}"
    peer="$((port + 1))"
    data=$(mktemp -d)
    etcd --data-dir "$data" \
        --listen-client-urls "http://127.0.0.1:${port}" \
        --advertise-client-urls "http://127.0.0.1:${port}" \
        --listen-peer-urls "http://127.0.0.1:${peer}" \
        --initial-advertise-peer-urls "http://127.0.0.1:${peer}" \
        --initial-cluster "default=http://127.0.0.1:${peer}" \
        >"$data/etcd.log" 2>&1 &
    pid=$!
    trap 'kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; rm -rf "$data"' EXIT

    for _ in $(seq 1 100); do
        if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
            exec 3>&-
            break
        fi
        sleep 0.1
    done
    zig build demo-etcd -- "127.0.0.1:${port}"

[doc("Compile the example in examples/, which is the README's code")]
[group("test")]
example:
    zig build example

[doc("Benchmarks (ReleaseFast by default); args pass to the binary: just bench --benchtime=250ms --filter=Frame")]
[group("bench")]
bench *ARGS:
    zig build bench -- {{ ARGS }}

[doc("Run every benchmark once: proves the suite builds and runs, without measuring")]
[group("bench")]
bench-smoke:
    zig build bench -- --benchtime=8x --count=1

[doc("Lint Zig, Nix, Markdown, the workflows, and this justfile: ziglint, zig fmt, nix fmt, oxfmt, actionlint, zizmor, and just --fmt")]
[group("lint")]
lint:
    ziglint
    zig fmt --check src bench examples build.zig
    nix fmt -- --ci
    oxfmt --check README.md AGENTS.md .github
    actionlint
    zizmor .
    just --fmt --check

[doc("Format everything lint checks")]
[group("lint")]
fmt:
    zig fmt src bench examples build.zig
    nix fmt
    oxfmt README.md AGENTS.md .github
    just --fmt

[doc("Everything CI runs")]
[group("ci")]
ci: test lint
