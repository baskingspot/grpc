# grpc: gRPC in Zig. Message framing, status codes, and calls over HTTP/2, with no code generation. `just --list` shows the recipes.
# AGENTS.md holds the working conventions.

[private]
default:
    @just --list

[doc("Unit tests, through the testingx runner, with a hard timeout so a hung socket test fails instead of waiting")]
[group("test")]
test:
    zig build test --summary all -- --timeout=2m

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
