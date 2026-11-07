# AGENTS.md

Working notes for AI coding agents in baskingspot/grpc.

## What this is

gRPC in Zig: message framing, status codes, and calls over HTTP/2, with no code generation. It is incubating: unstable, with no promises about its API.

## Environment

Zig 0.17, the current stable. The flake is the tool contract, from `baskingspot/nix`: `nix develop` provides Zig, `ziglint`, and the base tools, and nothing comes from the system PATH. The justfile is the command interface: `just test`, `just lint`, `just ci`. Run `just fmt` before you commit.

A repo that must also build on the previous release follows the dual-version pattern: import `zig."0.16"` and `zig."0.17"`, give the older one its own `zig-old` shell, hold the shims in `src/compat.zig`, run `just ci` on the default shell and `just check` on the compatibility lane, and set `minimum_zig_version` to the older one. `baskingspot/cli`, `baskingspot/json5`, and `baskingspot/toml` do that today. Write down which versions the repo supports here, in this paragraph, so there is one place to look.

## Rules

- Commit messages are one line, `area: summary`, and every commit is signed. Do not rewrite pushed history.
- Gate commands on their exit codes, and claim only what you ran.
- Format with `zig fmt` and lint with `ziglint`. `just fmt` and `just lint` run both.
- A dependency a user does not need is lazy. Call `b.lazyDependency`, and put every use that needs a value inside `if (dep) |d| { ... }`. The first configure pass returns null, and the build runner fetches the package after it. An unwrap outside that block panics, and it reads as a broken build script. A warm `zig-pkg` hides this, so check with `rm -rf zig-pkg .zig-cache && zig build test`.
- This repo's GitHub settings come from `baskingspot/.github`, with `just settings`. Do not change them by hand.
- To graduate the project, follow the `baskingspot-new-repo` skill: logos, a real README, and the audit passing.

## Changelog

`CHANGELOG.md` is one file, Keep a Changelog format, newest first. Add a bullet
under `## [Unreleased]` in the same change that alters something a user can
notice: the public API, expected behavior, performance a benchmark row would
show, a supported backend, or the build requirements. Docs, CI, and internal
refactors do not need one unless they change how someone uses the package.

Write from the user's side: what changed, and what they must do about it. Name
the symbol, flag, or error. One line, under Added, Changed, Fixed, or Removed.

To release, run `release-prep.py VERSION` in the repo, with the script from
`baskingspot/.github`, review the diff, commit it signed, push, then
`gh workflow run release.yml -f version=X.Y.Z`. The prep moves the Unreleased
section into a dated heading, sets `.version` in `build.zig.zon`, and points the
install command at the tag. The workflow changes no files and creates no commit,
and it refuses a version the changelog does not carry. Do not tag by hand.
