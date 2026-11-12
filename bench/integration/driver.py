#!/usr/bin/env python3
"""The driver for the local integration race: bench/integration/README.md.

It builds the three arms, then runs interleaved rounds of ghz against each:
a fresh server process per arm and size, one warmup pass, then a measured pass
per profile, with the server pinned to one set of cores and ghz to the
complementary set. Every pass lands in rounds.jsonl; summary.json and the
printed table carry the median of each cell and the ratios against the fastest
arm.

Usage: python3 bench/integration/driver.py [--rounds N] [--duration S]
  [--warmup S] [--sizes 8,1024] [--profiles c1,c64-2c,c64-16c]
  [--arms zig,go,rust] [--server-cores 0-3] [--client-cores 4-7] [--no-build]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import socket
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "bench" / "integration"
PROTO = BENCH / "bench.proto"

# One port per arm, all on loopback, so a stale process is easy to spot.
ARMS = {
    "zig": {"port": 50151, "binary": ROOT / "zig-out" / "bin" / "bench-integration-zig"},
    "go": {"port": 50152, "binary": BENCH / "bin" / "arm-go"},
    "rust": {"port": 50153, "binary": BENCH / "rust" / "target" / "release" / "bench-integration-rust"},
}

# Profile name -> (ghz concurrency, ghz connections). c1 is the closed-loop
# single-stream latency profile; the other two hold total concurrency at 64
# and vary the connection count, which is where the arm architectures differ.
PROFILES = {
    "c1": (1, 1),
    "c64-2c": (64, 2),
    "c64-16c": (64, 16),
}

CLOCK_TICKS = os.sysconf("SC_CLK_TCK")
UNITS = {"ns": 1e-6, "us": 1e-3, "µs": 1e-3, "ms": 1.0, "s": 1e3}


def log(message: str) -> None:
    print(message, flush=True)


def run(command: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(command, check=True, **kwargs)


def parse_duration(value) -> float:
    """A ghz latency, as milliseconds: a number is nanoseconds, a string may
    carry a unit suffix."""
    if isinstance(value, (int, float)):
        return float(value) * 1e-6
    for unit in ("ns", "us", "µs", "ms", "s"):
        if value.endswith(unit):
            return float(value[: -len(unit)]) * UNITS[unit]
    return float(value) * UNITS["s"]


def compose(size: int) -> bytes:
    """The serialized bench.v1.Payload with `size` bytes of data."""
    out = bytearray([0x0A])
    v = size
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)
    out.extend(b"a" * size)
    return bytes(out)


def build_arms() -> None:
    log("building arms")
    run(["zig", "build", "bench-integration-serve"], cwd=ROOT)
    BENCH.joinpath("bin").mkdir(exist_ok=True)
    run(["go", "build", "-o", str(BENCH / "bin" / "arm-go"), "."], cwd=BENCH / "go")
    run(["cargo", "build", "--release"], cwd=BENCH / "rust")


def collect_meta(args: argparse.Namespace) -> dict:
    def capture(command: list[str]) -> str:
        try:
            return subprocess.run(
                command, check=True, text=True, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            ).stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            return "unknown"

    def lock_version(name: str) -> str:
        text = (BENCH / "rust" / "Cargo.lock").read_text()
        block = re.search(
            rf'name = "{re.escape(name)}"\nversion = "([^"]+)"', text,
        )
        return block.group(1) if block else "unknown"

    go_mod = (BENCH / "go" / "go.mod").read_text() if (BENCH / "go" / "go.mod").exists() else ""
    grpc_go = re.search(r'google\.golang\.org/grpc (v[\w.\-]+)', go_mod)
    return {
        "started_utc": datetime.now(UTC).isoformat(),
        "git_rev": capture(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
        "zig": capture(["zig", "version"]),
        "ghz": capture(["ghz", "--version"]),
        "go": capture(["go", "version"]),
        "grpc_go": grpc_go.group(1) if grpc_go else "unknown",
        "rustc": capture(["rustc", "--version"]),
        "tonic": lock_version("tonic"),
        "tokio": lock_version("tokio"),
        "h2": lock_version("h2"),
        "hyper": lock_version("hyper"),
        "prost": lock_version("prost"),
        "kernel": capture(["uname", "-r"]),
        "cpu": capture(["sh", "-c", "lscpu | grep 'Model name' || true"]),
        "cores": os.cpu_count(),
        "server_cores": args.server_cores,
        "client_cores": args.client_cores,
        "duration_s": args.duration,
        "warmup_s": args.warmup,
        "rounds": args.rounds,
    }


def wait_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"port {port} never opened")


def proc_stat(pid: int) -> tuple[int, int]:
    """(utime, stime) clock ticks of one process."""
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return int(fields[11]), int(fields[12])


def stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def ghz(
    port: int,
    request_file: Path,
    concurrency: int,
    connections: int,
    duration: int,
    client_cores: str,
    output: Path,
) -> tuple[dict, float]:
    """One ghz pass: (report dict, ghz cpu seconds)."""
    command = [
        "taskset", "-c", client_cores,
        "ghz",
        "--insecure",
        "--proto", str(PROTO),
        "--call", "bench.v1.Bench/Unary",
        "--binary-file", str(request_file),
        "--concurrency", str(concurrency),
        "--connections", str(connections),
        "--duration", f"{duration}s",
        "--timeout", "10s",
        "--connect-timeout", "10s",
        "-O", "json",
        "-o", str(output),
        f"127.0.0.1:{port}",
    ]
    proc = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    _, status, usage = os.wait4(proc.pid, 0)
    proc.returncode = os.waitstatus_to_exitcode(status)
    stderr = proc.stderr.read().decode(errors="replace") if proc.stderr else ""
    if proc.returncode != 0:
        raise RuntimeError(f"ghz failed ({proc.returncode}): {stderr.strip()}")
    return json.loads(output.read_text()), usage.ru_utime + usage.ru_stime


def report_cell(report: dict) -> dict:
    """The numbers one pass keeps, from a ghz JSON report.

    Canceled calls are the in-flight requests ghz drops when the duration
    ends; they are not server failures. Every other non-OK status counts as an
    error.
    """
    latency = {
        str(entry["percentage"]): parse_duration(entry["latency"])
        for entry in report.get("latencyDistribution", [])
    }
    statuses = report.get("statusCodeDistribution", {})
    errors = sum(count for code, count in statuses.items()
                 if code not in ("OK", "Canceled"))
    return {
        "requests": report.get("count", 0),
        "errors": errors,
        "canceled": statuses.get("Canceled", 0),
        "rps": report.get("rps", 0.0),
        "average_ms": parse_duration(report.get("average", 0)),
        "p50_ms": latency.get("50", 0.0),
        "p90_ms": latency.get("90", 0.0),
        "p99_ms": latency.get("99", 0.0),
        "max_ms": parse_duration(report.get("slowest", 0)),
    }


def measure_arm(
    arm: str,
    size: int,
    profiles: list[str],
    request_file: Path,
    args: argparse.Namespace,
    out: Path,
) -> list[dict]:
    """One server process per (arm, size): warmup, then one pass per profile."""
    spec = ARMS[arm]
    cells: list[dict] = []
    with (out / f"server-{arm}.log").open("wb") as server_log:
        proc = subprocess.Popen(
            ["taskset", "-c", args.server_cores, str(spec["binary"]),
             str(spec["port"]), str(size)],
            stdout=server_log,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_port(spec["port"])
            # Warm the page cache, the allocators, and the connection path.
            # The pass is discarded.
            ghz(spec["port"], request_file, 64, 2, args.warmup,
                args.client_cores, out / "warmup.json")
            for profile in profiles:
                concurrency, connections = PROFILES[profile]
                before = proc_stat(proc.pid)
                report, ghz_cpu_s = ghz(
                    spec["port"], request_file, concurrency, connections,
                    args.duration, args.client_cores,
                    out / f"ghz-{arm}-{size}-{profile}.json",
                )
                after = proc_stat(proc.pid)
                server_cpu_s = (
                    (after[0] + after[1]) - (before[0] + before[1])
                ) / CLOCK_TICKS
                cell = report_cell(report)
                cell.update(
                    arm=arm,
                    size=size,
                    profile=profile,
                    server_cpu_s=server_cpu_s,
                    ghz_cpu_s=ghz_cpu_s,
                )
                log(
                    f"  {arm:>4} {size:>5}B {profile:>7}: "
                    f"{cell['rps']:>10.0f} rps  p99 {cell['p99_ms']:>7.3f}ms  "
                    f"server {server_cpu_s / args.duration:>5.1%}  "
                    f"errors {cell['errors']} cancels {cell['canceled']}"
                )
                cells.append(cell)
        finally:
            rss_kb = 0
            try:
                with Path(f"/proc/{proc.pid}/status").open() as status:
                    for line in status:
                        if line.startswith("VmHWM:"):
                            rss_kb = int(line.split()[1])
                            break
            except FileNotFoundError:
                pass
            for cell in cells:
                cell["server_rss_hwm_kb"] = rss_kb
            stop_process(proc)
    return cells


def summarize(cells: list[dict], out: Path, meta: dict) -> None:
    by_cell: dict[tuple[str, int, str], list[dict]] = {}
    for cell in cells:
        by_cell.setdefault((cell["arm"], cell["size"], cell["profile"]), []).append(cell)

    summary: dict[str, dict] = {}
    for (arm, size, profile), rounds in sorted(
        by_cell.items(), key=lambda kv: (kv[0][2], kv[0][1], kv[0][0])
    ):
        medians = {}
        for key in ("rps", "p50_ms", "p90_ms", "p99_ms", "server_cpu_s",
                    "ghz_cpu_s", "server_rss_hwm_kb"):
            medians[key] = statistics.median(
                [round_cell[key] for round_cell in rounds]
            )
        medians.update(arm=arm, size=size, profile=profile,
                       rounds=[round_cell["rps"] for round_cell in rounds],
                       errors=sum(round_cell["errors"] for round_cell in rounds))
        summary[f"{arm}-{size}-{profile}"] = medians

    # The ratio of each arm to the fastest arm, per profile and size.
    fastest: dict[tuple[int, str], float] = {}
    for cell in summary.values():
        key = (cell["size"], cell["profile"])
        fastest[key] = max(fastest.get(key, 0.0), cell["rps"])
    for cell in summary.values():
        cell["ratio_to_fastest"] = cell["rps"] / fastest[(cell["size"], cell["profile"])]

    log("\nmedians across rounds (ratio is against the fastest arm in the cell):")
    log("profile    size arm      rps  ratio   p50ms   p99ms  srvCPU  ghzCPU  RSS MB")
    for cell in summary.values():
        log(
            f"{cell['profile']:>7} {cell['size']:>6}B {cell['arm']:>4} "
            f"{cell['rps']:>10.0f} {cell['ratio_to_fastest']:>6.2f}x "
            f"{cell['p50_ms']:>8.3f} {cell['p99_ms']:>8.3f} "
            f"{cell['server_cpu_s'] / meta['duration_s']:>6.1%} "
            f"{cell['ghz_cpu_s'] / meta['duration_s']:>6.1%} "
            f"{cell['server_rss_hwm_kb'] / 1024:>7.1f}"
        )

    (out / "summary.json").write_text(
        json.dumps({"meta": meta, "cells": summary}, indent=2) + "\n"
    )
    log(f"\nsummary: {out / 'summary.json'}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--duration", type=int, default=15, help="seconds per pass")
    parser.add_argument("--warmup", type=int, default=3, help="warmup seconds")
    parser.add_argument("--sizes", default="8,1024", help="request/reply byte sizes")
    parser.add_argument("--profiles", default=",".join(PROFILES))
    parser.add_argument("--arms", default=",".join(ARMS))
    parser.add_argument("--server-cores", default="0-3")
    parser.add_argument("--client-cores", default="4-7")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    arms = args.arms.split(",")
    sizes = [int(size) for size in args.sizes.split(",")]
    profiles = args.profiles.split(",")
    for arm in arms:
        if arm not in ARMS:
            parser.error(f"unknown arm {arm!r}; choose from {', '.join(ARMS)}")
        if not ARMS[arm]["binary"].exists() and args.no_build:
            parser.error(f"{arm} binary missing; run without --no-build")

    out = args.out or BENCH / "results" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out.mkdir(parents=True, exist_ok=True)

    if not args.no_build:
        build_arms()

    meta = collect_meta(args)
    meta.update(arms=arms, sizes=sizes, profiles=profiles)

    # The request message each size sends, and the one ghz replays.
    for size in sizes:
        (out / f"request-{size}.bin").write_bytes(compose(size))

    cells: list[dict] = []
    rounds_path = out / "rounds.jsonl"
    with rounds_path.open("w") as rounds_file:
        for round_index in range(args.rounds):
            # Rotate the arm order each round so a slow drift in the box
            # hits the arms evenly.
            order = arms[round_index % len(arms):] + arms[:round_index % len(arms)]
            log(f"round {round_index + 1}/{args.rounds} (order: {', '.join(order)})")
            for arm in order:
                for size in sizes:
                    cells.extend(measure_arm(
                        arm, size, profiles, out / f"request-{size}.bin", args, out,
                    ))
                    for cell in cells[-len(profiles):]:
                        rounds_file.write(json.dumps(cell) + "\n")
                    rounds_file.flush()
    summarize(cells, out, meta)
    return 0


if __name__ == "__main__":
    sys.exit(main())
