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
    "evented": {"port": 50154, "binary": ROOT / "zig-out" / "bin" / "bench-integration-evented"},
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
    run(
        ["zig", "build", "bench-integration-serve", "bench-integration-serve-evented",
         "bench-integration-loadgen"],
        cwd=ROOT,
    )
    BENCH.joinpath("bin").mkdir(exist_ok=True)
    run(["go", "build", "-o", str(BENCH / "bin" / "arm-go"), "."], cwd=BENCH / "go")
    run(["cargo", "build", "--release"], cwd=BENCH / "rust")


LOADGEN = ROOT / "zig-out" / "bin" / "bench-integration-loadgen"


def core_count(spec: str) -> int:
    """How many cpus a taskset -c spec covers, like '0-3' or '5'."""
    total = 0
    for part in spec.split(","):
        if "-" in part:
            low, high = part.split("-")
            total += int(high) - int(low) + 1
        else:
            total += 1
    return total

# The capacity ladder: (connections, streams per connection) steps, in total
# in-flight order 64, 256, 1024, 4096. A server is done when its cores fill;
# the client is done the same way.
LADDER = ((4, 16), (16, 16), (16, 64), (64, 64))


def run_loadgen_window(
    port: int,
    duration: int,
    connections: int,
    streams: int,
    size: int,
    client_cores: str,
) -> tuple[dict, float, float]:
    """One capacity window with the loadgen's own CPU seconds, via wait4."""
    proc = subprocess.Popen(
        ["taskset", "-c", client_cores, str(LOADGEN), f"127.0.0.1:{port}",
         str(duration), str(connections), str(streams), str(size)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    _, status, usage = os.wait4(proc.pid, 0)
    proc.returncode = os.waitstatus_to_exitcode(status)
    stdout = proc.stdout.read().decode(errors="replace")
    stderr = proc.stderr.read().decode(errors="replace") if proc.stderr else ""
    if proc.returncode != 0:
        raise RuntimeError(
            f"loadgen failed ({proc.returncode}): {stderr.strip()[:200]}"
        )
    report: dict = {}
    for line in stdout.splitlines():
        if line.startswith("{"):
            report = json.loads(line)
    if not report:
        raise RuntimeError("loadgen printed no report")
    return report, usage.ru_utime + usage.ru_stime, 0.0


def capacity_mode(args: argparse.Namespace, out: Path) -> None:
    """The capacity ladder: every arm under the zig load generator, one fresh
    server per step, until the server's cores or the client's fill. ghz stays
    the reference client for the published race; the capacity numbers belong
    to whichever side saturates first."""
    if not args.no_build:
        build_arms()
    sizes = [int(size) for size in args.sizes.split(",")]
    cores = os.cpu_count() or 8
    rows: list[dict] = []
    rows_path = out / "capacity.jsonl"
    with rows_path.open("w") as rows_file:
        for size in sizes:
            (out / f"request-{size}.bin").write_bytes(compose(size))
        for arm in args.arms.split(","):
            spec = ARMS[arm]
            for size in sizes:
                for connections, streams in LADDER:
                    with (out / f"server-{arm}.log").open("wb") as server_log:
                        server = subprocess.Popen(
                            ["taskset", "-c", args.server_cores,
                             str(spec["binary"]), str(spec["port"]), str(size)],
                            stdout=server_log, stderr=subprocess.STDOUT,
                        )
                        try:
                            wait_port(spec["port"])
                            before = proc_stat(server.pid)
                            report, client_cpu_s, _ = run_loadgen_window(
                                spec["port"], args.duration, connections,
                                streams, size, args.client_cores,
                            )
                            after = proc_stat(server.pid)
                        finally:
                            stop_process(server)
                    wait_port_closed(spec["port"])
                    server_cpu_s = (
                        (after[0] + after[1]) - (before[0] + before[1])
                    ) / CLOCK_TICKS
                    row = {
                        "arm": arm,
                        "size": size,
                        "connections": connections,
                        "streams": streams,
                        "in_flight": connections * streams,
                        "rps": report["rps"],
                        "count": report["count"],
                        "errors": report["errors"],
                        "verify_errors": report.get("verify_errors", 0),
                        "p50_ms": report["p50_us"] / 1e3,
                        "p99_ms": report["p99_us"] / 1e3,
                        "server_cpu_s": server_cpu_s,
                        "client_cpu_s": client_cpu_s,
                    }
                    rows.append(row)
                    rows_file.write(json.dumps(row) + "\n")
                    rows_file.flush()
                    server_share = server_cpu_s / (args.duration + 1)
                    client_share = client_cpu_s / (args.duration + 1)
                    server_core_count = core_count(args.server_cores)
                    client_core_count = core_count(args.client_cores)
                    log(
                        f"  {arm:>4} {size:>5}B {connections * streams:>5} in flight: "
                        f"{row['rps']:>10.0f} rps  p50 {row['p50_ms']:>7.3f}ms  "
                        f"p99 {row['p99_ms']:>7.3f}ms  server {server_share:>5.1%}  "
                        f"client {client_share:>5.1%}  errors {row['errors']} "
                        f"verify {row['verify_errors']}"
                    )
                    if row["errors"] > row["count"] * 0.01:
                        log(f"    {arm} {size}B: over 1% errors, ladder stops")
                        break
                    if server_share >= 0.9 * server_core_count:
                        log(f"    {arm} {size}B: server cores full, ladder stops")
                        break
                    if client_share >= 0.9 * client_core_count:
                        log(f"    {arm} {size}B: client cores full, ladder stops")
                        break
    log("\ncapacity (highest rps step per arm before its side saturated):")
    for (arm, size), _rows in sorted(
        {(row["arm"], row["size"]): None for row in rows}.items()
    ):
        cells = [row for row in rows if row["arm"] == arm and row["size"] == size]
        best = max(cells, key=lambda row: row["rps"])
        log(
            f"  {arm:>4} {size:>5}B: {best['rps']:>10.0f} rps at "
            f"{best['in_flight']} in flight (p99 {best['p99_ms']:.3f} ms)"
        )
    log(f"\nrows: {rows_path}")


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


def parse_perf_stat(path: Path) -> dict[str, float]:
    """perf stat -x, output: one value,unit,event,... line per counter."""
    counts: dict[str, float] = {}
    for line in path.read_text().splitlines():
        fields = line.split(",")
        if len(fields) < 3:
            continue
        # The event name carries the privilege modifier ("cycles:u"); the
        # counters table is keyed without it.
        event = fields[2].split(":")[0].strip()
        try:
            counts[event] = float(fields[0].replace(",", ""))
        except ValueError:
            continue
    return counts


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




def wait_port_closed(port: int, timeout: float = 10.0) -> None:
    """The port must refuse connections after a pass: a leftover server would
    answer the next window's requests with the wrong binary."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                time.sleep(0.2)
        except OSError:
            return
    raise RuntimeError(f"port {port} still open; a server survived its pass")


def perf_stat_events() -> list[str]:
    # cycles, instructions, branches, and branch-misses need the hardware PMU,
    # which perf_event_paranoid=2 denies, so they read as not-counted; the
    # software events always work.
    return [
        "task-clock", "cycles", "instructions", "branches",
        "branch-misses", "page-faults",
    ]


def stop_wrapped(proc: subprocess.Popen) -> None:
    """Stop one arm under a wrapper (perf stat, perf record, strace) by
    stopping the arm itself: every one of these tools reports its summary
    when the command it measured ends, so a SIGTERM to the arm gets the
    counters, the data file, or the syscall table out cleanly. A signal to
    the wrapper is the wrong lever: nix's perf wrapper is a shell script that
    swallows SIGINT, and SIGKILL on perf stat loses the counters entirely.
    The arm is the wrapper's direct child (taskset execs into it); if it
    somehow survives the TERM, it is killed so it cannot hold the port."""
    children = Path(f"/proc/{proc.pid}/task/{proc.pid}/children")
    parts = children.read_text().split() if children.exists() else []
    child_pid = parts[0] if parts else ""
    if child_pid:
        try:
            os.kill(int(child_pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    if child_pid and Path(f"/proc/{child_pid}").exists():
        try:
            os.kill(int(child_pid), signal.SIGKILL)
        except ProcessLookupError:
            pass


def profile_stat(
    arm: str, size: int, profile: str, args: argparse.Namespace, out: Path,
) -> None:
    """One perf stat window per arm: counters for the whole server process,
    normalized per request from the same window's ghz report."""
    spec = ARMS[arm]
    stat_file = out / f"perfstat-{arm}-{size}-{profile}.csv"
    concurrency, connections = PROFILES[profile]
    with stat_file.open("wb") as captured:
        proc = subprocess.Popen(
            ["perf", "stat", "-x,", "-e", ",".join(perf_stat_events()),
             "--", "taskset", "-c", args.server_cores, str(spec["binary"]),
             str(spec["port"]), str(size)],
            stdout=captured,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_port(spec["port"])
            report, _ = ghz(
                spec["port"], out / f"request-{size}.bin", concurrency,
                connections, args.duration, args.client_cores,
                out / f"ghz-stat-{arm}.json",
            )
        finally:
            stop_wrapped(proc)
    wait_port_closed(spec["port"])
    counts = parse_perf_stat(stat_file)
    requests = report.get("count", 0) or 1
    log(f"  perf stat {arm} {size}B {profile}: {report.get('count', 0)} requests")
    for key in perf_stat_events():
        if key in counts:
            log(
                f"    {key:>16}: {counts[key]:>16,.2f}"
                + (
                    f", {counts[key] / requests:>10,.1f} per request"
                    if key in ("cycles", "instructions", "branches",
                               "branch-misses", "page-faults")
                    else ""
                )
            )
    if "instructions" in counts and "cycles" in counts and counts["cycles"]:
        log(f"    {'IPC':>16}: {counts['instructions'] / counts['cycles']:>16.2f}")


def profile_record(
    arm: str, size: int, profile: str, args: argparse.Namespace, out: Path,
) -> None:
    """One perf record window for the arm: a symbol profile of the server
    under load, on the software cpu-clock event (the hardware PMU is denied),
    with DWARF call graphs when they work."""
    spec = ARMS[arm]
    concurrency, connections = PROFILES[profile]
    data = out / f"perf-{arm}-{size}-{profile}.data"
    report_file = out / f"perf-{arm}-{size}-{profile}.txt"
    # --call-graph makes perf open the precise-IP event variant, which
    # perf_event_paranoid=2 denies on this box; the default cycles event
    # works, so the passes are flat symbol profiles.
    for call_graph in (None,):
        command = [
            "perf", "record", "-F", "499",
            "-o", str(data),
        ]
        if call_graph:
            command += ["--call-graph", call_graph]
        with (out / f"perf-record-{arm}-{profile}.stderr").open("wb") as err:
            proc = subprocess.Popen(
                command + ["--", "taskset", "-c", args.server_cores,
                           str(spec["binary"]), str(spec["port"]), str(size)],
                stdout=subprocess.DEVNULL,
                stderr=err,
            )
            try:
                wait_port(spec["port"])
                ghz(
                    spec["port"], out / f"request-{size}.bin", concurrency,
                    connections, args.duration, args.client_cores,
                    out / f"ghz-record-{arm}-{profile}.json",
                )
            finally:
                stop_wrapped(proc)
        wait_port_closed(spec["port"])
        if data.exists() and data.stat().st_size > 0:
            break
        log(f"  perf record {call_graph or 'flat'} produced nothing, retrying")
    else:
        raise RuntimeError("no perf record mode worked")

    subprocess.run(
        ["perf", "report", "--stdio", "--header", "-i", str(data),
         "--percent-limit", "1", "--sort", "symbol,dso"],
        check=True,
        stdout=report_file.open("w"),
    )
    log(f"  perf record {arm} {size}B {profile} (flat symbols): {report_file.name}")


def profile_strace(
    arm: str, size: int, args: argparse.Namespace, out: Path,
) -> None:
    """One strace -c window at the latency profile: syscall counts per
    request. yama ptrace_scope=1 forbids attaching to a sibling, so strace
    wraps the arm and is its parent. ptrace multiplies the cost of every
    syscall, so the window runs the closed-loop single-stream profile and
    counts, never times."""
    spec = ARMS[arm]
    trace_file = out / f"strace-{arm}-{size}.txt"
    with trace_file.open("wb") as captured:
        tracer = subprocess.Popen(
            ["strace", "-c", "-f", "taskset", "-c", args.server_cores,
             str(spec["binary"]), str(spec["port"]), str(size)],
            stdout=captured,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_port(spec["port"])
            report, _ = ghz(
                spec["port"], out / f"request-{size}.bin", 1, 1,
                args.duration, args.client_cores,
                out / f"ghz-strace-{arm}.json",
            )
        finally:
            stop_wrapped(tracer)
    wait_port_closed(spec["port"])
    requests = report.get("count", 0) or 1
    log(f"  strace {arm} {size}B c1: {requests} requests")
    for line in trace_file.read_text().splitlines():
        fields = line.split()
        if len(fields) == 6 and fields[5] == "syscall":
            continue
        if len(fields) == 6 and fields[3].replace(",", "").isdigit():
            calls = float(fields[3].replace(",", ""))
            log(
                f"    {fields[5]:>16}: {calls:>12,.0f} calls, "
                f"{calls / requests:>8.2f} per request"
            )
        elif "total calls" in line or "unfinished" in line.lower():
            log(f"    {line.strip()}")


def profile_counting(
    size: int, profile: str, args: argparse.Namespace, out: Path,
) -> None:
    """The zig arm with its counting allocator: steady-state allocations per
    request, straight off the arm's stderr marks."""
    spec = ARMS["zig"]
    log_path = out / f"counting-{size}-{profile}.log"
    with log_path.open("wb") as server_log:
        proc = subprocess.Popen(
            ["taskset", "-c", args.server_cores, str(spec["binary"]),
             str(spec["port"]), str(size), "count"],
            stdout=server_log,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_port(spec["port"])
            # One window under the named profile; the marks inside it are the
            # steady-state figure.
            concurrency, connections = PROFILES[profile]
            ghz(
                spec["port"], out / f"request-{size}.bin", concurrency,
                connections, args.duration, args.client_cores,
                out / "ghz-counting.json",
            )
        finally:
            stop_process(proc)
    wait_port_closed(spec["port"])
    lines = [
        line for line in log_path.read_text().splitlines()
        if line.startswith("count:")
    ]
    log(f"  counting zig {size}B {profile}: {len(lines)} marks")
    for line in lines[-3:]:
        log(f"    {line}")


def profile_mode(args: argparse.Namespace, out: Path) -> None:
    """The profiling passes: perf stat for every arm, perf record, strace, and
    allocation counting for the zig arm. Artifacts under the run directory;
    the interesting lines are echoed to the console."""
    if not args.no_build:
        build_arms()
    sizes = [int(size) for size in args.sizes.split(",")]
    for size in sizes:
        (out / f"request-{size}.bin").write_bytes(compose(size))
    # The loaded window is the multiplexing profile when the matrix includes
    # it, and the first named profile otherwise.
    profiles = args.profiles.split(",")
    loaded = "c64-2c" if "c64-2c" in profiles else profiles[0]
    log(f"profiling (windows {loaded} and c1, {args.duration}s per pass)")
    for arm in args.arms.split(","):
        profile_stat(arm, sizes[0], loaded, args, out)
    for arm in args.arms.split(","):
        if arm == "zig":
            profile_record(arm, sizes[0], loaded, args, out)
            if loaded != "c1":
                profile_record(arm, sizes[0], "c1", args, out)
            profile_strace(arm, sizes[0], args, out)
            profile_counting(sizes[0], loaded, args, out)
            if loaded != "c1":
                profile_counting(sizes[0], "c1", args, out)
    log(f"\nartifacts: {out}")


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
    parser.add_argument(
        "--profile", action="store_true",
        help="run the profiling passes instead of the matrix",
    )
    parser.add_argument(
        "--capacity", action="store_true",
        help="run the capacity ladder instead of the matrix",
    )
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

    if args.profile:
        for arm in args.arms.split(","):
            if arm not in ARMS:
                parser.error(f"unknown arm {arm!r}; choose from {', '.join(ARMS)}")
            if not ARMS[arm]["binary"].exists() and args.no_build:
                parser.error(f"{arm} binary missing; run without --no-build")
        profile_mode(args, out)
        return 0

    if args.capacity:
        for arm in args.arms.split(","):
            if arm not in ARMS:
                parser.error(f"unknown arm {arm!r}; choose from {', '.join(ARMS)}")
            if not ARMS[arm]["binary"].exists() and args.no_build:
                parser.error(f"{arm} binary missing; run without --no-build")
        capacity_mode(args, out)
        return 0

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
