# The mission

**`just conformance-tls` reports 644 passed, 0 failed.** Done.

The last failure was never a lost byte. `blocking.Transport.read` preferred a
raw-fd read whenever a call carried a deadline, and on a TLS connection the fd
carries ciphertext: the session read the first three bytes as a frame length and
poisoned itself with FRAME_SIZE_ERROR. Only the Deadline Propagation cases set a
deadline, which is why exactly those failed. `Client.initIo` now takes who owns
the deadline, and the TLS bridge owns it.

Historical note, kept because it explains two real fixes that did not move the
number: plaintext delivered before `handshake_complete` was dropped, and
`Connection.receive` accepts a prefix while dropping the tail.

Superseded detail: The last 8 are `Deadline Propagation/.../success` over TLS: client-stream,
server-stream, and bidi only (unary passes). Measured with `--trace`: the runner
sees the client cancel the RPC at **0.032 ms**, with `Grpc-Timeout: 2S` already
on the wire. So the stream is reset locally and immediately, not by the deadline
and not by the network. The harness's deadline correction only masks it as
DEADLINE_EXCEEDED; with the correction narrowed, the raw status is CANCELED from
`h2.client` reporting `.local`.

Measured, after h2 exposed `Connection.connectionErrorCode()` in `efa14b0`: the
session reports **FRAME_SIZE_ERROR** and the connection is poisoned. So the peer
rejects our frame stream, and the local reset is the session's verdict rather
than a transport close or a cancel.

That is grpc's write path through the bridge, not tls and not h2's parser. Next
step is to capture the bytes the bridge hands `slot.write` for one failing case
and compare frame boundaries against what `nextFrame` produced; the suspicion is
a partially queued frame joined by the next one, which is rule 1 of the seam and
exactly the failure it predicts.

That is every case the connectrpc suite defines for a gRPC client: protocol gRPC,
HTTP/2, h2c and TLS, the proto codec, identity and gzip, all five stream types,
and a message receive limit. Cleartext is already 409 of 409. The 235 TLS cases
are what remains.

The count is the only progress signal that counts. Unit tests, a compiling
transport, or a design note are not progress toward it unless they move that
number or unblock the thing that does.

## What "entire suite" does not include

Connect and gRPC-Web are different protocols. HTTP/1.1 and HTTP/3 are different
transports. This repository is a gRPC client, so those are separate products with
their own scope, not omissions from this one.

The server half of the suite (`--mode server`) is a separate target. This server
answers one message per call because http's engine buffers the body; server
conformance needs exchange streaming in http first.

## Server side

The server answers four of five shapes: unary, client-stream, server-stream, and
half-duplex bidi, buffered by the engine's body limit. Full-duplex needs a
streaming exchange in `http`, and proving any of it needs the suite's server mode
(`--mode server`), which is a second harness speaking `ServerCompatRequest`.

## Open blockers, with owners

1. `tls_nonblocking.Client` does not exist. **tls-lead**, critical path. No TLS
   case can pass without it.
2. `transport/tls_client.zig` does not exist. **h2**, follows (1), an afternoon.
3. The grpc-side adapter, the harness TLS branch, and the certificate material.
   **grpc-lead**, in progress; the four rules are in `docs/slop/TLS-SEAM.md`.

## Rules of conduct for this mission

- A blocker is not "waiting on X" until X has been asked for something pushable
  this session, with a date.
- `just conformance` must stay 409 of 409. Never trade cleartext for TLS.
- Every commit signed, `just ci` green, count reported from a real run.
- If a peer is idle and the mission is blocked, that gets raised, not narrated.
