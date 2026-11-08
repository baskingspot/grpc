# The mission

**`just conformance-tls` must report 644 passed, 0 failed.** Current: 636.

The last 8 are `Deadline Propagation/.../success` over TLS: cases that must
succeed, where the client reports DEADLINE_EXCEEDED. That code is produced by the
harness's own correction (a call with a deadline that ends in 1/13/14 is reported
as 4), which is masking the underlying failure. The next step is to remove the
mask for that class and read the real error, not to widen the correction again.

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
