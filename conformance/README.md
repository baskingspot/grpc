# Conformance

A client under test for the [connectrpc conformance suite](https://github.com/connectrpc/conformance).
The runner writes `ClientCompatRequest` messages to this program's stdin and
reads `ClientCompatResponse` messages from its stdout; `main.zig` issues each RPC
with `baskingspot/grpc` and reports what the server said.

`messages.zig` is the suite's own message set, written out by hand with field
numbers. No `.proto` file is read and nothing is generated.

## Run it

Install the runner from its releases page, then:

```sh
just conformance
```

`CONNECTCONFORMANCE` overrides the runner path. `conformance/config.yaml` says
what this client supports, and the runner filters its cases to those axes. A
passing run means every case that applies to that feature set passed.

## What it covers

The config claims gRPC over HTTP/2 and cleartext, the proto codec, identity and
gzip, all five stream types, and a message receive limit. That is every case the
suite defines for gRPC without TLS: 409 of them, all passing.

Those claims are the criterion, not a filter for the cases that happen to pass.
A feature added to `config.yaml` before the code supports it turns red, which is
the point of the file.

TLS is the one axis left out of that file, and it is named rather than hidden:
`supports_tls: false`. `baskingspot/tls` has no nonblocking client and
`baskingspot/http` has no client transport to wrap, so there is nothing here to
negotiate a connection with.

`config-tls.yaml` is the target, and `just conformance-tls` runs it. It mints a
CA and a leaf with `conformance/tls-certs.sh` and hands them to the runner with
`--cert` and `--key`, so the chain is one this repository controls rather than
whatever the runner generates. 644 cases run; the 235 that need TLS fail with
`UnsupportedTls` until the client transport exists, and the rest pass.

The certificate is issued for `DNS:localhost` because the tls client matches
dNSName entries only. A leaf carrying `IP:127.0.0.1` is rejected, so the harness
dials `localhost` when TLS is on, not the host the runner reports.
