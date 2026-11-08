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

TLS is the one axis left out, and it is named in the config rather than hidden:
`supports_tls: false`. `baskingspot/tls` has no nonblocking client and
`baskingspot/http` has no client transport to wrap, so there is nothing here to
negotiate a connection with. When those land, `unaryIo` already takes a reader
and a writer, and the claim becomes one line.
