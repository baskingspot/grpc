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

The config claims gRPC over HTTP/2, cleartext, unary calls, the proto codec, and
the identity encoding. Those claims are the criterion: the suite only runs cases
inside them, and a feature listed there must work on every case it selects.

All 83 cases in that set pass. Widening the claims is the roadmap: server
streaming first, then client and bidirectional streaming, then gzip, then a
message receive limit, then TLS once a client transport exists.

A claim added to `config.yaml` before the code supports it turns into failures,
which is the point: the file is a promise, not a filter for the cases that
happen to pass.
