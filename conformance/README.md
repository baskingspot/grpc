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

Closing the remaining failures is the roadmap for the client. The largest group
is `grpc-status-details-bin`: the suite expects a failing call to carry the
server's error details, and this client drops them. Next come response
validation (content type, codec, compression, message count), binary metadata
(`-bin` keys are base64 on the wire), deadlines, cancellation, and the streaming
stream types.
