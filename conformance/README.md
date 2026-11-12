# Conformance

The [connectrpc conformance suite](https://github.com/connectrpc/conformance) drives
`main.zig` as a client and `server_main.zig` as a server. Both programs exchange
length-prefixed protobuf messages with the runner over stdin and stdout.
`messages.zig` declares those messages by field number, without code generation.

## Run it

Enter `nix develop` before you run these commands.

```sh
just conformance       # cleartext client
just conformance-tls   # cleartext and TLS client
just conformance-server      # cleartext server
just conformance-server-tls  # cleartext and TLS server
```

The runner comes from the flake. The server recipes accept
additional runner flags as arguments.

`config.yaml` covers gRPC over cleartext HTTP/2, protobuf, identity and gzip,
all five stream types, and receive limits. `config-tls.yaml` adds TLS.
Neither configuration claims client certificate authentication.

The v1.0.7 runner selects 409 cleartext client cases and 644 combined
cleartext/TLS client cases. It selects 374 cleartext server cases and 568
combined cleartext/TLS server cases. Both server recipes run without filters,
including full-duplex. Every selected case must finish and pass.

`status.yaml` adds client regressions for server errors before a deadline
expires: 12 cleartext cases and 18 combined cases. The client recipes run
these after the official suite.

`just ci` runs all four conformance recipes, core tests, harness tests, and
lint. `just test-conformance` runs the harness tests alone. These cover
response details, empty streams, receive limits, and deadline/backpressure
edges outside the official fixtures.

The server uses `grpc.server.serveStreaming`, and `grpc_tls.serveStreaming`
over TLS, on the caller's `Io`.
Each call retains its upload, response cursor, and wake time across callbacks.
It sends full-duplex responses before request END_STREAM. `grpc-timeout`
starts at the request head and applies during upload and response delays.
Output backpressure preserves the unsent bytes. If expiry interrupts a frame,
the harness resets the stream. It sends no trailers after truncated bytes.
TLS uses the same gRPC adapter.

The harness raises the HTTP unread-input cap to 1 MiB. Library servers default
to 64 KiB through `ReadinessOptions.body_bytes` or TLS `ServerOptions.common.body_bytes`.
This cap includes message prefixes. The HTTP engine reuses consumed body
buffers, so the total upload can exceed the cap. The harness retains its
application messages separately. `Stream.message_limit` bounds each framed
message, and the harness also bounds decoded gzip messages.

## TLS certificates

`tls-certs.sh` uses `mkcert` with a temporary CA directory. It does not install
that CA in the system trust store. The script appends the root certificate to
the server certificate because the client contract supplies one PEM field for
both the chain and its trust anchor.

The certificate names `localhost`. The client harness dials and verifies that
name for TLS because the TLS verifier supports DNS names, not IP SANs.

In server mode, the runner supplies PEM credentials through `ServerCompatRequest`.
The harness decodes the chain with `tls.CertificateChain.fromPem` and loads its
signer with `tls.signature.PrivateKey.fromPemAuto`. The server recipes do not
accept `--cert` or `--key` because the runner reserves those flags for client mode.
