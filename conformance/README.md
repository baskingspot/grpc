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

Set `CONNECTCONFORMANCE` to override the runner. The server recipes accept
additional runner flags as arguments.

`config.yaml` covers gRPC over cleartext HTTP/2, protobuf, identity and gzip,
all five stream types, and receive limits. `config-tls.yaml` adds TLS.
Neither configuration claims client certificate authentication.

The v1.0.7 runner selects 409 official cleartext client cases and 644 combined
cleartext/TLS cases. `status.yaml` adds regressions for server errors before a
deadline expires. The client recipes run both the official cases and these
regressions. `just ci` runs both client recipes and builds the server harness.
`just test-conformance` runs the harness unit tests. The CI gate includes these
tests for cases outside the official fixtures.

The server harness supports buffered calls over cleartext and TLS.
Full-duplex calls require incremental request and response bodies in `http`.
The default server recipes include full-duplex. Diagnostic runs that exclude
full-duplex pass 334 cleartext cases and 508 combined cases.
These subsets do not meet the complete target.

Completion requires client and server runs across the claimed feature matrix.
Every selected case must finish and pass. A client timeout that prevents later
cases from execution is a failure to complete the run.

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
