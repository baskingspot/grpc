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
zig build conformance-server
connectconformance --mode server --conf conformance/config.yaml -- ./zig-out/bin/conformance-server
```

Set `CONNECTCONFORMANCE` to override the runner in the client recipes.

`config.yaml` covers gRPC over cleartext HTTP/2, protobuf, identity and gzip,
all five stream types, and receive limits. `config-tls.yaml` adds TLS.
Neither configuration claims client certificate authentication.

The v1.0.7 runner selects 409 official cleartext client cases and 644 combined
cleartext/TLS cases. `status.yaml` adds regressions for server errors before a
deadline expires. The client recipes run both the official cases and these
regressions. `just ci` runs both client recipes and builds the server harness.

The server harness supports buffered calls and refuses TLS requests.
Full-duplex calls require incremental request and response bodies in `http`.
The default server run includes full-duplex. A diagnostic run that excludes
full-duplex passes 334 cases, but does not meet the complete target.

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
