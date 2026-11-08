#!/usr/bin/env bash
# Mint the certificate the conformance runner hands the client.
#
# The runner gives the client exactly one PEM block: the server's certificate,
# in `server_tls_cert`. There is no separate CA field, so the same file has to be
# both the server's chain and the client's trust anchor. mkcert signs the leaf
# with its own root, so the root is appended here; the client's anchor loader
# then sees a CA and the chain resolves. A leaf alone fails as
# `CertificateIssuerNotFound`, which reads like a verification bug and is not.
#
# mkcert issues the leaf for DNS localhost (plus the IP forms, which the tls
# verifier ignores), with EKU serverAuth and KeyUsage digitalSignature, which is
# what the verifier enforces when it checks `leaf_usage = .server_auth`.
#
# The private CA lives under the output directory, never in the user's trust
# store: nothing here calls `mkcert -install`.
set -euo pipefail

out=$(realpath "${1:?usage: tls-certs.sh DIRECTORY}")
mkdir -p "$out/caroot"

export CAROOT="$out/caroot"
mkcert \
    -cert-file "$out/server.pem" \
    -key-file "$out/server.key" \
    localhost

# The single PEM the runner hands over carries the chain and the anchor.
cat "$CAROOT/rootCA.pem" >>"$out/server.pem"
