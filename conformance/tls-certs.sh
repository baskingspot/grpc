#!/usr/bin/env bash
# Generate the certificate pair the conformance runner hands to the client.
#
# The tls client matches dNSName subjectAltName entries only, so the leaf is
# issued for DNS:localhost and the client must dial that name. A certificate
# carrying an IP SAN would be rejected even though the address is the same.
#
# Signature algorithms the tls verifier accepts: RSA PKCS#1 SHA-256/384/512,
# ECDSA P-256/P-384 SHA-2, and Ed25519. `-newkey rsa:2048` signs with SHA-256,
# which is in that set. A future change to this script has to stay inside it.
set -euo pipefail

out=${1:?usage: tls-certs.sh DIRECTORY}
mkdir -p "$out"

openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -keyout "$out/ca.key" -out "$out/ca.pem" \
    -subj "/CN=baskingspot conformance CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>>"$out/openssl.log"

openssl req -newkey rsa:2048 -nodes \
    -keyout "$out/server.key" -out "$out/server.csr" \
    -subj "/CN=localhost" 2>>"$out/openssl.log"

cat > "$out/leaf.ext" <<'EXT'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost
EXT

openssl x509 -req -in "$out/server.csr" -days 2 \
    -CA "$out/ca.pem" -CAkey "$out/ca.key" -CAcreateserial \
    -extfile "$out/leaf.ext" -out "$out/server.pem" 2>>"$out/openssl.log"

# The tls verifier enforces EKU serverAuth and KeyUsage digitalSignature when
# they are present. Plain chain verification does not, so check with the same
# purpose it uses: a leaf that only anchors would otherwise fail in a handshake
# and send someone debugging the wrong layer.
openssl verify -purpose sslserver -CAfile "$out/ca.pem" \
    -verify_hostname localhost "$out/server.pem"

rm -f "$out/server.csr" "$out/leaf.ext" "$out/openssl.log" "$out/server.csr"
