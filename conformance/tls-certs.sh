#!/usr/bin/env bash
# Generate the certificate pair the conformance runner hands to the client.
#
# The tls client matches dNSName subjectAltName entries only, so the leaf is
# issued for DNS:localhost and the client must dial that name. A certificate
# carrying an IP SAN would be rejected even though the address is the same.
set -euo pipefail

out=${1:?usage: tls-certs.sh DIRECTORY}
mkdir -p "$out"

openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -keyout "$out/ca.key" -out "$out/ca.pem" \
    -subj "/CN=baskingspot conformance CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign"

openssl req -newkey rsa:2048 -nodes \
    -keyout "$out/server.key" -out "$out/server.csr" \
    -subj "/CN=localhost"

cat > "$out/leaf.ext" <<'EXT'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost
EXT

openssl x509 -req -in "$out/server.csr" -days 2 \
    -CA "$out/ca.pem" -CAkey "$out/ca.key" -CAcreateserial \
    -extfile "$out/leaf.ext" -out "$out/server.pem"

rm -f "$out/server.csr" "$out/leaf.ext" "$out/server.csr"
