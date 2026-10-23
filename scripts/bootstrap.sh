#!/usr/bin/env bash
set -euo pipefail
if [[ $# != 2 ]]; then
  echo "usage: harklight-bootstrap ABSOLUTE_STATE_DIRECTORY DNS_HOSTNAME" >&2
  exit 2
fi
state_dir=$1
hostname=$2
[[ "$state_dir" = /* ]] || { echo "State directory must be absolute" >&2; exit 2; }
[[ "$hostname" =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]] || { echo "Use the server's DNS hostname" >&2; exit 2; }
umask 077
mkdir -p -- "$(dirname -- "$state_dir")"
# Refuse existing state, including an empty directory or symlink.
mkdir -m 700 -- "$state_dir"
mkdir -m 700 "$state_dir/tls" "$state_dir/owner"
openssl req -x509 -newkey rsa:3072 -nodes -days 3650 \
  -subj "/CN=Harklight local CA" -keyout "$state_dir/tls/ca.key" -out "$state_dir/tls/ca.pem" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -new -newkey rsa:3072 -nodes -subj "/CN=$hostname" \
  -keyout "$state_dir/tls/server.key" -out "$state_dir/tls/server.csr"
printf 'subjectAltName=DNS:%s\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n' "$hostname" > "$state_dir/tls/server.ext"
openssl x509 -req -in "$state_dir/tls/server.csr" -CA "$state_dir/tls/ca.pem" \
  -CAkey "$state_dir/tls/ca.key" -CAcreateserial -days 365 -sha256 \
  -extfile "$state_dir/tls/server.ext" -out "$state_dir/tls/server.pem"
openssl req -x509 -newkey rsa:3072 -nodes -days 365 -subj "/CN=Harklight owner" \
  -keyout "$state_dir/owner/identity.key" -out "$state_dir/owner/identity.pem" \
  -addext "basicConstraints=critical,CA:FALSE" -addext "keyUsage=critical,digitalSignature" \
  -addext "extendedKeyUsage=clientAuth"
cli="$(cd -- "$(dirname -- "$0")" && pwd)/harklight"
"$cli" operator bootstrap --store "$state_dir/relay.sqlite3" \
  --owner-cert "$state_dir/owner/identity.pem" > "$state_dir/person-id"
{
  printf 'export ATTENTION_SERVER_ENABLE=1\n'
  printf 'export ATTENTION_SERVER_STORE_PATH=%q\n' "$state_dir/relay.sqlite3"
  printf 'export ATTENTION_SERVER_CERTFILE=%q\n' "$state_dir/tls/server.pem"
  printf 'export ATTENTION_SERVER_KEYFILE=%q\n' "$state_dir/tls/server.key"
  printf 'export ATTENTION_SERVER_CACERTFILE=%q\n' "$state_dir/tls/ca.pem"
  printf 'export ATTENTION_SERVER_JOIN_NAME=%q\n' "$hostname"
  printf 'export ATTENTION_SERVER_PORT=18443\nexport ATTENTION_SERVER_ENROLL_PORT=18444\n'
  printf 'export ATTENTION_SERVER_OWNER_PORT=18445\nexport ATTENTION_SERVER_AGENT_PORT=18446\n'
  printf 'export ATTENTION_SERVER_BIND=127.0.0.1\nexport ATTENTION_SERVER_ENROLL_BIND=127.0.0.1\n'
  printf 'export ATTENTION_SERVER_OWNER_BIND=127.0.0.1\nexport ATTENTION_SERVER_AGENT_BIND=127.0.0.1\n'
} > "$state_dir/server.env"
echo "Owner bootstrapped; person ID saved in $state_dir/person-id"
echo "Protect the state directory; share only tls/ca.pem as the client's trust root."
