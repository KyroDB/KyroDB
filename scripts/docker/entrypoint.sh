#!/bin/sh
set -eu

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

ensure_tls_material() {
  cert_path="${KYRODB__SERVER__TLS__CERT_PATH:-}"
  key_path="${KYRODB__SERVER__TLS__KEY_PATH:-}"

  if [ -n "$cert_path" ] && [ -n "$key_path" ] && [ -r "$cert_path" ] && [ -r "$key_path" ]; then
    return 0
  fi

  if ! is_truthy "${KYRODB_AUTO_GENERATE_TLS:-true}"; then
    echo "TLS is enabled but certificate material is missing." >&2
    echo "Mount real certs or enable KYRODB_AUTO_GENERATE_TLS for ephemeral self-signed startup." >&2
    exit 1
  fi

  tls_dir="${KYRODB_GENERATED_TLS_DIR:-/etc/kyrodb/tls}"
  cert_path="${cert_path:-$tls_dir/server.crt}"
  key_path="${key_path:-$tls_dir/server.key}"
  common_name="${KYRODB_TLS_COMMON_NAME:-localhost}"
  validity_days="${KYRODB_TLS_VALIDITY_DAYS:-30}"

  mkdir -p "$tls_dir"

  if [ ! -s "$cert_path" ] || [ ! -s "$key_path" ]; then
    openssl req \
      -x509 \
      -newkey rsa:2048 \
      -sha256 \
      -nodes \
      -days "$validity_days" \
      -keyout "$key_path" \
      -out "$cert_path" \
      -subj "/CN=$common_name" >/dev/null 2>&1
  fi

  export KYRODB__SERVER__TLS__CERT_PATH="$cert_path"
  export KYRODB__SERVER__TLS__KEY_PATH="$key_path"
}

if is_truthy "${KYRODB__SERVER__TLS__ENABLED:-false}"; then
  ensure_tls_material
fi

exec "$@"
