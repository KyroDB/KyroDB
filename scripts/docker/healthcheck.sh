#!/bin/sh
set -eu

first_api_key_from_file() {
  key_file="$1"
  if [ ! -r "$key_file" ]; then
    return 1
  fi

  sed -n 's/^[[:space:]-]*key:[[:space:]]*//p' "$key_file" | head -n 1
}

http_port="${KYRODB__SERVER__HTTP_PORT:-51051}"
health_url="${KYRODB_HEALTHCHECK_URL:-http://127.0.0.1:${http_port}/health}"
api_key="${KYRODB_HEALTHCHECK_API_KEY:-${API_SECRET:-}}"

if [ -z "$api_key" ]; then
  auth_file="${KYRODB__AUTH__API_KEYS_FILE:-}"
  if [ -n "$auth_file" ]; then
    api_key="$(first_api_key_from_file "$auth_file" || true)"
  fi
fi

if [ -z "$api_key" ] && [ -r "/etc/kyrodb/api_keys.yaml" ]; then
  api_key="$(first_api_key_from_file /etc/kyrodb/api_keys.yaml || true)"
fi

if [ -n "$api_key" ]; then
  exec curl -fsS -H "Authorization: Bearer $api_key" "$health_url" >/dev/null
fi

exec curl -fsS "$health_url" >/dev/null
