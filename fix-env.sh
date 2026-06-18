#!/usr/bin/env bash
#
# fix-env.sh — Fixes the two most common reasons this docker-compose fails
# the first time someone clones the repo:
#
#   1. POSTGRES_HOST=postgres in .env, but this compose has no "postgres"
#      service (Postgres is external, see readme.md). Airflow then fails
#      with: could not translate host name "postgres" to address.
#
#   2. ./logs and ./credentials are gitignored, so on the first
#      `docker compose up` Docker creates them as root:root on the host
#      (since they don't exist yet). The Airflow containers run as
#      AIRFLOW_UID:0 and can't write to them -> permission denied errors
#      writing task logs.
#
# Safe to re-run any time. Does not touch other .env values.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$ROOT_DIR/.env"
ENV_EXAMPLE="$ROOT_DIR/.env.example"

# Don't run this whole script with `sudo` — it only needs sudo for the
# final chown, which it requests itself. If you do run it via sudo anyway,
# fall back to $SUDO_UID so we still pick up YOUR uid, not root's (0).
if [[ "$(id -u)" -eq 0 && -n "${SUDO_UID:-}" ]]; then
  HOST_UID="$SUDO_UID"
else
  HOST_UID="$(id -u)"
fi

echo "Run this as your normal user (no leading sudo) — it asks for sudo itself when needed."
echo

if [[ ! -f "$ENV_FILE" ]]; then
  echo "No .env found, creating it from .env.example"
  cp "$ENV_EXAMPLE" "$ENV_FILE"
fi

echo "==> Checking .env"

if grep -q '^POSTGRES_HOST=postgres$' "$ENV_FILE"; then
  echo "  - POSTGRES_HOST=postgres has no matching service in this compose."
  echo "    Setting POSTGRES_HOST=host.docker.internal (Postgres is external)."
  awk '{ if ($0 == "POSTGRES_HOST=postgres") print "POSTGRES_HOST=host.docker.internal"; else print }' \
    "$ENV_FILE" > "$ENV_FILE.tmp" && mv "$ENV_FILE.tmp" "$ENV_FILE"
else
  echo "  - POSTGRES_HOST already looks fine, leaving it untouched."
fi

if grep -q '^AIRFLOW_UID=' "$ENV_FILE"; then
  CURRENT_UID_VALUE="$(grep '^AIRFLOW_UID=' "$ENV_FILE" | cut -d= -f2)"
  if [[ "$CURRENT_UID_VALUE" != "$HOST_UID" ]]; then
    echo "  - AIRFLOW_UID=$CURRENT_UID_VALUE does not match your host UID ($HOST_UID)."
    echo "    Setting AIRFLOW_UID=$HOST_UID so the container user can write to bind mounts."
    awk -v uid="$HOST_UID" '{ if ($0 ~ /^AIRFLOW_UID=/) print "AIRFLOW_UID=" uid; else print }' \
      "$ENV_FILE" > "$ENV_FILE.tmp" && mv "$ENV_FILE.tmp" "$ENV_FILE"
  else
    echo "  - AIRFLOW_UID already matches your host UID ($HOST_UID)."
  fi
fi

echo "==> Ensuring ./logs and ./credentials exist and are owned by ${HOST_UID}:0"
mkdir -p "$ROOT_DIR/logs" "$ROOT_DIR/credentials/oci" "$ROOT_DIR/credentials/aws" "$ROOT_DIR/credentials/gcp"

if [[ "$(stat -c '%u' "$ROOT_DIR/logs")" != "$HOST_UID" ]]; then
  echo "  - Fixing ownership (requires sudo)"
  if [[ "$(id -u)" -eq 0 ]]; then
    chown -R "$HOST_UID:0" "$ROOT_DIR/logs" "$ROOT_DIR/credentials"
  else
    sudo chown -R "$HOST_UID:0" "$ROOT_DIR/logs" "$ROOT_DIR/credentials"
  fi
else
  echo "  - Ownership already correct."
fi

echo "==> Done. You can now run: docker compose up -d --build"
