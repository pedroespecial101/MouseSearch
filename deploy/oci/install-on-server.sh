#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
APP_DIR="/opt/appdata/mousesearch"
COMPOSE_SOURCE="${REPO_ROOT}/deploy/oci/docker-compose.yml"
ENV_SOURCE="${REPO_ROOT}/deploy/oci/mousesearch.env.example"
SERVE_SOURCE="${REPO_ROOT}/deploy/oci/serve.json"
OWNER_UID="${SUDO_UID:-1000}"
OWNER_GID="${SUDO_GID:-1000}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run this script with sudo."
  exit 1
fi

for file in "${COMPOSE_SOURCE}" "${ENV_SOURCE}" "${SERVE_SOURCE}"; do
  if [[ ! -f "${file}" ]]; then
    echo "Missing required file: ${file}"
    exit 1
  fi
done

install -d -m 0755 "${APP_DIR}"
install -d -m 0755 "${APP_DIR}/data"
install -d -m 0755 "${APP_DIR}/tailscale"
install -m 0644 "${COMPOSE_SOURCE}" "${APP_DIR}/docker-compose.yml"
install -m 0644 "${SERVE_SOURCE}" "${APP_DIR}/serve.json"

if [[ ! -f "${APP_DIR}/.env" ]]; then
  install -m 0600 "${ENV_SOURCE}" "${APP_DIR}/.env"
  echo "Created ${APP_DIR}/.env from the example template."
fi

chown -R "${OWNER_UID}:${OWNER_GID}" "${APP_DIR}"

cat <<EOF
MouseSearch OCI files are in place at ${APP_DIR}

Next steps:
  1. Edit ${APP_DIR}/.env and replace the placeholder values.
     - Set PUID/PGID to the actual host UID/GID that should own the files.
     - Use a resolvable torrent client URL without a trailing slash.
  2. Confirm /home/ubuntu/projects/MouseSearch contains the repo checkout you want to build.
  3. Run:
       cd ${APP_DIR}
       docker compose --env-file .env config
       docker compose --env-file .env build
       docker compose --env-file .env up -d
EOF
