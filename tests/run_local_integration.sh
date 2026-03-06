#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_BASE="${TMPDIR:-/tmp}"
HTTP_LOG_FILE="${TMP_BASE}/mojoreq_mock_server_http.log"
HTTPS_LOG_FILE="${TMP_BASE}/mojoreq_mock_server_https.log"
CERT_FILE="${ROOT_DIR}/tests/certs/localhost-cert.pem"
KEY_FILE="${ROOT_DIR}/tests/certs/localhost-key.pem"
SERVER_PID=""

pick_port() {
  python3 - <<'PY'
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

wait_for_server() {
  local scheme="$1"
  local host="$2"
  local port="$3"
  local cafile="${4:-}"
  for _ in $(seq 1 120); do
    if python3 - "$scheme" "$host" "$port" "$cafile" <<'PY'
import ssl
import sys
import urllib.request

scheme = sys.argv[1]
host = sys.argv[2]
port = sys.argv[3]
cafile = sys.argv[4]
url = f"{scheme}://{host}:{port}/healthz"

try:
    if scheme == "https":
        context = ssl.create_default_context(cafile=cafile)
        with urllib.request.urlopen(url, timeout=0.2, context=context) as response:
            if response.status == 200:
                sys.exit(0)
    else:
        with urllib.request.urlopen(url, timeout=0.2) as response:
            if response.status == 200:
                sys.exit(0)
except Exception:
    pass

sys.exit(1)
PY
    then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

stop_server() {
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
    SERVER_PID=""
  fi
}

cleanup() {
  stop_server
}
trap cleanup EXIT

HTTP_PORT="$(pick_port)"
python3 "${ROOT_DIR}/tests/mock_http_server.py" --host 127.0.0.1 --port "${HTTP_PORT}" >"${HTTP_LOG_FILE}" 2>&1 &
SERVER_PID="$!"

if ! wait_for_server "http" "127.0.0.1" "${HTTP_PORT}"; then
  echo "mock HTTP server failed to start (log: ${HTTP_LOG_FILE})" >&2
  cat "${HTTP_LOG_FILE}" >&2 || true
  exit 1
fi

MOJOREQ_RUN_LOCAL_INTEGRATION_TESTS=1 \
MOJOREQ_IT_BASE_URL="http://127.0.0.1:${HTTP_PORT}" \
mojo run -I "${ROOT_DIR}/src" "${ROOT_DIR}/tests/integration_local_test.mojo"

stop_server

if [[ ! -f "${CERT_FILE}" || ! -f "${KEY_FILE}" ]]; then
  echo "TLS cert fixtures are missing: ${CERT_FILE} / ${KEY_FILE}" >&2
  exit 1
fi

HTTPS_PORT="$(pick_port)"
python3 "${ROOT_DIR}/tests/mock_http_server.py" \
  --host 127.0.0.1 \
  --port "${HTTPS_PORT}" \
  --tls-cert "${CERT_FILE}" \
  --tls-key "${KEY_FILE}" >"${HTTPS_LOG_FILE}" 2>&1 &
SERVER_PID="$!"

if ! wait_for_server "https" "localhost" "${HTTPS_PORT}" "${CERT_FILE}"; then
  echo "mock HTTPS server failed to start (log: ${HTTPS_LOG_FILE})" >&2
  cat "${HTTPS_LOG_FILE}" >&2 || true
  exit 1
fi

MOJOREQ_RUN_LOCAL_INTEGRATION_TESTS=1 \
MOJOREQ_IT_TLS_BASE_URL="https://localhost:${HTTPS_PORT}" \
SSL_CERT_FILE="${CERT_FILE}" \
mojo run -I "${ROOT_DIR}/src" "${ROOT_DIR}/tests/integration_local_tls_test.mojo"
