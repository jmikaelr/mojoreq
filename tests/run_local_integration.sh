#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_FILE="${TMPDIR:-/tmp}/mojoreq_mock_server.log"

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
  local port="$1"
  for _ in $(seq 1 100); do
    if python3 - <<PY
import urllib.request
import sys
try:
    with urllib.request.urlopen("http://127.0.0.1:${port}/healthz", timeout=0.2) as response:
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

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

PORT="$(pick_port)"
python3 "${ROOT_DIR}/tests/mock_http_server.py" --host 127.0.0.1 --port "${PORT}" >"${LOG_FILE}" 2>&1 &
SERVER_PID="$!"

if ! wait_for_server "${PORT}"; then
  echo "mock server failed to start (log: ${LOG_FILE})" >&2
  cat "${LOG_FILE}" >&2 || true
  exit 1
fi

MOJOREQ_RUN_LOCAL_INTEGRATION_TESTS=1 \
MOJOREQ_IT_BASE_URL="http://127.0.0.1:${PORT}" \
mojo run -I "${ROOT_DIR}/src" "${ROOT_DIR}/tests/integration_local_test.mojo"
