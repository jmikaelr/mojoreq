#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_BASE="${TMPDIR:-/tmp}"
HTTP_LOG_FILE="${TMP_BASE}/mojoreq_readme_smoke_http.log"
SERVER_PID=""
TMP_DIR=""
TMP_SCRIPT=""

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
  local host="$1"
  local port="$2"
  for _ in $(seq 1 120); do
    if python3 - "$host" "$port" <<'PY'
import sys
import urllib.request

host = sys.argv[1]
port = sys.argv[2]
url = f"http://{host}:{port}/healthz"

try:
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
  if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  fi
}
trap cleanup EXIT

HTTP_PORT="$(pick_port)"
python3 "${ROOT_DIR}/tests/mock_http_server.py" \
  --host 127.0.0.1 \
  --port "${HTTP_PORT}" >"${HTTP_LOG_FILE}" 2>&1 &
SERVER_PID="$!"

if ! wait_for_server "127.0.0.1" "${HTTP_PORT}"; then
  echo "readme smoke server failed to start (log: ${HTTP_LOG_FILE})" >&2
  cat "${HTTP_LOG_FILE}" >&2 || true
  exit 1
fi

TMP_DIR="$(mktemp -d "${TMP_BASE}/mojoreq_readme_smoke.XXXXXX")"
TMP_SCRIPT="${TMP_DIR}/main.mojo"

cat >"${TMP_SCRIPT}" <<'MOJO'
import requests
from std.os.env import getenv

fn main() raises:
    var base_url = getenv("MOJOREQ_SMOKE_BASE_URL")
    if len(base_url) == 0:
        raise Error("MOJOREQ_SMOKE_BASE_URL is not set")

    var response = requests.get(
        String(base_url, "/ok"),
        timeout_ms=10_000,
        max_redirects=5,
        max_retries=0,
    )
    if response.status_code != 200:
        raise Error("unexpected status code from README smoke request")

    var safe = requests.get_safe(
        "https://[::1]/",
        timeout_ms=1_000,
        max_retries=0,
    )
    if safe.ok:
        raise Error("expected get_safe invalid-url failure in smoke test")

    print("README smoke passed")
MOJO

(
  cd "${TMP_BASE}"
  MOJOREQ_SMOKE_BASE_URL="http://127.0.0.1:${HTTP_PORT}" \
    mojo run -I "${ROOT_DIR}/src" "${TMP_SCRIPT}"
)
