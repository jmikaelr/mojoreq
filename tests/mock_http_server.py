#!/usr/bin/env python3
import argparse
import gzip
import json
import threading
import urllib.parse
import zlib
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


_ATTEMPTS = defaultdict(int)
_ATTEMPTS_LOCK = threading.Lock()


def _next_attempt(key: str) -> int:
    with _ATTEMPTS_LOCK:
        _ATTEMPTS[key] += 1
        return _ATTEMPTS[key]


def _query_int(
    query: dict, name: str, default: int, minimum: int, maximum: int
) -> int:
    raw = query.get(name, [str(default)])[0]
    try:
        value = int(raw)
    except ValueError:
        return default
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value


class MockHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "mojoreq-mock/1.0"

    def log_message(self, fmt: str, *args):
        _ = (fmt, args)

    def _read_body(self) -> bytes:
        raw_length = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_length)
        except ValueError:
            length = 0
        if length <= 0:
            return b""
        return self.rfile.read(length)

    def _send_body(
        self,
        status: int,
        body: bytes,
        headers: dict | None = None,
        content_encoding: str = "",
    ):
        self.send_response(status)
        self.send_header("Connection", "close")
        if len(content_encoding) > 0:
            self.send_header("Content-Encoding", content_encoding)
        if headers:
            for name, value in headers.items():
                self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, status: int, text: str, headers: dict | None = None):
        merged = {"Content-Type": "text/plain; charset=utf-8"}
        if headers:
            merged.update(headers)
        self._send_body(status, text.encode("utf-8"), headers=merged)

    def _send_json(self, status: int, payload: dict, headers: dict | None = None):
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        merged = {"Content-Type": "application/json"}
        if headers:
            merged.update(headers)
        self._send_body(status, body, headers=merged)

    def _send_chunked(
        self, status: int, chunks: list[bytes], headers: dict | None = None
    ):
        self.send_response(status)
        self.send_header("Connection", "close")
        self.send_header("Transfer-Encoding", "chunked")
        if headers:
            for name, value in headers.items():
                self.send_header(name, value)
        self.end_headers()
        for chunk in chunks:
            self.wfile.write(f"{len(chunk):X}\r\n".encode("ascii"))
            self.wfile.write(chunk)
            self.wfile.write(b"\r\n")
        self.wfile.write(b"0\r\n\r\n")

    def _send_redirect(self, location: str):
        self.send_response(302)
        self.send_header("Connection", "close")
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _handle(self, method: str):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        path = parsed.path

        if path == "/healthz":
            self._send_text(200, "ok")
            return

        if path == "/ok":
            self._send_json(200, {"ok": True, "source": "mock"})
            return

        if path == "/redirect-once":
            self._send_redirect("/ok")
            return

        if path == "/redirect-loop":
            self._send_redirect("/redirect-loop")
            return

        if path == "/chunked":
            self._send_chunked(
                200,
                [b"hello ", b"chunked"],
                headers={"Content-Type": "text/plain"},
            )
            return

        if path == "/gzip":
            body = json.dumps(
                {"compressed": "gzip", "ok": True},
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            compressed = gzip.compress(body)
            self._send_body(
                200,
                compressed,
                headers={"Content-Type": "application/json"},
                content_encoding="gzip",
            )
            return

        if path == "/deflate":
            body = json.dumps(
                {"compressed": "deflate", "ok": True},
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            compressed = zlib.compress(body)
            self._send_body(
                200,
                compressed,
                headers={"Content-Type": "application/json"},
                content_encoding="deflate",
            )
            return

        if path == "/big-body":
            size = _query_int(query, "size", 32768, minimum=1, maximum=1048576)
            self._send_body(
                200,
                b"A" * size,
                headers={"Content-Type": "application/octet-stream"},
            )
            return

        if path == "/big-header":
            size = _query_int(query, "size", 32768, minimum=1, maximum=1048576)
            self._send_text(200, "ok", headers={"X-Big": "B" * size})
            return

        if path == "/post-echo":
            if method != "POST":
                self._send_text(405, "Method Not Allowed")
                return
            body = self._read_body()
            self._send_body(
                200,
                body,
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-Echo-Bytes": str(len(body)),
                },
            )
            return

        if path == "/flaky":
            key = query.get("key", [f"{method}:{path}:{parsed.query}"])[0]
            fails = _query_int(query, "fails", 1, minimum=0, maximum=20)
            status = _query_int(query, "status", 503, minimum=100, maximum=599)
            retry_after = query.get("after", [""])[0]

            attempt = _next_attempt(key)
            headers = {"X-Attempt": str(attempt), "X-Key": key}
            if attempt <= fails:
                if len(retry_after) > 0:
                    headers["Retry-After"] = retry_after
                self._send_json(
                    status,
                    {
                        "attempt": attempt,
                        "key": key,
                        "ok": False,
                        "status": status,
                    },
                    headers=headers,
                )
                return

            self._send_json(
                200,
                {"attempt": attempt, "key": key, "ok": True},
                headers=headers,
            )
            return

        self._send_text(404, "Not Found")

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=18080, type=int)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), MockHandler)
    print(
        f"mock_http_server listening on http://{args.host}:{args.port}",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
