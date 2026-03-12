# mojoreq

Pure Mojo HTTP/HTTPS request library with retries, redirects, compression decoding, and safety limits.

## Features

- HTTP and HTTPS (OpenSSL-backed TLS, certificate verification, SNI)
- Redirect following (`301`, `302`, `303`, `307`, `308`)
- Retry policy with exponential backoff + jitter
- Idempotent-method retry safety by default (`GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`, `TRACE`)
- `Retry-After` support for `429`/`503` (delta-seconds and HTTP-date)
- `gzip` and `deflate` response decoding
- Optional Brotli (`br`) response decoding when `libbrotlidec` is available
- Optional keep-alive connection pooling via `Client` (HTTP and HTTPS)
- Response size guards:
  - `max_header_bytes`
  - `max_body_bytes`
  - `max_decompressed_bytes`
- Safe API variant (`request_safe`/`get_safe`/`post_safe`) with typed error metadata

## Requirements

- Mojo `>=0.26.2.0.dev2026030205,<0.27` (see [pixi.toml](pixi.toml))
- macOS arm64 or Linux x86_64 environments supported in this project configuration

## Quick Start

```mojo
import requests
from std.collections import Dict

fn main() raises:
    var params = Dict[String, String]()
    params["q"] = "mojo requests"

    var response = requests.get(
        "https://example.com/search",
        params=params^,
        timeout_ms=10_000,
    )
    print(response.status_code)
    print(response.text())

    var created = requests.post(
        "https://httpbin.org/post",
        json='{"hello":"mojo"}',
        timeout_ms=10_000,
    )
    print(created.status_code)

    var safe = requests.get_safe("https://[::1]/")
    if not safe.ok:
        print(safe.error_kind)
        print(safe.error_message)
```

## API Overview

### Types

- `Request(method, url, headers, body)`
- `Response(status_code, headers, body)`
- `RequestResult(ok, status_code, headers, body, error_kind, error_message, error_retryable)`
- `RequestError(kind, message, retryable)`

### Main Functions

- `request(req, ...)` and `request(method, url, ...)`
- `request_safe(req, ...)` and `request_safe(method, url, ...)`
- `get(url, ..., headers=..., params=...)`
- `get_safe(url, ..., headers=..., params=...)`
- `post(url, ..., body=..., data=..., json=..., headers=..., params=...)`
- `post_safe(url, ..., body=..., data=..., json=..., headers=..., params=...)`
- `Client(...).request/get/post` and `Client(...).request_safe/get_safe/post_safe`

### Connection Pooling

`Client` can pool both HTTP and HTTPS keep-alive connections.

```mojo
import requests

fn main() raises:
    var client = requests.Client(enable_http_pool=True, enable_tls_pool=True)
    var a = client.get("http://127.0.0.1:18080/pool-probe")
    var b = client.get("http://127.0.0.1:18080/pool-probe")
    print(a.headers["X-Connection-Request"])  # 1
    print(b.headers["X-Connection-Request"])  # 2 (same TCP connection reused)
    var c = client.get("https://example.com/")
    var d = client.get("https://example.com/")
    print(c.status_code, d.status_code)
    client.close()
```

Common options:

- `headers` (`Dict[String, String]`)
- `params` (`Dict[String, String]`) appends URL query parameters with percent-encoding
- `data` (`Dict[String, String]`) encodes body as `application/x-www-form-urlencoded`
- `json` (`String`) sends JSON body and sets `Content-Type: application/json` by default
- `timeout_ms`
- `max_redirects`
- `max_retries`
- `retry_backoff_ms`
- `retry_max_backoff_ms`
- `retry_non_idempotent` (for `request/post` variants)
- `max_header_bytes`
- `max_body_bytes`
- `max_decompressed_bytes`
- `max_idle_connections` (`Client`): max idle pooled sockets kept
- `idle_ttl_ms` (`Client`): idle lifetime before pooled socket expires
- `max_requests_per_connection` (`Client`): hard cap before pooled socket rotation

## Error Kinds

`request_safe` returns `error_kind` values such as:

- `invalid_request`
- `redirect_error`
- `compression_error`
- `size_limit_error`
- `timeout`
- `dns_error`
- `connect_error`
- `tls_error`
- `parse_error`
- `io_error`
- `unknown`

## Testing

Run deterministic unit tests:

```bash
pixi run test
```

Run README quick-start smoke test from a clean temp directory:

```bash
pixi run test-smoke
```

Run deterministic local integration tests (starts local HTTP and HTTPS mock servers automatically):

```bash
pixi run test-integration
```

Run live internet integration tests (opt-in):

```bash
pixi run test-integration-live
```

Run pooled vs non-pooled benchmark guard locally:

```bash
pixi run benchmark
```

Benchmark tuning (optional env vars):

- `MOJOREQ_BENCH_ITERATIONS` (default `20`)
- `MOJOREQ_BENCH_ROUNDS` (default `3`, max `3`)
- `MOJOREQ_BENCH_MAX_REGRESSION_PCT` (default `25`)

Optional environment variables are documented in [.env.example](.env.example).

## CI and Releases

- CI workflow runs unit + README smoke + local integration tests on Linux and macOS.
- Live internet integration is in a separate gated job.
- Tagging `v*` (for example `v0.1.0`) triggers the release workflow.

## License

[MIT](LICENSE)
