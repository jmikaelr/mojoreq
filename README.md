# mojoreq

Pure Mojo HTTP/HTTPS request library with retries, redirects, compression decoding, and safety limits.

## Features

- HTTP and HTTPS (OpenSSL-backed TLS, certificate verification, SNI)
- Redirect following (`301`, `302`, `303`, `307`, `308`)
- Retry policy with exponential backoff + jitter
- Idempotent-method retry safety by default (`GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`, `TRACE`)
- `Retry-After` support for `429`/`503` (delta-seconds format)
- `gzip` and `deflate` response decoding
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

fn main() raises:
    var response = requests.get(
        "https://example.com/",
        timeout_ms=10_000,
        max_redirects=5,
    )
    print(response.status_code)
    print(response.text())

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

- `request(...) raises -> Response`
- `request_safe(...) -> RequestResult`
- `get(...) raises -> Response`
- `get_safe(...) -> RequestResult`
- `post(...) raises -> Response`
- `post_safe(...) -> RequestResult`

Common options:

- `timeout_ms`
- `max_redirects`
- `max_retries`
- `retry_backoff_ms`
- `retry_max_backoff_ms`
- `retry_non_idempotent` (for `request/post` variants)
- `max_header_bytes`
- `max_body_bytes`
- `max_decompressed_bytes`

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

Run deterministic local integration tests (starts local mock server automatically):

```bash
pixi run test-integration
```

Run live internet integration tests (opt-in):

```bash
pixi run test-integration-live
```

Optional environment variables are documented in [.env.example](.env.example).

## CI and Releases

- CI workflow runs unit + local integration tests on Linux and macOS.
- Live internet integration is in a separate gated job.
- Tagging `v*` (for example `v0.1.0`) triggers the release workflow.

## License

[MIT](LICENSE)
