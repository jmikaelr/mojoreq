# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-03-04

### Added

- Cross-platform socket networking support for macOS and Linux.
- HTTPS/TLS support with OpenSSL, certificate verification, and SNI.
- Redirect handling with method/header safety rules.
- Retry and backoff policy with idempotency defaults and `Retry-After` handling.
- `gzip`/`deflate` response decoding.
- Response size limits for headers, body, and decompressed payloads.
- `request_safe`, `get_safe`, `post_safe` typed error surfaces.
- Deterministic unit and local integration test suites.
- Optional live integration test suite and CI gating.
- CI workflow for Linux + macOS and release workflow on `v*` tags.
