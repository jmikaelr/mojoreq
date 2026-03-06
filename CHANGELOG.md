# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

## [0.2.0] - 2026-03-06

### Added

- Brotli (`br`) response decoding support when `libbrotlidec` is available.
- Brotli integration tests (local mock + live `httpbin` route).
- HTTP keep-alive connection pooling via new `Client` API and local integration coverage.
- HTTPS/TLS keep-alive pooling via `Client(enable_tls_pool=True)` and live integration smoke coverage.
- Deterministic local HTTPS integration suite with a TLS mock server and checked-in localhost test certificate.
- Pool hardening controls: `max_idle_connections`, `idle_ttl_ms`, and `max_requests_per_connection` with deterministic local coverage.

### Changed

- `Retry-After` now supports HTTP-date format in addition to delta-seconds.
- Default `Accept-Encoding` now includes `br` when Brotli runtime support is detected.

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
