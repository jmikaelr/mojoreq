import requests
from std.base64 import b64decode
from std.testing import (
    TestSuite,
    assert_equal,
    assert_raises,
    assert_true,
)


fn test_parse_url_https_defaults() raises:
    var parsed = requests._parse_url("https://example.com")
    assert_true(parsed.use_tls)
    assert_equal(parsed.host, "example.com")
    assert_equal(parsed.host_header, "example.com")
    assert_equal(parsed.port, 443)
    assert_equal(parsed.path, "/")


fn test_parse_url_http_with_port_path_query_fragment() raises:
    var parsed = requests._parse_url(
        "http://api.example.com:8080/v1/items?x=1#fragment"
    )
    assert_true(not parsed.use_tls)
    assert_equal(parsed.host, "api.example.com")
    assert_equal(parsed.host_header, "api.example.com:8080")
    assert_equal(parsed.port, 8080)
    assert_equal(parsed.path, "/v1/items?x=1")


fn test_parse_url_query_without_slash() raises:
    var parsed = requests._parse_url("https://example.com?x=1")
    assert_equal(parsed.path, "/?x=1")


fn test_parse_url_rejects_ipv6_literal() raises:
    with assert_raises(contains="IPv6 literal hosts are not supported yet"):
        _ = requests._parse_url("https://[::1]/")


fn test_resolve_redirect_url_absolute() raises:
    var parsed = requests._parse_url("https://example.com/path")
    var url = requests._resolve_redirect_url(
        parsed, "https://api.example.com/v2"
    )
    assert_equal(url, "https://api.example.com/v2")


fn test_resolve_redirect_url_scheme_relative() raises:
    var parsed = requests._parse_url("https://example.com/path")
    var url = requests._resolve_redirect_url(
        parsed, "//cdn.example.com/file.js"
    )
    assert_equal(url, "https://cdn.example.com/file.js")


fn test_resolve_redirect_url_root_relative() raises:
    var parsed = requests._parse_url("http://example.com/a/b")
    var url = requests._resolve_redirect_url(parsed, "/next")
    assert_equal(url, "http://example.com/next")


fn test_resolve_redirect_url_relative_path() raises:
    var parsed = requests._parse_url("https://example.com/dir/page?x=1")
    var url = requests._resolve_redirect_url(parsed, "other")
    assert_equal(url, "https://example.com/dir/other")


fn test_redirect_method_rules() raises:
    assert_equal(requests._redirect_method(301, "POST"), "GET")
    assert_equal(requests._redirect_method(302, "post"), "GET")
    assert_equal(requests._redirect_method(303, "PUT"), "GET")
    assert_equal(requests._redirect_method(303, "HEAD"), "HEAD")
    assert_equal(requests._redirect_method(307, "POST"), "POST")
    assert_equal(requests._redirect_method(308, "PATCH"), "PATCH")


fn test_prepare_redirect_headers_drops_host_auth_and_body_headers() raises:
    var headers = Dict[String, String]()
    headers["Host"] = "example.com"
    headers["Authorization"] = "Bearer secret"
    headers["Content-Length"] = "4"
    headers["Content-Type"] = "application/json"
    headers["User-Agent"] = "mojoreq/test"

    var next_headers = requests._prepare_redirect_headers(
        headers,
        drop_body_headers=True,
        current_host_header="example.com",
        next_host_header="other.example.com",
    )
    assert_true(not requests._has_header(next_headers, "Host"))
    assert_true(not requests._has_header(next_headers, "Authorization"))
    assert_true(not requests._has_header(next_headers, "Content-Length"))
    assert_true(not requests._has_header(next_headers, "Content-Type"))
    assert_equal(next_headers["User-Agent"], "mojoreq/test")


fn test_build_request_payload_adds_defaults() raises:
    var parsed = requests._parse_url("https://example.com/hello")
    var req = requests.Request(
        method="get",
        url="https://example.com/hello",
        headers=Dict[String, String](),
        body="",
    )
    var payload = requests._build_request_payload(req, parsed)

    assert_true(payload.startswith("GET /hello HTTP/1.1\r\n"))
    assert_true(payload.find("\r\nHost: example.com\r\n") != -1)
    assert_true(payload.find("\r\nConnection: close\r\n") != -1)
    if requests._brotli_available():
        assert_true(
            payload.find("\r\nAccept-Encoding: gzip, deflate, br\r\n") != -1
        )
    else:
        assert_true(
            payload.find("\r\nAccept-Encoding: gzip, deflate\r\n") != -1
        )
    assert_true(payload.endswith("\r\n\r\n"))


fn test_build_request_payload_preserves_custom_headers() raises:
    var parsed = requests._parse_url("https://example.com/upload")
    var headers = Dict[String, String]()
    headers["Host"] = "override.example.com"
    headers["Connection"] = "keep-alive"
    headers["Accept-Encoding"] = "identity"
    headers["X-Custom"] = "abc"

    var req = requests.Request(
        method="post",
        url="https://example.com/upload",
        headers=headers^,
        body="hello",
    )
    var payload = requests._build_request_payload(req, parsed)

    assert_true(payload.startswith("POST /upload HTTP/1.1\r\n"))
    assert_true(payload.find("\r\nHost: override.example.com\r\n") != -1)
    assert_true(payload.find("\r\nConnection: keep-alive\r\n") != -1)
    assert_true(payload.find("\r\nAccept-Encoding: identity\r\n") != -1)
    assert_true(payload.find("\r\nX-Custom: abc\r\n") != -1)
    assert_true(payload.find("\r\nContent-Length: 5\r\n") != -1)
    assert_true(payload.endswith("\r\n\r\nhello"))


fn test_parse_response_basic() raises:
    var raw = 'HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nX-Test: ok\r\n\r\n{"ok":true}'
    var response = requests._parse_response(raw)

    assert_equal(response.status_code, 201)
    assert_equal(response.headers["Content-Type"], "application/json")
    assert_equal(response.headers["X-Test"], "ok")
    assert_equal(response.body, '{"ok":true}')


fn test_parse_response_without_body() raises:
    var response = requests._parse_response(
        "HTTP/1.1 204 No Content\r\nDate: Tue\r\n\r\n"
    )
    assert_equal(response.status_code, 204)
    assert_equal(response.headers["Date"], "Tue")
    assert_equal(response.body, "")


fn test_parse_response_content_length_trims_extra_bytes() raises:
    var response = requests._parse_response(
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhelloEXTRA"
    )
    assert_equal(response.body, "hello")


fn test_parse_response_content_length_short_body_raises() raises:
    with assert_raises(contains="shorter than Content-Length"):
        _ = requests._parse_response(
            "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nhello"
        )


fn test_parse_response_chunked() raises:
    var response = requests._parse_response(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding:"
        " chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"
    )
    assert_equal(response.body, "hello world")


fn test_parse_response_chunked_with_extensions() raises:
    var response = requests._parse_response(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding:"
        " chunked\r\n\r\n5;foo=bar\r\nhello\r\n0\r\n\r\n"
    )
    assert_equal(response.body, "hello")


fn test_parse_response_headers_exceed_limit_raises() raises:
    var raw = "HTTP/1.1 200 OK\r\nX-Long: 1234567890\r\n\r\nok"
    with assert_raises(contains="max_header_bytes"):
        _ = requests._parse_response(raw, max_header_bytes=10)


fn test_parse_response_body_exceeds_limit_raises() raises:
    var raw = "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\n123456"
    with assert_raises(contains="max_body_bytes"):
        _ = requests._parse_response(raw, max_body_bytes=5)


fn test_parse_response_chunked_body_exceeds_limit_raises() raises:
    var raw = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"
    with assert_raises(contains="max_body_bytes"):
        _ = requests._parse_response(raw, max_body_bytes=4)


fn test_decode_content_encoding_gzip() raises:
    var gzip_body = b64decode(
        "H4sIALgKp2kC/8tIzcnJV0jOzy0oSi0uTk1RKM8vykkBAKEtlFMWAAAA"
    )
    var decoded = requests._decode_content_encoding(gzip_body, "gzip")
    assert_equal(decoded, "hello compressed world")


fn test_decode_content_encoding_deflate_zlib() raises:
    var deflated = b64decode("eJzLSM3JyVdIzs8tKEotLk5NUSjPL8pJAQBjhQiy")
    var decoded = requests._decode_content_encoding(deflated, "deflate")
    assert_equal(decoded, "hello compressed world")


fn test_decode_content_encoding_deflate_raw_fallback() raises:
    var deflated = b64decode("y0jNyclXSM7PLShKLS5OTVEozy/KSQEA")
    var decoded = requests._decode_content_encoding(deflated, "deflate")
    assert_equal(decoded, "hello compressed world")


fn test_decode_content_encoding_brotli() raises:
    if not requests._brotli_available():
        return

    var brotli_body = b64decode("HxUA+AU2xtb1j4RGUG0CT5HAUet2gs4A")
    var decoded = requests._decode_content_encoding(brotli_body, "br")
    assert_equal(decoded, "hello compressed world")


fn test_decode_content_encoding_unsupported_raises() raises:
    with assert_raises(contains="unsupported Content-Encoding"):
        _ = requests._decode_content_encoding("hello", "compress")


fn test_decode_content_encoding_respects_decompressed_limit() raises:
    var gzip_body = b64decode(
        "H4sIALgKp2kC/8tIzcnJV0jOzy0oSi0uTk1RKM8vykkBAKEtlFMWAAAA"
    )
    with assert_raises(contains="max_decompressed_bytes"):
        _ = requests._decode_content_encoding(
            gzip_body, "gzip", max_decompressed_bytes=5
        )


fn test_is_idempotent_method() raises:
    assert_true(requests._is_idempotent_method("GET"))
    assert_true(requests._is_idempotent_method("head"))
    assert_true(requests._is_idempotent_method("PUT"))
    assert_true(requests._is_idempotent_method("DELETE"))
    assert_true(not requests._is_idempotent_method("POST"))
    assert_true(not requests._is_idempotent_method("PATCH"))


fn test_is_retryable_status_code() raises:
    assert_true(requests._is_retryable_status_code(408))
    assert_true(requests._is_retryable_status_code(429))
    assert_true(requests._is_retryable_status_code(503))
    assert_true(not requests._is_retryable_status_code(200))
    assert_true(not requests._is_retryable_status_code(404))


fn test_retry_backoff_delay_ms_deterministic() raises:
    assert_equal(
        requests._retry_backoff_delay_ms(
            0, 100, 1_000, jitter_source_ns=UInt(20)
        ),
        100,
    )
    assert_equal(
        requests._retry_backoff_delay_ms(
            1, 100, 1_000, jitter_source_ns=UInt(40)
        ),
        200,
    )
    assert_equal(
        requests._retry_backoff_delay_ms(
            2, 100, 1_000, jitter_source_ns=UInt(80)
        ),
        400,
    )
    assert_equal(
        requests._retry_backoff_delay_ms(
            5, 300, 700, jitter_source_ns=UInt(140)
        ),
        700,
    )


fn test_retry_backoff_delay_ms_disabled_when_config_invalid() raises:
    assert_equal(
        requests._retry_backoff_delay_ms(0, 0, 1_000, jitter_source_ns=UInt(1)),
        0,
    )
    assert_equal(
        requests._retry_backoff_delay_ms(0, 100, 0, jitter_source_ns=UInt(1)),
        0,
    )
    assert_equal(
        requests._retry_backoff_delay_ms(
            -1, 100, 1_000, jitter_source_ns=UInt(1)
        ),
        0,
    )


fn test_retry_after_delay_ms_parses_delta_seconds() raises:
    var headers = Dict[String, String]()
    headers["Retry-After"] = "5"
    assert_equal(requests._retry_after_delay_ms(headers), 5_000)


fn test_retry_after_delay_ms_invalid_or_http_date_returns_zero() raises:
    var headers = Dict[String, String]()
    headers["Retry-After"] = "Wed, 21 Oct 2015 07:28:00 GMT"
    assert_equal(
        requests._retry_after_delay_ms_with_now(headers, now_epoch_seconds=0),
        300_000,
    )
    assert_equal(
        requests._retry_after_delay_ms_with_now(
            headers, now_epoch_seconds=1_760_000_000
        ),
        0,
    )

    headers["Retry-After"] = "oops"
    assert_equal(requests._retry_after_delay_ms(headers), 0)


fn test_retry_after_delay_ms_clamps_large_values() raises:
    var headers = Dict[String, String]()
    headers["Retry-After"] = "999999"
    assert_equal(requests._retry_after_delay_ms(headers), 300_000)


fn test_retry_after_delay_ms_http_date_future() raises:
    var headers = Dict[String, String]()
    headers["Retry-After"] = "Thu, 01 Jan 1970 00:00:10 GMT"
    assert_equal(
        requests._retry_after_delay_ms_with_now(headers, now_epoch_seconds=0),
        10_000,
    )


fn test_retry_after_delay_ms_http_date_past() raises:
    var headers = Dict[String, String]()
    headers["Retry-After"] = "Thu, 01 Jan 1970 00:00:10 GMT"
    assert_equal(
        requests._retry_after_delay_ms_with_now(headers, now_epoch_seconds=20),
        0,
    )


fn test_retry_delay_for_response_ms_respects_retry_after() raises:
    var headers = Dict[String, String]()
    headers["Retry-After"] = "10"
    var delay_ms = requests._retry_delay_for_response_ms(
        429,
        headers,
        retry_number=0,
        retry_backoff_ms=200,
        retry_max_backoff_ms=2_000,
        jitter_source_ns=UInt(40),
    )
    assert_equal(delay_ms, 10_000)


fn test_retry_delay_for_response_ms_falls_back_to_backoff() raises:
    var delay_ms = requests._retry_delay_for_response_ms(
        500,
        Dict[String, String](),
        retry_number=0,
        retry_backoff_ms=200,
        retry_max_backoff_ms=2_000,
        jitter_source_ns=UInt(40),
    )
    assert_equal(delay_ms, 200)


fn test_parse_response_interim_100_continue() raises:
    var response = requests._parse_response(
        "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 201 Created\r\nContent-Length:"
        " 2\r\n\r\nok"
    )
    assert_equal(response.status_code, 201)
    assert_equal(response.body, "ok")


fn test_parse_response_invalid_status_line() raises:
    with assert_raises(contains="invalid HTTP status line"):
        _ = requests._parse_response("NOT_HTTP\r\nHeader: value\r\n\r\n")


fn test_request_safe_invalid_url_returns_typed_error() raises:
    var result = requests.get_safe("https://[::1]/")
    assert_true(not result.ok)
    assert_equal(result.error_kind, "invalid_request")
    assert_true(not result.error_retryable)


fn test_request_safe_timeout_validation() raises:
    var result = requests.get_safe("https://example.com/", timeout_ms=0)
    assert_true(not result.ok)
    assert_equal(result.error_kind, "invalid_request")
    assert_true(result.error_message.find("timeout_ms must be > 0") != -1)


fn test_request_safe_max_redirects_validation() raises:
    var result = requests.get_safe("https://example.com/", max_redirects=-1)
    assert_true(not result.ok)
    assert_equal(result.error_kind, "invalid_request")
    assert_true(result.error_message.find("max_redirects must be >= 0") != -1)


fn test_request_safe_max_retries_validation() raises:
    var result = requests.get_safe("https://example.com/", max_retries=-1)
    assert_true(not result.ok)
    assert_equal(result.error_kind, "invalid_request")
    assert_true(result.error_message.find("max_retries must be >= 0") != -1)


fn test_request_safe_retry_backoff_validation() raises:
    var result = requests.get_safe("https://example.com/", retry_backoff_ms=-1)
    assert_true(not result.ok)
    assert_equal(result.error_kind, "invalid_request")
    assert_true(
        result.error_message.find("retry_backoff_ms must be >= 0") != -1
    )

    var result_2 = requests.get_safe(
        "https://example.com/", retry_max_backoff_ms=-1
    )
    assert_true(not result_2.ok)
    assert_equal(result_2.error_kind, "invalid_request")
    assert_true(
        result_2.error_message.find("retry_max_backoff_ms must be >= 0") != -1
    )

    var result_3 = requests.get_safe(
        "https://example.com/", retry_backoff_ms=1_000, retry_max_backoff_ms=10
    )
    assert_true(not result_3.ok)
    assert_equal(result_3.error_kind, "invalid_request")
    assert_true(
        result_3.error_message.find(
            "retry_backoff_ms must be <= retry_max_backoff_ms"
        )
        != -1
    )


fn test_classify_request_error_kind_size_limit_error() raises:
    assert_equal(
        requests._classify_request_error_kind(
            "response body exceeds max_body_bytes"
        ),
        "size_limit_error",
    )
    assert_equal(
        requests._classify_request_error_kind(
            "decoded body exceeds max_decompressed_bytes"
        ),
        "size_limit_error",
    )


fn test_stale_reused_connection_error_includes_tls_io_failures() raises:
    assert_true(
        requests._is_stale_reused_connection_error(
            "SSL_write failed: ssl_error=5, openssl=none"
        )
    )
    assert_true(
        requests._is_stale_reused_connection_error(
            "SSL_read failed: ssl_error=6, openssl=none"
        )
    )
    assert_true(
        not requests._is_stale_reused_connection_error(
            "TLS certificate verification failed: code=18"
        )
    )


fn test_request_safe_size_limit_validation() raises:
    var result = requests.get_safe("https://example.com/", max_header_bytes=0)
    assert_true(not result.ok)
    assert_equal(result.error_kind, "invalid_request")
    assert_true(result.error_message.find("max_header_bytes must be > 0") != -1)

    var result_2 = requests.get_safe("https://example.com/", max_body_bytes=0)
    assert_true(not result_2.ok)
    assert_equal(result_2.error_kind, "invalid_request")
    assert_true(result_2.error_message.find("max_body_bytes must be > 0") != -1)

    var result_3 = requests.get_safe(
        "https://example.com/", max_decompressed_bytes=0
    )
    assert_true(not result_3.ok)
    assert_equal(result_3.error_kind, "invalid_request")
    assert_true(
        result_3.error_message.find("max_decompressed_bytes must be > 0") != -1
    )


fn test_client_get_safe_invalid_url_returns_typed_error() raises:
    var client = requests.Client()
    var result = client.get_safe("https://[::1]/")
    client.close()
    assert_true(not result.ok)
    assert_equal(result.error_kind, "invalid_request")
    assert_true(not result.error_retryable)


fn test_client_get_safe_timeout_validation() raises:
    var client = requests.Client()
    var result = client.get_safe("https://example.com/", timeout_ms=0)
    client.close()
    assert_true(not result.ok)
    assert_equal(result.error_kind, "invalid_request")
    assert_true(result.error_message.find("timeout_ms must be > 0") != -1)


fn test_client_constructor_pool_option_validation() raises:
    try:
        _ = requests.Client(max_idle_connections=-1)
        assert_true(False)
    except e:
        assert_true(String(e).find("max_idle_connections must be >= 0") != -1)

    try:
        _ = requests.Client(idle_ttl_ms=-1)
        assert_true(False)
    except e:
        assert_true(String(e).find("idle_ttl_ms must be >= 0") != -1)

    try:
        _ = requests.Client(max_requests_per_connection=0)
        assert_true(False)
    except e:
        assert_true(
            String(e).find("max_requests_per_connection must be > 0") != -1
        )


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
