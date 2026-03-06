import requests
from std.os.env import getenv
from std.testing import (
    TestSuite,
    assert_equal,
    assert_true,
)


fn _run_local_integration() -> Bool:
    return getenv("MOJOREQ_RUN_LOCAL_INTEGRATION_TESTS") == "1"


fn _base_url() -> String:
    var value = getenv("MOJOREQ_IT_BASE_URL")
    if len(value) == 0:
        return "http://127.0.0.1:18080"
    return value


fn _url(path: StringSlice) -> String:
    return String(_base_url(), path)


fn test_local_get_ok() raises:
    if not _run_local_integration():
        return

    var response = requests.get(_url("/ok"), max_retries=0)
    assert_equal(response.status_code, 200)
    assert_true(response.body.find('"source":"mock"') != -1)


fn test_local_post_echo() raises:
    if not _run_local_integration():
        return

    var response = requests.post(
        _url("/post-echo"), '{"ping":1}', max_retries=0
    )
    assert_equal(response.status_code, 200)
    assert_equal(response.body, '{"ping":1}')


fn test_local_redirect_follow() raises:
    if not _run_local_integration():
        return

    var response = requests.get(_url("/redirect-once"), max_redirects=3)
    assert_equal(response.status_code, 200)
    assert_true(response.body.find('"ok":true') != -1)


fn test_local_redirect_loop_error() raises:
    if not _run_local_integration():
        return

    var result = requests.get_safe(_url("/redirect-loop"), max_redirects=2)
    assert_true(not result.ok)
    assert_equal(result.error_kind, "redirect_error")


fn test_local_retry_get_succeeds() raises:
    if not _run_local_integration():
        return

    var response = requests.get(
        _url("/flaky?key=retry_get&fails=2&status=503"),
        max_retries=2,
        retry_backoff_ms=0,
        retry_max_backoff_ms=0,
    )
    assert_equal(response.status_code, 200)
    assert_true(response.body.find('"attempt":3') != -1)


fn test_local_retry_after_header_path() raises:
    if not _run_local_integration():
        return

    var response = requests.get(
        _url("/flaky?key=retry_after&fails=1&status=503&after=0"),
        max_retries=1,
        retry_backoff_ms=0,
        retry_max_backoff_ms=0,
    )
    assert_equal(response.status_code, 200)
    assert_true(response.body.find('"attempt":2') != -1)


fn test_local_post_not_retried_by_default() raises:
    if not _run_local_integration():
        return

    var response = requests.post(
        _url("/flaky?key=post_no_retry&fails=1&status=503"),
        "{}",
        max_retries=2,
        retry_backoff_ms=0,
        retry_max_backoff_ms=0,
    )
    assert_equal(response.status_code, 503)


fn test_local_post_retry_when_enabled() raises:
    if not _run_local_integration():
        return

    var response = requests.post(
        _url("/flaky?key=post_with_retry&fails=1&status=503"),
        "{}",
        max_retries=2,
        retry_backoff_ms=0,
        retry_max_backoff_ms=0,
        retry_non_idempotent=True,
    )
    assert_equal(response.status_code, 200)
    assert_true(response.body.find('"attempt":2') != -1)


fn test_local_client_reuses_http_connection() raises:
    if not _run_local_integration():
        return

    var client = requests.Client()
    var first = client.get(_url("/pool-probe"), max_retries=0)
    var second = client.get(_url("/pool-probe"), max_retries=0)
    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_equal(first.headers["X-Connection-Request"], "1")
    assert_equal(second.headers["X-Connection-Request"], "2")
    assert_equal(
        first.headers["X-Connection-Id"], second.headers["X-Connection-Id"]
    )


fn test_local_client_pool_disabled_does_not_reuse() raises:
    if not _run_local_integration():
        return

    var client = requests.Client(enable_http_pool=False)
    var first = client.get(_url("/pool-probe"), max_retries=0)
    var second = client.get(_url("/pool-probe"), max_retries=0)
    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_equal(first.headers["X-Connection-Request"], "1")
    assert_equal(second.headers["X-Connection-Request"], "1")


fn test_local_client_max_requests_per_connection_rotates() raises:
    if not _run_local_integration():
        return

    var client = requests.Client(max_requests_per_connection=1)
    var first = client.get(_url("/pool-probe"), max_retries=0)
    var second = client.get(_url("/pool-probe"), max_retries=0)
    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_equal(first.headers["X-Connection-Request"], "1")
    assert_equal(second.headers["X-Connection-Request"], "1")


fn test_local_client_idle_ttl_expires_connection() raises:
    if not _run_local_integration():
        return

    var client = requests.Client(idle_ttl_ms=1)
    var first = client.get(_url("/pool-probe"), max_retries=0)
    requests._sleep_ms(10)
    var second = client.get(_url("/pool-probe"), max_retries=0)
    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_equal(first.headers["X-Connection-Request"], "1")
    assert_equal(second.headers["X-Connection-Request"], "1")


fn test_local_client_max_idle_zero_disables_reuse() raises:
    if not _run_local_integration():
        return

    var client = requests.Client(max_idle_connections=0)
    var first = client.get(_url("/pool-probe"), max_retries=0)
    var second = client.get(_url("/pool-probe"), max_retries=0)
    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_equal(first.headers["X-Connection-Request"], "1")
    assert_equal(second.headers["X-Connection-Request"], "1")


fn test_local_client_request_connection_close_disables_reuse() raises:
    if not _run_local_integration():
        return

    var client = requests.Client()

    var headers_1 = Dict[String, String]()
    headers_1["Connection"] = "close"
    var first = client.request(
        requests.Request(
            method="GET", url=_url("/pool-probe"), headers=headers_1^, body=""
        ),
        max_retries=0,
    )

    var headers_2 = Dict[String, String]()
    headers_2["Connection"] = "close"
    var second = client.request(
        requests.Request(
            method="GET", url=_url("/pool-probe"), headers=headers_2^, body=""
        ),
        max_retries=0,
    )

    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_equal(first.headers["X-Connection-Request"], "1")
    assert_equal(second.headers["X-Connection-Request"], "1")


fn test_local_chunked_decode() raises:
    if not _run_local_integration():
        return

    var response = requests.get(_url("/chunked"), max_retries=0)
    assert_equal(response.status_code, 200)
    assert_equal(response.body, "hello chunked")


fn test_local_gzip_decode() raises:
    if not _run_local_integration():
        return

    var response = requests.get(_url("/gzip"), max_retries=0)
    assert_equal(response.status_code, 200)
    assert_true(response.body.find('"compressed":"gzip"') != -1)


fn test_local_deflate_decode() raises:
    if not _run_local_integration():
        return

    var response = requests.get(_url("/deflate"), max_retries=0)
    assert_equal(response.status_code, 200)
    assert_true(response.body.find('"compressed":"deflate"') != -1)


fn test_local_brotli_decode() raises:
    if not _run_local_integration():
        return
    if not requests._brotli_available():
        return

    var response = requests.get(_url("/brotli"), max_retries=0)
    assert_equal(response.status_code, 200)
    assert_true(response.body.find('"compressed":"br"') != -1)


fn test_local_body_limit_error() raises:
    if not _run_local_integration():
        return

    var result = requests.get_safe(
        _url("/big-body?size=1024"),
        max_retries=0,
        max_body_bytes=128,
    )
    assert_true(not result.ok)
    assert_equal(result.error_kind, "size_limit_error")
    assert_true(result.error_message.find("max_body_bytes") != -1)


fn test_local_header_limit_error() raises:
    if not _run_local_integration():
        return

    var result = requests.get_safe(
        _url("/big-header?size=4096"),
        max_retries=0,
        max_header_bytes=512,
    )
    assert_true(not result.ok)
    assert_equal(result.error_kind, "size_limit_error")
    assert_true(result.error_message.find("max_header_bytes") != -1)


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
