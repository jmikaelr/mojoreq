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
