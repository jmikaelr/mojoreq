import requests
from std.os.env import getenv
from std.testing import (
    TestSuite,
    assert_equal,
    assert_true,
)


fn _run_live_integration() -> Bool:
    return (
        getenv("MOJOREQ_RUN_INTEGRATION_LIVE") == "1"
        or getenv("MOJOREQ_RUN_NETWORK_TESTS") == "1"
    )


fn test_live_https_example_com() raises:
    if not _run_live_integration():
        return

    var response = requests.get("https://example.com/")
    assert_equal(response.status_code, 200)
    assert_true(response.body.byte_length() > 0)


fn test_live_https_client_tls_pool_smoke() raises:
    if not _run_live_integration():
        return

    var client = requests.Client(enable_http_pool=False, enable_tls_pool=True)
    var first = client.get("https://example.com/")
    var second = client.get("https://example.com/")
    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_true(first.body.byte_length() > 0)
    assert_true(second.body.byte_length() > 0)


fn test_live_https_client_tls_pool_disabled_smoke() raises:
    if not _run_live_integration():
        return

    var client = requests.Client(enable_http_pool=False, enable_tls_pool=False)
    var first = client.get("https://example.com/")
    var second = client.get("https://example.com/")
    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_true(first.body.byte_length() > 0)
    assert_true(second.body.byte_length() > 0)


fn test_live_https_httpbin_post() raises:
    if not _run_live_integration():
        return

    var response = requests.post("https://httpbin.org/post", '{"ping":1}')
    assert_equal(response.status_code, 200)


fn test_live_https_httpbin_gzip() raises:
    if not _run_live_integration():
        return

    var response = requests.get("https://httpbin.org/gzip")
    assert_equal(response.status_code, 200)
    assert_true(
        response.body.find('"gzipped": true') != -1
        or response.body.find('"gzipped":true') != -1
    )


fn test_live_https_httpbin_deflate() raises:
    if not _run_live_integration():
        return

    var response = requests.get("https://httpbin.org/deflate")
    assert_equal(response.status_code, 200)
    assert_true(
        response.body.find('"deflated": true') != -1
        or response.body.find('"deflated":true') != -1
    )


fn test_live_https_httpbin_brotli() raises:
    if not _run_live_integration():
        return
    if not requests._brotli_available():
        return

    var response = requests.get("https://httpbin.org/brotli")
    assert_equal(response.status_code, 200)
    assert_true(
        response.body.find('"brotli": true') != -1
        or response.body.find('"brotli":true') != -1
    )


fn test_live_http_example_com_redirects_to_https() raises:
    if not _run_live_integration():
        return

    var response = requests.get("http://example.com/", max_redirects=5)
    assert_equal(response.status_code, 200)
    assert_true(response.body.byte_length() > 0)


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
