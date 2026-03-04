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


fn test_live_http_example_com_redirects_to_https() raises:
    if not _run_live_integration():
        return

    var response = requests.get("http://example.com/", max_redirects=5)
    assert_equal(response.status_code, 200)
    assert_true(response.body.byte_length() > 0)


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
