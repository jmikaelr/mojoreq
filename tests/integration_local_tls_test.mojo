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
    var value = getenv("MOJOREQ_IT_TLS_BASE_URL")
    if len(value) == 0:
        return "https://localhost:18443"
    return value


fn _url(path: StringSlice) -> String:
    return String(_base_url(), path)


fn test_local_tls_get_ok() raises:
    if not _run_local_integration():
        return

    var response = requests.get(_url("/ok"), max_retries=0)
    assert_equal(response.status_code, 200)
    assert_true(response.body.find('"source":"mock"') != -1)


fn test_local_tls_client_reuses_connection() raises:
    if not _run_local_integration():
        return

    var client = requests.Client(enable_http_pool=False, enable_tls_pool=True)
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


fn test_local_tls_pool_disabled_does_not_reuse() raises:
    if not _run_local_integration():
        return

    var client = requests.Client(enable_http_pool=False, enable_tls_pool=False)
    var first = client.get(_url("/pool-probe"), max_retries=0)
    var second = client.get(_url("/pool-probe"), max_retries=0)
    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_equal(first.headers["X-Connection-Request"], "1")
    assert_equal(second.headers["X-Connection-Request"], "1")


fn test_local_tls_max_requests_per_connection_rotates() raises:
    if not _run_local_integration():
        return

    var client = requests.Client(
        enable_http_pool=False,
        enable_tls_pool=True,
        max_requests_per_connection=1,
    )
    var first = client.get(_url("/pool-probe"), max_retries=0)
    var second = client.get(_url("/pool-probe"), max_retries=0)
    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_equal(first.headers["X-Connection-Request"], "1")
    assert_equal(second.headers["X-Connection-Request"], "1")


fn test_local_tls_idle_ttl_expires_connection() raises:
    if not _run_local_integration():
        return

    var client = requests.Client(
        enable_http_pool=False, enable_tls_pool=True, idle_ttl_ms=1
    )
    var first = client.get(_url("/pool-probe"), max_retries=0)
    requests._sleep_ms(10)
    var second = client.get(_url("/pool-probe"), max_retries=0)
    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_equal(first.headers["X-Connection-Request"], "1")
    assert_equal(second.headers["X-Connection-Request"], "1")


fn test_local_tls_max_idle_zero_disables_reuse() raises:
    if not _run_local_integration():
        return

    var client = requests.Client(
        enable_http_pool=False, enable_tls_pool=True, max_idle_connections=0
    )
    var first = client.get(_url("/pool-probe"), max_retries=0)
    var second = client.get(_url("/pool-probe"), max_retries=0)
    client.close()

    assert_equal(first.status_code, 200)
    assert_equal(second.status_code, 200)
    assert_equal(first.headers["X-Connection-Request"], "1")
    assert_equal(second.headers["X-Connection-Request"], "1")


fn test_local_tls_request_connection_close_disables_reuse() raises:
    if not _run_local_integration():
        return

    var client = requests.Client(enable_http_pool=False, enable_tls_pool=True)

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


fn main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
