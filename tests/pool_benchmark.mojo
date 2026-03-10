import requests
from std.os.env import getenv
from std.time import perf_counter_ns


fn _env_int(
    name: String, default_value: Int, minimum: Int, maximum: Int
) -> Int:
    var raw = getenv(name)
    if len(raw) == 0:
        return default_value
    try:
        var value = Int(StringSlice(raw))
        if value < minimum:
            return minimum
        if value > maximum:
            return maximum
        return value
    except:
        return default_value


fn _benchmark_unpooled(url: String, iterations: Int) raises -> UInt:
    var started = perf_counter_ns()
    for _ in range(iterations):
        var response = requests.get(url, max_retries=0, timeout_ms=5_000)
        if response.status_code != 200:
            raise Error("unexpected status in unpooled benchmark")
    return perf_counter_ns() - started


fn _benchmark_pooled_http(url: String, iterations: Int) raises -> UInt:
    var client = requests.Client(enable_http_pool=True, enable_tls_pool=False)
    try:
        var started = perf_counter_ns()
        for _ in range(iterations):
            var response = client.get(url, max_retries=0, timeout_ms=5_000)
            if response.status_code != 200:
                raise Error("unexpected status in pooled HTTP benchmark")
        var elapsed = perf_counter_ns() - started
        client.close()
        return elapsed
    except:
        client.close()
        raise


fn _benchmark_pooled_tls(url: String, iterations: Int) raises -> UInt:
    var client = requests.Client(enable_http_pool=False, enable_tls_pool=True)
    try:
        var started = perf_counter_ns()
        for _ in range(iterations):
            var response = client.get(url, max_retries=0, timeout_ms=5_000)
            if response.status_code != 200:
                raise Error("unexpected status in pooled TLS benchmark")
        var elapsed = perf_counter_ns() - started
        client.close()
        return elapsed
    except:
        client.close()
        raise


fn _median3(a: UInt, b: UInt, c: UInt) -> UInt:
    if a <= b:
        if b <= c:
            return b
        if a <= c:
            return c
        return a

    if a <= c:
        return a
    if b <= c:
        return c
    return b


fn _bench_protocol(
    label: StringSlice,
    url: String,
    iterations: Int,
    rounds: Int,
    max_regression_pct: Int,
    pooled_tls: Bool,
) raises:
    _ = requests.get(url, max_retries=0, timeout_ms=5_000)

    var unpooled_a = _benchmark_unpooled(url, iterations)
    var pooled_a = _benchmark_pooled_tls(
        url, iterations
    ) if pooled_tls else _benchmark_pooled_http(url, iterations)

    var unpooled_median = unpooled_a
    var pooled_median = pooled_a

    if rounds >= 2:
        var unpooled_b = _benchmark_unpooled(url, iterations)
        var pooled_b = _benchmark_pooled_tls(
            url, iterations
        ) if pooled_tls else _benchmark_pooled_http(url, iterations)
        unpooled_median = (unpooled_a + unpooled_b) // UInt(2)
        pooled_median = (pooled_a + pooled_b) // UInt(2)

        if rounds >= 3:
            var unpooled_c = _benchmark_unpooled(url, iterations)
            var pooled_c = _benchmark_pooled_tls(
                url, iterations
            ) if pooled_tls else _benchmark_pooled_http(url, iterations)
            unpooled_median = _median3(unpooled_a, unpooled_b, unpooled_c)
            pooled_median = _median3(pooled_a, pooled_b, pooled_c)

    var unpooled_ms = Float64(unpooled_median) / 1_000_000.0
    var pooled_ms = Float64(pooled_median) / 1_000_000.0
    var ratio = pooled_ms / unpooled_ms
    var max_ratio = Float64(100 + max_regression_pct) / 100.0

    print(
        label,
        " benchmark: unpooled=",
        unpooled_ms,
        "ms pooled=",
        pooled_ms,
        "ms ratio=",
        ratio,
    )

    if ratio > max_ratio:
        raise Error(
            label,
            " pooled benchmark regression: ratio=",
            ratio,
            " max_allowed=",
            max_ratio,
        )


fn main() raises:
    var http_base = getenv("MOJOREQ_BENCH_HTTP_BASE_URL")
    if len(http_base) == 0:
        http_base = "http://127.0.0.1:18080"

    var tls_base = getenv("MOJOREQ_BENCH_TLS_BASE_URL")
    if len(tls_base) == 0:
        tls_base = "https://localhost:18443"

    var iterations = _env_int(
        "MOJOREQ_BENCH_ITERATIONS", default_value=20, minimum=3, maximum=1_000
    )
    var rounds = _env_int(
        "MOJOREQ_BENCH_ROUNDS", default_value=3, minimum=1, maximum=3
    )
    var max_regression_pct = _env_int(
        "MOJOREQ_BENCH_MAX_REGRESSION_PCT",
        default_value=25,
        minimum=0,
        maximum=300,
    )

    var http_url = String(http_base, "/pool-probe")
    var tls_url = String(tls_base, "/pool-probe")

    print(
        "benchmark config: iterations=",
        iterations,
        " rounds=",
        rounds,
        " max_regression_pct=",
        max_regression_pct,
    )

    _bench_protocol(
        "HTTP",
        http_url,
        iterations,
        rounds,
        max_regression_pct,
        pooled_tls=False,
    )
    _bench_protocol(
        "HTTPS",
        tls_url,
        iterations,
        rounds,
        max_regression_pct,
        pooled_tls=True,
    )

    print("pool benchmark passed")
