from std.collections import Dict, InlineArray, List
from std.ffi import (
    OwnedDLHandle,
    ErrNo,
    c_char,
    c_int,
    c_long,
    c_ssize_t,
    c_uint,
    c_ulong,
    external_call,
    get_errno,
)
from std.os.env import getenv
from std.sys import CompilationTarget
from std.sys.info import size_of
from std.sys._libc import close
from std.time import perf_counter_ns


comptime _AF_UNSPEC = c_int(0)
comptime _SOCK_STREAM = c_int(1)
comptime _READ_CHUNK_SIZE = 4096
comptime _DEFAULT_TIMEOUT_MS = 30_000
comptime _DEFAULT_MAX_REDIRECTS = 10
comptime _DEFAULT_MAX_RETRIES = 2
comptime _DEFAULT_RETRY_BACKOFF_MS = 200
comptime _DEFAULT_RETRY_MAX_BACKOFF_MS = 2_000
comptime _DEFAULT_MAX_HEADER_BYTES = 65_536
comptime _DEFAULT_MAX_BODY_BYTES = 10_485_760
comptime _DEFAULT_MAX_DECOMPRESSED_BYTES = 20_971_520
comptime _RAW_RESPONSE_OVERHEAD_BYTES = 1_048_576
comptime _MAX_RETRY_AFTER_MS = 300_000
comptime _NSEC_PER_MSEC = 1_000_000
comptime _NSEC_PER_SEC = 1_000_000_000
comptime _SOL_SOCKET = (
    c_int(1) if CompilationTarget.is_linux() else c_int(
        0xFFFF
    ) if CompilationTarget.is_macos() else c_int(0)
)
comptime _SO_RCVTIMEO = (
    c_int(20) if CompilationTarget.is_linux() else c_int(
        0x1006
    ) if CompilationTarget.is_macos() else c_int(0)
)
comptime _SO_SNDTIMEO = (
    c_int(21) if CompilationTarget.is_linux() else c_int(
        0x1005
    ) if CompilationTarget.is_macos() else c_int(0)
)
comptime _SSL_VERIFY_PEER = c_int(1)
comptime _SSL_ERROR_WANT_READ = c_int(2)
comptime _SSL_ERROR_WANT_WRITE = c_int(3)
comptime _SSL_ERROR_ZERO_RETURN = c_int(6)
comptime _SSL_CTRL_SET_TLSEXT_HOSTNAME = c_int(55)
comptime _TLSEXT_NAMETYPE_HOST_NAME = c_long(0)
comptime _X509_V_OK = c_long(0)
comptime _OPENSSL_ERRBUF_SIZE = 256
comptime _Z_OK = c_int(0)
comptime _Z_STREAM_END = c_int(1)
comptime _Z_NO_FLUSH = c_int(0)
comptime _ZLIB_WINDOW_BITS = c_int(15)
comptime _ZLIB_GZIP_WINDOW_BITS = c_int(47)
comptime _ZLIB_RAW_WINDOW_BITS = c_int(-15)


@fieldwise_init
struct Request:
    var method: String
    var url: String
    var headers: Dict[String, String]
    var body: String


@fieldwise_init
struct Response:
    var status_code: Int
    var headers: Dict[String, String]
    var body: String

    fn text(self) -> String:
        return String(self.body)


@fieldwise_init
struct RequestError:
    var kind: String
    var message: String
    var retryable: Bool


@fieldwise_init
struct RequestResult:
    var ok: Bool
    var status_code: Int
    var headers: Dict[String, String]
    var body: String
    var error_kind: String
    var error_message: String
    var error_retryable: Bool

    fn text(self) -> String:
        return String(self.body)

    fn error(self) -> RequestError:
        return RequestError(
            kind=self.error_kind,
            message=self.error_message,
            retryable=self.error_retryable,
        )


@fieldwise_init
struct _ParsedURL:
    var host: String
    var host_header: String
    var port: Int
    var path: String
    var use_tls: Bool


@fieldwise_init
struct _AddrInfoMac:
    var ai_flags: c_int
    var ai_family: c_int
    var ai_socktype: c_int
    var ai_protocol: c_int
    var ai_addrlen: c_uint
    var ai_canonname: UnsafePointer[c_char, MutExternalOrigin]
    var ai_addr: OpaquePointer[MutExternalOrigin]
    var ai_next: UnsafePointer[_AddrInfoMac, MutExternalOrigin]


@fieldwise_init
struct _AddrInfoLinux:
    var ai_flags: c_int
    var ai_family: c_int
    var ai_socktype: c_int
    var ai_protocol: c_int
    var ai_addrlen: c_uint
    var ai_addr: OpaquePointer[MutExternalOrigin]
    var ai_canonname: UnsafePointer[c_char, MutExternalOrigin]
    var ai_next: UnsafePointer[_AddrInfoLinux, MutExternalOrigin]


@fieldwise_init
struct _TimeVal:
    var tv_sec: c_long
    var tv_usec: c_long


@fieldwise_init
struct _Timespec:
    var tv_sec: c_long
    var tv_nsec: c_long


@fieldwise_init
struct _ZStream:
    var next_in: UnsafePointer[NoneType, ImmutExternalOrigin]
    var avail_in: c_uint
    var total_in: c_ulong
    var next_out: UnsafePointer[NoneType, MutExternalOrigin]
    var avail_out: c_uint
    var total_out: c_ulong
    var msg: UnsafePointer[c_char, MutExternalOrigin]
    var state: OpaquePointer[MutExternalOrigin]
    var zalloc: OpaquePointer[MutExternalOrigin]
    var zfree: OpaquePointer[MutExternalOrigin]
    var opaque: OpaquePointer[MutExternalOrigin]
    var data_type: c_int
    var adler: c_ulong
    var reserved: c_ulong


struct _Zlib(Movable):
    var libz: OwnedDLHandle

    fn __init__(out self) raises:
        self.libz = OwnedDLHandle(unsafe_uninitialized=True)

        var conda_prefix = getenv("CONDA_PREFIX")
        var z_fallback_v = String()
        var z_fallback_compat = String()
        if len(conda_prefix) > 0:
            comptime if CompilationTarget.is_macos():
                z_fallback_v = String(conda_prefix, "/lib/libz.1.dylib")
                z_fallback_compat = String(conda_prefix, "/lib/libz.dylib")
            elif CompilationTarget.is_linux():
                z_fallback_v = String(conda_prefix, "/lib/libz.so.1")
                z_fallback_compat = String(conda_prefix, "/lib/libz.so")

        comptime if CompilationTarget.is_macos():
            self.libz = _open_shared_library(
                "libz.1.dylib",
                "libz.dylib",
                z_fallback_v,
                z_fallback_compat,
            )
        elif CompilationTarget.is_linux():
            self.libz = _open_shared_library(
                "libz.so.1",
                "libz.so",
                z_fallback_v,
                z_fallback_compat,
            )
        else:
            CompilationTarget.unsupported_target_error[
                NoneType, operation="gzip/deflate zlib setup"
            ]()


struct _OpenSSL(Movable):
    var libssl: OwnedDLHandle
    var libcrypto: OwnedDLHandle

    fn __init__(out self) raises:
        self.libssl = OwnedDLHandle(unsafe_uninitialized=True)
        self.libcrypto = OwnedDLHandle(unsafe_uninitialized=True)

        var conda_prefix = getenv("CONDA_PREFIX")
        var ssl_fallback_v = String()
        var ssl_fallback_compat = String()
        var crypto_fallback_v = String()
        var crypto_fallback_compat = String()
        if len(conda_prefix) > 0:
            comptime if CompilationTarget.is_macos():
                ssl_fallback_v = String(conda_prefix, "/lib/libssl.3.dylib")
                ssl_fallback_compat = String(conda_prefix, "/lib/libssl.dylib")
                crypto_fallback_v = String(
                    conda_prefix, "/lib/libcrypto.3.dylib"
                )
                crypto_fallback_compat = String(
                    conda_prefix, "/lib/libcrypto.dylib"
                )
            elif CompilationTarget.is_linux():
                ssl_fallback_v = String(conda_prefix, "/lib/libssl.so.3")
                ssl_fallback_compat = String(conda_prefix, "/lib/libssl.so")
                crypto_fallback_v = String(conda_prefix, "/lib/libcrypto.so.3")
                crypto_fallback_compat = String(
                    conda_prefix, "/lib/libcrypto.so"
                )

        comptime if CompilationTarget.is_macos():
            self.libssl = _open_shared_library(
                "libssl.3.dylib",
                "libssl.dylib",
                ssl_fallback_v,
                ssl_fallback_compat,
            )
            self.libcrypto = _open_shared_library(
                "libcrypto.3.dylib",
                "libcrypto.dylib",
                crypto_fallback_v,
                crypto_fallback_compat,
            )
        elif CompilationTarget.is_linux():
            self.libssl = _open_shared_library(
                "libssl.so.3",
                "libssl.so",
                ssl_fallback_v,
                ssl_fallback_compat,
            )
            self.libcrypto = _open_shared_library(
                "libcrypto.so.3",
                "libcrypto.so",
                crypto_fallback_v,
                crypto_fallback_compat,
            )
        else:
            CompilationTarget.unsupported_target_error[
                NoneType, operation="HTTPS/OpenSSL setup"
            ]()

    fn last_error(self) -> String:
        var err_code = self.libcrypto.call[
            "ERR_get_error", return_type=c_ulong
        ]()
        if err_code == c_ulong(0):
            return "none"

        var buffer = InlineArray[UInt8, _OPENSSL_ERRBUF_SIZE](fill=0)
        self.libcrypto.call["ERR_error_string_n"](
            err_code,
            buffer.unsafe_ptr().bitcast[c_char](),
            UInt(_OPENSSL_ERRBUF_SIZE),
        )
        return String(unsafe_from_utf8_ptr=buffer.unsafe_ptr())


fn _open_shared_library(
    primary: String,
    secondary: String = "",
    fallback_primary: String = "",
    fallback_secondary: String = "",
) raises -> OwnedDLHandle:
    try:
        return OwnedDLHandle(primary)
    except:
        if len(secondary) > 0:
            try:
                return OwnedDLHandle(secondary)
            except:
                pass

        if len(fallback_primary) > 0:
            try:
                return OwnedDLHandle(fallback_primary)
            except:
                pass

        if len(fallback_secondary) > 0:
            try:
                return OwnedDLHandle(fallback_secondary)
            except:
                pass

    raise Error(
        "unable to load shared library; tried: ",
        primary,
        ", ",
        secondary,
        ", ",
        fallback_primary,
        ", ",
        fallback_secondary,
    )


fn _has_header(headers: Dict[String, String], name: StringSlice) -> Bool:
    var wanted = String(name).lower()
    for key in headers:
        if key.lower() == wanted:
            return True
    return False


fn _set_default_header(
    mut headers: Dict[String, String], name: String, value: String
):
    if not _has_header(headers, name):
        headers[name] = value


fn _string_from_bytes(bytes: List[UInt8]) -> String:
    var result = String(capacity=len(bytes))
    for b in bytes:
        result._unsafe_append_byte(b)
    return result^


fn _empty_response() -> Response:
    return Response(status_code=0, headers=Dict[String, String](), body="")


fn _none_error() -> RequestError:
    return RequestError(kind="none", message="", retryable=False)


fn _result_from_response(response: Response) -> RequestResult:
    return RequestResult(
        ok=True,
        status_code=response.status_code,
        headers=response.headers.copy(),
        body=String(response.body),
        error_kind="none",
        error_message="",
        error_retryable=False,
    )


fn _deadline_from_timeout_ms(timeout_ms: Int) raises -> UInt:
    if timeout_ms <= 0:
        raise Error("timeout_ms must be > 0")

    return perf_counter_ns() + UInt(timeout_ms) * UInt(_NSEC_PER_MSEC)


fn _deadline_expired(deadline_ns: UInt) -> Bool:
    return perf_counter_ns() >= deadline_ns


fn _remaining_timeout_ms(deadline_ns: UInt) -> Int:
    var now = perf_counter_ns()
    if now >= deadline_ns:
        return 0
    var remaining_ns = deadline_ns - now
    return Int(
        (remaining_ns + UInt(_NSEC_PER_MSEC - 1)) // UInt(_NSEC_PER_MSEC)
    )


fn _max_raw_response_bytes(max_header_bytes: Int, max_body_bytes: Int) -> Int:
    return max_header_bytes + max_body_bytes + _RAW_RESPONSE_OVERHEAD_BYTES


fn _timeout_timeval(timeout_ms: Int) -> _TimeVal:
    var seconds = timeout_ms // 1000
    var microseconds = (timeout_ms % 1000) * 1000
    return _TimeVal(tv_sec=c_long(seconds), tv_usec=c_long(microseconds))


fn _set_socket_timeout(fd: c_int, option: c_int, timeout_ms: Int) raises:
    var timeout = _timeout_timeval(timeout_ms)
    var rc = external_call["setsockopt", c_int](
        fd,
        _SOL_SOCKET,
        option,
        Pointer(to=timeout),
        c_uint(size_of[_TimeVal]()),
    )
    if rc != 0:
        raise Error(
            "setsockopt failed for option ",
            option,
            ": errno=",
            String(get_errno()),
        )


fn _apply_socket_timeouts(fd: c_int, timeout_ms: Int) raises:
    _set_socket_timeout(fd, _SO_RCVTIMEO, timeout_ms)
    _set_socket_timeout(fd, _SO_SNDTIMEO, timeout_ms)


fn _sleep_ms(delay_ms: Int) raises:
    if delay_ms <= 0:
        return

    var request = _Timespec(
        tv_sec=c_long(delay_ms // 1000),
        tv_nsec=c_long((delay_ms % 1000) * _NSEC_PER_MSEC),
    )
    var remaining = _Timespec(tv_sec=0, tv_nsec=0)

    while True:
        var rc = external_call["nanosleep", c_int](
            Pointer(to=request), Pointer(to=remaining)
        )
        if rc == 0:
            return

        var err = get_errno()
        if err == ErrNo.EINTR:
            request.tv_sec = remaining.tv_sec
            request.tv_nsec = remaining.tv_nsec
            continue

        raise Error("nanosleep failed: errno=", String(err))


fn _wait_before_retry(delay_ms: Int, deadline_ns: UInt, timeout_ms: Int) raises:
    if delay_ms <= 0:
        return

    var remaining_ms = _remaining_timeout_ms(deadline_ns)
    if remaining_ms <= 0:
        raise Error("timeout after ", timeout_ms, "ms")

    var bounded_delay_ms = delay_ms
    if bounded_delay_ms > remaining_ms:
        bounded_delay_ms = remaining_ms

    _sleep_ms(bounded_delay_ms)
    if _deadline_expired(deadline_ns):
        raise Error("timeout after ", timeout_ms, "ms")


fn _is_idempotent_method(method: StringSlice) -> Bool:
    var upper = String(method).upper()
    return (
        upper == "GET"
        or upper == "HEAD"
        or upper == "PUT"
        or upper == "DELETE"
        or upper == "OPTIONS"
        or upper == "TRACE"
    )


fn _is_retryable_status_code(status_code: Int) -> Bool:
    return (
        status_code == 408
        or status_code == 425
        or status_code == 429
        or status_code == 500
        or status_code == 502
        or status_code == 503
        or status_code == 504
    )


fn _retry_backoff_delay_ms(
    retry_number: Int,
    initial_backoff_ms: Int,
    max_backoff_ms: Int,
    jitter_source_ns: UInt = UInt(0),
) -> Int:
    if retry_number < 0 or initial_backoff_ms <= 0 or max_backoff_ms <= 0:
        return 0

    var delay_ms = initial_backoff_ms
    for _ in range(retry_number):
        if delay_ms >= max_backoff_ms:
            delay_ms = max_backoff_ms
            break
        var doubled = delay_ms * 2
        if doubled < delay_ms:
            delay_ms = max_backoff_ms
            break
        delay_ms = doubled

    if delay_ms > max_backoff_ms:
        delay_ms = max_backoff_ms

    var jitter_window_ms = delay_ms // 5
    if jitter_window_ms <= 0:
        return delay_ms

    var source = jitter_source_ns
    if source == UInt(0):
        source = perf_counter_ns()

    var jitter_span = jitter_window_ms * 2 + 1
    var jitter_offset = Int(source % UInt(jitter_span)) - jitter_window_ms
    var jittered_delay_ms = delay_ms + jitter_offset
    if jittered_delay_ms < 0:
        jittered_delay_ms = 0
    if jittered_delay_ms > max_backoff_ms:
        jittered_delay_ms = max_backoff_ms

    return jittered_delay_ms


fn _classify_request_error_kind(message: StringSlice) -> String:
    var lower = String(message).lower()
    if (
        lower.find("url ") != -1
        or lower.find("ipv6 literal hosts") != -1
        or lower.find("timeout_ms must be > 0") != -1
        or lower.find("max_redirects must be >= 0") != -1
        or lower.find("max_retries must be >= 0") != -1
        or lower.find("retry_backoff_ms must be >= 0") != -1
        or lower.find("retry_max_backoff_ms must be >= 0") != -1
        or lower.find("retry_backoff_ms must be <= retry_max_backoff_ms") != -1
        or lower.find("max_header_bytes must be > 0") != -1
        or lower.find("max_body_bytes must be > 0") != -1
        or lower.find("max_decompressed_bytes must be > 0") != -1
    ):
        return "invalid_request"
    if (
        lower.find("max_header_bytes") != -1
        or lower.find("max_body_bytes") != -1
        or lower.find("max_decompressed_bytes") != -1
        or lower.find("max_raw_response_bytes") != -1
    ):
        return "size_limit_error"
    if lower.find("redirect") != -1:
        return "redirect_error"
    if (
        lower.find("content-encoding") != -1
        or lower.find("inflate") != -1
        or lower.find("zlib") != -1
    ):
        return "compression_error"
    if lower.find("timeout") != -1:
        return "timeout"
    if lower.find("getaddrinfo failed") != -1:
        return "dns_error"
    if lower.find("unable to connect") != -1 or lower.find("connect") != -1:
        return "connect_error"
    if lower.find("tls") != -1 or lower.find("ssl") != -1:
        return "tls_error"
    if (
        lower.find("invalid http") != -1
        or lower.find("content-length") != -1
        or lower.find("chunked") != -1
        or lower.find("response is empty") != -1
    ):
        return "parse_error"
    if (
        lower.find("send() failed") != -1
        or lower.find("read() failed") != -1
        or lower.find("setsockopt") != -1
    ):
        return "io_error"
    return "unknown"


fn _is_retryable_error_kind(kind: StringSlice) -> Bool:
    return (
        kind == "timeout"
        or kind == "dns_error"
        or kind == "connect_error"
        or kind == "tls_error"
        or kind == "io_error"
    )


fn _parse_url(url: String) raises -> _ParsedURL:
    var use_tls = False
    var rest = String(url)
    if rest.startswith("https://"):
        use_tls = True
        rest = String(rest[8:])
    elif rest.startswith("http://"):
        rest = String(rest[7:])

    var slash_index = rest.find("/")
    var query_index = rest.find("?")
    var fragment_index = rest.find("#")
    var path_start = slash_index
    if query_index != -1 and (path_start == -1 or query_index < path_start):
        path_start = query_index
    if fragment_index != -1 and (
        path_start == -1 or fragment_index < path_start
    ):
        path_start = fragment_index

    var authority = String(rest)
    var path = String("/")

    if path_start != -1:
        authority = String(rest[:path_start])
        path = String(rest[path_start:])
        if path.startswith("?") or path.startswith("#"):
            path = String("/", path)
        var path_fragment_index = path.find("#")
        if path_fragment_index != -1:
            path = String(path[:path_fragment_index])
        if len(path) == 0:
            path = "/"

    if len(authority) == 0:
        raise Error("URL is missing a host: ", url)

    if authority.startswith("["):
        raise Error("IPv6 literal hosts are not supported yet")

    var host = String(authority)
    var host_header = String(authority)
    var port = 443 if use_tls else 80

    var colon_index = authority.rfind(":")
    if colon_index != -1:
        host = String(authority[:colon_index])
        if len(host) == 0:
            raise Error("URL has an invalid host: ", url)
        var port_text = String(authority[colon_index + 1 :])
        if len(port_text) == 0:
            raise Error("URL has an invalid port: ", url)
        port = Int(StringSlice(port_text))
    else:
        host_header = String(host)

    return _ParsedURL(
        host=host,
        host_header=host_header,
        port=port,
        path=path,
        use_tls=use_tls,
    )


fn _url_scheme(use_tls: Bool) -> String:
    return "https" if use_tls else "http"


fn _is_redirect_status(status_code: Int) -> Bool:
    return (
        status_code == 301
        or status_code == 302
        or status_code == 303
        or status_code == 307
        or status_code == 308
    )


fn _redirect_method(status_code: Int, method: StringSlice) -> String:
    var current = String(method).upper()

    if status_code == 303 and current != "HEAD":
        return "GET"
    if (status_code == 301 or status_code == 302) and current == "POST":
        return "GET"
    return current


fn _resolve_redirect_url(
    parsed: _ParsedURL, location: StringSlice
) raises -> String:
    var value = String(String(location).strip())
    if len(value) == 0:
        raise Error("redirect response has empty Location header")

    if value.startswith("https://") or value.startswith("http://"):
        return value

    var scheme = _url_scheme(parsed.use_tls)
    if value.startswith("//"):
        return String(scheme, ":", value)
    if value.startswith("/"):
        return String(scheme, "://", parsed.host_header, value)

    var base_path = String(parsed.path)
    var query_index = base_path.find("?")
    if query_index != -1:
        base_path = String(base_path[:query_index])
    if len(base_path) == 0 or not base_path.startswith("/"):
        base_path = String("/", base_path)

    var last_slash = base_path.rfind("/")
    var directory = String("/")
    if last_slash > 0:
        directory = String(base_path[: last_slash + 1])

    return String(scheme, "://", parsed.host_header, directory, value)


fn _prepare_redirect_headers(
    headers: Dict[String, String],
    drop_body_headers: Bool,
    current_host_header: StringSlice,
    next_host_header: StringSlice,
) -> Dict[String, String]:
    var next = Dict[String, String]()
    var same_host = (
        String(current_host_header).lower() == String(next_host_header).lower()
    )

    for header in headers.items():
        var name_lower = header.key.lower()
        if name_lower == "host":
            continue
        if not same_host and name_lower == "authorization":
            continue
        if drop_body_headers and (
            name_lower == "content-length"
            or name_lower == "content-type"
            or name_lower == "transfer-encoding"
        ):
            continue
        next[header.key] = header.value

    return next^


fn _build_request_payload(req: Request, parsed: _ParsedURL) -> String:
    var headers = req.headers.copy()

    _set_default_header(headers, "Host", parsed.host_header)
    _set_default_header(headers, "Connection", "close")
    _set_default_header(headers, "Accept-Encoding", "gzip, deflate")

    if req.body.byte_length() > 0 and not _has_header(
        headers, "Content-Length"
    ):
        headers["Content-Length"] = String(req.body.byte_length())

    var payload = String(req.method.upper(), " ", parsed.path, " HTTP/1.1\r\n")

    for header in headers.items():
        payload += String(header.key, ": ", header.value, "\r\n")

    payload += "\r\n"
    payload += req.body
    return payload


fn _open_socket_macos(parsed: _ParsedURL, timeout_ms: Int) raises -> c_int:
    var hints = _AddrInfoMac(
        ai_flags=0,
        ai_family=_AF_UNSPEC,
        ai_socktype=_SOCK_STREAM,
        ai_protocol=0,
        ai_addrlen=0,
        ai_canonname=UnsafePointer[c_char, MutExternalOrigin](),
        ai_addr=OpaquePointer[MutExternalOrigin](),
        ai_next=UnsafePointer[_AddrInfoMac, MutExternalOrigin](),
    )

    var results = UnsafePointer[_AddrInfoMac, MutExternalOrigin]()
    var host = String(parsed.host)
    var port_string = String(parsed.port)
    var rc = external_call["getaddrinfo", c_int](
        host.as_c_string_slice().unsafe_ptr(),
        port_string.as_c_string_slice().unsafe_ptr(),
        Pointer(to=hints),
        Pointer(to=results),
    )

    if rc != 0 or not results:
        raise Error("getaddrinfo failed for host: ", parsed.host)

    var cursor = results
    while cursor:
        var fd = external_call["socket", c_int](
            cursor[].ai_family, cursor[].ai_socktype, cursor[].ai_protocol
        )
        if fd < 0:
            cursor = cursor[].ai_next
            continue

        _apply_socket_timeouts(fd, timeout_ms)

        var connected = external_call["connect", c_int](
            fd, cursor[].ai_addr, cursor[].ai_addrlen
        )
        if connected == 0:
            external_call["freeaddrinfo", NoneType](results)
            return fd

        _ = close(fd)
        cursor = cursor[].ai_next

    external_call["freeaddrinfo", NoneType](results)
    raise Error("unable to connect to ", parsed.host, ":", parsed.port)


fn _open_socket_linux(parsed: _ParsedURL, timeout_ms: Int) raises -> c_int:
    var hints = _AddrInfoLinux(
        ai_flags=0,
        ai_family=_AF_UNSPEC,
        ai_socktype=_SOCK_STREAM,
        ai_protocol=0,
        ai_addrlen=0,
        ai_addr=OpaquePointer[MutExternalOrigin](),
        ai_canonname=UnsafePointer[c_char, MutExternalOrigin](),
        ai_next=UnsafePointer[_AddrInfoLinux, MutExternalOrigin](),
    )

    var results = UnsafePointer[_AddrInfoLinux, MutExternalOrigin]()
    var host = String(parsed.host)
    var port_string = String(parsed.port)
    var rc = external_call["getaddrinfo", c_int](
        host.as_c_string_slice().unsafe_ptr(),
        port_string.as_c_string_slice().unsafe_ptr(),
        Pointer(to=hints),
        Pointer(to=results),
    )

    if rc != 0 or not results:
        raise Error("getaddrinfo failed for host: ", parsed.host)

    var cursor = results
    while cursor:
        var fd = external_call["socket", c_int](
            cursor[].ai_family, cursor[].ai_socktype, cursor[].ai_protocol
        )
        if fd < 0:
            cursor = cursor[].ai_next
            continue

        _apply_socket_timeouts(fd, timeout_ms)

        var connected = external_call["connect", c_int](
            fd, cursor[].ai_addr, cursor[].ai_addrlen
        )
        if connected == 0:
            external_call["freeaddrinfo", NoneType](results)
            return fd

        _ = close(fd)
        cursor = cursor[].ai_next

    external_call["freeaddrinfo", NoneType](results)
    raise Error("unable to connect to ", parsed.host, ":", parsed.port)


fn _open_socket(parsed: _ParsedURL, timeout_ms: Int) raises -> c_int:
    comptime if CompilationTarget.is_macos():
        return _open_socket_macos(parsed, timeout_ms)
    elif CompilationTarget.is_linux():
        return _open_socket_linux(parsed, timeout_ms)
    else:
        return CompilationTarget.unsupported_target_error[
            c_int, operation="socket networking"
        ]()


fn _send_all(
    fd: c_int, payload: String, deadline_ns: UInt, timeout_ms: Int
) raises:
    var bytes = payload.as_bytes()
    var sent = 0
    while sent < len(bytes):
        if _deadline_expired(deadline_ns):
            raise Error("send timeout after ", timeout_ms, "ms")

        var wrote = external_call["send", c_ssize_t](
            fd, bytes.unsafe_ptr() + sent, len(bytes) - sent, c_int(0)
        )
        if wrote == 0:
            raise Error("send() returned 0 before request completed")
        if wrote < 0:
            var err = get_errno()
            if err == ErrNo.EINTR:
                continue
            if err == ErrNo.EAGAIN or err == ErrNo.EWOULDBLOCK:
                if _deadline_expired(deadline_ns):
                    raise Error("send timeout after ", timeout_ms, "ms")
                continue
            raise Error("send() failed: errno=", String(err))
        sent += Int(wrote)


fn _read_all(
    fd: c_int,
    deadline_ns: UInt,
    timeout_ms: Int,
    max_raw_response_bytes: Int,
) raises -> String:
    var data = List[UInt8]()
    var chunk = InlineArray[UInt8, _READ_CHUNK_SIZE](fill=0)

    while True:
        if _deadline_expired(deadline_ns):
            raise Error("read timeout after ", timeout_ms, "ms")

        var read_count = external_call["read", c_ssize_t](
            fd, chunk.unsafe_ptr(), _READ_CHUNK_SIZE
        )
        if read_count < 0:
            var err = get_errno()
            if err == ErrNo.EINTR:
                continue
            if err == ErrNo.EAGAIN or err == ErrNo.EWOULDBLOCK:
                if _deadline_expired(deadline_ns):
                    raise Error("read timeout after ", timeout_ms, "ms")
                continue
            raise Error("read() failed: errno=", String(err))
        if read_count == 0:
            break

        if len(data) + Int(read_count) > max_raw_response_bytes:
            raise Error("response exceeds max_raw_response_bytes")

        for i in range(read_count):
            data.append(chunk[i])

    if len(data) == 0:
        return String()

    return _string_from_bytes(data)


fn _configure_tls_trust_store(
    ref openssl: _OpenSSL, ctx: OpaquePointer[MutExternalOrigin]
) -> Bool:
    var loaded_any = False

    if (
        openssl.libssl.call[
            "SSL_CTX_set_default_verify_paths", return_type=c_int
        ](ctx)
        == 1
    ):
        loaded_any = True

    var cert_file = getenv("SSL_CERT_FILE")
    if (
        len(cert_file) > 0
        and openssl.libssl.call["SSL_CTX_load_verify_file", return_type=c_int](
            ctx, cert_file.as_c_string_slice().unsafe_ptr()
        )
        == 1
    ):
        loaded_any = True

    var conda_prefix = getenv("CONDA_PREFIX")
    if len(conda_prefix) > 0:
        var conda_bundle = String(conda_prefix, "/ssl/cacert.pem")
        if (
            openssl.libssl.call["SSL_CTX_load_verify_file", return_type=c_int](
                ctx, conda_bundle.as_c_string_slice().unsafe_ptr()
            )
            == 1
        ):
            loaded_any = True

    comptime if CompilationTarget.is_macos():
        var system_bundle_1 = String("/etc/ssl/cert.pem")
        if (
            openssl.libssl.call["SSL_CTX_load_verify_file", return_type=c_int](
                ctx, system_bundle_1.as_c_string_slice().unsafe_ptr()
            )
            == 1
        ):
            loaded_any = True

        var system_bundle_2 = String("/private/etc/ssl/cert.pem")
        if (
            openssl.libssl.call["SSL_CTX_load_verify_file", return_type=c_int](
                ctx, system_bundle_2.as_c_string_slice().unsafe_ptr()
            )
            == 1
        ):
            loaded_any = True
    elif CompilationTarget.is_linux():
        var linux_bundle_1 = String("/etc/ssl/certs/ca-certificates.crt")
        if (
            openssl.libssl.call["SSL_CTX_load_verify_file", return_type=c_int](
                ctx, linux_bundle_1.as_c_string_slice().unsafe_ptr()
            )
            == 1
        ):
            loaded_any = True

        var linux_bundle_2 = String("/etc/pki/tls/certs/ca-bundle.crt")
        if (
            openssl.libssl.call["SSL_CTX_load_verify_file", return_type=c_int](
                ctx, linux_bundle_2.as_c_string_slice().unsafe_ptr()
            )
            == 1
        ):
            loaded_any = True

        var linux_bundle_3 = String("/etc/ssl/ca-bundle.pem")
        if (
            openssl.libssl.call["SSL_CTX_load_verify_file", return_type=c_int](
                ctx, linux_bundle_3.as_c_string_slice().unsafe_ptr()
            )
            == 1
        ):
            loaded_any = True

        var linux_bundle_4 = String(
            "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"
        )
        if (
            openssl.libssl.call["SSL_CTX_load_verify_file", return_type=c_int](
                ctx, linux_bundle_4.as_c_string_slice().unsafe_ptr()
            )
            == 1
        ):
            loaded_any = True

    return loaded_any


fn _tls_send_all(
    ref openssl: _OpenSSL,
    ssl: OpaquePointer[MutExternalOrigin],
    payload: String,
    deadline_ns: UInt,
    timeout_ms: Int,
) raises:
    var bytes = payload.as_bytes()
    var sent = 0
    while sent < len(bytes):
        if _deadline_expired(deadline_ns):
            raise Error("TLS write timeout after ", timeout_ms, "ms")

        var remaining = len(bytes) - sent
        var write_size = min(remaining, Int(Int32.MAX))
        var wrote = openssl.libssl.call["SSL_write", return_type=c_int](
            ssl, bytes.unsafe_ptr() + sent, c_int(write_size)
        )
        if wrote <= 0:
            var ssl_error = openssl.libssl.call[
                "SSL_get_error", return_type=c_int
            ](ssl, wrote)
            if (
                ssl_error == _SSL_ERROR_WANT_READ
                or ssl_error == _SSL_ERROR_WANT_WRITE
            ):
                if _deadline_expired(deadline_ns):
                    raise Error("TLS write timeout after ", timeout_ms, "ms")
                continue
            raise Error(
                "SSL_write failed: ssl_error=",
                ssl_error,
                ", openssl=",
                openssl.last_error(),
            )
        sent += Int(wrote)


fn _tls_read_all(
    ref openssl: _OpenSSL,
    ssl: OpaquePointer[MutExternalOrigin],
    deadline_ns: UInt,
    timeout_ms: Int,
    max_raw_response_bytes: Int,
) raises -> String:
    var data = List[UInt8]()
    var chunk = InlineArray[UInt8, _READ_CHUNK_SIZE](fill=0)

    while True:
        if _deadline_expired(deadline_ns):
            raise Error("TLS read timeout after ", timeout_ms, "ms")

        var read_count = openssl.libssl.call["SSL_read", return_type=c_int](
            ssl, chunk.unsafe_ptr(), c_int(_READ_CHUNK_SIZE)
        )
        if read_count > 0:
            if len(data) + Int(read_count) > max_raw_response_bytes:
                raise Error("response exceeds max_raw_response_bytes")
            for i in range(read_count):
                data.append(chunk[i])
            continue

        var ssl_error = openssl.libssl.call["SSL_get_error", return_type=c_int](
            ssl, read_count
        )
        if ssl_error == _SSL_ERROR_ZERO_RETURN or read_count == 0:
            break
        if (
            ssl_error == _SSL_ERROR_WANT_READ
            or ssl_error == _SSL_ERROR_WANT_WRITE
        ):
            if _deadline_expired(deadline_ns):
                raise Error("TLS read timeout after ", timeout_ms, "ms")
            continue

        raise Error(
            "SSL_read failed: ssl_error=",
            ssl_error,
            ", openssl=",
            openssl.last_error(),
        )

    if len(data) == 0:
        return String()

    return _string_from_bytes(data)


fn _tls_cleanup(
    ref openssl: _OpenSSL,
    ssl: OpaquePointer[MutExternalOrigin],
    ctx: OpaquePointer[MutExternalOrigin],
):
    if ssl:
        _ = openssl.libssl.call["SSL_shutdown", return_type=c_int](ssl)
        openssl.libssl.call["SSL_free"](ssl)
    if ctx:
        openssl.libssl.call["SSL_CTX_free"](ctx)


fn _request_tls(
    fd: c_int,
    host: String,
    payload: String,
    deadline_ns: UInt,
    timeout_ms: Int,
    max_raw_response_bytes: Int,
) raises -> String:
    var openssl = _OpenSSL()
    var ctx = OpaquePointer[MutExternalOrigin]()
    var ssl = OpaquePointer[MutExternalOrigin]()
    var host_copy = String(host)

    try:
        var method = openssl.libssl.call[
            "TLS_client_method",
            return_type = OpaquePointer[MutExternalOrigin],
        ]()
        if not method:
            raise Error("TLS_client_method failed: ", openssl.last_error())

        ctx = openssl.libssl.call[
            "SSL_CTX_new", return_type = OpaquePointer[MutExternalOrigin]
        ](method)
        if not ctx:
            raise Error("SSL_CTX_new failed: ", openssl.last_error())

        openssl.libssl.call["SSL_CTX_set_verify"](
            ctx, _SSL_VERIFY_PEER, OpaquePointer[MutExternalOrigin]()
        )
        if not _configure_tls_trust_store(openssl, ctx):
            raise Error("unable to configure TLS trust store")

        ssl = openssl.libssl.call[
            "SSL_new", return_type = OpaquePointer[MutExternalOrigin]
        ](ctx)
        if not ssl:
            raise Error("SSL_new failed: ", openssl.last_error())

        if openssl.libssl.call["SSL_set_fd", return_type=c_int](ssl, fd) != 1:
            raise Error("SSL_set_fd failed: ", openssl.last_error())

        if (
            openssl.libssl.call["SSL_set1_host", return_type=c_int](
                ssl, host_copy.as_c_string_slice().unsafe_ptr()
            )
            != 1
        ):
            raise Error("SSL_set1_host failed: ", openssl.last_error())

        if (
            openssl.libssl.call["SSL_ctrl", return_type=c_long](
                ssl,
                _SSL_CTRL_SET_TLSEXT_HOSTNAME,
                _TLSEXT_NAMETYPE_HOST_NAME,
                host_copy.as_c_string_slice().unsafe_ptr(),
            )
            != 1
        ):
            raise Error("unable to set SNI hostname: ", openssl.last_error())

        while True:
            if _deadline_expired(deadline_ns):
                raise Error("TLS handshake timeout after ", timeout_ms, "ms")

            var connect_result = openssl.libssl.call[
                "SSL_connect", return_type=c_int
            ](ssl)
            if connect_result == 1:
                break

            var ssl_error = openssl.libssl.call[
                "SSL_get_error", return_type=c_int
            ](ssl, connect_result)
            if (
                ssl_error == _SSL_ERROR_WANT_READ
                or ssl_error == _SSL_ERROR_WANT_WRITE
            ):
                if _deadline_expired(deadline_ns):
                    raise Error(
                        "TLS handshake timeout after ", timeout_ms, "ms"
                    )
                continue

            raise Error(
                "TLS handshake failed: ssl_error=",
                ssl_error,
                ", openssl=",
                openssl.last_error(),
            )

        var verify_result = openssl.libssl.call[
            "SSL_get_verify_result", return_type=c_long
        ](ssl)
        if verify_result != _X509_V_OK:
            raise Error(
                "TLS certificate verification failed: code=", verify_result
            )

        _tls_send_all(openssl, ssl, payload, deadline_ns, timeout_ms)
        var raw_response = _tls_read_all(
            openssl,
            ssl,
            deadline_ns,
            timeout_ms,
            max_raw_response_bytes,
        )
        _tls_cleanup(openssl, ssl, ctx)
        return raw_response
    except:
        _tls_cleanup(openssl, ssl, ctx)
        raise


fn _header_value(headers: Dict[String, String], name: StringSlice) -> String:
    var wanted = String(name).lower()
    for header in headers.items():
        if header.key.lower() == wanted:
            return String(header.value)
    return String()


fn _retry_after_delay_ms(headers: Dict[String, String]) -> Int:
    var raw_value = _header_value(headers, "Retry-After").strip()
    if len(raw_value) == 0:
        return 0

    try:
        var seconds = Int(StringSlice(raw_value))
        if seconds <= 0:
            return 0

        var delay_ms = seconds * 1000
        if delay_ms > _MAX_RETRY_AFTER_MS:
            return _MAX_RETRY_AFTER_MS
        return delay_ms
    except:
        # Retry-After can also be an HTTP date; unsupported for now.
        return 0


fn _retry_delay_for_response_ms(
    status_code: Int,
    headers: Dict[String, String],
    retry_number: Int,
    retry_backoff_ms: Int,
    retry_max_backoff_ms: Int,
    jitter_source_ns: UInt = UInt(0),
) -> Int:
    var delay_ms = _retry_backoff_delay_ms(
        retry_number,
        retry_backoff_ms,
        retry_max_backoff_ms,
        jitter_source_ns=jitter_source_ns,
    )
    if status_code == 429 or status_code == 503:
        var retry_after = _retry_after_delay_ms(headers)
        if retry_after > delay_ms:
            delay_ms = retry_after

    return delay_ms


fn _parse_chunk_size_hex(size_text: StringSlice) raises -> Int:
    if len(size_text) == 0:
        raise Error("invalid chunked body: empty chunk size")

    var value = 0
    var bytes = String(size_text).as_bytes()
    for b in bytes:
        if b >= UInt8(ord("0")) and b <= UInt8(ord("9")):
            value = value * 16 + Int(b - UInt8(ord("0")))
        elif b >= UInt8(ord("a")) and b <= UInt8(ord("f")):
            value = value * 16 + 10 + Int(b - UInt8(ord("a")))
        elif b >= UInt8(ord("A")) and b <= UInt8(ord("F")):
            value = value * 16 + 10 + Int(b - UInt8(ord("A")))
        else:
            raise Error("invalid chunk size digit: ", String(b))

    return value


fn _decode_chunked_body(
    raw_body: String, max_body_bytes: Int = _DEFAULT_MAX_BODY_BYTES
) raises -> String:
    var cursor = 0
    var decoded = String()
    var decoded_bytes = 0

    while True:
        var line_end = raw_body.find("\r\n", start=cursor)
        if line_end == -1:
            raise Error("invalid chunked body: missing chunk-size terminator")

        var size_text = String(String(raw_body[cursor:line_end]).strip())
        var extension_index = size_text.find(";")
        if extension_index != -1:
            size_text = String(size_text[:extension_index].strip())

        var chunk_size = _parse_chunk_size_hex(size_text)
        cursor = line_end + 2

        if chunk_size == 0:
            return decoded

        if cursor + chunk_size > len(raw_body):
            raise Error("invalid chunked body: chunk exceeds available bytes")

        if decoded_bytes + chunk_size > max_body_bytes:
            raise Error("response body exceeds max_body_bytes")

        decoded += String(raw_body[cursor : cursor + chunk_size])
        decoded_bytes += chunk_size
        cursor += chunk_size

        if raw_body.find("\r\n", start=cursor) != cursor:
            raise Error("invalid chunked body: missing chunk terminator")
        cursor += 2


fn _zlib_inflate_with_window(
    compressed: String,
    window_bits: c_int,
    max_output_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) raises -> String:
    if len(compressed) == 0:
        return String()

    var zlib = _Zlib()
    var stream = _ZStream(
        next_in=UnsafePointer[NoneType, ImmutExternalOrigin](),
        avail_in=0,
        total_in=0,
        next_out=UnsafePointer[NoneType, MutExternalOrigin](),
        avail_out=0,
        total_out=0,
        msg=UnsafePointer[c_char, MutExternalOrigin](),
        state=OpaquePointer[MutExternalOrigin](),
        zalloc=OpaquePointer[MutExternalOrigin](),
        zfree=OpaquePointer[MutExternalOrigin](),
        opaque=OpaquePointer[MutExternalOrigin](),
        data_type=0,
        adler=0,
        reserved=0,
    )

    var compressed_bytes = compressed.as_bytes()
    stream.next_in = (
        compressed_bytes.unsafe_ptr()
        .bitcast[NoneType]()
        .unsafe_origin_cast[ImmutExternalOrigin]()
    )
    stream.avail_in = c_uint(len(compressed_bytes))

    var zlib_version = zlib.libz.call[
        "zlibVersion",
        return_type = UnsafePointer[c_char, MutExternalOrigin],
    ]()

    var init_rc = zlib.libz.call["inflateInit2_", return_type=c_int](
        Pointer(to=stream),
        window_bits,
        zlib_version,
        c_int(size_of[_ZStream]()),
    )
    if init_rc != _Z_OK:
        raise Error("inflateInit2_ failed: rc=", init_rc)

    var output = List[UInt8]()
    var out_chunk = InlineArray[UInt8, _READ_CHUNK_SIZE](fill=0)
    var output_bytes = 0
    try:
        while True:
            stream.next_out = (
                out_chunk.unsafe_ptr()
                .bitcast[NoneType]()
                .unsafe_origin_cast[MutExternalOrigin]()
            )
            stream.avail_out = c_uint(_READ_CHUNK_SIZE)

            var inflate_rc = zlib.libz.call["inflate", return_type=c_int](
                Pointer(to=stream), _Z_NO_FLUSH
            )
            var produced = _READ_CHUNK_SIZE - Int(stream.avail_out)
            if output_bytes + produced > max_output_bytes:
                raise Error("decoded body exceeds max_decompressed_bytes")
            for i in range(produced):
                output.append(out_chunk[i])
            output_bytes += produced

            if inflate_rc == _Z_STREAM_END:
                break
            if inflate_rc != _Z_OK:
                raise Error("inflate failed: rc=", inflate_rc)

            if stream.avail_in == 0 and produced == 0:
                raise Error(
                    "inflate failed: unexpected end of compressed stream"
                )
    except:
        _ = zlib.libz.call["inflateEnd", return_type=c_int](Pointer(to=stream))
        raise

    var end_rc = zlib.libz.call["inflateEnd", return_type=c_int](
        Pointer(to=stream)
    )
    if end_rc != _Z_OK:
        raise Error("inflateEnd failed: rc=", end_rc)

    if len(output) == 0:
        return String()
    return _string_from_bytes(output)


fn _decode_gzip_body(
    body: String,
    max_decompressed_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) raises -> String:
    return _zlib_inflate_with_window(
        body,
        _ZLIB_GZIP_WINDOW_BITS,
        max_output_bytes=max_decompressed_bytes,
    )


fn _decode_deflate_body(
    body: String,
    max_decompressed_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) raises -> String:
    try:
        return _zlib_inflate_with_window(
            body,
            _ZLIB_WINDOW_BITS,
            max_output_bytes=max_decompressed_bytes,
        )
    except e:
        if String(e).find("max_decompressed_bytes") != -1:
            raise
        return _zlib_inflate_with_window(
            body,
            _ZLIB_RAW_WINDOW_BITS,
            max_output_bytes=max_decompressed_bytes,
        )


fn _decode_content_encoding(
    body: String,
    encoding_header: StringSlice,
    max_decompressed_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) raises -> String:
    var decoded = String(body)
    var encodings = encoding_header.split(",")
    var idx = len(encodings)

    while idx > 0:
        idx -= 1
        var encoding = String(encodings[idx].strip()).lower()
        if len(encoding) == 0 or encoding == "identity":
            continue
        if encoding == "gzip":
            decoded = _decode_gzip_body(
                decoded, max_decompressed_bytes=max_decompressed_bytes
            )
            continue
        if encoding == "deflate":
            decoded = _decode_deflate_body(
                decoded, max_decompressed_bytes=max_decompressed_bytes
            )
            continue

        raise Error("unsupported Content-Encoding: ", encoding)

    return decoded


fn _parse_response(
    raw_response: String,
    max_header_bytes: Int = _DEFAULT_MAX_HEADER_BYTES,
    max_body_bytes: Int = _DEFAULT_MAX_BODY_BYTES,
    max_decompressed_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) raises -> Response:
    if max_header_bytes <= 0:
        raise Error("max_header_bytes must be > 0")
    if max_body_bytes <= 0:
        raise Error("max_body_bytes must be > 0")
    if max_decompressed_bytes <= 0:
        raise Error("max_decompressed_bytes must be > 0")

    if len(raw_response) == 0:
        raise Error("response is empty")

    var search_start = 0
    var body_start: Int
    var status_code: Int
    var headers: Dict[String, String]

    while True:
        var separator_index = raw_response.find("\r\n\r\n", start=search_start)
        if separator_index == -1:
            if len(raw_response) > max_header_bytes:
                raise Error("response headers exceed max_header_bytes")
            raise Error("response is missing header terminator")

        var header_block_bytes = separator_index - search_start + 4
        if header_block_bytes > max_header_bytes:
            raise Error("response headers exceed max_header_bytes")

        var raw_headers = raw_response[search_start:separator_index]
        var lines = raw_headers.split("\r\n")
        if len(lines) == 0 or len(lines[0]) == 0:
            raise Error("response is empty")

        var status_parts = lines[0].split(" ", maxsplit=2)
        if len(status_parts) < 2:
            raise Error("invalid HTTP status line: ", lines[0])

        var parsed_headers = Dict[String, String]()
        for i in range(1, len(lines)):
            var line = lines[i]
            if len(line) == 0:
                continue
            var colon_index = line.find(":")
            if colon_index <= 0:
                continue

            var key = String(line[:colon_index].strip())
            var value = String(line[colon_index + 1 :].strip())
            parsed_headers[key] = value

        status_code = Int(StringSlice(status_parts[1]))
        headers = parsed_headers^
        body_start = separator_index + 4

        if status_code >= 100 and status_code < 200 and status_code != 101:
            search_start = body_start
            if search_start >= len(raw_response):
                raise Error("response ended after interim status")
            continue
        break

    var body = String(raw_response[body_start:])
    var transfer_encoding = _header_value(headers, "Transfer-Encoding").lower()

    if transfer_encoding.find("chunked") != -1:
        body = _decode_chunked_body(body, max_body_bytes=max_body_bytes)
    else:
        var content_length_value = _header_value(
            headers, "Content-Length"
        ).strip()
        if len(content_length_value) > 0:
            var content_length = Int(StringSlice(content_length_value))
            if content_length < 0:
                raise Error("invalid Content-Length: ", content_length_value)
            if content_length > max_body_bytes:
                raise Error("response body exceeds max_body_bytes")
            if body.byte_length() < content_length:
                raise Error("response body shorter than Content-Length")
            body = String(body[:content_length])

        if body.byte_length() > max_body_bytes:
            raise Error("response body exceeds max_body_bytes")

    var content_encoding = _header_value(headers, "Content-Encoding").strip()
    if len(content_encoding) > 0:
        body = _decode_content_encoding(
            body,
            content_encoding,
            max_decompressed_bytes=max_decompressed_bytes,
        )

    if body.byte_length() > max_decompressed_bytes:
        raise Error("response body exceeds max_decompressed_bytes")

    return Response(status_code=status_code, headers=headers^, body=body)


fn _request_once(
    req: Request,
    parsed: _ParsedURL,
    deadline_ns: UInt,
    timeout_ms: Int,
    max_header_bytes: Int,
    max_body_bytes: Int,
    max_decompressed_bytes: Int,
) raises -> Response:
    var payload = _build_request_payload(req, parsed)
    var max_raw_response_bytes = _max_raw_response_bytes(
        max_header_bytes, max_body_bytes
    )
    var fd = _open_socket(parsed, timeout_ms)
    try:
        if parsed.use_tls:
            var raw_tls = _request_tls(
                fd,
                parsed.host,
                payload,
                deadline_ns,
                timeout_ms,
                max_raw_response_bytes,
            )
            _ = close(fd)
            return _parse_response(
                raw_tls,
                max_header_bytes=max_header_bytes,
                max_body_bytes=max_body_bytes,
                max_decompressed_bytes=max_decompressed_bytes,
            )

        _send_all(fd, payload, deadline_ns, timeout_ms)
        var raw_http = _read_all(
            fd,
            deadline_ns,
            timeout_ms,
            max_raw_response_bytes=max_raw_response_bytes,
        )
        _ = close(fd)
        return _parse_response(
            raw_http,
            max_header_bytes=max_header_bytes,
            max_body_bytes=max_body_bytes,
            max_decompressed_bytes=max_decompressed_bytes,
        )
    except:
        _ = close(fd)
        raise


fn _request_with_redirects(
    req: Request,
    deadline_ns: UInt,
    timeout_ms: Int,
    max_redirects: Int,
    max_header_bytes: Int,
    max_body_bytes: Int,
    max_decompressed_bytes: Int,
) raises -> Response:
    var current_req = Request(
        method=String(req.method),
        url=String(req.url),
        headers=req.headers.copy(),
        body=String(req.body),
    )
    var redirects_followed = 0

    while True:
        var parsed = _parse_url(current_req.url)
        var response = _request_once(
            current_req,
            parsed,
            deadline_ns,
            timeout_ms,
            max_header_bytes=max_header_bytes,
            max_body_bytes=max_body_bytes,
            max_decompressed_bytes=max_decompressed_bytes,
        )

        if not _is_redirect_status(response.status_code):
            return Response(
                status_code=response.status_code,
                headers=response.headers.copy(),
                body=String(response.body),
            )

        var location = _header_value(response.headers, "Location").strip()
        if len(location) == 0:
            return Response(
                status_code=response.status_code,
                headers=response.headers.copy(),
                body=String(response.body),
            )

        if redirects_followed >= max_redirects:
            raise Error("too many redirects (max=", max_redirects, ")")

        var next_url = _resolve_redirect_url(parsed, location)
        var next_parsed = _parse_url(next_url)
        var current_method = String(current_req.method).upper()
        var next_method = _redirect_method(response.status_code, current_method)
        var drop_body_headers = next_method != current_method
        var next_body = String(current_req.body)
        if drop_body_headers:
            next_body = ""

        var next_headers = _prepare_redirect_headers(
            current_req.headers,
            drop_body_headers,
            parsed.host_header,
            next_parsed.host_header,
        )

        current_req = Request(
            method=next_method,
            url=next_url,
            headers=next_headers^,
            body=next_body,
        )
        redirects_followed += 1


fn request(
    req: Request,
    timeout_ms: Int = _DEFAULT_TIMEOUT_MS,
    max_redirects: Int = _DEFAULT_MAX_REDIRECTS,
    max_retries: Int = _DEFAULT_MAX_RETRIES,
    retry_backoff_ms: Int = _DEFAULT_RETRY_BACKOFF_MS,
    retry_max_backoff_ms: Int = _DEFAULT_RETRY_MAX_BACKOFF_MS,
    retry_non_idempotent: Bool = False,
    max_header_bytes: Int = _DEFAULT_MAX_HEADER_BYTES,
    max_body_bytes: Int = _DEFAULT_MAX_BODY_BYTES,
    max_decompressed_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) raises -> Response:
    if max_redirects < 0:
        raise Error("max_redirects must be >= 0")
    if max_retries < 0:
        raise Error("max_retries must be >= 0")
    if retry_backoff_ms < 0:
        raise Error("retry_backoff_ms must be >= 0")
    if retry_max_backoff_ms < 0:
        raise Error("retry_max_backoff_ms must be >= 0")
    if retry_backoff_ms > retry_max_backoff_ms:
        raise Error("retry_backoff_ms must be <= retry_max_backoff_ms")
    if max_header_bytes <= 0:
        raise Error("max_header_bytes must be > 0")
    if max_body_bytes <= 0:
        raise Error("max_body_bytes must be > 0")
    if max_decompressed_bytes <= 0:
        raise Error("max_decompressed_bytes must be > 0")

    var deadline_ns = _deadline_from_timeout_ms(timeout_ms)
    var retries_used = 0
    var retry_allowed = retry_non_idempotent or _is_idempotent_method(
        req.method
    )

    while True:
        try:
            var response = _request_with_redirects(
                req,
                deadline_ns,
                timeout_ms,
                max_redirects,
                max_header_bytes=max_header_bytes,
                max_body_bytes=max_body_bytes,
                max_decompressed_bytes=max_decompressed_bytes,
            )
            if (
                retry_allowed
                and retries_used < max_retries
                and _is_retryable_status_code(response.status_code)
            ):
                var retry_delay_ms = _retry_delay_for_response_ms(
                    response.status_code,
                    response.headers,
                    retries_used,
                    retry_backoff_ms,
                    retry_max_backoff_ms,
                )
                retries_used += 1
                _wait_before_retry(retry_delay_ms, deadline_ns, timeout_ms)
                continue

            return Response(
                status_code=response.status_code,
                headers=response.headers.copy(),
                body=String(response.body),
            )
        except e:
            var message = String(e)
            var kind = _classify_request_error_kind(message)
            if (
                not retry_allowed
                or retries_used >= max_retries
                or not _is_retryable_error_kind(kind)
            ):
                raise

            var retry_delay_ms = _retry_backoff_delay_ms(
                retries_used,
                retry_backoff_ms,
                retry_max_backoff_ms,
            )
            retries_used += 1
            _wait_before_retry(retry_delay_ms, deadline_ns, timeout_ms)


fn request_safe(
    req: Request,
    timeout_ms: Int = _DEFAULT_TIMEOUT_MS,
    max_redirects: Int = _DEFAULT_MAX_REDIRECTS,
    max_retries: Int = _DEFAULT_MAX_RETRIES,
    retry_backoff_ms: Int = _DEFAULT_RETRY_BACKOFF_MS,
    retry_max_backoff_ms: Int = _DEFAULT_RETRY_MAX_BACKOFF_MS,
    retry_non_idempotent: Bool = False,
    max_header_bytes: Int = _DEFAULT_MAX_HEADER_BYTES,
    max_body_bytes: Int = _DEFAULT_MAX_BODY_BYTES,
    max_decompressed_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) -> RequestResult:
    try:
        return _result_from_response(
            request(
                req,
                timeout_ms=timeout_ms,
                max_redirects=max_redirects,
                max_retries=max_retries,
                retry_backoff_ms=retry_backoff_ms,
                retry_max_backoff_ms=retry_max_backoff_ms,
                retry_non_idempotent=retry_non_idempotent,
                max_header_bytes=max_header_bytes,
                max_body_bytes=max_body_bytes,
                max_decompressed_bytes=max_decompressed_bytes,
            )
        )
    except e:
        var message = String(e)
        var kind = _classify_request_error_kind(message)
        return RequestResult(
            ok=False,
            status_code=0,
            headers=Dict[String, String](),
            body="",
            error_kind=kind,
            error_message=message,
            error_retryable=_is_retryable_error_kind(kind),
        )


fn get(
    url: String,
    timeout_ms: Int = _DEFAULT_TIMEOUT_MS,
    max_redirects: Int = _DEFAULT_MAX_REDIRECTS,
    max_retries: Int = _DEFAULT_MAX_RETRIES,
    retry_backoff_ms: Int = _DEFAULT_RETRY_BACKOFF_MS,
    retry_max_backoff_ms: Int = _DEFAULT_RETRY_MAX_BACKOFF_MS,
    max_header_bytes: Int = _DEFAULT_MAX_HEADER_BYTES,
    max_body_bytes: Int = _DEFAULT_MAX_BODY_BYTES,
    max_decompressed_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) raises -> Response:
    return request(
        Request(
            method="GET",
            url=url,
            headers=Dict[String, String](),
            body="",
        ),
        timeout_ms=timeout_ms,
        max_redirects=max_redirects,
        max_retries=max_retries,
        retry_backoff_ms=retry_backoff_ms,
        retry_max_backoff_ms=retry_max_backoff_ms,
        max_header_bytes=max_header_bytes,
        max_body_bytes=max_body_bytes,
        max_decompressed_bytes=max_decompressed_bytes,
    )


fn get_safe(
    url: String,
    timeout_ms: Int = _DEFAULT_TIMEOUT_MS,
    max_redirects: Int = _DEFAULT_MAX_REDIRECTS,
    max_retries: Int = _DEFAULT_MAX_RETRIES,
    retry_backoff_ms: Int = _DEFAULT_RETRY_BACKOFF_MS,
    retry_max_backoff_ms: Int = _DEFAULT_RETRY_MAX_BACKOFF_MS,
    max_header_bytes: Int = _DEFAULT_MAX_HEADER_BYTES,
    max_body_bytes: Int = _DEFAULT_MAX_BODY_BYTES,
    max_decompressed_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) -> RequestResult:
    return request_safe(
        Request(
            method="GET",
            url=url,
            headers=Dict[String, String](),
            body="",
        ),
        timeout_ms=timeout_ms,
        max_redirects=max_redirects,
        max_retries=max_retries,
        retry_backoff_ms=retry_backoff_ms,
        retry_max_backoff_ms=retry_max_backoff_ms,
        max_header_bytes=max_header_bytes,
        max_body_bytes=max_body_bytes,
        max_decompressed_bytes=max_decompressed_bytes,
    )


fn post(
    url: String,
    body: String = "",
    timeout_ms: Int = _DEFAULT_TIMEOUT_MS,
    max_redirects: Int = _DEFAULT_MAX_REDIRECTS,
    max_retries: Int = _DEFAULT_MAX_RETRIES,
    retry_backoff_ms: Int = _DEFAULT_RETRY_BACKOFF_MS,
    retry_max_backoff_ms: Int = _DEFAULT_RETRY_MAX_BACKOFF_MS,
    retry_non_idempotent: Bool = False,
    max_header_bytes: Int = _DEFAULT_MAX_HEADER_BYTES,
    max_body_bytes: Int = _DEFAULT_MAX_BODY_BYTES,
    max_decompressed_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) raises -> Response:
    return request(
        Request(
            method="POST",
            url=url,
            headers=Dict[String, String](),
            body=body,
        ),
        timeout_ms=timeout_ms,
        max_redirects=max_redirects,
        max_retries=max_retries,
        retry_backoff_ms=retry_backoff_ms,
        retry_max_backoff_ms=retry_max_backoff_ms,
        retry_non_idempotent=retry_non_idempotent,
        max_header_bytes=max_header_bytes,
        max_body_bytes=max_body_bytes,
        max_decompressed_bytes=max_decompressed_bytes,
    )


fn post_safe(
    url: String,
    body: String = "",
    timeout_ms: Int = _DEFAULT_TIMEOUT_MS,
    max_redirects: Int = _DEFAULT_MAX_REDIRECTS,
    max_retries: Int = _DEFAULT_MAX_RETRIES,
    retry_backoff_ms: Int = _DEFAULT_RETRY_BACKOFF_MS,
    retry_max_backoff_ms: Int = _DEFAULT_RETRY_MAX_BACKOFF_MS,
    retry_non_idempotent: Bool = False,
    max_header_bytes: Int = _DEFAULT_MAX_HEADER_BYTES,
    max_body_bytes: Int = _DEFAULT_MAX_BODY_BYTES,
    max_decompressed_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) -> RequestResult:
    return request_safe(
        Request(
            method="POST",
            url=url,
            headers=Dict[String, String](),
            body=body,
        ),
        timeout_ms=timeout_ms,
        max_redirects=max_redirects,
        max_retries=max_retries,
        retry_backoff_ms=retry_backoff_ms,
        retry_max_backoff_ms=retry_max_backoff_ms,
        retry_non_idempotent=retry_non_idempotent,
        max_header_bytes=max_header_bytes,
        max_body_bytes=max_body_bytes,
        max_decompressed_bytes=max_decompressed_bytes,
    )


fn main():
    print("mojoreq sample:")

    try:
        var response = get(
            "http://example.com/",
            timeout_ms=10_000,
            max_redirects=5,
        )
        print("GET status:", response.status_code)
        print("GET body bytes:", response.body.byte_length())
        print("GET has Server header:", _has_header(response.headers, "Server"))
    except e:
        print("GET failed:", String(e))

    try:
        var post_response = post(
            "https://httpbin.org/post", '{"hello":"mojo"}', timeout_ms=10_000
        )
        print("POST status:", post_response.status_code)
    except e:
        print("POST failed:", String(e))

    var safe_result = get_safe("https://[::1]/", timeout_ms=3_000)
    if safe_result.ok:
        print("Safe request status:", safe_result.status_code)
    else:
        print("Safe request kind:", safe_result.error_kind)
        print("Safe request retryable:", safe_result.error_retryable)
        print("Safe request message:", safe_result.error_message)
