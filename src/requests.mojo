from std.collections import Dict, InlineArray, List
from std.ffi import (
    OwnedDLHandle,
    ErrNo,
    c_char,
    c_int,
    c_long,
    c_size_t,
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
comptime _BROTLI_DECODER_RESULT_ERROR = c_int(0)
comptime _BROTLI_DECODER_RESULT_SUCCESS = c_int(1)
comptime _BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT = c_int(2)
comptime _BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT = c_int(3)
comptime _SEC_PER_MINUTE = 60
comptime _SEC_PER_HOUR = 3_600
comptime _SEC_PER_DAY = 86_400


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
struct _HTTPReadResult:
    var raw_response: String
    var close_delimited: Bool


@fieldwise_init
struct _TLSConnectResult:
    var ctx: OpaquePointer[MutExternalOrigin]
    var ssl: OpaquePointer[MutExternalOrigin]


@fieldwise_init
struct _HTTPPoolTakeResult:
    var reused: Bool
    var fd: c_int
    var request_count: Int


@fieldwise_init
struct _TLSPoolTakeResult:
    var reused: Bool
    var key_match: Bool
    var fd: c_int
    var ctx: OpaquePointer[MutExternalOrigin]
    var ssl: OpaquePointer[MutExternalOrigin]
    var request_count: Int


struct Client(Movable):
    var enable_http_pool: Bool
    var enable_tls_pool: Bool
    var max_idle_connections: Int
    var idle_ttl_ms: Int
    var max_requests_per_connection: Int
    var http_pool: Dict[String, c_int]
    var http_pool_idle_since_ns: Dict[String, UInt]
    var http_pool_request_count: Dict[String, Int]
    var tls_runtime: _OpenSSL
    var tls_pool_active: Bool
    var tls_pool_key: String
    var tls_pool_fd: c_int
    var tls_pool_ctx: OpaquePointer[MutExternalOrigin]
    var tls_pool_ssl: OpaquePointer[MutExternalOrigin]
    var tls_pool_idle_since_ns: UInt
    var tls_pool_request_count: Int

    fn __init__(
        out self,
        enable_http_pool: Bool = True,
        enable_tls_pool: Bool = True,
        max_idle_connections: Int = 16,
        idle_ttl_ms: Int = 30_000,
        max_requests_per_connection: Int = 100,
    ) raises:
        if max_idle_connections < 0:
            raise Error("max_idle_connections must be >= 0")
        if idle_ttl_ms < 0:
            raise Error("idle_ttl_ms must be >= 0")
        if max_requests_per_connection <= 0:
            raise Error("max_requests_per_connection must be > 0")

        self.enable_http_pool = enable_http_pool
        self.enable_tls_pool = enable_tls_pool
        self.max_idle_connections = max_idle_connections
        self.idle_ttl_ms = idle_ttl_ms
        self.max_requests_per_connection = max_requests_per_connection
        self.http_pool = Dict[String, c_int]()
        self.http_pool_idle_since_ns = Dict[String, UInt]()
        self.http_pool_request_count = Dict[String, Int]()
        self.tls_runtime = _OpenSSL()
        self.tls_pool_active = False
        self.tls_pool_key = ""
        self.tls_pool_fd = c_int(-1)
        self.tls_pool_ctx = OpaquePointer[MutExternalOrigin]()
        self.tls_pool_ssl = OpaquePointer[MutExternalOrigin]()
        self.tls_pool_idle_since_ns = UInt(0)
        self.tls_pool_request_count = 0

    fn close(mut self):
        for entry in self.http_pool.items():
            if entry.value >= c_int(0):
                _ = close(entry.value)
        self.http_pool = Dict[String, c_int]()
        self.http_pool_idle_since_ns = Dict[String, UInt]()
        self.http_pool_request_count = Dict[String, Int]()
        _client_clear_tls_pool(self)

    fn request(
        mut self,
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
        return _client_request(
            self,
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

    fn request_safe(
        mut self,
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
        return _client_request_safe(
            self,
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

    fn get(
        mut self,
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
        return self.request(
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
        mut self,
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
        return self.request_safe(
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
        mut self,
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
        return self.request(
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
        mut self,
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
        return self.request_safe(
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


struct _Brotli(Movable):
    var libbrotli: OwnedDLHandle

    fn __init__(out self) raises:
        self.libbrotli = OwnedDLHandle(unsafe_uninitialized=True)

        var conda_prefix = getenv("CONDA_PREFIX")
        var brotli_fallback_v = String()
        var brotli_fallback_compat = String()
        if len(conda_prefix) > 0:
            comptime if CompilationTarget.is_macos():
                brotli_fallback_v = String(
                    conda_prefix, "/lib/libbrotlidec.1.dylib"
                )
                brotli_fallback_compat = String(
                    conda_prefix, "/lib/libbrotlidec.dylib"
                )
            elif CompilationTarget.is_linux():
                brotli_fallback_v = String(
                    conda_prefix, "/lib/libbrotlidec.so.1"
                )
                brotli_fallback_compat = String(
                    conda_prefix, "/lib/libbrotlidec.so"
                )

        comptime if CompilationTarget.is_macos():
            self.libbrotli = _open_shared_library(
                "libbrotlidec.1.dylib",
                "libbrotlidec.dylib",
                brotli_fallback_v,
                brotli_fallback_compat,
            )
        elif CompilationTarget.is_linux():
            self.libbrotli = _open_shared_library(
                "libbrotlidec.so.1",
                "libbrotlidec.so",
                brotli_fallback_v,
                brotli_fallback_compat,
            )
        else:
            CompilationTarget.unsupported_target_error[
                NoneType, operation="brotli decode setup"
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


fn _brotli_available() -> Bool:
    try:
        _ = _Brotli()
        return True
    except:
        return False


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
        or lower.find("brotli") != -1
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
    if _brotli_available():
        _set_default_header(headers, "Accept-Encoding", "gzip, deflate, br")
    else:
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


fn _request_header_connection_value(req: Request) -> String:
    return String(_header_value(req.headers, "Connection").lower().strip())


fn _request_connection_prefers_close(req: Request) -> Bool:
    return _request_header_connection_value(req).find("close") != -1


fn _response_connection_prefers_close(response: Response) -> Bool:
    return (
        _header_value(response.headers, "Connection").lower().find("close")
        != -1
    )


fn _pool_key(parsed: _ParsedURL) -> String:
    return String(parsed.host.lower(), ":", parsed.port)


fn _pool_idle_expired(idle_since_ns: UInt, idle_ttl_ms: Int) -> Bool:
    if idle_ttl_ms <= 0:
        return True
    if idle_since_ns == UInt(0):
        return True
    var now = perf_counter_ns()
    if now <= idle_since_ns:
        return False
    var ttl_ns = UInt(idle_ttl_ms) * UInt(_NSEC_PER_MSEC)
    return now - idle_since_ns >= ttl_ns


fn _client_idle_connection_count(client: Client) -> Int:
    var count = 0
    for item in client.http_pool.items():
        if item.value >= c_int(0):
            count += 1
    if client.tls_pool_active:
        count += 1
    return count


fn _evict_one_http_idle_connection(mut client: Client) -> Bool:
    for item in client.http_pool.items():
        if item.value >= c_int(0):
            _ = close(item.value)
            client.http_pool[item.key] = c_int(-1)
            client.http_pool_idle_since_ns[item.key] = UInt(0)
            client.http_pool_request_count[item.key] = 0
            return True
    return False


fn _evict_one_idle_connection(mut client: Client) -> Bool:
    if _evict_one_http_idle_connection(client):
        return True
    if client.tls_pool_active:
        _client_clear_tls_pool(client)
        return True
    return False


fn _pool_take_http_connection(
    mut client: Client, parsed: _ParsedURL
) -> _HTTPPoolTakeResult:
    var key = _pool_key(parsed)
    for item in client.http_pool.items():
        if item.key != key:
            continue

        var fd = item.value
        client.http_pool[item.key] = c_int(-1)

        var idle_since_ns = UInt(0)
        for idle in client.http_pool_idle_since_ns.items():
            if idle.key == key:
                idle_since_ns = idle.value
                client.http_pool_idle_since_ns[idle.key] = UInt(0)
                break

        var request_count = 0
        for count in client.http_pool_request_count.items():
            if count.key == key:
                request_count = count.value
                client.http_pool_request_count[count.key] = 0
                break

        if fd < c_int(0):
            return _HTTPPoolTakeResult(
                reused=False, fd=c_int(-1), request_count=0
            )
        if _pool_idle_expired(idle_since_ns, client.idle_ttl_ms):
            _ = close(fd)
            return _HTTPPoolTakeResult(
                reused=False, fd=c_int(-1), request_count=0
            )
        if request_count >= client.max_requests_per_connection:
            _ = close(fd)
            return _HTTPPoolTakeResult(
                reused=False, fd=c_int(-1), request_count=0
            )

        return _HTTPPoolTakeResult(
            reused=True,
            fd=fd,
            request_count=request_count,
        )

    return _HTTPPoolTakeResult(reused=False, fd=c_int(-1), request_count=0)


fn _pool_put_http_connection(
    mut client: Client, parsed: _ParsedURL, fd: c_int, request_count: Int
):
    if fd < c_int(0):
        return
    if request_count >= client.max_requests_per_connection:
        _ = close(fd)
        return
    if client.max_idle_connections == 0:
        _ = close(fd)
        return

    var key = _pool_key(parsed)
    var key_has_active = False
    for item in client.http_pool.items():
        if item.key == key and item.value >= c_int(0):
            key_has_active = True
            break

    if not key_has_active:
        while (
            _client_idle_connection_count(client) >= client.max_idle_connections
        ):
            if not _evict_one_idle_connection(client):
                break

    if (
        not key_has_active
        and _client_idle_connection_count(client) >= client.max_idle_connections
    ):
        _ = close(fd)
        return

    var idle_since_ns = perf_counter_ns()
    for item in client.http_pool.items():
        if item.key == key:
            if item.value >= c_int(0):
                _ = close(item.value)
            client.http_pool[item.key] = fd
            client.http_pool_idle_since_ns[item.key] = idle_since_ns
            client.http_pool_request_count[item.key] = request_count
            return

    client.http_pool[key] = fd
    client.http_pool_idle_since_ns[key] = idle_since_ns
    client.http_pool_request_count[key] = request_count


fn _client_clear_tls_pool(mut client: Client):
    if client.tls_pool_ssl or client.tls_pool_ctx:
        _tls_cleanup(
            client.tls_runtime, client.tls_pool_ssl, client.tls_pool_ctx
        )
    if client.tls_pool_fd >= c_int(0):
        _ = close(client.tls_pool_fd)
    client.tls_pool_active = False
    client.tls_pool_key = ""
    client.tls_pool_fd = c_int(-1)
    client.tls_pool_ctx = OpaquePointer[MutExternalOrigin]()
    client.tls_pool_ssl = OpaquePointer[MutExternalOrigin]()
    client.tls_pool_idle_since_ns = UInt(0)
    client.tls_pool_request_count = 0


fn _pool_take_tls_connection(
    mut client: Client, parsed: _ParsedURL
) -> _TLSPoolTakeResult:
    var key = _pool_key(parsed)
    if not client.tls_pool_active:
        return _TLSPoolTakeResult(
            reused=False,
            key_match=False,
            fd=c_int(-1),
            ctx=OpaquePointer[MutExternalOrigin](),
            ssl=OpaquePointer[MutExternalOrigin](),
            request_count=0,
        )

    if client.tls_pool_key != key:
        _client_clear_tls_pool(client)
        return _TLSPoolTakeResult(
            reused=False,
            key_match=False,
            fd=c_int(-1),
            ctx=OpaquePointer[MutExternalOrigin](),
            ssl=OpaquePointer[MutExternalOrigin](),
            request_count=0,
        )

    if _pool_idle_expired(client.tls_pool_idle_since_ns, client.idle_ttl_ms):
        _client_clear_tls_pool(client)
        return _TLSPoolTakeResult(
            reused=False,
            key_match=True,
            fd=c_int(-1),
            ctx=OpaquePointer[MutExternalOrigin](),
            ssl=OpaquePointer[MutExternalOrigin](),
            request_count=0,
        )
    if client.tls_pool_request_count >= client.max_requests_per_connection:
        _client_clear_tls_pool(client)
        return _TLSPoolTakeResult(
            reused=False,
            key_match=True,
            fd=c_int(-1),
            ctx=OpaquePointer[MutExternalOrigin](),
            ssl=OpaquePointer[MutExternalOrigin](),
            request_count=0,
        )

    var fd = client.tls_pool_fd
    var ctx = client.tls_pool_ctx
    var ssl = client.tls_pool_ssl
    var request_count = client.tls_pool_request_count
    client.tls_pool_active = False
    client.tls_pool_key = ""
    client.tls_pool_fd = c_int(-1)
    client.tls_pool_ctx = OpaquePointer[MutExternalOrigin]()
    client.tls_pool_ssl = OpaquePointer[MutExternalOrigin]()
    client.tls_pool_idle_since_ns = UInt(0)
    client.tls_pool_request_count = 0
    return _TLSPoolTakeResult(
        reused=(fd >= c_int(0) and ctx and ssl),
        key_match=True,
        fd=fd,
        ctx=ctx,
        ssl=ssl,
        request_count=request_count,
    )


fn _pool_put_tls_connection(
    mut client: Client,
    parsed: _ParsedURL,
    fd: c_int,
    ctx: OpaquePointer[MutExternalOrigin],
    ssl: OpaquePointer[MutExternalOrigin],
    request_count: Int,
):
    if fd < c_int(0) or not ctx or not ssl:
        if ssl or ctx:
            _tls_cleanup(client.tls_runtime, ssl, ctx)
        if fd >= c_int(0):
            _ = close(fd)
        return

    if request_count >= client.max_requests_per_connection:
        _tls_cleanup(client.tls_runtime, ssl, ctx)
        _ = close(fd)
        return
    if client.max_idle_connections == 0:
        _tls_cleanup(client.tls_runtime, ssl, ctx)
        _ = close(fd)
        return

    var key = _pool_key(parsed)
    var replacing_existing = (
        client.tls_pool_active and client.tls_pool_key == key
    )
    if not replacing_existing:
        while (
            _client_idle_connection_count(client) >= client.max_idle_connections
        ):
            if not _evict_one_idle_connection(client):
                break

    if (
        not replacing_existing
        and _client_idle_connection_count(client) >= client.max_idle_connections
    ):
        _tls_cleanup(client.tls_runtime, ssl, ctx)
        _ = close(fd)
        return

    if client.tls_pool_active:
        _client_clear_tls_pool(client)

    client.tls_pool_active = True
    client.tls_pool_key = key
    client.tls_pool_fd = fd
    client.tls_pool_ctx = ctx
    client.tls_pool_ssl = ssl
    client.tls_pool_idle_since_ns = perf_counter_ns()
    client.tls_pool_request_count = request_count


fn _is_stale_reused_connection_error(message: StringSlice) -> Bool:
    var lower = String(message).lower()
    return (
        lower.find("send() failed") != -1
        or lower.find("send() returned 0") != -1
        or lower.find("read() failed") != -1
        or lower.find("ssl_write failed") != -1
        or lower.find("ssl_read failed") != -1
        or lower.find("response is empty") != -1
        or lower.find("response is missing header terminator") != -1
    )


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


fn _http_message_expected_total_bytes(raw_response: String) raises -> Int:
    if len(raw_response) == 0:
        return -1

    var search_start = 0
    while True:
        var separator_index = raw_response.find("\r\n\r\n", start=search_start)
        if separator_index == -1:
            return -1

        var raw_headers = raw_response[search_start:separator_index]
        var lines = raw_headers.split("\r\n")
        if len(lines) == 0 or len(lines[0]) == 0:
            raise Error("response is empty")

        var status_parts = lines[0].split(" ", maxsplit=2)
        if len(status_parts) < 2:
            raise Error("invalid HTTP status line: ", lines[0])

        var status_code = Int(StringSlice(status_parts[1]))
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

        var headers = parsed_headers^
        var body_start = separator_index + 4
        if status_code >= 100 and status_code < 200 and status_code != 101:
            search_start = body_start
            continue

        if status_code == 101 or status_code == 204 or status_code == 304:
            return body_start

        var transfer_encoding = _header_value(
            headers, "Transfer-Encoding"
        ).lower()
        if transfer_encoding.find("chunked") != -1:
            var cursor = body_start
            while True:
                var line_end = raw_response.find("\r\n", start=cursor)
                if line_end == -1:
                    return -1

                var size_text = String(
                    String(raw_response[cursor:line_end]).strip()
                )
                var extension_index = size_text.find(";")
                if extension_index != -1:
                    size_text = String(size_text[:extension_index].strip())

                var chunk_size = _parse_chunk_size_hex(size_text)
                cursor = line_end + 2
                if chunk_size == 0:
                    while True:
                        var trailer_end = raw_response.find(
                            "\r\n", start=cursor
                        )
                        if trailer_end == -1:
                            return -1
                        if trailer_end == cursor:
                            return trailer_end + 2
                        cursor = trailer_end + 2

                if cursor + chunk_size > len(raw_response):
                    return -1

                cursor += chunk_size
                if raw_response.find("\r\n", start=cursor) != cursor:
                    if cursor + 2 > len(raw_response):
                        return -1
                    raise Error(
                        "invalid chunked body: missing chunk terminator"
                    )
                cursor += 2

        var content_length_value = _header_value(
            headers, "Content-Length"
        ).strip()
        if len(content_length_value) > 0:
            var content_length = Int(StringSlice(content_length_value))
            if content_length < 0:
                raise Error("invalid Content-Length: ", content_length_value)
            if len(raw_response) < body_start + content_length:
                return -1
            return body_start + content_length

        # close-delimited body framing (cannot reuse connection safely).
        return -2


fn _read_http_response_for_keepalive(
    fd: c_int,
    deadline_ns: UInt,
    timeout_ms: Int,
    max_raw_response_bytes: Int,
) raises -> _HTTPReadResult:
    var data = List[UInt8]()
    var chunk = InlineArray[UInt8, _READ_CHUNK_SIZE](fill=0)
    var close_delimited = False

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
            return _HTTPReadResult(
                raw_response=_string_from_bytes(data),
                close_delimited=close_delimited,
            )

        if len(data) + Int(read_count) > max_raw_response_bytes:
            raise Error("response exceeds max_raw_response_bytes")

        for i in range(read_count):
            data.append(chunk[i])

        var current = _string_from_bytes(data)
        var expected_total = _http_message_expected_total_bytes(current)
        if expected_total == -1:
            continue
        if expected_total == -2:
            close_delimited = True
            continue

        var raw_response = current
        if len(raw_response) > expected_total:
            raw_response = String(raw_response[:expected_total])

        return _HTTPReadResult(
            raw_response=raw_response,
            close_delimited=close_delimited,
        )


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


fn _tls_read_http_response_for_keepalive(
    ref openssl: _OpenSSL,
    ssl: OpaquePointer[MutExternalOrigin],
    deadline_ns: UInt,
    timeout_ms: Int,
    max_raw_response_bytes: Int,
) raises -> _HTTPReadResult:
    var data = List[UInt8]()
    var chunk = InlineArray[UInt8, _READ_CHUNK_SIZE](fill=0)
    var close_delimited = False

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

            var current = _string_from_bytes(data)
            var expected_total = _http_message_expected_total_bytes(current)
            if expected_total == -1:
                continue
            if expected_total == -2:
                close_delimited = True
                continue

            var raw_response = current
            if len(raw_response) > expected_total:
                raw_response = String(raw_response[:expected_total])
            return _HTTPReadResult(
                raw_response=raw_response,
                close_delimited=close_delimited,
            )

        var ssl_error = openssl.libssl.call["SSL_get_error", return_type=c_int](
            ssl, read_count
        )
        if ssl_error == _SSL_ERROR_ZERO_RETURN or read_count == 0:
            return _HTTPReadResult(
                raw_response=_string_from_bytes(data),
                close_delimited=close_delimited,
            )
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


fn _tls_connect(
    ref openssl: _OpenSSL,
    fd: c_int,
    host: String,
    deadline_ns: UInt,
    timeout_ms: Int,
) raises -> _TLSConnectResult:
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

        return _TLSConnectResult(ctx=ctx, ssl=ssl)
    except:
        _tls_cleanup(openssl, ssl, ctx)
        raise


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

    try:
        var connected = _tls_connect(openssl, fd, host, deadline_ns, timeout_ms)
        ctx = connected.ctx
        ssl = connected.ssl
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


fn _epoch_now_seconds() -> Int:
    return Int(
        external_call["time", c_long](
            UnsafePointer[c_long, MutExternalOrigin]()
        )
    )


fn _is_leap_year(year: Int) -> Bool:
    if year % 4 != 0:
        return False
    if year % 100 != 0:
        return True
    return year % 400 == 0


fn _days_in_month(year: Int, month: Int) -> Int:
    if month == 1 or month == 3 or month == 5 or month == 7:
        return 31
    if month == 8 or month == 10 or month == 12:
        return 31
    if month == 4 or month == 6 or month == 9 or month == 11:
        return 30
    if month == 2:
        return 29 if _is_leap_year(year) else 28
    return 0


fn _month_from_abbrev(text: StringSlice) raises -> Int:
    if text == "Jan":
        return 1
    if text == "Feb":
        return 2
    if text == "Mar":
        return 3
    if text == "Apr":
        return 4
    if text == "May":
        return 5
    if text == "Jun":
        return 6
    if text == "Jul":
        return 7
    if text == "Aug":
        return 8
    if text == "Sep":
        return 9
    if text == "Oct":
        return 10
    if text == "Nov":
        return 11
    if text == "Dec":
        return 12
    raise Error("invalid month in Retry-After HTTP-date: ", text)


fn _parse_retry_after_decimal(
    text: StringSlice, start: Int, width: Int, field: StringSlice
) raises -> Int:
    if start < 0 or width <= 0:
        raise Error("invalid Retry-After HTTP-date field: ", field)

    var raw = String(text)
    if start + width > len(raw):
        raise Error("invalid Retry-After HTTP-date field length: ", field)

    var bytes = raw.as_bytes()
    var value = 0
    for i in range(width):
        var b = bytes[start + i]
        if b < UInt8(ord("0")) or b > UInt8(ord("9")):
            raise Error("invalid Retry-After HTTP-date field: ", field)
        value = value * 10 + Int(b - UInt8(ord("0")))

    return value


fn _days_before_year(year: Int) -> Int:
    var days = 0
    if year >= 1970:
        for y in range(1970, year):
            days += 366 if _is_leap_year(y) else 365
        return days

    for y in range(year, 1970):
        days -= 366 if _is_leap_year(y) else 365
    return days


fn _days_before_month(year: Int, month: Int) -> Int:
    var days = 0
    for m in range(1, month):
        days += _days_in_month(year, m)
    return days


fn _retry_after_http_date_to_epoch_seconds(value: StringSlice) raises -> Int:
    var text = String(value).strip()
    if len(text) != 29:
        raise Error("invalid Retry-After HTTP-date length")
    var bytes = text.as_bytes()
    if bytes[3] != UInt8(ord(",")) or bytes[4] != UInt8(ord(" ")):
        raise Error("invalid Retry-After HTTP-date prefix")
    if (
        bytes[7] != UInt8(ord(" "))
        or bytes[11] != UInt8(ord(" "))
        or bytes[16] != UInt8(ord(" "))
    ):
        raise Error("invalid Retry-After HTTP-date separators")
    if (
        bytes[19] != UInt8(ord(":"))
        or bytes[22] != UInt8(ord(":"))
        or bytes[25] != UInt8(ord(" "))
    ):
        raise Error("invalid Retry-After HTTP-date time separators")
    if String(text[26:]) != "GMT":
        raise Error("Retry-After HTTP-date must use GMT")

    var day = _parse_retry_after_decimal(text, 5, 2, "day")
    var month = _month_from_abbrev(text[8:11])
    var year = _parse_retry_after_decimal(text, 12, 4, "year")
    var hour = _parse_retry_after_decimal(text, 17, 2, "hour")
    var minute = _parse_retry_after_decimal(text, 20, 2, "minute")
    var second = _parse_retry_after_decimal(text, 23, 2, "second")

    if year < 1970:
        raise Error("Retry-After HTTP-date year before 1970 is unsupported")
    if hour > 23 or minute > 59 or second > 59:
        raise Error("invalid time in Retry-After HTTP-date")

    var month_days = _days_in_month(year, month)
    if day <= 0 or day > month_days:
        raise Error("invalid day in Retry-After HTTP-date")

    var days = (
        _days_before_year(year) + _days_before_month(year, month) + (day - 1)
    )
    return (
        days * _SEC_PER_DAY
        + hour * _SEC_PER_HOUR
        + minute * _SEC_PER_MINUTE
        + second
    )


fn _clamp_retry_after_delay_ms(delay_ms: Int) -> Int:
    if delay_ms <= 0:
        return 0
    if delay_ms > _MAX_RETRY_AFTER_MS:
        return _MAX_RETRY_AFTER_MS
    return delay_ms


fn _retry_after_delay_ms_with_now(
    headers: Dict[String, String], now_epoch_seconds: Int
) -> Int:
    var raw_value = _header_value(headers, "Retry-After").strip()
    if len(raw_value) == 0:
        return 0

    try:
        var seconds = Int(StringSlice(raw_value))
        return _clamp_retry_after_delay_ms(seconds * 1000)
    except:
        pass

    try:
        var retry_after_epoch = _retry_after_http_date_to_epoch_seconds(
            raw_value
        )
        var delta_seconds = retry_after_epoch - now_epoch_seconds
        return _clamp_retry_after_delay_ms(delta_seconds * 1000)
    except:
        return 0


fn _retry_after_delay_ms(headers: Dict[String, String]) -> Int:
    return _retry_after_delay_ms_with_now(headers, _epoch_now_seconds())


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


fn _decode_brotli_body(
    body: String,
    max_decompressed_bytes: Int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) raises -> String:
    if len(body) == 0:
        return String()

    var brotli = _Brotli()
    var input_bytes = body.as_bytes()
    var output_capacity = max(256, len(input_bytes))
    if output_capacity > max_decompressed_bytes:
        output_capacity = max_decompressed_bytes

    while True:
        if output_capacity <= 0:
            raise Error("decoded body exceeds max_decompressed_bytes")

        var output = List[UInt8]()
        for _ in range(output_capacity):
            output.append(UInt8(0))

        var decoded_size = c_size_t(output_capacity)
        var rc = brotli.libbrotli.call[
            "BrotliDecoderDecompress", return_type=c_int
        ](
            c_size_t(len(input_bytes)),
            input_bytes.unsafe_ptr(),
            Pointer(to=decoded_size),
            output.unsafe_ptr(),
        )

        if rc == _BROTLI_DECODER_RESULT_SUCCESS:
            var produced = Int(decoded_size)
            var decoded = String(capacity=produced)
            for i in range(produced):
                decoded._unsafe_append_byte(output[i])
            return decoded^

        if rc == _BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT:
            if output_capacity >= max_decompressed_bytes:
                raise Error("decoded body exceeds max_decompressed_bytes")

            var next_capacity = output_capacity * 2
            if next_capacity < output_capacity:
                output_capacity = max_decompressed_bytes
            elif next_capacity > max_decompressed_bytes:
                output_capacity = max_decompressed_bytes
            else:
                output_capacity = next_capacity
            continue

        if rc == _BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT:
            raise Error("brotli decode failed: truncated input")

        raise Error("brotli decode failed: rc=", rc)


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
        if encoding == "br":
            decoded = _decode_brotli_body(
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


fn _request_once_with_client(
    mut client: Client,
    req: Request,
    parsed: _ParsedURL,
    deadline_ns: UInt,
    timeout_ms: Int,
    max_header_bytes: Int,
    max_body_bytes: Int,
    max_decompressed_bytes: Int,
) raises -> Response:
    var wire_req = Request(
        method=String(req.method),
        url=String(req.url),
        headers=req.headers.copy(),
        body=String(req.body),
    )
    if (
        (client.enable_http_pool and not parsed.use_tls)
        or (client.enable_tls_pool and parsed.use_tls)
    ) and not _has_header(wire_req.headers, "Connection"):
        wire_req.headers["Connection"] = "keep-alive"

    var can_pool_http = (
        client.enable_http_pool
        and not parsed.use_tls
        and not _request_connection_prefers_close(wire_req)
    )
    var can_pool_tls = (
        client.enable_tls_pool
        and parsed.use_tls
        and not _request_connection_prefers_close(wire_req)
    )

    var payload = _build_request_payload(wire_req, parsed)
    var max_raw_response_bytes = _max_raw_response_bytes(
        max_header_bytes, max_body_bytes
    )

    if parsed.use_tls:
        if can_pool_tls:
            var pooled = _pool_take_tls_connection(client, parsed)
            var fd = pooled.fd
            var ctx = pooled.ctx
            var ssl = pooled.ssl
            var reused_connection = pooled.reused
            var request_count = pooled.request_count
            if not reused_connection:
                fd = _open_socket(parsed, timeout_ms)
                request_count = 0
                try:
                    var connected = _tls_connect(
                        client.tls_runtime,
                        fd,
                        parsed.host,
                        deadline_ns,
                        timeout_ms,
                    )
                    ctx = connected.ctx
                    ssl = connected.ssl
                except:
                    _ = close(fd)
                    raise

            var allow_reconnect_after_reused_failure = reused_connection
            while True:
                try:
                    _tls_send_all(
                        client.tls_runtime,
                        ssl,
                        payload,
                        deadline_ns,
                        timeout_ms,
                    )
                    var raw_result = _tls_read_http_response_for_keepalive(
                        client.tls_runtime,
                        ssl,
                        deadline_ns,
                        timeout_ms,
                        max_raw_response_bytes,
                    )
                    var response = _parse_response(
                        raw_result.raw_response,
                        max_header_bytes=max_header_bytes,
                        max_body_bytes=max_body_bytes,
                        max_decompressed_bytes=max_decompressed_bytes,
                    )
                    var keep_connection = (
                        not raw_result.close_delimited
                        and not _response_connection_prefers_close(response)
                    )
                    if keep_connection:
                        _pool_put_tls_connection(
                            client, parsed, fd, ctx, ssl, request_count + 1
                        )
                    else:
                        _tls_cleanup(client.tls_runtime, ssl, ctx)
                        _ = close(fd)
                    return Response(
                        status_code=response.status_code,
                        headers=response.headers.copy(),
                        body=String(response.body),
                    )
                except e:
                    _tls_cleanup(client.tls_runtime, ssl, ctx)
                    _ = close(fd)
                    if (
                        allow_reconnect_after_reused_failure
                        and _is_stale_reused_connection_error(String(e))
                    ):
                        fd = _open_socket(parsed, timeout_ms)
                        try:
                            var connected = _tls_connect(
                                client.tls_runtime,
                                fd,
                                parsed.host,
                                deadline_ns,
                                timeout_ms,
                            )
                            ctx = connected.ctx
                            ssl = connected.ssl
                            request_count = 0
                            allow_reconnect_after_reused_failure = False
                            continue
                        except:
                            _ = close(fd)
                            raise
                    raise

        var fd = _open_socket(parsed, timeout_ms)
        try:
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
        except:
            _ = close(fd)
            raise

    var fd = c_int(-1)
    var reused_fd = False
    var request_count = 0
    if can_pool_http:
        var pooled_http = _pool_take_http_connection(client, parsed)
        fd = pooled_http.fd
        reused_fd = pooled_http.reused
        request_count = pooled_http.request_count
    if fd < c_int(0):
        fd = _open_socket(parsed, timeout_ms)
        request_count = 0

    var allow_reconnect_after_reused_failure = reused_fd and can_pool_http

    while True:
        try:
            _send_all(fd, payload, deadline_ns, timeout_ms)

            if can_pool_http:
                var raw_result = _read_http_response_for_keepalive(
                    fd,
                    deadline_ns,
                    timeout_ms,
                    max_raw_response_bytes,
                )
                var response = _parse_response(
                    raw_result.raw_response,
                    max_header_bytes=max_header_bytes,
                    max_body_bytes=max_body_bytes,
                    max_decompressed_bytes=max_decompressed_bytes,
                )

                var keep_connection = (
                    not raw_result.close_delimited
                    and not _response_connection_prefers_close(response)
                )

                if keep_connection:
                    _pool_put_http_connection(
                        client, parsed, fd, request_count + 1
                    )
                else:
                    _ = close(fd)

                return Response(
                    status_code=response.status_code,
                    headers=response.headers.copy(),
                    body=String(response.body),
                )

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
        except e:
            _ = close(fd)
            if (
                can_pool_http
                and allow_reconnect_after_reused_failure
                and _is_stale_reused_connection_error(String(e))
            ):
                fd = _open_socket(parsed, timeout_ms)
                request_count = 0
                allow_reconnect_after_reused_failure = False
                continue
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


fn _request_with_redirects_with_client(
    mut client: Client,
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
        var response = _request_once_with_client(
            client,
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


fn _validate_request_options(
    timeout_ms: Int,
    max_redirects: Int,
    max_retries: Int,
    retry_backoff_ms: Int,
    retry_max_backoff_ms: Int,
    max_header_bytes: Int,
    max_body_bytes: Int,
    max_decompressed_bytes: Int,
) raises:
    _ = _deadline_from_timeout_ms(timeout_ms)

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


fn _client_request(
    mut client: Client,
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
    _validate_request_options(
        timeout_ms,
        max_redirects,
        max_retries,
        retry_backoff_ms,
        retry_max_backoff_ms,
        max_header_bytes,
        max_body_bytes,
        max_decompressed_bytes,
    )

    var deadline_ns = _deadline_from_timeout_ms(timeout_ms)
    var retries_used = 0
    var retry_allowed = retry_non_idempotent or _is_idempotent_method(
        req.method
    )

    while True:
        try:
            var response = _request_with_redirects_with_client(
                client,
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


fn _client_request_safe(
    mut client: Client,
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
            _client_request(
                client,
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
    var client = Client(enable_http_pool=False, enable_tls_pool=False)
    return _client_request(
        client,
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
        var client = Client(enable_http_pool=False, enable_tls_pool=False)
        return _client_request_safe(
            client,
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
