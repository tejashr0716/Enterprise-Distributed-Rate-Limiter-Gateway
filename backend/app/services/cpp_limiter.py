import ctypes
import os
import threading
from typing import Dict, Any, Optional
from ..config import settings

class RateLimitResultC(ctypes.Structure):
    _pack_ = 8
    _fields_ = [
        ("allowed", ctypes.c_int32),
        ("remaining_tokens", ctypes.c_double),
        ("capacity", ctypes.c_double),
        ("reset_after_ms", ctypes.c_uint64),
        ("latency_ns", ctypes.c_uint64),
        ("current_timestamp_ns", ctypes.c_uint64),
    ]

class LimiterMetricsC(ctypes.Structure):
    _pack_ = 8
    _fields_ = [
        ("total_requests_evaluated", ctypes.c_uint64),
        ("total_requests_allowed", ctypes.c_uint64),
        ("total_requests_throttled", ctypes.c_uint64),
        ("avg_latency_ns", ctypes.c_double),
        ("min_latency_ns", ctypes.c_uint64),
        ("max_latency_ns", ctypes.c_uint64),
        ("active_buckets_count", ctypes.c_uint64),
    ]

class CppRateLimiter:
    def __init__(self, lib_path: Optional[str] = None):
        self.lib_path = lib_path or settings.CPP_LIB_PATH
        if not os.path.isabs(self.lib_path):
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.lib_path = os.path.join(base_dir, "cpp", "libgatekeeper.dylib")

        if not os.path.exists(self.lib_path):
            fallback = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../cpp/libgatekeeper.dylib"))
            if os.path.exists(fallback):
                self.lib_path = fallback
            else:
                raise FileNotFoundError(f"GateKeeper C++ library not found at: {self.lib_path}")

        self._lib = ctypes.CDLL(self.lib_path)
        self._lock = threading.Lock()
        self._setup_bindings()
        self._handle = self._lib.create_rate_limiter()

    def _setup_bindings(self):
        self._lib.create_rate_limiter.restype = ctypes.c_void_p
        self._lib.destroy_rate_limiter.argtypes = [ctypes.c_void_p]
        self._lib.destroy_rate_limiter.restype = None

        self._lib.reset_rate_limiter.argtypes = [ctypes.c_void_p]
        self._lib.reset_rate_limiter.restype = None

        self._lib.configure_policy.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_double,
            ctypes.c_double
        ]
        self._lib.configure_policy.restype = None

        self._lib.check_and_consume.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_double
        ]
        self._lib.check_and_consume.restype = RateLimitResultC

        self._lib.get_limiter_metrics.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(LimiterMetricsC)
        ]
        self._lib.get_limiter_metrics.restype = None

    def configure_policy(self, client_id: str, capacity: float, refill_rate: float):
        with self._lock:
            self._lib.configure_policy(
                self._handle,
                client_id.encode("utf-8"),
                ctypes.c_double(capacity),
                ctypes.c_double(refill_rate)
            )

    def check_and_consume(self, client_id: str, tokens: float = 1.0) -> Dict[str, Any]:
        with self._lock:
            res = self._lib.check_and_consume(
                self._handle,
                client_id.encode("utf-8"),
                ctypes.c_double(tokens)
            )

        return {
            "allowed": bool(res.allowed == 1),
            "remaining_tokens": round(res.remaining_tokens, 2),
            "capacity": round(res.capacity, 2),
            "reset_after_ms": res.reset_after_ms,
            "latency_ns": res.latency_ns,
            "timestamp_ns": res.current_timestamp_ns,
        }

    def get_metrics(self) -> Dict[str, Any]:
        m = LimiterMetricsC()
        with self._lock:
            self._lib.get_limiter_metrics(self._handle, ctypes.byref(m))

        return {
            "total_requests_evaluated": m.total_requests_evaluated,
            "total_requests_allowed": m.total_requests_allowed,
            "total_requests_throttled": m.total_requests_throttled,
            "avg_latency_ns": round(m.avg_latency_ns, 2),
            "min_latency_ns": m.min_latency_ns,
            "max_latency_ns": m.max_latency_ns,
            "active_buckets_count": m.active_buckets_count,
        }

    def reset(self):
        with self._lock:
            self._lib.reset_rate_limiter(self._handle)

cpp_limiter = CppRateLimiter()
