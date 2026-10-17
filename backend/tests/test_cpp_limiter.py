import pytest
import time
from backend.app.services.cpp_limiter import cpp_limiter

def test_token_bucket_initial_allow():
    cpp_limiter.configure_policy("CLIENT_TEST_1", capacity=10.0, refill_rate=2.0)
    res = cpp_limiter.check_and_consume("CLIENT_TEST_1", tokens=4.0)
    assert res["allowed"] is True
    assert res["remaining_tokens"] == pytest.approx(6.0, 0.1)
    assert res["latency_ns"] < 5000 # Under 5 microseconds

def test_token_bucket_exhaustion_throttling():
    cpp_limiter.configure_policy("CLIENT_TEST_2", capacity=5.0, refill_rate=1.0)
    # Drain 5 tokens
    res1 = cpp_limiter.check_and_consume("CLIENT_TEST_2", tokens=5.0)
    assert res1["allowed"] is True
    assert res1["remaining_tokens"] == pytest.approx(0.0, 0.1)

    # Next request must be throttled
    res2 = cpp_limiter.check_and_consume("CLIENT_TEST_2", tokens=1.0)
    assert res2["allowed"] is False
    assert res2["reset_after_ms"] > 0

def test_token_bucket_time_refill():
    cpp_limiter.configure_policy("CLIENT_TEST_3", capacity=10.0, refill_rate=20.0) # 20 tokens/sec
    # Drain completely
    cpp_limiter.check_and_consume("CLIENT_TEST_3", tokens=10.0)
    res_empty = cpp_limiter.check_and_consume("CLIENT_TEST_3", tokens=1.0)
    assert res_empty["allowed"] is False

    # Sleep 0.1s -> should refill ~2.0 tokens
    time.sleep(0.12)
    res_refilled = cpp_limiter.check_and_consume("CLIENT_TEST_3", tokens=1.0)
    assert res_refilled["allowed"] is True

def test_client_isolation():
    cpp_limiter.configure_policy("CLIENT_A", capacity=5.0, refill_rate=1.0)
    cpp_limiter.configure_policy("CLIENT_B", capacity=50.0, refill_rate=10.0)

    # Drain Client A
    cpp_limiter.check_and_consume("CLIENT_A", tokens=5.0)
    assert cpp_limiter.check_and_consume("CLIENT_A", tokens=1.0)["allowed"] is False

    # Client B must remain unaffected
    assert cpp_limiter.check_and_consume("CLIENT_B", tokens=10.0)["allowed"] is True
