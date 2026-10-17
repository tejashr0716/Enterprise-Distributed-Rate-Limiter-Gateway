import pytest
from backend.app.services.redis_limiter import redis_limiter

@pytest.mark.asyncio
async def test_redis_atomic_token_bucket():
    client_id = "REDIS_TEST_CLIENT_1"
    res1 = await redis_limiter.check_token_bucket(client_id, capacity=5.0, refill_rate=1.0, cost=3.0)
    assert res1["allowed"] is True
    assert res1["remaining_tokens"] == pytest.approx(2.0, 0.1)

    # Next 3 tokens should be throttled
    res2 = await redis_limiter.check_token_bucket(client_id, capacity=5.0, refill_rate=1.0, cost=3.0)
    assert res2["allowed"] is False
    assert res2["reset_after_ms"] > 0

@pytest.mark.asyncio
async def test_redis_sliding_window_log():
    client_id = "REDIS_TEST_CLIENT_2"
    # Allow up to 3 requests per 10-second window
    res1 = await redis_limiter.check_sliding_window(client_id, limit=3, window_seconds=10)
    assert res1["allowed"] is True

    res2 = await redis_limiter.check_sliding_window(client_id, limit=3, window_seconds=10)
    assert res2["allowed"] is True

    res3 = await redis_limiter.check_sliding_window(client_id, limit=3, window_seconds=10)
    assert res3["allowed"] is True

    # 4th request must be throttled
    res4 = await redis_limiter.check_sliding_window(client_id, limit=3, window_seconds=10)
    assert res4["allowed"] is False
