import time
import logging
import redis.asyncio as aioredis
from typing import Optional, Dict, Any, Tuple
from ..config import settings

logger = logging.getLogger("GateKeeper.Redis")

TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_updated = now
else
    local elapsed = math.max(0, now - last_updated)
    tokens = math.min(capacity, tokens + (elapsed * refill_rate))
    last_updated = now
end

local allowed = 0
local reset_after_ms = 0

if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
    local needed = capacity - tokens
    reset_after_ms = math.floor((needed / refill_rate) * 1000)
else
    allowed = 0
    local deficit = cost - tokens
    reset_after_ms = math.floor((deficit / refill_rate) * 1000)
end

redis.call('HMSET', key, 'tokens', tokens, 'last_updated', now)
local ttl = math.max(60, math.ceil(capacity / refill_rate) * 2)
redis.call('EXPIRE', key, ttl)

return {allowed, tostring(tokens), reset_after_ms}
"""

SLIDING_WINDOW_LUA = """
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local clear_before = now_ms - window_ms

redis.call('ZREMRANGEBYSCORE', key, 0, clear_before)
local current_requests = redis.call('ZCARD', key)

if current_requests < limit then
    redis.call('ZADD', key, now_ms, now_ms .. '-' .. redis.call('INCR', key .. ':seq'))
    redis.call('PEXPIRE', key, window_ms)
    return {1, limit - current_requests - 1, 0}
else
    return {0, 0, math.floor(window_ms / 1000)}
end
"""

class RedisRateLimiter:
    def __init__(self):
        self.redis: Optional[aioredis.Redis] = None
        self._tb_script = None
        self._sw_script = None

    async def connect(self):
        if not self.redis:
            try:
                self.redis = aioredis.from_url(
                    settings.REDIS_URL,
                    decode_responses=True,
                    max_connections=20
                )
                await self.redis.ping()
                self._tb_script = self.redis.register_script(TOKEN_BUCKET_LUA)
                self._sw_script = self.redis.register_script(SLIDING_WINDOW_LUA)
                logger.info("Connected to Redis Rate Limiter pool and registered Lua scripts.")
            except Exception as e:
                logger.warning(f"Redis Rate Limiter connection error: {e}. Fallback to local C++ engine.")
                self.redis = None

    async def disconnect(self):
        if self.redis:
            await self.redis.close()
            self.redis = None

    async def check_token_bucket(
        self,
        client_id: str,
        capacity: float,
        refill_rate: float,
        cost: float = 1.0
    ) -> Dict[str, Any]:
        if not self.redis or not self._tb_script:
            return {"allowed": True, "remaining_tokens": capacity, "reset_after_ms": 0, "source": "local_fallback"}

        key = f"ratelimit:tb:{client_id}"
        now_sec = time.time()
        res = await self._tb_script(
            keys=[key],
            args=[capacity, refill_rate, cost, now_sec]
        )

        allowed = bool(res[0] == 1)
        remaining = float(res[1])
        reset_ms = int(res[2])

        return {
            "allowed": allowed,
            "remaining_tokens": round(remaining, 2),
            "capacity": capacity,
            "reset_after_ms": reset_ms,
            "source": "redis_atomic_lua"
        }

    async def check_sliding_window(
        self,
        client_id: str,
        limit: int,
        window_seconds: int = 60
    ) -> Dict[str, Any]:
        if not self.redis or not self._sw_script:
            return {"allowed": True, "remaining_requests": limit, "retry_after_sec": 0, "source": "local_fallback"}

        key = f"ratelimit:sw:{client_id}"
        now_ms = int(time.time() * 1000)
        window_ms = window_seconds * 1000

        res = await self._sw_script(
            keys=[key],
            args=[now_ms, window_ms, limit]
        )

        allowed = bool(res[0] == 1)
        remaining = int(res[1])
        retry_after = int(res[2])

        return {
            "allowed": allowed,
            "remaining_requests": remaining,
            "limit": limit,
            "retry_after_sec": retry_after,
            "source": "redis_sliding_window_lua"
        }

redis_limiter = RedisRateLimiter()
