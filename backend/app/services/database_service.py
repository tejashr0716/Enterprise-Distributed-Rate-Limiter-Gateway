import asyncpg
import logging
from typing import Optional, Dict, Any, List
from ..config import settings

logger = logging.getLogger("GateKeeper.Database")

class DatabaseService:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if not self.pool:
            try:
                self.pool = await asyncpg.create_pool(
                    dsn=settings.DATABASE_URL,
                    min_size=2,
                    max_size=20,
                    command_timeout=30.0
                )
                logger.info("Connected to PostgreSQL GateKeeper database pool.")
            except Exception as e:
                logger.error(f"GateKeeper database connection error: {e}")
                raise

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Closed GateKeeper database pool.")

    async def get_client_policy(self, client_id: str, endpoint: str = "*") -> Dict[str, Any]:
        async with self.pool.acquire() as conn:
            # Check specific policy
            row = await conn.fetchrow(
                """
                SELECT client_id, endpoint_pattern, capacity, refill_rate, window_seconds
                FROM rate_limit_policies
                WHERE client_id = $1 AND (endpoint_pattern = $2 OR endpoint_pattern = '*')
                ORDER BY (CASE WHEN endpoint_pattern = $2 THEN 1 ELSE 2 END)
                LIMIT 1
                """,
                client_id, endpoint
            )
            if row:
                return {
                    "client_id": row["client_id"],
                    "endpoint_pattern": row["endpoint_pattern"],
                    "capacity": float(row["capacity"]),
                    "refill_rate": float(row["refill_rate"]),
                    "window_seconds": row["window_seconds"]
                }
            
            # Fallback to tier defaults or standard sandbox
            return {
                "client_id": client_id,
                "endpoint_pattern": "*",
                "capacity": 100.0,
                "refill_rate": 10.0,
                "window_seconds": 60
            }

    async def upsert_policy(
        self,
        client_id: str,
        endpoint_pattern: str,
        capacity: float,
        refill_rate: float,
        window_seconds: int = 60
    ):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO rate_limit_policies (client_id, endpoint_pattern, capacity, refill_rate, window_seconds, updated_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (client_id, endpoint_pattern) DO UPDATE SET
                    capacity = EXCLUDED.capacity,
                    refill_rate = EXCLUDED.refill_rate,
                    window_seconds = EXCLUDED.window_seconds,
                    updated_at = NOW()
                """,
                client_id, endpoint_pattern, capacity, refill_rate, window_seconds
            )

    async def record_violation(
        self,
        client_id: str,
        endpoint: str,
        ip_address: str,
        requested_tokens: float,
        remaining_tokens: float,
        latency_ns: int
    ):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO violation_audits (
                    client_id, endpoint, ip_address, requested_tokens, remaining_tokens, latency_ns, violation_time
                ) VALUES ($1, $2, $3, $4, $5, $6, NOW())
                """,
                client_id, endpoint, ip_address, requested_tokens, remaining_tokens, latency_ns
            )

    async def get_recent_violations(self, limit: int = 50) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT audit_id, client_id, endpoint, ip_address, requested_tokens, remaining_tokens, latency_ns, violation_time
                FROM violation_audits
                ORDER BY violation_time DESC
                LIMIT $1
                """,
                limit
            )
            return [dict(r) for r in rows]

    async def record_traffic_metric(self, client_id: str, endpoint: str, status_code: int, latency_ns: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO gateway_traffic_metrics (client_id, endpoint, status_code, latency_ns, recorded_at)
                VALUES ($1, $2, $3, $4, NOW())
                """,
                client_id, endpoint, status_code, latency_ns
            )

    async def get_traffic_summary(self) -> Dict[str, Any]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT 
                    COUNT(*) as total_requests,
                    COUNT(*) FILTER (WHERE status_code = 200) as allowed_requests,
                    COUNT(*) FILTER (WHERE status_code = 429) as throttled_requests,
                    COALESCE(AVG(latency_ns), 83.0) as avg_latency_ns
                FROM gateway_traffic_metrics
                """
            )
            return {
                "total": row["total_requests"] if row else 0,
                "allowed": row["allowed_requests"] if row else 0,
                "throttled": row["throttled_requests"] if row else 0,
                "avg_latency_ns": round(float(row["avg_latency_ns"]), 2) if row else 83.0
            }

db_service = DatabaseService()
