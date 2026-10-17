import pytest
from httpx import AsyncClient, ASGITransport
from backend.app.main import app

@pytest.mark.asyncio
async def test_health_check_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res = await ac.get("/api/v1/telemetry/health")
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "UP"
        assert "cpp_token_bucket_core" in data["components"]

@pytest.mark.asyncio
async def test_check_rate_limit_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        req_payload = {
            "client_id": "CLIENT_ALICE",
            "endpoint": "/api/v1/orders",
            "tokens_requested": 1.0,
            "ip_address": "127.0.0.1"
        }
        res = await ac.post("/api/v1/gateway/check", json=req_payload)
        assert res.status_code == 200
        data = res.json()
        assert data["allowed"] is True
        assert data["remaining_tokens"] >= 0

@pytest.mark.asyncio
async def test_proxy_endpoint_headers_and_throttling():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Client abuser has small capacity of 10.0
        # Send 15 requests to trigger 429
        last_status = None
        for i in range(15):
            res = await ac.get(
                "/api/v1/gateway/proxy/orders",
                headers={"X-Client-ID": "CLIENT_ABUSER"}
            )
            last_status = res.status_code
            assert "X-RateLimit-Limit" in res.headers
            assert "X-RateLimit-Remaining" in res.headers

        assert last_status == 429

@pytest.mark.asyncio
async def test_policy_crud_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        new_policy = {
            "client_id": "CLIENT_VIP",
            "endpoint_pattern": "/api/v1/vip",
            "capacity": 500.0,
            "refill_rate": 25.0,
            "window_seconds": 60
        }
        post_res = await ac.post("/api/v1/policies", json=new_policy)
        assert post_res.status_code == 200
        data = post_res.json()
        assert data["client_id"] == "CLIENT_VIP"
        assert data["capacity"] == 500.0
