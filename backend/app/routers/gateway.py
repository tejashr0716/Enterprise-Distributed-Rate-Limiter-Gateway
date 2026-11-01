import time
from datetime import datetime
from fastapi import APIRouter, Request, Response, HTTPException, status
from typing import Dict, Any

from ..schemas.limiter import RateLimitCheckRequest, RateLimitCheckResponse
from ..services.cpp_limiter import cpp_limiter
from ..services.redis_limiter import redis_limiter
from ..services.database_service import db_service
from ..services.rabbitmq_service import rabbitmq_service
from ..services.cosmos_service import cosmos_service

router = APIRouter(prefix="/gateway", tags=["API Gateway & Rate Limiter"])

@router.post("/check", response_model=RateLimitCheckResponse)
async def check_rate_limit(req: RateLimitCheckRequest):
    # 1. Fetch policy
    policy = await db_service.get_client_policy(req.client_id, req.endpoint)
    capacity = policy["capacity"]
    refill_rate = policy["refill_rate"]

    # Ensure C++ engine is configured with latest policy
    cpp_limiter.configure_policy(req.client_id, capacity, refill_rate)

    # 2. Check and consume via C++ core
    result = cpp_limiter.check_and_consume(req.client_id, req.tokens_requested)
    
    # 3. Synchronize with Redis for distributed multi-region tracking
    redis_res = await redis_limiter.check_token_bucket(
        client_id=req.client_id,
        capacity=capacity,
        refill_rate=refill_rate,
        cost=req.tokens_requested
    )

    allowed = result["allowed"] and redis_res.get("allowed", True)
    rem_tokens = min(result["remaining_tokens"], redis_res.get("remaining_tokens", result["remaining_tokens"]))
    reset_ms = max(result["reset_after_ms"], redis_res.get("reset_after_ms", result["reset_after_ms"]))

    # 4. If throttled, record audit & emit violation event
    if not allowed:
        violation_payload = {
            "client_id": req.client_id,
            "endpoint": req.endpoint,
            "ip_address": req.ip_address,
            "requested_tokens": req.tokens_requested,
            "remaining_tokens": rem_tokens,
            "latency_ns": result["latency_ns"],
            "timestamp": datetime.utcnow().isoformat()
        }
        await db_service.record_violation(
            client_id=req.client_id,
            endpoint=req.endpoint,
            ip_address=req.ip_address,
            requested_tokens=req.tokens_requested,
            remaining_tokens=rem_tokens,
            latency_ns=result["latency_ns"]
        )
        await rabbitmq_service.publish_violation(violation_payload)
        await cosmos_service.record_event("RATE_LIMIT_EXCEEDED", violation_payload)

    await db_service.record_traffic_metric(
        client_id=req.client_id,
        endpoint=req.endpoint,
        status_code=200 if allowed else 429,
        latency_ns=result["latency_ns"]
    )

    try:
        from .telemetry import telemetry_ws_manager
        await telemetry_ws_manager.broadcast({
            "type": "CHECK",
            "client_id": req.client_id,
            "endpoint": req.endpoint,
            "allowed": allowed,
            "remaining_tokens": rem_tokens,
            "capacity": capacity,
            "refill_rate": refill_rate,
            "reset_after_ms": reset_ms,
            "latency_ns": result["latency_ns"],
            "timestamp": datetime.utcnow().isoformat()
        })
    except Exception:
        pass

    return RateLimitCheckResponse(
        allowed=allowed,
        client_id=req.client_id,
        remaining_tokens=rem_tokens,
        capacity=capacity,
        reset_after_ms=reset_ms,
        latency_ns=result["latency_ns"],
        algorithm="C++ Token Bucket with Nanosecond Precision & Redis Distributed Lua",
        evaluated_at=datetime.utcnow()
    )

@router.get("/proxy/{path:path}")
@router.post("/proxy/{path:path}")
async def gateway_reverse_proxy(path: str, request: Request, response: Response):
    client_id = request.headers.get("X-Client-ID", "CLIENT_ALICE")
    ip_address = request.client.host if request.client else "127.0.0.1"
    endpoint = f"/{path}"

    policy = await db_service.get_client_policy(client_id, endpoint)
    capacity = policy["capacity"]
    refill_rate = policy["refill_rate"]

    cpp_limiter.configure_policy(client_id, capacity, refill_rate)
    result = cpp_limiter.check_and_consume(client_id, 1.0)
    allowed = result["allowed"]

    # Inject standard RFC rate-limiting headers
    response.headers["X-RateLimit-Limit"] = str(int(capacity))
    response.headers["X-RateLimit-Remaining"] = str(int(result["remaining_tokens"]))
    response.headers["X-RateLimit-Reset"] = str(int(result["reset_after_ms"] / 1000.0) + 1)
    response.headers["X-RateLimit-Engine-Latency-Ns"] = str(result["latency_ns"])

    if not allowed:
        retry_seconds = max(1, int(result["reset_after_ms"] / 1000.0))
        error_headers = {
            "X-RateLimit-Limit": str(int(capacity)),
            "X-RateLimit-Remaining": str(int(result["remaining_tokens"])),
            "X-RateLimit-Reset": str(int(result["reset_after_ms"] / 1000.0) + 1),
            "X-RateLimit-Engine-Latency-Ns": str(result["latency_ns"]),
            "Retry-After": str(retry_seconds),
        }

        violation_payload = {
            "client_id": client_id,
            "endpoint": endpoint,
            "ip_address": ip_address,
            "requested_tokens": 1.0,
            "remaining_tokens": result["remaining_tokens"],
            "latency_ns": result["latency_ns"]
        }
        await db_service.record_violation(client_id, endpoint, ip_address, 1.0, result["remaining_tokens"], result["latency_ns"])
        await rabbitmq_service.publish_violation(violation_payload)

        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error": "Rate limit exceeded. Too many requests.",
                "client_id": client_id,
                "capacity": capacity,
                "remaining_tokens": result["remaining_tokens"],
                "retry_after_seconds": retry_seconds,
                "engine_latency_ns": result["latency_ns"]
            },
            headers=error_headers
        )

    return {
        "status": "PROXIED_OK",
        "proxied_endpoint": endpoint,
        "client_id": client_id,
        "remaining_tokens": result["remaining_tokens"],
        "engine_latency_ns": result["latency_ns"],
        "upstream_response": f"Successfully processed request to {endpoint} via GateKeeper high-performance gateway."
    }
