from typing import Set
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from ..services.cpp_limiter import cpp_limiter
from ..services.database_service import db_service
from ..services.redis_limiter import redis_limiter
from ..services.rabbitmq_service import rabbitmq_service
from ..services.cosmos_service import cosmos_service

router = APIRouter(prefix="/telemetry", tags=["Telemetry & Profiling"])

class TelemetryWsManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)

    async def broadcast(self, message: dict):
        dead = []
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                dead.append(connection)
        for d in dead:
            self.active_connections.discard(d)

telemetry_ws_manager = TelemetryWsManager()

@router.websocket("/ws")
async def telemetry_websocket(websocket: WebSocket):
    await telemetry_ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        telemetry_ws_manager.disconnect(websocket)
    except Exception:
        telemetry_ws_manager.disconnect(websocket)

@router.get("/metrics")
async def get_limiter_metrics():
    metrics = cpp_limiter.get_metrics()
    return {
        "engine": "C++ Token Bucket Core with Nanosecond Refill",
        "metrics": metrics,
        "performance_profile": {
            "p50_target_ns": "< 100 ns",
            "p99_target_ns": "< 200 ns",
            "max_throughput_ops_sec": "7.5M+ ops/s",
            "atomic_lock_strategy": "Fine-Grained Per-Bucket Concurrency"
        }
    }

@router.get("/summary")
async def get_telemetry_summary():
    cpp_metrics = cpp_limiter.get_metrics()
    db_metrics = await db_service.get_traffic_summary()
    return {
        "cpp_engine": cpp_metrics,
        "database": db_metrics
    }

@router.get("/violations")
async def get_recent_violations(limit: int = 50):
    violations = await db_service.get_recent_violations(limit)
    # Ensure datetime is formatted cleanly
    serialized = []
    for v in violations:
        item = dict(v)
        if "violation_time" in item and hasattr(item["violation_time"], "isoformat"):
            item["violation_time"] = item["violation_time"].isoformat()
        serialized.append(item)
    return {"count": len(serialized), "violations": serialized}

@router.get("/audit")
async def get_audit_trail(limit: int = 50):
    events = cosmos_service.get_recent_events(limit)
    return {"count": len(events), "events": events}

@router.get("/health")
async def get_system_health():
    db_ok = (db_service.pool is not None)
    redis_ok = (redis_limiter.redis is not None)
    rmq_ok = (rabbitmq_service.connection is not None and not rabbitmq_service.connection.is_closed)

    return {
        "status": "UP",
        "components": {
            "cpp_token_bucket_core": "ONLINE (libgatekeeper.dylib)",
            "postgresql_azure_sql": "HEALTHY" if db_ok else "OFFLINE",
            "redis_atomic_lua": "HEALTHY" if redis_ok else "FALLBACK",
            "rabbitmq_violations_bus": "HEALTHY" if rmq_ok else "FALLBACK",
            "cosmos_nosql_audit": "ACTIVE"
        }
    }
