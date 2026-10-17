import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .services.database_service import db_service
from .services.redis_limiter import redis_limiter
from .services.rabbitmq_service import rabbitmq_service
from .services.cosmos_service import cosmos_service
from .services.cpp_limiter import cpp_limiter

from .routers import gateway, policies, telemetry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [GateKeeper] %(message)s")
logger = logging.getLogger("GateKeeper")

def seed_default_cpp_policies():
    logger.info("Initializing C++ rate limiter tiers and policies...")
    cpp_limiter.configure_policy("CLIENT_ALICE", 60.0, 1.0)
    cpp_limiter.configure_policy("CLIENT_BOB", 1000.0, 50.0)
    cpp_limiter.configure_policy("CLIENT_ACME_CORP", 50000.0, 2000.0)
    cpp_limiter.configure_policy("CLIENT_ABUSER", 10.0, 0.2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Booting GateKeeper Enterprise Distributed Rate Limiter Gateway...")
    await db_service.connect()
    await redis_limiter.connect()
    await rabbitmq_service.connect()
    await cosmos_service.connect()
    seed_default_cpp_policies()
    logger.info("GateKeeper Sentinel is ready to intercept and throttle API requests.")
    yield
    logger.info("Shutting down GateKeeper Gateway...")
    await db_service.disconnect()
    await redis_limiter.disconnect()
    await rabbitmq_service.disconnect()
    await cosmos_service.disconnect()

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Sub-microsecond Token Bucket & Sliding Window API Gateway with C++ SIMD, Redis Lua, and RabbitMQ.",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(gateway.router, prefix=settings.API_V1_PREFIX)
app.include_router(policies.router, prefix=settings.API_V1_PREFIX)
app.include_router(telemetry.router, prefix=settings.API_V1_PREFIX)

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    with open(template_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())
