import pytest
import pytest_asyncio
from backend.app.services.cpp_limiter import cpp_limiter
from backend.app.services.database_service import db_service
from backend.app.services.redis_limiter import redis_limiter
from backend.app.services.rabbitmq_service import rabbitmq_service

@pytest_asyncio.fixture(autouse=True)
async def setup_services():
    await db_service.connect()
    await redis_limiter.connect()
    await rabbitmq_service.connect()
    yield
    await db_service.disconnect()
    await redis_limiter.disconnect()
    await rabbitmq_service.disconnect()

@pytest.fixture(autouse=True)
def reset_limiter():
    cpp_limiter.reset()
    yield
    cpp_limiter.reset()
