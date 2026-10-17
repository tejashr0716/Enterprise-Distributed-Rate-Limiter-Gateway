import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "GateKeeper - Enterprise Distributed Rate Limiter & Token-Bucket API Gateway"
    API_V1_PREFIX: str = "/api/v1"

    # Database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", 
        f"postgresql://{os.getenv('USER', 'postgres')}@localhost:5432/gatekeeper_db"
    )

    # Redis (using DB 1 for Rate Limiter)
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/1")

    # RabbitMQ
    RABBITMQ_URL: str = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    RABBITMQ_EXCHANGE_VIOLATIONS: str = "gateway.violations"
    RABBITMQ_QUEUE_VIOLATIONS: str = "queue.violations.audit"

    # Cosmos DB / NoSQL
    COSMOS_ENDPOINT: str = os.getenv("COSMOS_ENDPOINT", "")
    COSMOS_KEY: str = os.getenv("COSMOS_KEY", "")

    # C++ Shared Library
    CPP_LIB_PATH: str = os.getenv(
        "CPP_LIB_PATH",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../cpp/libgatekeeper.dylib"))
    )

    class Config:
        env_file = ".env"

settings = Settings()
