import logging
from typing import Dict, Any, List
from collections import deque
from datetime import datetime
from ..config import settings

logger = logging.getLogger("GateKeeper.Cosmos")

class CosmosAuditService:
    def __init__(self):
        self.endpoint = settings.COSMOS_ENDPOINT
        self.key = settings.COSMOS_KEY
        self.client = None
        self.container = None
        self.local_audit_buffer: deque = deque(maxlen=2000)

    async def connect(self):
        if self.endpoint and self.key:
            try:
                from azure.cosmos.aio import CosmosClient
                self.client = CosmosClient(self.endpoint, credential=self.key)
                database = self.client.get_database_client("GateKeeperTelemetry")
                self.container = database.get_container_client("TrafficViolations")
                logger.info("Connected to Azure Cosmos DB NoSQL container.")
            except Exception as e:
                logger.warning(f"Cosmos DB connection warning: {e}. Fallback to in-memory store.")
                self.client = None
        else:
            logger.info("Cosmos DB credentials not configured; operating in local in-memory store.")

    async def disconnect(self):
        if self.client:
            await self.client.close()
            self.client = None

    async def record_event(self, event_type: str, payload: Dict[str, Any]):
        doc = {
            "id": f"{event_type}_{payload.get('client_id', 'unknown')}_{datetime.utcnow().timestamp()}",
            "event_type": event_type,
            "payload": payload,
            "timestamp": datetime.utcnow().isoformat(),
        }
        self.local_audit_buffer.append(doc)

        if self.container:
            try:
                await self.container.upsert_item(doc)
            except Exception as e:
                logger.error(f"Failed to write audit to Cosmos DB: {e}")

    def get_recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        return list(reversed(self.local_audit_buffer))[:limit]

cosmos_service = CosmosAuditService()
