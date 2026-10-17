import json
import logging
import asyncio
import aio_pika
from typing import Optional, Dict, Any
from ..config import settings

logger = logging.getLogger("GateKeeper.RabbitMQ")

class RabbitMQService:
    def __init__(self):
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.exchange: Optional[aio_pika.RobustExchange] = None
        self._fallback_queue: asyncio.Queue = asyncio.Queue()

    async def connect(self):
        try:
            self.connection = await aio_pika.connect_robust(
                settings.RABBITMQ_URL,
                timeout=10.0
            )
            self.channel = await self.connection.channel()
            self.exchange = await self.channel.declare_exchange(
                settings.RABBITMQ_EXCHANGE_VIOLATIONS,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            queue = await self.channel.declare_queue(
                settings.RABBITMQ_QUEUE_VIOLATIONS,
                durable=True
            )
            await queue.bind(self.exchange, routing_key="violation.#")
            logger.info("Connected to RabbitMQ GateKeeper violations exchange.")
        except Exception as e:
            logger.warning(f"RabbitMQ connection warning: {e}. Running with memory queue fallback.")
            self.connection = None
            self.channel = None
            self.exchange = None

    async def disconnect(self):
        if self.connection:
            await self.connection.close()
            self.connection = None

    async def publish_violation(self, violation_data: Dict[str, Any]):
        payload = json.dumps(violation_data, default=str)
        if self.channel and not self.channel.is_closed and self.exchange:
            try:
                message = aio_pika.Message(
                    body=payload.encode("utf-8"),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json"
                )
                await self.exchange.publish(message, routing_key=f"violation.{violation_data.get('client_id', 'unknown')}")
                return True
            except Exception as e:
                logger.error(f"Error publishing violation to RabbitMQ: {e}")
        
        await self._fallback_queue.put(violation_data)
        return True

rabbitmq_service = RabbitMQService()
