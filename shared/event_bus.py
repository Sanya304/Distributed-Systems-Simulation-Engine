"""In-memory event bus — the message transport layer of the simulation."""

from __future__ import annotations
import asyncio
from shared.models import Event, ServiceConfig


class EventBus:

    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}

    def register(self, service_name: str, config: ServiceConfig) -> None:
        if service_name not in self._queues:
            self._queues[service_name] = asyncio.Queue(maxsize=config.max_queue_size)

    async def publish(self, target_service: str, event: Event) -> bool:
        queue = self._queues.get(target_service)
        if queue is None:
            return False
        try:
            queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            return False

    async def consume(self, service_name: str) -> Event:
        queue = self._queues.get(service_name)
        if queue is None:
            raise KeyError(f"No queue registered for service '{service_name}'")
        return await queue.get()

    def queue_depth(self, service_name: str) -> int:
        queue = self._queues.get(service_name)
        return queue.qsize() if queue else 0

    def is_full(self, service_name: str) -> bool:
        queue = self._queues.get(service_name)
        if queue is None:
            return False
        return queue.full()

    def registered_services(self) -> list[str]:
        return list(self._queues.keys())


bus = EventBus()
