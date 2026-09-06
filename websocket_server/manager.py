"""WebSocket connection manager."""

import asyncio
import json
import logging
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger("websocket_server.manager")


class ConnectionManager:


    def __init__(self):
        self._active: set[WebSocket] = set()
        self.total_connected    = 0
        self.total_disconnected = 0
        self.total_messages_sent = 0

    async def connect(self, websocket: WebSocket) -> None:

        await websocket.accept()
        self._active.add(websocket)
        self.total_connected += 1
        client = self._client_id(websocket)
        logger.info(f"WebSocket connected: {client}  (total active: {len(self._active)})")

    def disconnect(self, websocket: WebSocket) -> None:

        self._active.discard(websocket)
        self.total_disconnected += 1
        logger.info(f"WebSocket disconnected  (total active: {len(self._active)})")

    async def send_to(self, websocket: WebSocket, data: str) -> bool:

        try:
            await websocket.send_text(data)
            return True
        except Exception:
            return False

    async def broadcast(self, data: str) -> None:

        if not self._active:
            return

        connections = list(self._active)
        results = await asyncio.gather(
            *[self.send_to(ws, data) for ws in connections],
            return_exceptions=True,
        )

        for ws, result in zip(connections, results):
            if result is True:
                self.total_messages_sent += 1
            else:
                self.disconnect(ws)

    async def send_history(self, websocket: WebSocket, history: list) -> None:

        if not history:
            return

        history_msg = json.dumps({"type": "history", "entries": history})
        await self.send_to(websocket, history_msg)

    @property
    def active_count(self) -> int:
        return len(self._active)

    def stats(self) -> dict:
        return {
            "active_connections":   self.active_count,
            "total_connected":      self.total_connected,
            "total_disconnected":   self.total_disconnected,
            "total_messages_sent":  self.total_messages_sent,
        }

    def _client_id(self, websocket: WebSocket) -> str:
        client = websocket.client
        if client:
            return f"{client.host}:{client.port}"
        return "unknown"
