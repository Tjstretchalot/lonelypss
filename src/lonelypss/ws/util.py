import asyncio
from typing import cast

from fastapi import WebSocket

from lonelypss.util.websocket_message import WSMessage


def make_websocket_read_task(websocket: WebSocket) -> asyncio.Task[WSMessage]:
    """Creats an asyncio task that provides a better typed version of websocket.receive()"""
    return cast(asyncio.Task[WSMessage], asyncio.create_task(websocket.receive()))
