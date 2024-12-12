import asyncio

from lonelypss.ws.state import (
    SimplePendingSendPreFormatted,
    SimplePendingSendType,
    StateOpen,
)


def send_simple_asap(state: StateOpen, data: bytes) -> None:
    """Queues a simple message to be sent to the subscriber as soon as possible"""

    if state.send_task is None:
        state.send_task = asyncio.create_task(state.websocket.send_bytes(data))
        return

    state.unsent_messages.append(
        SimplePendingSendPreFormatted(
            type=SimplePendingSendType.PRE_FORMATTED, data=data
        )
    )
