import asyncio
from typing import TYPE_CHECKING

from lonelypss.ws.state import (
    SimplePendingSend,
    SimplePendingSendPreFormatted,
    SimplePendingSendType,
    StateOpen,
)

if TYPE_CHECKING:
    from lonelypss.ws.handlers.open.senders.send_any import send_any
else:
    send_any = None


def send_asap(state: StateOpen, pending: SimplePendingSend) -> None:
    """Queues a simple message to be sent to the subscriber as soon as possible,
    where that message has already been described via one of the SimplePendingSend
    options
    """
    global send_any
    if send_any is None:
        from lonelypss.ws.handlers.open.senders.send_any import send_any

    if state.send_task is None:
        state.send_task = asyncio.create_task(send_any(state, pending))
        return

    state.unsent_messages.append(pending)


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
