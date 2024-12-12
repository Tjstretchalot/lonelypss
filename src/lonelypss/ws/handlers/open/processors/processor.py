from typing import TYPE_CHECKING
from lonelypss.ws.handlers.open.processors.protocol import S2B_MessageProcessor
from lonelypss.ws.state import StateOpen
from lonelypsp.stateful.message import S2B_Message


async def process_any(state: StateOpen, message: S2B_Message) -> None:
    raise NotImplementedError


if TYPE_CHECKING:
    _: S2B_MessageProcessor[S2B_Message] = process_any
