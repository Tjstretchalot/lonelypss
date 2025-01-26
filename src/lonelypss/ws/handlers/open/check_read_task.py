import asyncio
import io
from typing import cast

from lonelypsp.stateful.constants import SubscriberToBroadcasterStatefulMessageType
from lonelypsp.stateful.parser import S2B_AnyMessageParser
from lonelypsp.stateful.parser_helpers import parse_s2b_message_prefix

from lonelypss.util.websocket_message import WSMessageBytes
from lonelypss.ws.handlers.open.check_result import CheckResult
from lonelypss.ws.handlers.open.errors import NormalDisconnectException
from lonelypss.ws.handlers.open.processors.processor import process_any
from lonelypss.ws.state import (
    StateOpen,
)
from lonelypss.ws.util import make_websocket_read_task


async def check_read_task(state: StateOpen) -> CheckResult:
    """Makes progress using the result of the read task, if possible. Raises
    an exception to indicate that we should begin the cleanup and shutdown
    process
    """
    if not state.read_task.done():
        return CheckResult.CONTINUE

    result = state.read_task.result()
    if result["type"] == "websocket.disconnect":
        raise NormalDisconnectException

    if "bytes" not in result:
        raise Exception("unexpected message type (expected bytes)")

    payload = cast(WSMessageBytes, result)["bytes"]
    payload_reader = io.BytesIO(payload)
    prefix = parse_s2b_message_prefix(payload_reader)
    if prefix.type == SubscriberToBroadcasterStatefulMessageType.CONFIGURE:
        raise ValueError("already configured")

    message = S2B_AnyMessageParser.parse(prefix.flags, prefix.type, payload_reader)
    state.read_task = make_websocket_read_task(state.websocket)

    if state.process_task is None:
        state.process_task = asyncio.create_task(process_any(state, message))
    else:
        state.unprocessed_messages.append(message)

    return CheckResult.RESTART
