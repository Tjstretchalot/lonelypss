import time
from typing import TYPE_CHECKING

from lonelypsp.auth.config import AuthResult
from lonelypsp.stateful.messages.continue_receive import S2B_ContinueReceive

from lonelypss.ws.handlers.open.processors.protocol import S2B_MessageProcessor
from lonelypss.ws.handlers.open.websocket_url import (
    make_for_receive_websocket_url_and_change_counter,
)
from lonelypss.ws.state import StateOpen


async def process_continue_receive(
    state: StateOpen, message: S2B_ContinueReceive
) -> None:
    expected_ack = state.expecting_acks.get_nowait()
    if expected_ack.type != message.type:
        raise Exception(
            f"unexpected continue receive (expecting a {expected_ack.type})"
        )
    if expected_ack.identifier != message.identifier:
        raise Exception(
            f"unexpected continue receive (expecting identifier {expected_ack.identifier!r}, got {message.identifier!r})"
        )
    if expected_ack.part_id != message.part_id:
        raise Exception(
            f"unexpected continue receive (expecting part_id {expected_ack.part_id}, got {message.part_id})"
        )

    receive_url = make_for_receive_websocket_url_and_change_counter(state)
    auth_result = await state.broadcaster_config.is_stateful_continue_receive_allowed(
        url=receive_url,
        message=message,
        now=time.time(),
    )
    if auth_result != AuthResult.OK:
        raise Exception(f"continue receive auth: {auth_result}")


if TYPE_CHECKING:
    _: S2B_MessageProcessor[S2B_ContinueReceive] = process_continue_receive
