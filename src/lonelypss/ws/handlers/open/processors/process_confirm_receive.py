import time
from typing import TYPE_CHECKING

from lonelypsp.auth.config import AuthResult
from lonelypsp.stateful.messages.confirm_receive import S2B_ConfirmReceive

from lonelypss.ws.handlers.open.processors.protocol import S2B_MessageProcessor
from lonelypss.ws.handlers.open.websocket_url import (
    make_for_receive_websocket_url_and_change_counter,
)
from lonelypss.ws.state import StateOpen


async def process_confirm_receive(
    state: StateOpen, message: S2B_ConfirmReceive
) -> None:
    expected_ack = state.expecting_acks.get_nowait()
    if expected_ack.type != message.type:
        raise Exception(f"unexpected confirm receive (expecting a {expected_ack.type})")
    if expected_ack.identifier != message.identifier:
        raise Exception(
            f"unexpected confirm receive (expecting identifier {expected_ack.identifier!r}, got {message.identifier!r})"
        )

    receive_url = make_for_receive_websocket_url_and_change_counter(state)
    auth_result = await state.broadcaster_config.is_confirm_receive_allowed(
        tracing=message.tracing,
        identifier=message.identifier,
        num_subscribers=message.num_subscribers,
        url=receive_url,
        now=time.time(),
        authorization=message.authorization,
    )
    if auth_result != AuthResult.OK:
        raise Exception(f"confirm receive auth: {auth_result}")


if TYPE_CHECKING:
    _: S2B_MessageProcessor[S2B_ConfirmReceive] = process_confirm_receive
