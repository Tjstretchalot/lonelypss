import asyncio
from typing import TYPE_CHECKING

from lonelypss.ws.handlers.protocol import StateHandler
from lonelypss.ws.state import State, StateClosing, StateType, StateWaitingConfigure
from lonelypss.ws.util import make_websocket_read_task


async def handle_accepting(state: State) -> State:
    """Accepts the websocket connection within the timeout period then moves to
    waiting for the configure message
    """

    assert state.type == StateType.ACCEPTING
    try:
        await asyncio.wait_for(
            state.websocket.accept(),
            timeout=state.broadcaster_config.websocket_accept_timeout,
        )
    except asyncio.TimeoutError:
        return StateClosing(type=StateType.CLOSING, websocket=state.websocket)

    return StateWaitingConfigure(
        type=StateType.WAITING_CONFIGURE,
        websocket=state.websocket,
        broadcaster_config=state.broadcaster_config,
        internal_receiver=state.internal_receiver,
        read_task=make_websocket_read_task(state.websocket),
    )


if TYPE_CHECKING:
    _: StateHandler = handle_accepting
