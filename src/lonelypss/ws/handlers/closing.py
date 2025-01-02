from typing import TYPE_CHECKING

from fastapi.websockets import WebSocketState

from lonelypss.ws.handlers.protocol import StateHandler
from lonelypss.ws.state import State, StateClosed, StateType


async def handle_closing(state: State) -> State:
    """Closes the websocket normally then raises the indicated exception, if any,
    otherwise moves to CLOSED
    """

    assert state.type == StateType.CLOSING
    if state.websocket.client_state != WebSocketState.DISCONNECTED:
        await state.websocket.close()
    if state.exception is not None:
        raise state.exception
    return StateClosed(type=StateType.CLOSED)


if TYPE_CHECKING:
    _: StateHandler = handle_closing
