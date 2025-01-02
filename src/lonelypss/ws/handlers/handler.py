from typing import TYPE_CHECKING, Dict

from lonelypss.ws.handlers.accepting import handle_accepting
from lonelypss.ws.handlers.closing import handle_closing
from lonelypss.ws.handlers.open.handler import handle_open
from lonelypss.ws.handlers.protocol import StateHandler
from lonelypss.ws.handlers.waiting_configure import handle_waiting_configure
from lonelypss.ws.state import State, StateType

_handlers: Dict[StateType, StateHandler] = {
    StateType.ACCEPTING: handle_accepting,
    StateType.WAITING_CONFIGURE: handle_waiting_configure,
    StateType.OPEN: handle_open,
    StateType.CLOSING: handle_closing,
}


async def handle_any(state: State) -> State:
    """Handle any state by delegating to the appropriate handler. This will raise
    a KeyError for the CLOSED state, which does not need any further handling.

    Raises KeyError if no handler is found for the state type.
    """
    return await _handlers[state.type](state)


if TYPE_CHECKING:
    _: StateHandler = handle_any
