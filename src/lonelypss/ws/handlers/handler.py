from typing import TYPE_CHECKING, Dict

from lonelypss.ws.handlers.accepting import handle_accepting
from lonelypss.ws.handlers.protocol import StateHandler
from lonelypss.ws.state import State, StateType

_handlers: Dict[StateType, StateHandler] = {
    StateType.ACCEPTING: handle_accepting,
}


async def handle_any(state: State) -> State:
    """Handle any state by delegating to the appropriate handler.

    Raises KeyError if no handler is found for the state type.
    """
    return await _handlers[state.type](state)


if TYPE_CHECKING:
    _: StateHandler = handle_any
