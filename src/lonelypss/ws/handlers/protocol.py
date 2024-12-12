from typing import Protocol

from lonelypss.ws.state import State


class StateHandler(Protocol):
    """Describes something capable of  making some progress within the
    websocket state machine, returning the next state (or the same state),
    such that if we call the appropriate state handlers in a loop we eventually
    end in StateClosed

    This protocol is mainly to ensure we are being internally consistent with
    the function signature

    The signature is a generic State rather than a contravariant typevar because
    in order for this to be useful we need to collect the state handlers in a dict,
    and afaik theres no way to type a dictionary whose keys and values are related,
    e.g., `Dict[K => StateHandler[K]]` (this is doable in e.g typescript)
    """

    async def __call__(self, state: State) -> State: ...
