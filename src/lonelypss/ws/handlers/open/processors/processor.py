from typing import TYPE_CHECKING, cast

from lonelypsp.stateful.constants import SubscriberToBroadcasterStatefulMessageType
from lonelypsp.stateful.message import S2B_Message

from lonelypss.ws.handlers.open.processors.process_notify import process_notify
from lonelypss.ws.handlers.open.processors.process_notify_stream import (
    process_notify_stream,
)
from lonelypss.ws.handlers.open.processors.process_subscribe_exact import (
    process_subscribe_exact,
)
from lonelypss.ws.handlers.open.processors.process_subscribe_glob import (
    process_subscribe_glob,
)
from lonelypss.ws.handlers.open.processors.process_unsubscribe_exact import (
    process_unsubscribe_exact,
)
from lonelypss.ws.handlers.open.processors.process_unsubscribe_glob import (
    process_unsubscribe_glob,
)
from lonelypss.ws.handlers.open.processors.protocol import S2B_MessageProcessor
from lonelypss.ws.state import StateOpen

PROCESSORS = {
    SubscriberToBroadcasterStatefulMessageType.NOTIFY_STREAM: process_notify_stream,
    SubscriberToBroadcasterStatefulMessageType.NOTIFY: process_notify,
    SubscriberToBroadcasterStatefulMessageType.SUBSCRIBE_EXACT: process_subscribe_exact,
    SubscriberToBroadcasterStatefulMessageType.SUBSCRIBE_GLOB: process_subscribe_glob,
    SubscriberToBroadcasterStatefulMessageType.UNSUBSCRIBE_EXACT: process_unsubscribe_exact,
    SubscriberToBroadcasterStatefulMessageType.UNSUBSCRIBE_GLOB: process_unsubscribe_glob,
}


async def process_any(state: StateOpen, message: S2B_Message) -> None:
    """Processes a message from the subscriber to the broadcaster. This is
    not async safe in the sense we only expect to be processing one message
    at a time per websocket in order to give a predictable ack order, but
    can handle the send task running in a separate coroutine, which may access
    some similar resources

    Raises an exception if the broadcaster should disconnect the subscriber and
    cleanup resources
    """
    await cast(S2B_MessageProcessor[S2B_Message], PROCESSORS[message.type])(
        state, message
    )


if TYPE_CHECKING:
    _: S2B_MessageProcessor[S2B_Message] = process_any
