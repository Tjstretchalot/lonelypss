from fastapi import APIRouter, WebSocket

from lonelypss.middleware.config import get_config_from_request
from lonelypss.middleware.ws_receiver import get_ws_receiver_from_request
from lonelypss.ws.handlers.handler import handle_any
from lonelypss.ws.state import State, StateAccepting, StateType

router = APIRouter()


@router.websocket("/v1/websocket")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """Allows sending and receiving notifications over a websocket connection,
    as opposed to the typical way this library is used (HTTP requests). This is
    helpful for the following scenarios:

    - You need to send a large number of notifications, OR
    - You need to receive a large number of notifications, OR
    - You need to receive notifications for a short period of time before unsubscribing, OR
    - You need to receive some notifications, but you cannot accept incoming HTTP requests

    For maximum compatibility with websocket clients, we only communicate
    over the websocket itself (not the http-level header fields).

    Each websocket message is independently authorized, which means if using a
    suitable authorization scheme (eg HMAC) and secrecy is not required, it may
    be reasonable to serve this over WS instead of WSS. It is only recommended
    that this be done on internal networks where the auth is only intended to
    mitigate vulnerability amplification.

    ## COMPRESSION

    For notifications (both posted and received) over websockets, this supports
    using zstandard compression. It will either use an embedded dictionary, a
    precomputed dictionary, or a trained dictionary. Under the typical settings, this:

    - Only considers messages that are between 32 and 16384 bytes for training
    - Will train once after 100kb of data is ready, and once more after 10mb of data is ready,
      then will sample 10mb every 24 hours
    - Will only used the trained dictionary on messages that would be used for training

    ## MESSAGES

    messages always begin as follows

    - 2 bytes (F): flags (interpret as big-endian):
        - least significant bit (1): 0 if headers are expanded, 1 if headers are minimal
    - 2 bytes (T): type of message; see below, depends on if it's sent by a subscriber
      or the broadcaster big-endian encoded, unsigned

    EXPANDED HEADERS:
        - 2 bytes (N): number of headers, big-endian encoded, unsigned
        - REPEAT N:
            - 2 bytes (M): length of header name, big-endian encoded, unsigned
            - M bytes: header name, ascii-encoded
            - 2 bytes (L): length of header value, big-endian encoded, unsigned
            - L bytes: header value

    MINIMAL HEADERS:
    the order of the headers are fixed based on the type, in the order documented.
    Given N headers:
    - Repeat N:
        - 2 bytes (L): length of header value, big-endian encoded, unsigned
        - L bytes: header value

    See the `lonelypsp` repository for precise details on the protocol, specifically,
    `lonelypsp.stateful.constants` is a good place to start
    """
    config = get_config_from_request(websocket)
    receiver = get_ws_receiver_from_request(websocket)

    state: State = StateAccepting(
        type=StateType.ACCEPTING,
        websocket=websocket,
        broadcaster_config=config,
        internal_receiver=receiver,
    )
    while state.type != StateType.CLOSED:
        state = await handle_any(state)
