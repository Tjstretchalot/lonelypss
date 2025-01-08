import base64
import json
import time
from typing import Annotated, Optional

from fastapi import APIRouter, Header, Request, Response

from lonelypss.middleware.config import get_config_from_request
from lonelypss.middleware.ws_receiver import get_ws_receiver_from_request

router = APIRouter()


@router.post(
    "/v1/missed_for_websockets",
)
async def missed_for_websockets(
    request: Request,
    authorization: Annotated[Optional[str], Header()] = None,
    x_topic: Annotated[Optional[str], Header()] = None,
) -> Response:
    """As a broadcaster, in order to handle websocket connections, we need to be
    notified about messages that were sent to other broadcasters. If the other
    broadcaster fails to notify us about a message, it is helpful for that
    broadcaster to let us know as soon as possible, without building up an
    excessive amount of state that will likely lead to cascading errors.

    This is the endpoint that broadcasters (including ourself via the shared db)
    uses to notify us that we may have missed a `receive_for_websockets` call on
    a topic we were subscribed to, so we can forward that information to the
    websocket connections to trigger the same recovery method they use if their
    websocket connection temporarily drops.
    """
    config = get_config_from_request(request)
    receiver = get_ws_receiver_from_request(request)

    if str(request.url) != receiver.missed_url:
        return Response(
            status_code=400,
            headers={"Content-Type": "application/json; charset=utf-8"},
            content=b'{"unsubscribe": true, "reason": "invalid missed URL"}',
        )

    if x_topic is None:
        return Response(
            status_code=400,
            headers={"Content-Type": "application/json; charset=utf-8"},
            content=b'{"unsubscribe": true, "reason": "missing x-topic header"}',
        )

    try:
        topic = base64.b64decode(x_topic + "==")
    except BaseException:
        return Response(
            status_code=400,
            headers={"Content-Type": "application/json; charset=utf-8"},
            content=b'{"unsubscribe": true, "reason": "invalid x-topic header"}',
        )

    auth_result = await config.is_missed_allowed(
        recovery=receiver.missed_url,
        topic=topic,
        now=time.time(),
        authorization=authorization,
    )
    if auth_result == "unavailable":
        return Response(status_code=503)
    if auth_result != "ok":
        return Response(
            status_code=403,
            headers={"Content-Type": "application/json; charset=utf-8"},
            content=b'{"unsubscribe": true, "reason": '
            + json.dumps(auth_result).encode("utf-8")
            + b"}",
        )

    await receiver.on_missed(topic=topic)
    return Response(status_code=200)
