import time
from typing import Annotated, Optional

from fastapi import APIRouter, Header, Request, Response
from lonelypsp.auth.config import AuthResult
from lonelypsp.compat import assert_never
from lonelypsp.stateless.constants import SubscriberToBroadcasterStatelessMessageType
from lonelypsp.sync_io import PreallocatedBytesIO

from lonelypss.middleware.config import get_config_from_request
from lonelypss.middleware.ws_receiver import get_ws_receiver_from_request
from lonelypss.util.async_io import async_read_exact
from lonelypss.util.request_body_io import AsyncIterableAIO

router = APIRouter()


@router.post(
    "/v1/missed_for_websockets",
)
async def missed_for_websockets(
    request: Request, authorization: Annotated[Optional[str], Header()] = None
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

    stream = request.stream()
    try:
        body = AsyncIterableAIO(stream.__aiter__())
        tracing_length_bytes = await async_read_exact(body, 2)
        tracing_length = int.from_bytes(tracing_length_bytes, "big")
        tracing = await async_read_exact(body, tracing_length)
        topic_length_bytes = await async_read_exact(body, 2)
        topic_length = int.from_bytes(topic_length_bytes, "big")
        topic = await async_read_exact(body, topic_length)
    finally:
        await stream.aclose()

    auth_result = await config.is_missed_allowed(
        tracing=tracing,
        recovery=receiver.missed_url,
        topic=topic,
        now=time.time(),
        authorization=authorization,
    )
    if auth_result == AuthResult.UNAVAILABLE:
        return Response(status_code=503)
    if auth_result == AuthResult.FORBIDDEN:
        return Response(status_code=403)
    if auth_result == AuthResult.UNAUTHORIZED:
        return Response(status_code=401)
    if auth_result != AuthResult.OK:
        assert_never(auth_result)

    await receiver.on_missed(topic=topic)

    resp_tracing = b""  # TODO: tracing
    resp_authorization = await config.authorize_confirm_missed(
        tracing=resp_tracing,
        topic=topic,
        url=receiver.missed_url,
        now=time.time(),
    )
    resp_authorization_bytes = (
        b"" if resp_authorization is None else resp_authorization.encode("utf-8")
    )
    resp_body = PreallocatedBytesIO(
        2 + 2 + len(resp_authorization_bytes) + 2 + len(resp_tracing)
    )
    resp_body.write(
        int(
            SubscriberToBroadcasterStatelessMessageType.RESPONSE_CONFIRM_MISSED
        ).to_bytes(2, "big")
    )
    resp_body.write(len(resp_authorization_bytes).to_bytes(2, "big"))
    resp_body.write(resp_authorization_bytes)
    resp_body.write(len(resp_tracing).to_bytes(2, "big"))
    resp_body.write(resp_tracing)
    return Response(
        content=memoryview(resp_body.buffer),
        status_code=200,
        headers={"Content-Type": "application/octet-stream"},
    )
