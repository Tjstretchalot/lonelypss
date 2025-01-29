import base64
import hashlib
import io
import tempfile
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
    "/v1/receive_for_websockets",
)
async def receive_for_websockets(
    request: Request,
    authorization: Annotated[Optional[str], Header()] = None,
    repr_digest: Annotated[Optional[str], Header()] = None,
) -> Response:
    """As a broadcaster, in order to handle websocket connections, we need to be notified
    about messages that were sent to other broadcasters. To facilitate this, the broadcaster
    acts as a subscriber for itself, using this endpoint to receive messages, then forwards
    these to an in-memory structure to fan it out to all the websocket connections.

    This is not the endpoint that subscribers use to notify broadcasters. Use `/v1/notify` for
    that.
    """
    config = get_config_from_request(request)
    receiver = get_ws_receiver_from_request(request)

    if repr_digest is None:
        return Response(
            status_code=400,
            headers={"Content-Type": "application/octet-stream"},
            content=int(
                SubscriberToBroadcasterStatelessMessageType.RESPONSE_UNSUBSCRIBE_IMMEDIATE
            ).to_bytes(2, "big"),
        )

    expected_digest_b64: Optional[str] = None
    for digest_pair in repr_digest.split(","):
        split_digest_pair = digest_pair.split("=", 1)
        if len(split_digest_pair) != 2:
            continue
        digest_type, digest_value = split_digest_pair
        if digest_type != "sha-512":
            continue

        expected_digest_b64 = digest_value

    if expected_digest_b64 is None:
        return Response(
            status_code=400,
            headers={"Content-Type": "application/octet-stream"},
            content=int(
                SubscriberToBroadcasterStatelessMessageType.RESPONSE_UNSUBSCRIBE_IMMEDIATE
            ).to_bytes(2, "big"),
        )

    try:
        expected_digest = base64.b64decode(expected_digest_b64 + "==")
    except BaseException:
        return Response(
            status_code=400,
            headers={"Content-Type": "application/octet-stream"},
            content=int(
                SubscriberToBroadcasterStatelessMessageType.RESPONSE_UNSUBSCRIBE_IMMEDIATE
            ).to_bytes(2, "big"),
        )

    request_url = str(request.url)
    if request_url != receiver.receiver_url:
        return Response(
            status_code=400,
            headers={"Content-Type": "application/octet-stream"},
            content=int(
                SubscriberToBroadcasterStatelessMessageType.RESPONSE_UNSUBSCRIBE_IMMEDIATE
            ).to_bytes(2, "big"),
        )

    stream = request.stream()
    try:
        body = AsyncIterableAIO(stream)

        try:
            tracing_length_bytes = await async_read_exact(body, 2)
            tracing_length = int.from_bytes(tracing_length_bytes, "big")
            tracing = await async_read_exact(body, tracing_length)

            topic_length_bytes = await async_read_exact(body, 2)
            topic_length = int.from_bytes(topic_length_bytes, "big")
            topic = await async_read_exact(body, topic_length)

            identifier_length_bytes = await async_read_exact(body, 1)
            identifier_length = int.from_bytes(identifier_length_bytes, "big")
            identifier = await async_read_exact(body, identifier_length)

            message_length_bytes = await async_read_exact(body, 8)
            message_length = int.from_bytes(message_length_bytes, "big")
        except ValueError:
            return Response(status_code=400)

        if not receiver.is_relevant(topic):
            return Response(
                status_code=400,
                headers={"Content-Type": "application/octet-stream"},
                content=int(
                    SubscriberToBroadcasterStatelessMessageType.RESPONSE_UNSUBSCRIBE_IMMEDIATE
                ).to_bytes(2, "big"),
            )

        auth_result = await config.is_receive_allowed(
            tracing=tracing,
            url=str(request.url),
            topic=topic,
            message_sha512=expected_digest,
            identifier=identifier,
            now=time.time(),
            authorization=authorization,
        )
        if auth_result == AuthResult.UNAVAILABLE:
            return Response(status_code=503)
        if auth_result == AuthResult.FORBIDDEN:
            return Response(
                status_code=403,
                headers={"Content-Type": "application/octet-stream"},
                content=int(
                    SubscriberToBroadcasterStatelessMessageType.RESPONSE_UNSUBSCRIBE_IMMEDIATE
                ).to_bytes(2, "big"),
            )
        if auth_result == AuthResult.UNAUTHORIZED:
            return Response(
                status_code=401,
                headers={"Content-Type": "application/octet-stream"},
                content=int(
                    SubscriberToBroadcasterStatelessMessageType.RESPONSE_UNSUBSCRIBE_IMMEDIATE
                ).to_bytes(2, "big"),
            )
        if auth_result != AuthResult.OK:
            assert_never(auth_result)

        with tempfile.SpooledTemporaryFile(
            max_size=config.message_body_spool_size, mode="w+b"
        ) as spooled_request_body:
            read_length = 0
            hasher = hashlib.sha512()
            while True:
                chunk = await body.read(io.DEFAULT_BUFFER_SIZE)
                if not chunk:
                    break
                hasher.update(chunk)
                read_length += len(chunk)
                spooled_request_body.write(chunk)

                if read_length > message_length:
                    return Response(status_code=400)

            await stream.aclose()
            if read_length != message_length:
                return Response(status_code=400)

            real_digest = hasher.digest()
            if real_digest != expected_digest:
                return Response(status_code=400)

            spooled_request_body.seek(0)
            if read_length < config.message_body_spool_size:
                small_body = spooled_request_body.read()
                count = await receiver.on_small_incoming(
                    small_body, topic=topic, sha512=real_digest
                )
            else:
                count = await receiver.on_large_exclusive_incoming(
                    spooled_request_body,
                    topic=topic,
                    sha512=real_digest,
                    length=read_length,
                )

        resp_tracing = b""  # TODO: tracing
        resp_authorization = await config.authorize_confirm_receive(
            tracing=resp_tracing,
            identifier=identifier,
            num_subscribers=count,
            url=str(request.url),
            now=time.time(),
        )
        resp_authorization_bytes = (
            b"" if resp_authorization is None else resp_authorization.encode("utf-8")
        )
        resp_body = PreallocatedBytesIO(
            2
            + 2
            + len(resp_authorization_bytes)
            + 2
            + len(resp_tracing)
            + 1
            + len(identifier)
            + 4
        )
        resp_body.write(
            int(
                SubscriberToBroadcasterStatelessMessageType.RESPONSE_CONFIRM_RECEIVE
            ).to_bytes(2, "big")
        )
        resp_body.write(len(resp_authorization_bytes).to_bytes(2, "big"))
        resp_body.write(resp_authorization_bytes)
        resp_body.write(len(resp_tracing).to_bytes(2, "big"))
        resp_body.write(resp_tracing)
        resp_body.write(len(identifier).to_bytes(1, "big"))
        resp_body.write(identifier)
        resp_body.write(count.to_bytes(4, "big"))
        return Response(
            status_code=200,
            headers={"Content-Type": "application/octet-stream"},
            content=memoryview(resp_body.buffer),
        )
    finally:
        await stream.aclose()
