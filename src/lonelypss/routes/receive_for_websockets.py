import base64
import hashlib
import json
import tempfile
import time
from typing import Annotated, Optional

from fastapi import APIRouter, Header, Request, Response

from lonelypss.middleware.config import get_config_from_request
from lonelypss.middleware.ws_receiver import get_ws_receiver_from_request

router = APIRouter()


@router.post(
    "/v1/receive_for_websockets",
)
async def receive_for_websockets(
    request: Request,
    authorization: Annotated[Optional[str], Header()] = None,
    repr_digest: Annotated[Optional[str], Header()] = None,
    x_topic: Annotated[Optional[str], Header()] = None,
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
            headers={"Content-Type": "application/json; charset=utf-8"},
            content=b'{"unsubscribe": true, "reason": "missing repr-digest header"}',
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

    if not receiver.is_relevant(topic):
        return Response(
            status_code=400,
            headers={"Content-Type": "application/json; charset=utf-8"},
            content=b'{"unsubscribe": true, "reason": "topic not relevant"}',
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
            headers={"Content-Type": "application/json; charset=utf-8"},
            content=b'{"unsubscribe": true, "reason": "missing sha-512 repr-digest"}',
        )

    try:
        expected_digest = base64.b64decode(expected_digest_b64 + "==")
    except BaseException:
        return Response(
            status_code=400,
            headers={"Content-Type": "application/json; charset=utf-8"},
            content=b'{"unsubscribe": true, "reason": "unparseable sha-512 repr-digest (not base64)"}',
        )

    request_url = str(request.url)
    if request_url != receiver.receiver_url:
        return Response(
            status_code=400,
            headers={"Content-Type": "application/json; charset=utf-8"},
            content=b'{"unsubscribe": true, "reason": "invalid receiver URL"}',
        )

    auth_result = await config.is_receive_allowed(
        url=str(request.url),
        topic=topic,
        message_sha512=expected_digest,
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

    with tempfile.SpooledTemporaryFile(
        max_size=config.message_body_spool_size, mode="w+b"
    ) as spooled_request_body:
        read_length = 0
        hasher = hashlib.sha512()
        stream_iter = request.stream().__aiter__()
        while True:
            try:
                chunk = await stream_iter.__anext__()
            except StopAsyncIteration:
                break
            hasher.update(chunk)
            read_length += len(chunk)
            spooled_request_body.write(chunk)

        real_digest = hasher.digest()
        if real_digest != expected_digest:
            return Response(
                status_code=403,
                headers={"Content-Type": "application/json; charset=utf-8"},
                content=b'{"unsubscribe": true, "reason": "incorrect sha-512 repr-digest"}',
            )

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
    return Response(
        status_code=200,
        headers={"Content-Type": "application/json; charset=utf-8"},
        content=b'{"subscribers": ' + str(count).encode("ascii") + b"}",
    )
