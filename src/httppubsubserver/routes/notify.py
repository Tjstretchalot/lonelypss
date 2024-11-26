import base64
import json
import tempfile
import time
from typing import Annotated, Dict, Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, Header, Request, Response
import hashlib
import aiohttp
import logging

from httppubsubserver.middleware.config import get_config_from_request


class NotifyResponse(BaseModel):
    notified: int = Field(description="The number of subscribers successfully notified")


router = APIRouter()


@router.post(
    "/v1/notify",
    status_code=200,
    response_model=NotifyResponse,
    responses={
        "400": {"description": "The body was not formatted correctly"},
        "401": {"description": "Authorization header is required but not provided"},
        "403": {"description": "Authorization header is provided but invalid"},
        "500": {"description": "Unexpected error occurred"},
        "503": {"description": "Service is unavailable, try again soon"},
    },
)
async def notify(
    request: Request, authorization: Annotated[Optional[str], Header()] = None
) -> Response:
    """Sends the given message to subscribers for the given topic. The body should be
    formatted as the following sequence:

    - 2 bytes: the length of the topic, big-endian, unsigned
    - N bytes: the topic
    - 64 bytes: the sha-512 hash of the message. will be rechecked
    - 8 bytes: the length of the message, big-endian, unsigned
    - M bytes: the message to send. must have the same hash as the provided hash

    The response has one of the following status codes, where the body is arbitrary
    unless otherwise specified.

    - 200 Okay: subscribers were notified. Response body is in JSON format,
      containing the `notified` key with the number of subscribers notified.
    - 400 Bad Request: the body was not formatted correctly
    - 401 Unauthorized: authorization is required but not provided
    - 403 Forbidden: authorization is provided but invalid
    - 500 Internal Server Error: unexpected error occurred
    - 503 Service Unavailable: servce (generally, database) is unavailable
    """
    config = get_config_from_request(request)

    with tempfile.SpooledTemporaryFile(
        max_size=config.message_body_spool_size, mode="w+b"
    ) as request_body:
        read_length = 0
        saw_end = False

        stream_iter = request.stream().__aiter__()
        while True:
            try:
                chunk = await stream_iter.__anext__()
            except StopAsyncIteration:
                saw_end = True
                break

            request_body.write(chunk)
            read_length += len(chunk)
            if read_length >= 2 + 65535 + 64 + 8:
                break

        request_body.seek(0)
        topic_length = int.from_bytes(request_body.read(2), "big")
        topic = request_body.read(topic_length)
        message_hash = request_body.read(64)
        message_length = int.from_bytes(request_body.read(8), "big")

        auth_at = time.time()
        auth_result = await config.is_notify_allowed(
            topic=topic,
            message_sha512=message_hash,
            now=auth_at,
            authorization=authorization,
        )

        if auth_result == "unauthorized":
            return Response(status_code=401)
        elif auth_result == "forbidden":
            return Response(status_code=403)
        elif auth_result == "unavailable":
            return Response(status_code=503)
        elif auth_result != "ok":
            return Response(status_code=500)

        hasher = hashlib.sha512()

        while True:
            chunk = request_body.read(4096)
            if not chunk:
                break
            hasher.update(chunk)

        if not saw_end:
            while True:
                try:
                    chunk = await stream_iter.__anext__()
                except StopAsyncIteration:
                    saw_end = True
                    break

                hasher.update(chunk)
                request_body.write(chunk)
                read_length += len(chunk)

                if read_length > 2 + topic_length + 64 + 8 + message_length:
                    return Response(status_code=400)

        if read_length != 2 + topic_length + 64 + 8 + message_length:
            return Response(status_code=400)

        actual_hash = hasher.digest()
        if actual_hash != message_hash:
            return Response(status_code=400)

        message_starts_at = 2 + topic_length + 64 + 8
        num_succeeded = 0

        headers: Dict[str, str] = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(message_length),
            "Repr-Digest": f"sha-512={base64.b64encode(message_hash).decode('ascii')}",
            "X-Topic": base64.b64encode(topic).decode("ascii"),
        }

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(
                total=config.outgoing_http_timeout_total,
                connect=config.outgoing_http_timeout_connect,
                sock_read=config.outgoing_http_timeout_sock_read,
                sock_connect=config.outgoing_http_timeout_sock_connect,
            )
        ) as session:
            async for subscriber in config.get_subscribers(topic=topic):
                if subscriber["type"] == "unavailable":
                    return Response(status_code=503)

                url = subscriber["url"]
                my_authorization = await config.setup_authorization(
                    url=url, topic=topic, message_sha512=message_hash, now=time.time()
                )
                if my_authorization is None:
                    headers.pop("Authorization", None)
                else:
                    headers["Authorization"] = my_authorization

                request_body.seek(message_starts_at)
                try:
                    async with session.post(
                        url,
                        data=request_body,
                        headers=headers,
                    ) as resp:
                        if resp.ok:
                            logging.debug(
                                f"Successfully notified {url} about {topic!r}"
                            )
                            num_succeeded += 1
                        else:
                            logging.warning(
                                f"Failed to notify {url} about {topic!r}: {resp.status}"
                            )

                            if resp.status >= 400 and resp.status < 500:
                                content_type = resp.headers.get("Content-Type")
                                if (
                                    content_type is not None
                                    and content_type.startswith("application/json")
                                ):
                                    content = await resp.json()
                                    if (
                                        isinstance(content, dict)
                                        and content.get("unsubscribe") is True
                                    ):
                                        logging.info(
                                            f"Unsubscribing {url} from {topic!r} due to response: {json.dumps(content)}"
                                        )

                                        if subscriber["type"] == "exact":
                                            await config.unsubscribe_exact(
                                                url=url, exact=topic
                                            )
                                        else:
                                            await config.unsubscribe_glob(
                                                url=url, glob=subscriber["glob"]
                                            )
                except aiohttp.ClientError:
                    logging.error(
                        f"Failed to notify {url} about {topic!r}", exc_info=True
                    )

        return Response(
            status_code=200,
            content=NotifyResponse.__pydantic_serializer__.to_json(
                NotifyResponse(notified=num_succeeded)
            ),
            headers={
                "Content-Type": "application/json; charset=utf-8",
            },
        )
