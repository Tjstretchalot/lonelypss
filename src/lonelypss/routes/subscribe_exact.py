import time
from typing import Annotated, Optional

from fastapi import APIRouter, Header, Request, Response

from lonelypss.middleware.config import get_config_from_request
from lonelypss.util.async_io import async_read_exact
from lonelypss.util.request_body_io import AsyncIterableAIO

router = APIRouter()


@router.post(
    "/v1/subscribe/exact",
    status_code=202,
    responses={
        "400": {"description": "The body was not formatted correctly"},
        "401": {"description": "Authorization header is required but not provided"},
        "403": {"description": "Authorization header is provided but invalid"},
        "409": {"description": "The subscription already exists"},
        "500": {"description": "Unexpected error occurred"},
        "503": {"description": "Service is unavailable, try again soon"},
    },
)
async def subscribe_exact(
    request: Request, authorization: Annotated[Optional[str], Header()] = None
) -> Response:
    """Subscribes the given URL to the given topic. The body is formatted as follows:

    - 2 bytes (N): the length of the url, big-endian, unsigned
    - N bytes: the url. must be valid utf-8
    - 2 bytes (M): the length of the topic.
    - M bytes: the topic
    - 2 bytes (R): either 0, to indicate no missed messages are desired, or the length
      of the url to post missed messages to, big-endian, unsigned
    - R bytes: the url to post missed messages to, utf-8 encoded

    NOTE: if you want to use the same path and topic for multiple subscriptions
    to get multiple notifications, you can include a hash that disambiguates them,
    for example http://192.0.2.0:8080/#uid=abc123

    The response has an arbitrary body (generally empty) and one of the
    following status codes:

    - 202 Accepted: the subscription was added
    - 400 Bad Request: the body was not formatted correctly
    - 401 Unauthorized: authorization is required but not provided
    - 403 Forbidden: authorization is provided but invalid
    - 409 Conflict: the subscription already exists
    - 500 Internal Server Error: unexpected error occurred
    - 503 Service Unavailable: servce (generally, database) is unavailable
    """
    config = get_config_from_request(request)

    try:
        stream = request.stream()
        try:
            body = AsyncIterableAIO(stream.__aiter__())

            url_length_bytes = await async_read_exact(body, 2)
            url_length = int.from_bytes(url_length_bytes, "big")
            url_bytes = await async_read_exact(body, url_length)
            url = url_bytes.decode("utf-8")

            topic_length_bytes = await async_read_exact(body, 2)
            topic_length = int.from_bytes(topic_length_bytes, "big")
            topic = await async_read_exact(body, topic_length)

            recovery_url_length_bytes = await async_read_exact(body, 2)
            recovery_url_length = int.from_bytes(recovery_url_length_bytes, "big")
            recovery_url: Optional[str] = None
            if recovery_url_length > 0:
                recovery_url_bytes = await async_read_exact(body, recovery_url_length)
                recovery_url = recovery_url_bytes.decode("utf-8")
        finally:
            await stream.aclose()
    except ValueError:
        return Response(status_code=400)

    auth_at = time.time()
    auth_result = await config.is_subscribe_exact_allowed(
        url=url,
        recovery=recovery_url,
        exact=topic,
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

    db_result = await config.subscribe_exact(
        url=url, recovery=recovery_url, exact=topic
    )

    if db_result == "conflict":
        return Response(status_code=409)
    elif db_result == "unavailable":
        return Response(status_code=503)
    elif db_result != "success":
        return Response(status_code=500)

    return Response(status_code=202)
