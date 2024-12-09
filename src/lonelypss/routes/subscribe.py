from enum import Enum, auto
import time
from typing import Annotated, Literal, Optional, Union
from fastapi import APIRouter, Header, Request, Response
import io
from dataclasses import dataclass
from lonelypss.middleware.config import get_config_from_request
from lonelypss.util.sync_io import SyncReadableBytesIO, read_exact


router = APIRouter()


class SubscribeType(Enum):
    EXACT = auto()
    GLOB = auto()


@dataclass
class SubscribePayloadExact:
    type: Literal[SubscribeType.EXACT]
    url: str
    topic: bytes


@dataclass
class SubscribePayloadGlob:
    type: Literal[SubscribeType.GLOB]
    url: str
    glob: str


SubscribePayload = Union[SubscribePayloadExact, SubscribePayloadGlob]


def parse_subscribe_payload(body: SyncReadableBytesIO) -> SubscribePayload:
    """Parses the body intended for the /v1/subscribe endpoint. Raises
    ValueError if the body is malformed.

    The body is expected to be buffered as this may make many small reads
    """
    url_len = int.from_bytes(read_exact(body, 2), "big", signed=False)
    url_bytes = read_exact(body, url_len)
    url = url_bytes.decode("utf-8", errors="strict")

    match_type = int.from_bytes(read_exact(body, 1), "big", signed=False)
    if match_type not in (0, 1):
        raise ValueError(f"unknown match type (0 or 1 expected, got {match_type})")

    is_exact = match_type == 0

    pattern_len = int.from_bytes(read_exact(body, 2), "big", signed=False)
    pattern_bytes = read_exact(body, pattern_len)

    if is_exact:
        return SubscribePayloadExact(
            type=SubscribeType.EXACT, url=url, topic=pattern_bytes
        )

    glob = pattern_bytes.decode("utf-8")
    return SubscribePayloadGlob(type=SubscribeType.GLOB, url=url, glob=glob)


@router.post(
    "/v1/subscribe",
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
async def subscribe(
    request: Request, authorization: Annotated[Optional[str], Header()] = None
) -> Response:
    """Subscribes the given URL to the given pattern. The body should be
    formatted as the following sequence:

    - 2 bytes: the length of the url, big-endian, unsigned
    - N bytes: the url. must be valid utf-8
    - 1 byte: either 0 or 1 (big-endian, unsigned) to indicate an exact match
      (0) or glob-style match (1).
    - 2 bytes: the length of the pattern or exact match.
    - M bytes: the pattern or exact match. if glob-style, must be utf-8,
      otherwise unrestricted

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

    body = await request.body()
    if len(body) < 5 or len(body) > 2 + 65535 + 1 + 2 + 65535:
        return Response(status_code=400)

    try:
        parsed = parse_subscribe_payload(io.BytesIO(body))
    except ValueError:
        return Response(status_code=400)

    auth_at = time.time()
    if parsed.type == SubscribeType.EXACT:
        auth_result = await config.is_subscribe_exact_allowed(
            url=parsed.url, exact=parsed.topic, now=auth_at, authorization=authorization
        )
    else:
        auth_result = await config.is_subscribe_glob_allowed(
            url=parsed.url, glob=parsed.glob, now=auth_at, authorization=authorization
        )

    if auth_result == "unauthorized":
        return Response(status_code=401)
    elif auth_result == "forbidden":
        return Response(status_code=403)
    elif auth_result == "unavailable":
        return Response(status_code=503)
    elif auth_result != "ok":
        return Response(status_code=500)

    if parsed.type == SubscribeType.EXACT:
        db_result = await config.subscribe_exact(url=parsed.url, exact=parsed.topic)
    else:
        db_result = await config.subscribe_glob(url=parsed.url, glob=parsed.glob)

    if db_result == "conflict":
        return Response(status_code=409)
    elif db_result == "unavailable":
        return Response(status_code=503)
    elif db_result != "success":
        return Response(status_code=500)

    return Response(status_code=202)
