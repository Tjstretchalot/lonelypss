import time
from typing import Annotated, Optional
from fastapi import APIRouter, Header, Request, Response
import io

from lonelypss.middleware.config import get_config_from_request
from lonelypss.routes.subscribe import SubscribeType, parse_subscribe_payload


router = APIRouter()


@router.post(
    "/v1/unsubscribe",
    status_code=202,
    responses={
        "400": {"description": "The body was not formatted correctly"},
        "401": {"description": "Authorization header is required but not provided"},
        "403": {"description": "Authorization header is provided but invalid"},
        "409": {"description": "The subscription does not exist"},
        "500": {"description": "Unexpected error occurred"},
        "503": {"description": "Service is unavailable, try again soon"},
    },
)
async def unsubscribe(
    request: Request, authorization: Annotated[Optional[str], Header()] = None
) -> Response:
    """Unsubscribes the given URL from the given pattern. The body should be
    formatted as the following sequence:

    - 2 bytes: the length of the url, big-endian, unsigned
    - N bytes: the url. must be valid utf-8
    - 1 byte: either 0 or 1 (big-endian, unsigned) to indicate an exact match
      (0) or glob-style match (1).
    - 2 bytes: the length of the pattern or exact match.
    - M bytes: the pattern or exact match. if glob-style, must be utf-8,
      otherwise unrestricted

    The response has an arbitrary body (generally empty) and one of the
    following status codes:

    - 200 Okay: the subscription was removed
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
        db_result = await config.unsubscribe_exact(url=parsed.url, exact=parsed.topic)
    else:
        db_result = await config.unsubscribe_glob(url=parsed.url, glob=parsed.glob)

    if db_result == "not_found":
        return Response(status_code=409)
    elif db_result == "unavailable":
        return Response(status_code=503)
    elif db_result != "success":
        return Response(status_code=500)

    return Response(status_code=200)
