from fastapi.requests import HTTPConnection
from starlette.types import ASGIApp, Receive, Scope, Send

from lonelypss.config.config import Config


class ConfigMiddleware:
    """Injects `lonelypss_config` into the scope of the request. Required for all
    the routes to function correctly.
    """

    def __init__(self, app: ASGIApp, config: Config):
        self.app = app
        self.config = config

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope["lonelypss_config"] = self.config
        await self.app(scope, receive, send)


def get_config_from_request(request: HTTPConnection) -> Config:
    """Retrieves the `lonelypss_config` from the request scope. This is only
    available if the `ConfigMiddleware` is being used
    """
    try:
        return request.scope["lonelypss_config"]
    except KeyError:
        raise Exception("ConfigMiddleware not injecting into request scope")
