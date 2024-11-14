from fastapi import Request
from starlette.types import ASGIApp, Scope, Receive, Send

from httppubsubserver.config.config import Config


class ConfigMiddleware:
    """Injects `httppubsubserver_config` into the scope of the request. Required for all
    the routes to function correctly.
    """

    def __init__(self, app: ASGIApp, config: Config):
        self.app = app
        self.config = config

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope["httppubsubserver_config"] = self.config
        await self.app(scope, receive, send)


def get_config_from_request(request: Request) -> Config:
    """Retrieves the `httppubsubserver_config` from the request scope. This is only
    available if the `ConfigMiddleware` is being used
    """
    try:
        return request.scope["httppubsubserver_config"]
    except KeyError:
        raise Exception("ConfigMiddleware not injected into request scope")
