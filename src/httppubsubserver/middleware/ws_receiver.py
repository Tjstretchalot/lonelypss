from fastapi.requests import HTTPConnection
from starlette.types import ASGIApp, Scope, Receive, Send

from httppubsubserver.util.ws_receiver import FanoutWSReceiver


class WSReceiverMiddleware:
    """Injects `httppubsubserver_ws_receiver` into the scope of the request. Required for
    websocket related routes (receive_for_websockets, websocket_endpoint) to function
    correctly
    """

    def __init__(self, app: ASGIApp, ws_receiver: FanoutWSReceiver):
        self.app = app
        self.ws_receiver = ws_receiver

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope["httppubsubserver_ws_receiver"] = self.ws_receiver
        await self.app(scope, receive, send)


def get_ws_receiver_from_request(request: HTTPConnection) -> FanoutWSReceiver:
    """Retrieves the `httppubsubserver_ws_receiver` from the request scope. This is only
    available if the `WSReceiverMiddleware` is being used
    """
    try:
        return request.scope["httppubsubserver_ws_receiver"]
    except KeyError:
        raise Exception("WSReceiverMiddleware not injecting into request scope")
