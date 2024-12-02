from fastapi import APIRouter
import httppubsubserver.routes.subscribe
import httppubsubserver.routes.unsubscribe
import httppubsubserver.routes.notify
import httppubsubserver.routes.websocket_endpoint

router = APIRouter()
router.include_router(httppubsubserver.routes.subscribe.router)
router.include_router(httppubsubserver.routes.unsubscribe.router)
router.include_router(httppubsubserver.routes.notify.router)
router.include_router(httppubsubserver.routes.websocket_endpoint.router)
