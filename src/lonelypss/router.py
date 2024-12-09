from fastapi import APIRouter
import lonelypss.routes.subscribe
import lonelypss.routes.unsubscribe
import lonelypss.routes.notify
import lonelypss.routes.websocket_endpoint

router = APIRouter()
router.include_router(lonelypss.routes.subscribe.router)
router.include_router(lonelypss.routes.unsubscribe.router)
router.include_router(lonelypss.routes.notify.router)
router.include_router(lonelypss.routes.websocket_endpoint.router)
