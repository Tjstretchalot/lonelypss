from fastapi import APIRouter

import lonelypss.routes.check_subscriptions
import lonelypss.routes.missed_for_websockets
import lonelypss.routes.notify
import lonelypss.routes.receive_for_websockets
import lonelypss.routes.set_subscriptions
import lonelypss.routes.subscribe_exact
import lonelypss.routes.subscribe_glob
import lonelypss.routes.unsubscribe_exact
import lonelypss.routes.unsubscribe_glob
import lonelypss.routes.websocket_endpoint

router = APIRouter()
router.include_router(lonelypss.routes.check_subscriptions.router)
router.include_router(lonelypss.routes.missed_for_websockets.router)
router.include_router(lonelypss.routes.notify.router)
router.include_router(lonelypss.routes.receive_for_websockets.router)
router.include_router(lonelypss.routes.set_subscriptions.router)
router.include_router(lonelypss.routes.subscribe_exact.router)
router.include_router(lonelypss.routes.subscribe_glob.router)
router.include_router(lonelypss.routes.unsubscribe_exact.router)
router.include_router(lonelypss.routes.unsubscribe_glob.router)
router.include_router(lonelypss.routes.websocket_endpoint.router)
