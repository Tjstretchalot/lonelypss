from fastapi import APIRouter
import httppubsubserver.routes.subscribe
import httppubsubserver.routes.unsubscribe
import httppubsubserver.routes.notify

router = APIRouter()
router.include_router(httppubsubserver.routes.subscribe.router)
router.include_router(httppubsubserver.routes.unsubscribe.router)
router.include_router(httppubsubserver.routes.notify.router)