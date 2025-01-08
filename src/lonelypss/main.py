# ruff: noqa: T201
import json
import os
import secrets
from argparse import ArgumentParser
from typing import Literal, Optional, Set


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Always required for clarity of the operation.",
    )
    parser.add_argument(
        "--db",
        default="sqlite",
        choices=["sqlite", "rqlite"],
        help="Which backing database to use",
    )
    parser.add_argument(
        "--incoming-auth",
        default="hmac",
        choices=["hmac", "token", "none"],
        help="How to verify incoming requests to subscribe or unsubscribe from endpoints",
    )
    parser.add_argument(
        "--incoming-auth-token",
        help="If specified, the secret to use for incoming auth. Ignored unless the incoming auth strategy requires a secret (hmac, token)",
    )
    parser.add_argument(
        "--outgoing-auth",
        default="hmac",
        choices=["hmac", "token", "none"],
        help="How to verify outgoing requests notifying subscribers",
    )
    parser.add_argument(
        "--outgoing-auth-token",
        help="If specified, the secret to use for outgoing auth. Ignored unless the outgoing auth strategy requires a secret (hmac, token)",
    )
    args = parser.parse_args()
    if not args.setup:
        raise Exception("must provide --setup")

    setup_locally(
        db=args.db,
        incoming_auth=args.incoming_auth,
        incoming_auth_token=args.incoming_auth_token,
        outgoing_auth=args.outgoing_auth,
        outgoing_auth_token=args.outgoing_auth_token,
    )


def setup_locally(
    *,
    db: Literal["sqlite", "rqlite"],
    incoming_auth: Literal["hmac", "token", "none"],
    incoming_auth_token: Optional[str],
    outgoing_auth: Literal["hmac", "token", "none"],
    outgoing_auth_token: Optional[str],
) -> None:
    print(
        "httppubserver - Setup\n"
        f"  - db: {db}\n"
        f"  - incoming-auth: {incoming_auth}\n"
        f"  - incoming-auth-token: {'not specified' if incoming_auth_token is None else 'specified'}\n"
        f"  - outgoing-auth: {outgoing_auth}\n"
        f"  - outgoing-auth-token: {'not specified' if outgoing_auth_token is None else 'specified'}"
    )

    print("Prechecking...")
    for file in [
        "broadcast-secrets.json",
        "subscriber-secrets.json",
        "main.py",
        "requirements.txt",
    ]:
        if os.path.exists(file):
            raise Exception(f"{file} already exists, refusing to overwrite")

    print("Storing secrets...")
    if incoming_auth_token is None:
        incoming_auth_token = secrets.token_urlsafe(64)

    if outgoing_auth_token is None:
        outgoing_auth_token = secrets.token_urlsafe(64)

    auth_for_requests_to_broadcasters = (
        {
            "type": incoming_auth,
            "secret": incoming_auth_token,
        }
        if incoming_auth != "none"
        else None
    )
    auth_for_requests_to_subscribers = (
        {
            "type": outgoing_auth,
            "secret": outgoing_auth_token,
        }
        if outgoing_auth != "none"
        else None
    )

    with open("broadcaster-secrets.json", "w") as f:
        f.write(
            json.dumps(
                {
                    "version": "1",
                    **(
                        {"incoming": auth_for_requests_to_broadcasters}
                        if auth_for_requests_to_broadcasters is not None
                        else {}
                    ),
                    **(
                        {"outgoing": (auth_for_requests_to_subscribers)}
                        if auth_for_requests_to_subscribers is not None
                        else {}
                    ),
                },
                indent=2,
            )
            + "\n"
        )
    with open("subscriber-secrets.json", "w") as f:
        f.write(
            json.dumps(
                {
                    "version": "1",
                    **(
                        {"outgoing": auth_for_requests_to_broadcasters}
                        if auth_for_requests_to_broadcasters is not None
                        else {}
                    ),
                    **(
                        {"incoming": (auth_for_requests_to_subscribers)}
                        if auth_for_requests_to_subscribers is not None
                        else {}
                    ),
                },
                indent=2,
            )
            + "\n"
        )

    print("Building entrypoint...")

    requirements: Set[str] = set()

    if db == "sqlite":
        db_code = 'SqliteDBConfig("subscriptions.db")'
    else:
        db_code = "TODO()"

    need_secrets = False
    if incoming_auth == "token":
        incoming_auth_code = (
            "IncomingTokenAuth(\n"
            '        subscriber_token=auth_secrets["incoming"]["secret"],\n'
            '        broadcaster_token=auth_secrets["outgoing"]["secret"],\n'
            "    )"
        )
        need_secrets = True
    elif incoming_auth == "hmac":
        hmac_db = "TODO()"
        if db == "sqlite":
            hmac_db = (
                "incoming_auth_config.IncomingHmacAuthSqliteDBConfig(\n"
                '            "recent-hmac-tokens.db"\n'
                "        )"
            )
        incoming_auth_code = (
            "IncomingHmacAuth(\n"
            '        subscriber_secret=auth_secrets["incoming"]["secret"],\n'
            '        broadcaster_secret=auth_secrets["outgoing"]["secret"],\n'
            f"        db_config={hmac_db},\n"
            "    )"
        )
        need_secrets = True
    elif incoming_auth == "none":
        incoming_auth_code = "IncomingNoneAuth()"
    else:
        incoming_auth_code = "TODO()"

    if outgoing_auth == "token":
        outgoing_auth_code = (
            'OutgoingTokenAuth(\n        auth_secrets["outgoing"]["secret"]\n    )'
        )
        need_secrets = True
    elif outgoing_auth == "hmac":
        outgoing_auth_code = (
            'OutgoingHmacAuth(\n        auth_secrets["outgoing"]["secret"]\n    )'
        )
    elif outgoing_auth == "none":
        outgoing_auth_code = "OutgoingNoneAuth()"
    else:
        outgoing_auth_code = "TODO()"

    load_auth_secrets = (
        ""
        if not need_secrets
        else """
    with open("broadcaster-secrets.json", "r") as f:
        auth_secrets = json.load(f)
"""
    )
    import_json = "import json\n" if need_secrets else ""

    import_config = "\n".join(
        sorted(
            [
                f"import lonelypss.config.helpers.{db}_db_config as db_config",
                f"import lonelypss.config.helpers.{incoming_auth}_auth_config as incoming_auth_config",
                f"import lonelypss.config.helpers.{outgoing_auth}_auth_config as outgoing_auth_config",
            ]
        )
    )

    with open("main.py", "w") as f:
        f.write(
            f"""{import_json}from contextlib import asynccontextmanager
from typing import AsyncIterator

{import_config}
from fastapi import FastAPI
from lonelypss.bknd.sweep_missed import sweep_missed
from lonelypss.config.auth_config import AuthConfigFromParts
from lonelypss.config.config import (
    CompressionConfigFromParts,
    Config,
    ConfigFromParts,
    GenericConfigFromValues,
    MissedRetryStandard,
)
from lonelypss.config.lifespan import setup_config, teardown_config
from lonelypss.middleware.config import ConfigMiddleware
from lonelypss.middleware.ws_receiver import WSReceiverMiddleware
from lonelypss.router import router as HttpPubSubRouter
from lonelypss.util.ws_receiver import SimpleFanoutWSReceiver


def _make_config() -> Config:{load_auth_secrets}
    db = db_config.{db_code}
    incoming_auth = incoming_auth_config.{incoming_auth_code}
    outgoing_auth = outgoing_auth_config.{outgoing_auth_code}

    return ConfigFromParts(
        auth=AuthConfigFromParts(incoming=incoming_auth, outgoing=outgoing_auth),
        db=db,
        generic=GenericConfigFromValues(
            message_body_spool_size=1024 * 1024 * 10,
            outgoing_http_timeout_total=30,
            outgoing_http_timeout_connect=None,
            outgoing_http_timeout_sock_read=5,
            outgoing_http_timeout_sock_connect=5,
            websocket_accept_timeout=2,
            websocket_max_pending_sends=255,
            websocket_max_unprocessed_receives=255,
            websocket_large_direct_send_timeout=0.3,
            websocket_send_max_unacknowledged=3,
            websocket_minimal_headers=True,
            sweep_missed_interval=10,
        ),
        missed=MissedRetryStandard(
            expo_factor=1,
            expo_base=2,
            expo_max=10,
            max_retries=20,
            constant=1,
            jitter=2,
        ),
        compression=CompressionConfigFromParts(
            compression_allowed=True,
            compression_dictionary_by_id=dict(),
            outgoing_max_ws_message_size=16 * 1024 * 1024,
            allow_training=True,
            compression_min_size=32,
            compression_trained_max_size=16 * 1024,
            compression_training_low_watermark=100 * 1024,
            compression_training_high_watermark=10 * 1024 * 1024,
            compression_retrain_interval_seconds=60 * 60 * 60,
            decompression_max_window_size=8 * 1024 * 1024,
        ),
    )


config = _make_config()
fanout = SimpleFanoutWSReceiver(
    receiver_url="http://127.0.0.1:3003/v1/receive_for_websockets",
    recovery="http://127.0.0.1:3003/v1/missed_for_websockets",
    db=config,
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await setup_config(config)
    try:
        async with fanout, sweep_missed(config):
            yield
    finally:
        await teardown_config(config)


app = FastAPI(lifespan=lifespan)
app.add_middleware(ConfigMiddleware, config=config)
app.add_middleware(WSReceiverMiddleware, ws_receiver=fanout)
app.include_router(HttpPubSubRouter)
app.router.redirect_slashes = False
"""
        )

    with open("requirements.txt", "w") as f:
        f.write("\n".join(list(sorted(requirements))))

    print("Done! Make sure to install from requirements.txt and pip freeze again!")


if __name__ == "__main__":
    main()
