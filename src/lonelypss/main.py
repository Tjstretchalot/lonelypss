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
        "--to-broadcasters-auth",
        default="hmac",
        choices=["hmac", "token", "none"],
        help="How to verify requests to the broadcaster",
    )
    parser.add_argument(
        "--to-broadcasters-secret",
        help="If specified, the secret to use for requests to broadcasters. Ignored unless the to-broadcaster auth strategy requires a secret (hmac, token)",
    )
    parser.add_argument(
        "--to-subscribers-auth",
        default="hmac",
        choices=["hmac", "token", "none"],
        help="How to verify requests to subscribers",
    )
    parser.add_argument(
        "--to-subscribers-secret",
        help="If specified, the secret to use for requests to subscribers. Ignored unless the to-subscriber auth strategy requires a secret (hmac, token)",
    )
    args = parser.parse_args()
    if not args.setup:
        raise Exception("must provide --setup")

    setup_locally(
        db=args.db,
        to_broadcasters_auth=args.to_broadcasters_auth,
        to_broadcasters_secret=args.to_broadcasters_secret,
        to_subscribers_auth=args.to_subscribers_auth,
        to_subscribers_secret=args.to_subscribers_secret,
    )


def setup_locally(
    *,
    db: Literal["sqlite", "rqlite"],
    to_broadcasters_auth: Literal["hmac", "token", "none"],
    to_broadcasters_secret: Optional[str],
    to_subscribers_auth: Literal["hmac", "token", "none"],
    to_subscribers_secret: Optional[str],
) -> None:
    print(
        "httppubserver - Setup\n"
        f"  - db: {db}\n"
        f"  - to-broadcasters-auth: {to_broadcasters_auth}\n"
        f"  - to-broadcasters-secret: {'not specified' if to_broadcasters_secret is None else 'specified'}\n"
        f"  - to-subscribers-auth: {to_subscribers_auth}\n"
        f"  - to-subscribers-secret: {'not specified' if to_subscribers_secret is None else 'specified'}"
    )

    print("Prechecking...")
    for file in [
        "broadcaster-secrets.json",
        "subscriber-secrets.json",
        "main.py",
        "requirements.txt",
    ]:
        if os.path.exists(file):
            raise Exception(f"{file} already exists, refusing to overwrite")

    print("Storing secrets...")
    if to_broadcasters_secret is None:
        to_broadcasters_secret = secrets.token_urlsafe(64)

    if to_subscribers_secret is None:
        to_subscribers_secret = secrets.token_urlsafe(64)

    auth_for_requests_to_broadcasters = (
        {
            "type": to_broadcasters_auth,
            "secret": to_broadcasters_secret,
        }
        if to_broadcasters_auth != "none"
        else None
    )
    auth_for_requests_to_subscribers = (
        {
            "type": to_subscribers_auth,
            "secret": to_subscribers_secret,
        }
        if to_subscribers_auth != "none"
        else None
    )

    for target in ["broadcaster", "subscriber"]:
        with open(f"{target}-secrets.json", "w") as f:
            f.write(
                json.dumps(
                    {
                        "version": "2",
                        **(
                            {"to-broadcaster": auth_for_requests_to_broadcasters}
                            if auth_for_requests_to_broadcasters is not None
                            else {}
                        ),
                        **(
                            {"to-subscriber": (auth_for_requests_to_subscribers)}
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

    auth_setup_code = ""

    need_secrets = False
    if to_broadcasters_auth == "token":
        to_broadcaster_auth_code = (
            "ToBroadcasterTokenAuth(\n"
            '        token=auth_secrets["to-broadcaster"]["secret"],\n'
            "    )"
        )
        need_secrets = True
    elif to_broadcasters_auth == "hmac":
        if db == "sqlite":
            auth_setup_code = (
                "hmac_db: incoming_auth_config.IncomingHmacAuthDBConfig = (\n"
                '        incoming_auth_config.IncomingHmacAuthSqliteDBConfig("recent-hmacs.db")\n'
                "    )"
            )
        to_broadcaster_auth_code = (
            "ToBroadcasterHmacAuth(\n"
            '        secret=auth_secrets["to-broadcaster"]["secret"],\n'
            "        db_config=hmac_db,\n"
            "    )"
        )
        need_secrets = True
    elif to_broadcasters_auth == "none":
        to_broadcaster_auth_code = "ToBroadcasterNoneAuth()"
    else:
        to_broadcaster_auth_code = "TODO()"

    if to_subscribers_auth == "token":
        to_subscriber_auth_code = 'ToSubscriberTokenAuth(\n        auth_secrets["to-subscriber"]["secret"]\n    )'
        need_secrets = True
    elif to_subscribers_auth == "hmac":
        if db == "sqlite":
            if to_broadcasters_auth != "hmac":
                auth_setup_code = (
                    "hmac_db = incoming_auth_config.IncomingHmacAuthSqliteDBConfig(\n"
                    '        "recent-hmacs.db"\n'
                    "    )"
                )
            else:
                auth_setup_code += "\n    hmac_db = incoming_auth_config.IncomingHmacAuthDBReentrantConfig(hmac_db)"

        to_subscriber_auth_code = (
            "ToSubscriberHmacAuth(\n"
            '        secret=auth_secrets["to-subscriber"]["secret"],\n'
            "        db_config=hmac_db,\n"
            "    )"
        )
    elif to_subscribers_auth == "none":
        to_subscriber_auth_code = "ToSubscriberNoneAuth()"
    else:
        to_subscriber_auth_code = "TODO()"

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
                f"import lonelypsp.auth.helpers.{to_broadcasters_auth}_auth_config as incoming_auth_config",
                f"import lonelypsp.auth.helpers.{to_subscribers_auth}_auth_config as outgoing_auth_config",
            ]
        )
    )

    if auth_setup_code:
        auth_setup_code = f"\n    {auth_setup_code}"

    with open("main.py", "w") as f:
        f.write(
            f"""{import_json}from contextlib import asynccontextmanager
from typing import AsyncIterator

{import_config}
import lonelypss.config.helpers.{db}_db_config as db_config
from fastapi import FastAPI
from lonelypsp.auth.config import AuthConfigFromParts
from lonelypss.bknd.sweep_missed import sweep_missed
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
    db = db_config.{db_code}{auth_setup_code}
    to_broadcaster_auth = incoming_auth_config.{to_broadcaster_auth_code}
    to_subscriber_auth = outgoing_auth_config.{to_subscriber_auth_code}

    return ConfigFromParts(
        auth=AuthConfigFromParts(
            to_broadcaster=to_broadcaster_auth, to_subscriber=to_subscriber_auth
        ),
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
