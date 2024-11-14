from argparse import ArgumentParser
import json
import os
import secrets
from typing import Literal, Optional, Set


def main():
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
):
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

    if incoming_auth != "none" or outgoing_auth != "none":
        to_dump = (
            json.dumps(
                {
                    "version": "1",
                    **(
                        {
                            "incoming": {
                                "type": incoming_auth,
                                "secret": incoming_auth_token,
                            }
                        }
                        if incoming_auth != "none"
                        else {}
                    ),
                    **(
                        {
                            "outgoing": {
                                "type": outgoing_auth,
                                "secret": outgoing_auth_token,
                            }
                        }
                        if outgoing_auth != "none"
                        else {}
                    ),
                },
                indent=2,
            )
            + "\n"
        )
        with open("broadcaster-secrets.json", "w") as f:
            f.write(to_dump)
        with open("subscriber-secrets.json", "w") as f:
            f.write(to_dump)

    print("Building entrypoint...")

    requirements: Set[str] = set()

    if db == "sqlite":
        db_code = 'SqliteDBConfig("subscriptions.db")'
    else:
        db_code = "TODO()"

    if incoming_auth == "token":
        incoming_auth_code = (
            f'IncomingTokenAuth(\n        secrets["incoming"]["secret"]\n    )'
        )
    else:
        incoming_auth_code = "TODO()"

    if outgoing_auth == "token":
        outgoing_auth_code = (
            f'OutgoingTokenAuth(\n        secrets["outgoing"]["secret"]\n    )'
        )
    else:
        outgoing_auth_code = "TODO()"

    with open("main.py", "w") as f:

        f.write(
            f"""from contextlib import asynccontextmanager
from fastapi import FastAPI
import httppubsubserver.config.helpers.{db}_db_config as db_config
import httppubsubserver.config.helpers.{incoming_auth}_auth_config as incoming_auth_config
import httppubsubserver.config.helpers.{outgoing_auth}_auth_config as outgoing_auth_config
from httppubsubserver.middleware.config import ConfigMiddleware
from httppubsubserver.config.config import (
    AuthConfigFromParts,
    ConfigFromParts,
    GenericConfigFromValues,
)
from httppubsubserver.router import router as HttpPubSubRouter
import json


def _make_config():
    with open("broadcaster-secrets.json", "r") as f:
        secrets = json.load(f)

    db = db_config.{db_code}
    incoming_auth = incoming_auth_config.{incoming_auth_code}
    outgoing_auth = outgoing_auth_config.{outgoing_auth_code}

    return (
        (db, incoming_auth, outgoing_auth),
        ConfigFromParts(
            auth=AuthConfigFromParts(incoming=incoming_auth, outgoing=outgoing_auth),
            db=db, 
            generic=GenericConfigFromValues(
                message_body_spool_size=1024 * 1024 * 10,
                outgoing_http_timeout_total=30,
                outgoing_http_timeout_connect=None,
                outgoing_http_timeout_sock_read=5,
                outgoing_http_timeout_sock_connect=5,
            ),
        ),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    for acm in config_acms:
        await acm.__aenter__()
    yield
    for acm in config_acms:
        await acm.__aexit__(None, None, None)


config_acms, config = _make_config()

app = FastAPI()
app.add_middleware(ConfigMiddleware, config=config)
app.include_router(HttpPubSubRouter)
app.router.redirect_slashes = False
"""
        )

    with open("requirements.txt", "w") as f:
        f.write("\n".join(list(sorted(requirements))))

    print("Done! Make sure to install from requirements.txt and pip freeze again!")


if __name__ == "__main__":
    main()
