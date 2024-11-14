from argparse import ArgumentParser
from typing import Literal, Optional


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--setup",
        action="store_true",
        description="Always required for clarity of the operation.",
    )
    parser.add_argument(
        "--db",
        default="sqlite",
        choices=["memory", "sqlite", "rqlite"],
        description="Which backing database to use",
    )
    parser.add_argument(
        "--incoming-auth",
        default="hmac",
        choices=["hmac", "token", "none"],
        description="How to verify incoming requests to subscribe or unsubscribe from endpoints",
    )
    parser.add_argument(
        "--incoming-auth-token",
        description="If specified, the secret to use for incoming auth. Ignored unless the incoming auth strategy requires a secret (hmac, token)",
    )
    parser.add_argument(
        "--outgoing-auth",
        default="hmac",
        choices=["hmac", "token", "none"],
        description="How to verify outgoing requests notifying subscribers",
    )
    parser.add_argument(
        "--outgoing-auth-token",
        description="If specified, the secret to use for outgoing auth. Ignored unless the outgoing auth strategy requires a secret (hmac, token)",
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
    db: Literal["memory", "sqlite", "rqlite"],
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


if __name__ == "__main__":
    main()
