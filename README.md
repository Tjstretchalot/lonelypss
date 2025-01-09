# lonelypss

## PROJECT STAGE - PRE-ALPHA

This project is in the development stage 2 - pre-alpha. This means that
the project is still in the early stages of development, and is not yet
stable. The current primary focus of the library is building the test
suite in `lonelypst`

## Overview

This library is for when you need best-effort pub/sub behavior across the
network, but don't want to maintain open connections with all your subscribers
(i.e., you want some lonely clients), don't want polling, don't want message
buffering, and might want very large (>4gb) messages. For example, when
subscribing to restart/upgrade requests within CI/CD, or for eagerly filling or
busting local instance disk caches, or when revoking JSON web tokens early due
to explicit logout (while still avoiding a network roundtrip on every
authorization check by maintaining the revocation list locally).

This is a Python server that registers subscriptions into a relational database,
and sends messages via webhooks (HTTP POST) requests to subscribers. It's
agnostic to whether it's hosted within your network or across the internet, and
can use http or https.

When you know increased volume is expected for a particular subscriber, you can
gracefully upgrade by registering over a websocket connection instead. In that
case, this library will internally register itself as an http subscriber and
forward messages across the websocket connection; when running a single
broadcaster server this results in minimal overhead, and when running multiple
this ensures you still receive messages even if they are sent to a different
server.

The main thing that you need to watch out for / maintain is subscriptions that
are no longer needed, but since they are all visible within a simple SQL
structure on your database, your existing tools will be able to help you with
that. Further, this has idempotent endpoints for http subscribers to ensure they
naturally recover from errors (rather than cascading them)

## Additional Features

- glob-style pattern matching for subscriptions (e.g., `foo/*` will match
  `foo/bar`, `foo/baz`, etc.). exact semantics may differ but will generally behave
  like unix file globbing syntax.
- can optionally require an authorization token for receiving configuration requests (https required)
- can optionally require hmac-based message signing for receiving messages (https not required)
- can optionally provide an authorization token for sending messages (https required)
- can optionally provide an hmac-based message signing key for sending messages (https not required)
- if you have an existing FastAPI project and you don't want to run a
  separate server, you can just add the `APIRouter` provided to your existing
  FastAPI app

## Terminology

We'll call this server, which accepts requests to subscribe/unsubscribe and
initiates the sending of messages the broadcaster. We'll call the clients that
receive messages the subscribers. Note that the subscribers also run an HTTP
server that is capable of receiving POST requests from the broadcaster.

For messages themselves, we say they have a topic and body, both of which are
arbitrary bytes.

## Restrictions

- **urls**, including any query parameters and fragments, must be valid utf-8 and
  cannot be longer than 2^16 - 1 (65535) bytes
- **topics** cannot be longer than 2^16 - 1 (65535) bytes (no charset restrictions).
  Malicious topic names can cause denial of service via slow to evaluate
  patterns, but only if they pass the authorization step.
- **patterns** cannot be longer than 2^16 - 1 (65535) bytes and must be valid utf-8.
  exact subscriptions are supported and cannot be longer than 2^16 - 1 (65535) bytes
  and have no charset restrictions. note that patterns will only match topics that
  are valid utf-8, so foo/\* will not match `b"foo/stuff\xc3\x28"`
- **messages** cannot be longer than 2^64 - 1 bytes (no charset restrictions), and
  will page to file by default at 10MB. It is not unusual for another part of the
  stack to restrict POST body sizes at a much smaller value, such as 50MB, by default.

## Notes

If the same url has multiple subscriptions that match the same message (via glob
patterns), it will receive multiple copies of the message. If the same url tries
to register the exact same subscription multiple times, it will only be
registered once (a 409 Conflict will be returned from /v1/subscribe/\*)

## Installation

The following is the basic way to setup the broadcaster via HTTP. If you want
subscribers sending configuration messages (e.g., subscription requests) to
connect to the broadcaster over HTTPS, refer to
https://www.uvicorn.org/deployment/#running-with-https

```bash
python -m venv venv
source venv/bin/activate
python -m pip install -U pip
pip install lonelypss[standard]

lonelypss --setup --db sqlite --to-broadcasters-auth hmac --to-subscribers-auth hmac
pip install -r requirements.txt
pip freeze > requirements.txt

# main.py will be generated that exports a Config object; feel free to modify as you wish
# broadcaster-secrets.json contains the secrets that the broadcaster needs - by default,
#   main.py will load from this to reduce the odds you accidentally check them in

# subscriber-secrets.json contains the secrets that the subscribers need; see the
#  `lonelypsc` package for more information on how to use this.

# Run the server
uvicorn main:app
```

Comes with the following database options:

- `sqlite`: execute SQL commands on a local, memory or disk-backed SQLite database via the
  [sqlite3](https://docs.python.org/3/library/sqlite3.html) built-in library.
- `rqlite`: execute SQL commands on a [rqlite](https://github.com/rqlite/rqlite)
  cluster via [rqdb](https://github.com/Tjstretchalot/rqdb) to connect to a

Adding other options, such as postgres, is relatively easy and accepted via pull request.
These options are not stored in the core server - they are just templates that generate
the required Config object.
