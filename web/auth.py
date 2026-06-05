"""Basic authentication for the web UI."""

import base64
import binascii
import os
import secrets
from typing import Optional

from starlette.responses import PlainTextResponse


AUTH_REALM = "AF DAGs Helper"
PUBLIC_PATHS = {"/health"}
PROTECTED_SCOPE_TYPES = {"http", "websocket"}


class BasicAuthMiddleware:
    """Protect all web UI routes except public health checks."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in PROTECTED_SCOPE_TYPES or scope.get("path") in PUBLIC_PATHS:
            await self.app(scope, receive, send)
            return

        authorization = _get_header(scope, "authorization")
        username = os.environ.get("AF_DAGS_HELPER_AUTH_USER", "admin")
        password = os.environ.get("AF_DAGS_HELPER_AUTH_PASSWORD", "")
        if not password:
            await _reject(scope, receive, send, 503, "Authentication is not configured")
            return

        if not _is_valid_authorization(authorization, username, password):
            await _reject(
                scope,
                receive,
                send,
                401,
                "Authentication required",
                {"WWW-Authenticate": f'Basic realm="{AUTH_REALM}"'},
            )
            return

        await self.app(scope, receive, send)


def _get_header(scope, name: str) -> Optional[str]:
    target = name.lower().encode("latin1")
    for key, value in scope.get("headers", []):
        if key.lower() == target:
            return value.decode("latin1")
    return None


async def _reject(scope, receive, send, status_code: int, body: str, headers=None) -> None:
    if scope["type"] == "websocket":
        await send({"type": "websocket.close", "code": 1008})
        return

    response = PlainTextResponse(body, status_code=status_code, headers=headers)
    await response(scope, receive, send)


def _is_valid_authorization(header: Optional[str], expected_user: str, expected_password: str) -> bool:
    if not header or not header.startswith("Basic "):
        return False

    try:
        decoded = base64.b64decode(header[6:], validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        return False

    username, separator, password = decoded.partition(":")
    if not separator:
        return False

    return (
        secrets.compare_digest(username, expected_user)
        and secrets.compare_digest(password, expected_password)
    )
