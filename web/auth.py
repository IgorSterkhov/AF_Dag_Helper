"""Basic authentication for the web UI."""

import base64
import binascii
import hashlib
import hmac
from http.cookies import CookieError, SimpleCookie
import os
import secrets
from typing import Optional

from starlette.responses import PlainTextResponse


AUTH_REALM = "AF DAGs Helper"
PUBLIC_PATHS = {"/health"}
PROTECTED_SCOPE_TYPES = {"http", "websocket"}
SESSION_COOKIE = "af_dags_helper_auth"


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

        has_valid_session = _has_valid_session_cookie(scope, username, password)
        has_valid_basic_auth = _is_valid_authorization(authorization, username, password)
        if not has_valid_session and not has_valid_basic_auth:
            await _reject(
                scope,
                receive,
                send,
                401,
                "Authentication required",
                {"WWW-Authenticate": f'Basic realm="{AUTH_REALM}"'},
            )
            return

        if scope["type"] == "http" and has_valid_basic_auth:
            await self.app(scope, receive, _with_session_cookie(send, username, password))
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


def _with_session_cookie(send, username: str, password: str):
    async def send_with_cookie(message):
        if message["type"] == "http.response.start":
            headers = list(message.get("headers", []))
            headers.append((b"set-cookie", _build_session_cookie(username, password).encode("latin1")))
            message = {**message, "headers": headers}
        await send(message)

    return send_with_cookie


def _build_session_cookie(username: str, password: str) -> str:
    return f"{SESSION_COOKIE}={_session_token(username, password)}; HttpOnly; SameSite=Lax; Path=/"


def _has_valid_session_cookie(scope, expected_user: str, expected_password: str) -> bool:
    header = _get_header(scope, "cookie")
    if not header:
        return False

    try:
        cookie = SimpleCookie(header)
    except CookieError:
        return False

    morsel = cookie.get(SESSION_COOKIE)
    if morsel is None:
        return False

    expected_token = _session_token(expected_user, expected_password)
    return secrets.compare_digest(morsel.value, expected_token)


def _session_token(username: str, password: str) -> str:
    return hmac.new(password.encode("utf-8"), username.encode("utf-8"), hashlib.sha256).hexdigest()


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
