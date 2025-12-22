"""Authentication endpoints for password-based login with JWT tokens."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
import jwt
from fastapi import APIRouter, Cookie, Response
from pydantic import BaseModel

from app.config import get_settings

router = APIRouter(prefix="/auth")
logger = logging.getLogger(__name__)


class LoginRequest(BaseModel):
    """Login request with password."""

    password: str


class AuthResponse(BaseModel):
    """Authentication response."""

    authenticated: bool
    message: str


def create_token(token_type: str, expires_delta: timedelta) -> str:
    """Create a JWT token."""
    settings = get_settings()
    expire = datetime.now(timezone.utc) + expires_delta
    payload = {
        "type": token_type,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, settings.auth_jwt_secret, algorithm="HS256")


def verify_token(token: str, token_type: str) -> bool:
    """Verify a JWT token."""
    settings = get_settings()
    if not settings.auth_jwt_secret:
        return False

    try:
        payload = jwt.decode(token, settings.auth_jwt_secret, algorithms=["HS256"])
        return payload.get("type") == token_type
    except jwt.ExpiredSignatureError:
        logger.debug("Token expired")
        return False
    except jwt.InvalidTokenError as e:
        logger.debug(f"Invalid token: {e}")
        return False


def set_auth_cookies(response: Response, access_token: str, refresh_token: str) -> None:
    """Set authentication cookies on response."""
    settings = get_settings()

    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=True,
        samesite="none",
        max_age=settings.auth_access_token_expire,
        path="/",
    )
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=True,
        samesite="none",
        max_age=settings.auth_refresh_token_expire,
        path="/api/v1/auth",
    )


def clear_auth_cookies(response: Response) -> None:
    """Clear authentication cookies."""
    response.delete_cookie(key="access_token", path="/")
    response.delete_cookie(key="refresh_token", path="/api/v1/auth")


@router.post("/login", response_model=AuthResponse)
async def login(request: LoginRequest, response: Response):
    """Authenticate with password and receive JWT tokens in cookies."""
    settings = get_settings()

    if not settings.auth_enabled:
        return AuthResponse(
            authenticated=False,
            message="Authentication not configured on server",
        )

    try:
        password_bytes = request.password.encode("utf-8")
        hash_bytes = settings.auth_password_hash.encode("utf-8")

        if bcrypt.checkpw(password_bytes, hash_bytes):
            access_token = create_token(
                "access",
                timedelta(seconds=settings.auth_access_token_expire),
            )
            refresh_token = create_token(
                "refresh",
                timedelta(seconds=settings.auth_refresh_token_expire),
            )

            set_auth_cookies(response, access_token, refresh_token)
            logger.info("User authenticated successfully")

            return AuthResponse(
                authenticated=True,
                message="Login successful",
            )
        else:
            logger.warning("Failed login attempt - invalid password")
            return AuthResponse(
                authenticated=False,
                message="Invalid password",
            )

    except Exception as e:
        logger.error(f"Login error: {e}")
        return AuthResponse(
            authenticated=False,
            message="Authentication error",
        )


@router.post("/logout", response_model=AuthResponse)
async def logout(response: Response):
    """Clear authentication tokens."""
    clear_auth_cookies(response)
    return AuthResponse(
        authenticated=False,
        message="Logged out successfully",
    )


@router.post("/refresh", response_model=AuthResponse)
async def refresh(
    response: Response,
    refresh_token: Optional[str] = Cookie(default=None),
):
    """Refresh access token using refresh token."""
    settings = get_settings()

    if not settings.auth_enabled:
        return AuthResponse(
            authenticated=False,
            message="Authentication not configured",
        )

    if not refresh_token:
        return AuthResponse(
            authenticated=False,
            message="No refresh token provided",
        )

    if verify_token(refresh_token, "refresh"):
        new_access_token = create_token(
            "access",
            timedelta(seconds=settings.auth_access_token_expire),
        )
        new_refresh_token = create_token(
            "refresh",
            timedelta(seconds=settings.auth_refresh_token_expire),
        )

        set_auth_cookies(response, new_access_token, new_refresh_token)

        return AuthResponse(
            authenticated=True,
            message="Token refreshed",
        )

    clear_auth_cookies(response)
    return AuthResponse(
        authenticated=False,
        message="Invalid or expired refresh token",
    )


@router.get("/verify", response_model=AuthResponse)
async def verify(
    access_token: Optional[str] = Cookie(default=None),
):
    """Check if current session is authenticated."""
    settings = get_settings()

    if not settings.auth_enabled:
        return AuthResponse(
            authenticated=True,
            message="Authentication disabled",
        )

    if not access_token:
        return AuthResponse(
            authenticated=False,
            message="Not authenticated",
        )

    if verify_token(access_token, "access"):
        return AuthResponse(
            authenticated=True,
            message="Authenticated",
        )

    return AuthResponse(
        authenticated=False,
        message="Invalid or expired token",
    )
