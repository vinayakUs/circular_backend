"""Resolve the caller's identity from the access-token cookie."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from app.auth.ldap_auth import LDAPAuth
from app.auth.session.cookie import get_access_token


logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class Principal:
    """An identity whose JWT signature verified against our public key."""

    username: str
    user_db_id: str
    # True when the signature is good but exp has passed. The token is dead
    # for API calls but still proves the cookie was not forged, which is
    # what lets the caller choose between "refresh this" and "log them out".
    expired: bool = False


def resolve_principal() -> Principal | None:
    """Return the caller, or None if there is no usable identity — no
    cookie, unparseable cookie, bad signature, missing claims, or a
    soft-deleted/unknown user.
    """
    token = get_access_token()
    if not token:
        return None

    payload = LDAPAuth.decode_token(token, verify_exp=False)
    if payload is None:
        return None

    username = payload.get("sub")
    user_db_id = payload.get("user_db_id")
    if not username or not user_db_id:
        return None

    # M1 soft-delete defense. get_by_uuid already filters is_deleted=FALSE,
    # so None here means deleted or unknown. One DB query per protected
    # request.
    from db.postgres_client import get_postgres_client
    from ingestion.repository.users_repository import UsersRepository

    try:
        users_repo = UsersRepository(db_pool=get_postgres_client().get_pool())
        user = users_repo.get_by_uuid(user_db_id)
    except Exception as exc:
        logger.warning("M1 soft-delete check failed: %s", exc)
        return None

    if user is None:
        logger.warning("M1: token references a soft-deleted or unknown user: %s", username)
        return None

    exp = payload.get("exp")
    expired = exp is not None and exp <= datetime.now(timezone.utc).timestamp()

    return Principal(username=username, user_db_id=user_db_id, expired=expired)
