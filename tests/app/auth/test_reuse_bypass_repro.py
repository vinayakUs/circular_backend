"""Throwaway repro: grace-period minting vs. revocation.

Case A: a token replayed INSIDE the grace window mints a fresh successor
        instead of tripping reuse detection.
Case B: a family revoked by reuse detection can be resurrected, because
        revoke_family() only sets consumed=TRUE + consumed_at=NOW(), which
        lands the row back inside the grace window.
"""
from __future__ import annotations

import sys
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/root/circular_backend/tests/app/auth")

from test_refresh_token_service import FakeRefreshTokenRepository  # noqa: E402

from app.auth.session.refresh_token_service import (  # noqa: E402
    RefreshTokenReuseError,
    RefreshTokenService,
)


def issue(svc, user_id, ip, ua):
    return svc.issue(user_db_id=user_id, issued_ip=ip, issued_user_agent=ua)[0]


def rotate(svc, token, ip, ua):
    return svc.rotate(plaintext_token=token, issued_ip=ip, issued_user_agent=ua)[0]


def case_a():
    print("=== CASE A: replay inside the grace window ===")
    repo = FakeRefreshTokenRepository()
    svc = RefreshTokenService(repo=repo)
    user_id = uuid.uuid4()

    t0 = issue(svc, user_id, "10.0.0.1", "victim")
    t1 = rotate(svc, t0, "10.0.0.1", "victim")           # legitimate rotation
    print(f"live tokens: {repo.live_count()}")

    # Attacker stole t0 and replays it microseconds later.
    try:
        t2 = rotate(svc, t0, "10.0.0.3", "attacker")
        print(f"NO ALARM. Attacker minted {t2[:12]}... into the victim's family.")
        print(f"live tokens now: {repo.live_count()} (victim has t1, attacker has t2)")
    except RefreshTokenReuseError as exc:
        print(f"reuse detected: {exc}")


def case_b():
    print("\n=== CASE B: resurrecting a revoked family ===")
    repo = FakeRefreshTokenRepository()
    svc = RefreshTokenService(repo=repo)
    user_id = uuid.uuid4()

    t0 = issue(svc, user_id, "10.0.0.1", "victim")
    t1 = rotate(svc, t0, "10.0.0.1", "victim")

    # Backdate t0's consumption so the replay falls OUTSIDE the grace
    # window and reuse detection actually fires.
    rec = repo.get_by_hash(svc._hash(t0))
    repo._put(replace(rec, consumed_at=datetime.now(timezone.utc) - timedelta(hours=1)))

    try:
        rotate(svc, t0, "10.0.0.3", "attacker")
        print("FAIL: expected RefreshTokenReuseError")
    except RefreshTokenReuseError as exc:
        print(f"reuse detected, family revoked: {exc}")

    print(f"live tokens after revocation: {repo.live_count()}")
    revoked_at = datetime.now(timezone.utc)
    print(f"revoke_family() set consumed_at = NOW() ({revoked_at.isoformat()})")

    # The attacker still holds t1, which is now 'consumed' as of NOW().
    try:
        t2 = rotate(svc, t1, "10.0.0.3", "attacker")
        print(f"*** BYPASS: minted {t2[:12]}... into the REVOKED family.")
        print(f"*** live tokens in the revoked family: {repo.live_count()}")
    except Exception as exc:
        print(f"no bypass: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    case_a()
    case_b()