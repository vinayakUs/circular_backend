# MFA via OTP — Design Spec

| | |
|---|---|
| **Status** | Draft, awaiting user review |
| **Date** | 2026-09-23 |
| **Author** | Claude (architectural brainstorming session) |
| **Replaces** | Text CAPTCHA on `/api/auth/login` |
| **Implementation plan** | To follow via `writing-plans` skill after this spec is approved |

---

## 1. Goal

Replace the existing text-CAPTCHA on `/api/auth/login` with **mandatory MFA via one-time password (OTP)** delivered through the user's registered email and/or SMS. Strengthen authentication against credential stuffing, bot enumeration, and password reuse while keeping the login flow low-friction for legitimate users.

## 2. Locked decisions

| Decision | Value | Rationale |
|---|---|---|
| Delivery channels | **Email + SMS** via org internal splitter | Already have `EmailService` + SMTP; org has internal splitter that handles both |
| Global kill-switch | **`MFA_ENABLED` env flag, default `true`** | Lets ops roll out, test, and roll back without redeploy |
| Enforcement | **Mandatory for every user on next login** when `MFA_ENABLED=true` | Compliance posture; eliminates soft opt-in limbo |
| Captcha | **Kept in front of MFA** as defense-in-depth | Stops bots reaching LDAP at all; CAPTCHA cost is negligible compared to OTP round-trip |
| Remember device / trusted browser | **No** — OTP required every login | Strictest policy |
| Backup codes | **No** | User opted out; admin reset is the recovery path |
| Login flow shape | **Two-step** | `/login` → `mfa_pending` token → `/mfa/verify` → JWT |
| OTP parameters | **6 digits, 300 s TTL, 3 attempts per code** | Standard Google/GitHub defaults; balances security and typo tolerance |
| OTP hash | **HMAC-SHA256 with server-side pepper** | Reversible to bcrypt if pepper leaks, but pepper is rotatable per env |
| `mfa_pending` token | **Opaque random 32-byte, SHA-256 hashed in DB, 5-minute TTL** | NOT a JWT — separates "password ok" from "MFA ok", each with own TTL/revocation |
| Channel selection | **User picks at OTP-request step** (no stored preference) | Avoids lockout if preferred channel is down |
| Enrollment table | **None — three columns on `users`** | Mandatory + 2 channels + no enrollment ceremony → derivation from `users` is sufficient |
| Rate limiter | **In-memory token bucket per process** | MVP scope; flagged for Redis upgrade if multi-worker correctness becomes an issue |

## 3. Architecture

### 3.1 High-level component diagram

```
┌──────────────┐     ┌─────────────────┐     ┌──────────────────┐
│ Flask route  │────▶│ OtpService      │────▶│ OtpDeliveryChannel│ (ABC: email/sms/composite)
│ /api/auth/*  │     │ (orchestrator)  │     └──────────────────┘
└──────────────┘     │                 │     ┌──────────────────┐
       │             │                 │────▶│ OtpChallengeRepo │ (ABC + Postgres)
       │             │                 │     └──────────────────┘
       │             │                 │     ┌──────────────────┐
       │             │                 │────▶│ OtpGenerator     │ (secure-random)
       │             └─────────────────┘     └──────────────────┘
       │             ┌─────────────────┐     ┌──────────────────┐
       └────────────▶│ PendingTokenSvc │────▶│ PendingTokenRepo │
                     └─────────────────┘     └──────────────────┘
                     ┌─────────────────┐
                     │ CapabilityResolv│ (pure function over User; no I/O)
                     └─────────────────┘
                     ┌─────────────────┐     ┌──────────────────┐
                     │ LoginAuditSvc   │────▶│ AuditRepo        │
                     └─────────────────┘     └──────────────────┘
```

### 3.2 SOLID mapping

| Service | Single responsibility | Depends on (DIP) |
|---|---|---|
| `OtpService` | issue + verify + consume OTP challenge | `OtpGenerator`, `OtpDeliveryChannel`, `OtpChallengeRepository`, `OtpHasher`, `RateLimiter` |
| `MfaPendingTokenService` | mint + verify + consume opaque step-1 token | `MfaPendingTokenRepository` |
| `MfaCapabilityResolver` | derive "is MFA required, and via which channels?" from `User` | none (pure) |
| `LoginAuditService` | append-only audit rows | `LoginAuditRepository` |
| Route handlers | HTTP ↔ service translation | services |

### 3.3 Design principles applied

- **SRP** — five services, one job each; route handlers do HTTP, services orchestrate, repos do I/O, ABCs define ports
- **OCP** — new channel (push, TOTP) = new `OtpDeliveryChannel` impl, no `OtpService` edits
- **LSP** — every channel / repo satisfies its ABC; services are agnostic to which concrete class is wired
- **ISP** — small role-specific ABCs (`OtpDeliveryChannel` has 2 methods, not a god interface)
- **DIP** — services import ABCs; composition root in `make_*_service()` factories wires concretes
- **DRY** — structure mirrors `app/auth/captcha/` exactly (ABC + Postgres impl + composition root + lazy singleton)
- **KISS** — no TOTP, push, WebAuthn; OTP via the two channels the user picked
- **YAGNI** — no backup codes (per user), no remember-device (per user), no Redis (yet)
- **12-factor** — all config via env: `MFA_ENABLED` (global kill-switch, default `true`), `OTP_LENGTH`, `OTP_TTL_SECONDS`, `OTP_MAX_ATTEMPTS`, `OTP_PEPPER_SECRET`, `MFA_PENDING_TOKEN_TTL_SECONDS`, `INTERNAL_SPLITTER_URL`, `INTERNAL_SPLITTER_AUTH_TOKEN`
- **Defense in depth** — five independent gates, each with own TTL and revocation: captcha → LDAP password → `mfa_pending` token → OTP → JWT
- **Least privilege** — services depend on smallest interface that satisfies their need
- **Fail closed** — if delivery fails on all channels, refuse login; never allow an unverifiable user through

## 4. Module / file layout

Mirrors `app/auth/captcha/` so the codebase stays consistent.

```
app/auth/mfa/
├── __init__.py
├── otp_service.py                       # OtpService — issue/verify/consume
├── pending_token_service.py             # MfaPendingTokenService — mint/consume
├── capability_resolver.py               # MfaCapabilityResolver — pure function
├── login_audit_service.py               # LoginAuditService — append-only
├── generators/
│   ├── base.py                          # OtpGenerator ABC
│   └── secure_random_generator.py       # SecureRandomOtpGenerator (secrets.choice)
├── hashing/
│   ├── base.py                          # OtpHasher ABC
│   └── hmac_sha256_hasher.py            # HmacSha256OtpHasher (pepper secret)
├── rate_limit/
│   ├── base.py                          # RateLimiter ABC
│   └── in_memory_token_bucket.py        # InMemoryTokenBucket (per-process)
├── channels/
│   ├── base.py                          # OtpDeliveryChannel ABC
│   ├── email_channel.py                 # EmailOtpChannel → wraps EmailService
│   ├── sms_channel.py                   # SmsOtpChannel → POST internal splitter
│   └── composite.py                     # CompositeOtpChannel — registry/router (one channel at a time)
├── templates/
│   ├── otp_email.html                   # Jinja2 — email body
│   └── otp_sms.txt                      # Plain text — SMS body
└── repository/
    ├── otp_challenge_repository.py              # ABC + dataclass OtpChallenge
    ├── pending_token_repository.py              # ABC + dataclass MfaPendingToken
    ├── login_audit_repository.py                # ABC + dataclass LoginAuditEntry
    ├── postgres_otp_challenge_repository.py
    ├── postgres_pending_token_repository.py
    └── postgres_login_audit_repository.py
```

Plus changes to:

- `app/routes/auth.py` — register `/mfa/request`, `/mfa/verify`, `/mfa/contact`
- `config.py` — add `OTP_*`, `MFA_*`, `INTERNAL_SPLITTER_*` env vars
- `db/postgres_schema.sql` — add 3 columns on `users`, 3 new tables
- `frontend/src/app/login/login.component.*` — success path now routes to `/login/mfa?token=`
- `frontend/src/app/login-mfa/login-mfa.component.*` — NEW
- `frontend/src/app/services/login.service.ts` — add `mfaRequest()`, `mfaVerify()`, `setPhoneContact()`

## 5. Data model

### 5.1 ALTER on `users`

```sql
-- Idempotent migration. Safe on fresh and existing databases.
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS phone_e164        VARCHAR(32),
  ADD COLUMN IF NOT EXISTS phone_verified_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS mfa_enabled       BOOLEAN NOT NULL DEFAULT TRUE;
```

### 5.2 New tables

```sql
-- otp_challenges: hashed OTP code, attempts counter, atomic consume
CREATE TABLE IF NOT EXISTS otp_challenges (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_db_id  UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel     VARCHAR(16) NOT NULL CHECK (channel IN ('email','sms')),
    code_hash   VARCHAR(64) NOT NULL,                          -- HMAC-SHA256(pepper, code)
    attempts    INT         NOT NULL DEFAULT 0,
    consumed    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_otp_user_active
    ON otp_challenges (user_db_id, consumed, expires_at);

-- mfa_pending_tokens: opaque step-1 token, hashed, short TTL
CREATE TABLE IF NOT EXISTS mfa_pending_tokens (
    token_id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash        VARCHAR(64) NOT NULL UNIQUE,             -- SHA-256(token)
    user_db_id        UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    issued_ip         INET,
    issued_user_agent TEXT,
    consumed          BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at        TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pending_user_active
    ON mfa_pending_tokens (user_db_id, consumed, expires_at);

-- login_audit: append-only trail of every login step
CREATE TABLE IF NOT EXISTS login_audit (
    id                  BIGSERIAL   PRIMARY KEY,
    user_db_id          UUID        REFERENCES users(id) ON DELETE SET NULL,
    username_attempted  VARCHAR(255) NOT NULL,
    ip                  INET,
    user_agent          TEXT,
    step                VARCHAR(32) NOT NULL CHECK (step IN
                          ('captcha','password','mfa_request','mfa_verify','mfa_locked','phone_set','success')),
    outcome             VARCHAR(16) NOT NULL CHECK (outcome IN ('ok','fail','locked')),
    failure_reason      TEXT,
    channel             VARCHAR(16),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_audit_user_ts ON login_audit (user_db_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_ip_ts   ON login_audit (ip, created_at DESC);
```

## 6. API contracts

### 6.1 `POST /api/auth/login` (modified)

**In:** `{ username, password, captcha_id, captcha_answer }`

**Out 200:** `{ mfa_required: true, mfa_token, channels: ["email","sms"], otp_length: 6, ttl_seconds: 300 }`

**Out 401:** `{ error: "Invalid credentials or verification code" }` — generic, covers captcha / password / lockout

**Out 403:** `{ error: "MFA required but no delivery channel on file" }` — only when mandatory + zero channels (ops alert triggered)

**Out 429:** `{ error: "Too many attempts" }`

**Out 503:** `{ error: "Service temporarily unavailable" }` — LDAP or DB outage

**Side effects:**
- Audit row: `step='captcha'`, then `step='password'`, each with outcome
- `mfa_pending_tokens` row inserted on password success
- Generic error message regardless of which gate failed

### 6.2 `POST /api/auth/mfa/request` (new)

**In:** `{ mfa_token, channel }` where `channel ∈ {"email","sms"}`

**Out 200:** `{ sent: true, expires_at, ttl_seconds: 300 }`

**Out 400:** `{ error: "Channel not enabled for this user" }` — user has no email OR no verified phone

**Out 401:** `{ error: "Invalid credentials or verification code" }` — token bad/expired/consumed

**Out 429:** `{ error: "Too many requests" }` — per-user bucket

**Out 502:** `{ error: "Delivery failed; try the other channel" }` — SMTP/splitter error

**Side effects:**
- `otp_challenges` row inserted (atomic; aborts any prior unconsumed challenge for same user)
- `LoginAuditService.record(step='mfa_request', outcome, channel)`
- All previous unconsumed `otp_challenges` for the same user are marked `consumed=TRUE` (one active at a time)

### 6.3 `POST /api/auth/mfa/verify` (new)

**In:** `{ mfa_token, otp }`

**Out 200:** `{ access_token, token_type: "bearer" }` + `Set-Cookie: access_token=<JWT>; HttpOnly; SameSite=Strict; Secure; Path=/`

**Out 401:** `{ error: "Invalid credentials or verification code" }` — wrong OTP / expired / consumed

**Out 423:** `{ error: "Too many attempts; request a new code" }` — locked, must request fresh

**Out 429:** `{ error: "Too many attempts" }`

**Side effects on success:**
- `otp_challenges.consumed=TRUE` (atomic `UPDATE…RETURNING`)
- `mfa_pending_tokens.consumed=TRUE`
- `LoginAuditService.record(step='success')` — first such row per `user_db_id` is the de-facto enrollment timestamp; queryable from `login_audit` when reporting is needed
- Existing `LDAPAuth.create_token()` called; JWT minted exactly as today

### 6.4 `GET /api/auth/mfa/enrollment` (new, `@require_auth`)

**Out 200:** `{ user_db_id, email, phone_e164, phone_verified_at, mfa_enabled, channels: ["email","sms"] }`

Pure read; lets the frontend render channel availability before the user clicks "Send code".

### 6.5 `PATCH /api/auth/mfa/contact` (new, `@require_auth`)

**In:** `{ phone_e164?: "+91XXXXXXXXXX" }`

**Out 200:** `{ phone_e164, phone_verified_at }`

**Side effects:**
- `UPDATE users SET phone_e164=?, phone_verified_at=NOW()` (we trust the user's input in MVP; flagged below for SMS challenge-verify)
- `LoginAuditService.record(step='phone_set')`

SMS challenge-verify ("send a code to the new number, require entry before flipping `phone_verified_at`") is **out of scope** for MVP — flagged in §13.

### 6.6 Error matrix

| Step | Failure | HTTP | Body | Audit |
|---|---|---|---|---|
| Captcha | invalid / expired | 401 | generic | `step=captcha, outcome=fail` |
| Captcha | required field missing | 401 | generic | `step=captcha, outcome=fail` |
| Rate limit | login_ip / login_username exceeded | 429 | "Too many attempts" | `step=password, outcome=fail, reason=rate_limit` |
| LDAP | bad credentials | 401 | generic | `step=password, outcome=fail` |
| LDAP | server unreachable | 503 | "Service temporarily unavailable" | `step=password, outcome=fail, reason=ldap_unreachable` |
| LDAP | success, user not in `users` table | 403 | "User not provisioned in DB" | `step=password, outcome=fail` |
| Capability | mandatory, no channels | 403 | "MFA required but no delivery channel on file" | `step=password, outcome=fail, reason=no_channel` (CRITICAL alert) |
| Pending token | invalid / expired / consumed | 401 | generic | `step=mfa_request, outcome=fail` |
| Capability | channel not enabled for user | 400 | "Channel not enabled for this user" | `step=mfa_request, outcome=fail` |
| Delivery | SMTP / splitter error | 502 | "Delivery failed; try the other channel" | `step=mfa_request, outcome=fail, reason=delivery` |
| OTP | wrong code (attempts < 3) | 401 | generic | `step=mfa_verify, outcome=fail` |
| OTP | wrong code (attempts == 3) | 423 | "Too many attempts; request a new code" | `step=mfa_locked, outcome=locked` |
| OTP | expired / consumed | 401 | generic | `step=mfa_verify, outcome=fail, reason=expired_or_consumed` |
| Verify | success | 200 | token + cookie | `step=success` |

**Rule:** any failure that isn't a programmer error returns the same generic string. Never enumerate which factor failed or which usernames exist.

## 7. Frontend changes

### 7.1 Route map

```
/login             → LoginComponent (username + password + captcha)
   │ onSubmit() → backend → on 200{mfa_required}: router.navigate(['/login/mfa'],
   │                                       {queryParams:{token}})
   ▼
/login/mfa?token   → LoginMfaComponent (NEW)
   │ page renders:
   │   channel picker:  [Email] [SMS]   (disabled if channel not enabled)
   │   "Send code" button (auto-sends to email on mount if available)
   │   6-digit OTP input (autocomplete="one-time-code", paste-friendly)
   │   "Verify" button (disabled until 6 digits)
   │   countdown timer showing TTL
   ▼
/                  → HomeComponent (existing; unchanged)
```

### 7.2 Files

- `frontend/src/app/login/login.component.ts` — on 200, check for `mfa_required` flag, navigate to `/login/mfa?token=…` instead of `/`
- `frontend/src/app/login-mfa/login-mfa.component.{ts,html,css}` — new
- `frontend/src/app/services/login.service.ts` — add `mfaRequest(token, channel)`, `mfaVerify(token, otp)`, `getEnrollment()`, `setPhoneContact(phone)`

### 7.3 UX rules

- 6-digit input: `inputmode="numeric"`, `pattern="[0-9]{6}"`, `autocomplete="one-time-code"` so iOS/Android suggest the SMS code automatically
- "Send code" auto-runs once on mount with the first available channel
- After verify failure, the input is cleared and focused; a "request new code" link appears only on 423
- TTL countdown (mm:ss) hides the form and shows "Code expired" at 0
- All error messages use the generic backend string verbatim — don't translate or reinterpret

## 8. Security

### 8.1 What is logged vs. what is never logged

| Logged | Never logged |
|---|---|
| username (or attempted) | password |
| user_db_id | otp code (plain or hash) |
| ip | mfa_token (plain) |
| user_agent (truncated to 200 chars) | JWT |
| step, outcome, failure_reason, channel | email service credentials |
| ts | `OTP_PEPPER_SECRET` |

### 8.2 Threat model & mitigations

| Threat | Mitigation |
|---|---|
| Brute force OTP | 6-digit = 10⁶; 3 attempts per code; per-user verify rate limit; per-IP login rate limit |
| Replay attack | OTP `consumed=TRUE` is atomic UPDATE...RETURNING — once consumed, reject |
| Token leak in URL | `mfa_token` is opaque random 32 bytes; never logged; SHA-256 hashed at rest |
| OTP code leak via timing | `hmac.compare_digest` for OTP hash compare; same for `mfa_token` hash compare |
| Enumeration | Generic "Invalid credentials or verification code" for every login-failure path |
| Email interception | 5-min TTL + one active code per user at a time (new request kills old) |
| SMS interception (SS7/SIM swap) | Email fallback always available; both channels deliver the same code, user picks |
| Stolen mfa_token | 5-min TTL; bound to issued IP+UA (stored at issue); any new login from same user invalidates prior `mfa_pending_tokens` |
| Race condition on OTP consume | `UPDATE otp_challenges SET consumed=TRUE WHERE id=? AND consumed=FALSE RETURNING id` — only one transaction wins |
| Pepper secret leak | HMAC is reversible to plain SHA-256 if pepper leaks; rotate via env, no DB migration needed |
| Mandatory MFA + delivery outage | Fail closed — `MfaCapabilityResolver` returns `blocking=True` if zero channels; ops alert fires; manual admin reset is recovery path |
| Brute force mfa_pending token | Token is 32 bytes random (256-bit entropy); rate limit per user; 5-min TTL |
| Soft-deleted user bypass | Existing M1 check in `require_auth` already blocks soft-deleted users on every protected route; unchanged |

### 8.3 Constant-time operations

- OTP hash compare: `hmac.compare_digest(candidate_hash, row.code_hash)`
- `mfa_token` hash compare: `hmac.compare_digest(candidate_hash, row.token_hash)`
- Both wrapped in repository methods; never `==` on hashes in route or service code

## 9. Rate limiting

| Bucket | Key | Limit | Window |
|---|---|---|---|
| `login_ip` | client IP | 20 attempts | 5 min |
| `login_username` | username (case-insensitive) | 8 attempts | 15 min |
| `mfa_request_user` | user_db_id | 5 requests | 5 min |
| `mfa_verify_user` | user_db_id | 10 attempts | 5 min |

Implementation: `InMemoryTokenBucket` per `(bucket_name, key)` in a dict guarded by `threading.Lock`. **Per-process only.** If multiple gunicorn workers become a correctness issue, the same `RateLimiter` ABC has a Redis backend ready (out of scope for MVP; flagged in §13).

## 10. Testing strategy

### 10.1 Unit tests (pytest)

| Component | What to test |
|---|---|
| `SecureRandomOtpGenerator` | distribution (chi-square on 10k samples), uniqueness, no leading-zero drop |
| `HmacSha256OtpHasher` | known-answer test, rotation behavior |
| `InMemoryTokenBucket` | refill math, eviction, per-key isolation, thread-safety under concurrent calls |
| `MfaCapabilityResolver` | matrix of `(email?, phone?, verified?, mfa_enabled?)` → expected `MfaCapability` |
| `MfaPendingTokenService` | mint uniqueness, verify rejects expired/consumed/wrong, consume is idempotent |
| `OtpService.issue` | one-active-per-user invariant (new issue consumes prior) |
| `OtpService.verify` | success, wrong-but-attempts<3, wrong-and-attempts==3 (locked), expired, already consumed, cross-user reuse |
| `EmailOtpChannel` | rendered template includes code + TTL + no other PII |
| `SmsOtpChannel` | POSTs correct payload to splitter URL, handles 5xx, handles 4xx, retry policy |
| `CompositeOtpChannel` | dispatches to chosen channel only (not both) |

### 10.2 Integration tests (testcontainers Postgres)

| Scenario | Expected |
|---|---|
| Full happy path: captcha → password → OTP request (email) → OTP verify | JWT cookie, audit has 4 success rows |
| Wrong password 3x in 15 min from same username | 429 on 4th attempt; `login_username` bucket tripped |
| Wrong OTP 3x | 423 on 3rd attempt; subsequent verify on same challenge returns 423; new request resets attempts |
| Mandatory MFA + user with no email, no phone | 403 at `/login`; alert log; audit `reason=no_channel` |
| Replay of consumed OTP | 401; audit `reason=consumed` |
| Replay of consumed `mfa_token` | 401; audit `reason=consumed` |
| Token TTL expiry | `/mfa/request` after 5 min → 401; `/mfa/verify` after 5 min → 401 |
| Concurrent verify on same OTP challenge | exactly one wins (atomic UPDATE); other gets 401 |
| Captcha still enforced | bad captcha = 401 even with valid LDAP creds |
| Soft-deleted user (`is_deleted=TRUE`) | blocked at login (unchanged from today) |
| `mfa_enabled=FALSE` (admin override) | `/login` returns 200 with `mfa_required=false` and JWT directly |

### 10.3 Frontend tests (Jasmine/Karma)

| Test | Verifies |
|---|---|
| Login success → route to `/login/mfa` | navigation fires only on `mfa_required=true` |
| Channel picker disables disabled channels | mirrors `GET /mfa/enrollment` response |
| OTP input pastes 6 digits | single paste populates correctly (covers mobile autofill) |
| 423 response shows "request new code" link | UI state machine |
| TTL countdown reaches 0 | input disabled, "Code expired" shown |
| Verify button disabled until 6 digits | form validation |

### 10.4 Security tests (manual + automated)

| Probe | Pass criterion |
|---|---|
| Timing attack on OTP compare | <5% variance across 1000 attempts |
| Enumerate usernames via error messages | All login failures return identical body |
| Replay consumed OTP | Rejected with 401 |
| Forge `mfa_token` | Rejected (256-bit entropy) |
| Leak OTP in logs | grep across test logs returns 0 matches |
| Leak password in logs | grep across test logs returns 0 matches |
| Leak `OTP_PEPPER_SECRET` in logs | grep across test logs returns 0 matches |

## 11. Migration / rollout plan

### 11.1 Pre-deploy

1. Run schema migration on prod Postgres (additive only; backward-compatible default `TRUE`)
2. Backfill `phone_e164` for known users (manual export from HR/ActiveDirectory if available; otherwise leave NULL — they'll fall back to email)
3. Set env vars: `OTP_PEPPER_SECRET` (32 random bytes), `INTERNAL_SPLITTER_URL`, `INTERNAL_SPLITTER_AUTH_TOKEN`
4. Deploy JWT key already required (`JWT_PRIVATE_KEY`, `JWT_PUBLIC_KEY`) — confirm both present in env

### 11.2 Deploy

1. Deploy backend with feature flag `MFA_ENABLED=false` → no behavior change
2. Deploy frontend (login component now handles `mfa_required` response, but flag is off so backend doesn't return it)
3. Smoke-test captcha-only path end-to-end
4. Flip `MFA_ENABLED=true` → mandatory MFA live
5. Watch `login_audit` for `reason=no_channel` failures (users who have neither email nor phone); ops to follow up with each

### 11.3 Post-deploy monitoring

- Alert: any `login_audit` row with `reason=no_channel` (page on-call; user is locked out)
- Alert: `step=mfa_request, outcome=fail, reason=delivery` rate > 5% over 5 min (delivery provider outage)
- Alert: `step=mfa_locked` rate > 2% over 15 min (potential attack)
- Dashboard: success rate by step, p50/p95/p99 latency of `/mfa/verify`

### 11.4 Rollback

Feature flag `MFA_ENABLED=false` reverts to today's captcha-only flow. No data loss — `otp_challenges`, `mfa_pending_tokens`, `login_audit` rows are inert. `users.mfa_enabled` and `users.phone_e164` columns persist; harmless.

## 12. Out of scope (future work)

- TOTP / authenticator-app support
- WebAuthn / hardware keys
- Push notification OTP
- SMS challenge-verify before flipping `phone_verified_at`
- Backup codes (intentionally omitted per user)
- Trusted-device / remember-browser (intentionally omitted per user)
- Redis-backed `RateLimiter` (only if multi-worker correctness becomes an issue)
- Account-lockout after N consecutive login failures (currently rate-limit only)
- Admin UI to toggle `mfa_enabled` per user (currently direct DB or env-based flag)
- Per-channel preference (currently always user-picks at request time)

## 13. Risks & open questions

| # | Risk | Mitigation |
|---|---|---|
| R1 | SMTP or splitter outage locks all users out | Fail-closed; ops alert on `reason=delivery`; admin can flip `mfa_enabled=false` per user; future: fallback to other channel automatically if first fails |
| R2 | Users provisioned before MFA rollout have no email and no phone | Mandatory policy returns 403 with ops alert; ops manually adds an email or sets `mfa_enabled=false` for service accounts |
| R3 | `OTP_PEPPER_SECRET` accidentally logged | Strict logging review in code review; CI check for `OTP_PEPPER` substring in `print()` and `logger` calls |
| R4 | In-memory rate limit doesn't share state across gunicorn workers | Document in `make_rate_limiter` that it's per-process; Redis upgrade path documented in §12 |
| R5 | Frontend leaves `mfa_token` in browser history | Component clears query param on unmount via `router.navigate` with `replaceUrl: true` |
| R6 | Long-lived `mfa_pending_token` becomes a target | 5-min TTL; bound to IP+UA; new `/login` from same user invalidates prior tokens (same atomic pattern as OTP) |

---

## 14. Sign-off

This spec is the contract between design and implementation. The implementation plan (next step via `writing-plans` skill) will decompose this spec into ordered, testable tasks. Any change to scope, threat model, or contracts after sign-off should trigger a spec revision + re-review.
