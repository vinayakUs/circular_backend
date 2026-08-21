# API Changes — Comments FK Migration

**Date:** 2026-07-30
**Scope:** `comments.user_id` and `comment_mentions.mentioned_by` foreign-key hardening.

## TL;DR for frontend

- `user_id` / `username` on comments → replaced by `user_db_id` (UUID) + `author_user_id` / `author_name` / `author_email` (joined from `users`).
- `mentioned_by` on mentions → replaced by `mentioned_by_user_id` + `mentioned_by_name`.
- Self-mention notifications are now hidden from `/api/mentions` and `/api/mentions/unread-count` (they were previously counted as unread).
- No request bodies changed. Only response shapes.

---

## Endpoints with response changes

### 1. `POST /api/circulars/<circ_id>/experts/<expert_id>/comments`

**Response:** `{"comment": { ... }}`

**Before:**
```json
{
  "comment": {
    "id": "...",
    "expert_id": "...",
    "user_id": "test1",
    "username": "test1",
    "text": "...",
    "created_at": "..."
  }
}
```

**After:**
```json
{
  "comment": {
    "id": "...",
    "expert_id": "...",
    "user_db_id": "a865a688-3f35-4987-847d-a10957089ce1",
    "author_user_id": "test1",
    "author_name": "Test One",
    "author_email": "test1@example.com",
    "text": "...",
    "created_at": "..."
  }
}
```

| Field | Status |
|---|---|
| `user_id` | **REMOVED** |
| `username` | **REMOVED** |
| `user_db_id` | **NEW** — UUID, FK to `users.id` |
| `author_user_id` | **NEW** — LDAP uid, joined from `users.user_id` |
| `author_name` | **NEW** — display name, joined from `users.name` |
| `author_email` | **NEW** — email, joined from `users.email` |

### 2. `GET /api/circulars/<circ_id>/experts/<expert_id>/comments`

**Response:** `{"comments": [ { ... } ]}` — same per-item shape as #1.

### 3. `GET /api/mentions`

**Response:** `{"items": [ { ... } ], "total": N, "limit": ..., "offset": ..., "unread_count": N}`

**Per-item changes:**

| Field | Status |
|---|---|
| `mentioned_by` | **REMOVED** |
| `mentioned_by_user_id` | **NEW** — LDAP uid, joined from `users.user_id` |
| `mentioned_by_name` | **NEW** — display name, joined from `users.name` |

All other item fields (`id`, `status`, `read_at`, `created_at`, `sent_at`, `target_label`, `target_type`, `expert_id`, `comment_id`, `comment_snippet`, `expert_name`, `comment_url`) are unchanged.

### 4. `GET /api/mentions/unread-count`

**Response:** `{"unread_count": N}` — shape unchanged.

**Semantic change:** rows with `status='SKIPPED'` (self-mentions) are now excluded from the count. Before this change, a self-mention incremented the unread count.

---

## Endpoints with NO changes

| Endpoint | Notes |
|---|---|
| `POST /api/auth/login` | Unchanged |
| `GET /api/auth/me` | Unchanged |
| `GET /api/admin/departments/<id>/users` | Reads `users` table directly |
| `POST /api/admin/departments/<id>/users` | Writes to `users` table |
| `DELETE /api/admin/departments/<id>/users/<uid>` | Soft-delete on `users` table |
| `GET /api/admin/users/<uid>` | Reads `users` table |
| `GET /api/mentions/search` | The @mention picker — reads `users` table |
| `POST /api/mentions/<id>/read` | Marks single read |
| `POST /api/mentions/read-all` | Marks all read |
| `GET /api/mentions?status=...` | Existing `status` query param still works |

---

## Frontend migration checklist

### Comment display components
- [ ] Replace `comment.user_id` with `comment.author_user_id` (string) when displaying the LDAP uid
- [ ] Replace `comment.username` with `comment.author_name` when displaying the display name
- [ ] Optionally display `comment.author_email` if your UI shows email
- [ ] If you need the internal UUID for any reason, use `comment.user_db_id`

### Comment form submit handler
- [ ] Same replacements as above for the response of `POST /comments`

### Mentions inbox
- [ ] Replace `mention.mentioned_by` with `mention.mentioned_by_user_id` (string LDAP uid) for display
- [ ] Optionally display `mention.mentioned_by_name` for human-friendly display
- [ ] The unread badge automatically stops counting self-mentions — no change needed unless you were relying on the old (buggy) behavior

### Anywhere using `users.user_id` (LDAP uid) for display
- [ ] No change — `users.user_id` is still the LDAP uid and is still present in the same places (e.g. `/api/admin/departments/<id>/users`, `/api/auth/me`, `/api/mentions/search`)

---

## Why these changes

Old schema:
- `comments.user_id VARCHAR(255)` — no FK, just a string
- `comments.username VARCHAR(255)` — denormalized LDAP uid
- `comment_mentions.mentioned_by VARCHAR(255)` — no FK

These had no referential integrity and no protection against orphaned rows.

New schema:
- `comments.user_db_id UUID REFERENCES users(id) ON DELETE RESTRICT`
- `comment_mentions.mentioned_by_user_db_id UUID REFERENCES users(id) ON DELETE RESTRICT`
- Author/mention display is read via JOIN against `users` at query time

This guarantees:
- No orphaned comments or mentions on user hard-delete
- Display data (name, email) is always in sync with `users`
- Smaller indexes (16 bytes UUID vs 255 bytes VARCHAR)
- Stable linkage even if a user's LDAP uid changes

---

## Reference: Postman collection

A Postman collection reflecting the new structure is at:
`postman_collection.json`

It includes:
- Auth (login as test / test1)
- Comments (POST no-mention, POST with @test1, GET list)
- Mentions (GET inbox, GET unread-count)

Tokens are auto-captured into collection variables via the `Tests` script on the login requests.

---

## Reference: curl examples

```bash
# Login
TOKEN=$(curl -s -X POST http://localhost:5000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"test","password":"admin"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# POST comment with @test1 mention
curl -X POST http://localhost:5000/api/circulars/<CIRC>/experts/<EXPERT>/comments \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"text":"hi @test1 please review"}'

# GET comments
curl http://localhost:5000/api/circulars/<CIRC>/experts/<EXPERT>/comments \
  -H "Authorization: Bearer $TOKEN"

# GET mentions inbox (as test1)
TOKEN1=$(curl -s -X POST http://localhost:5000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"test1","password":"admin"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl "http://localhost:5000/api/mentions?limit=10" \
  -H "Authorization: Bearer $TOKEN1"

# GET unread count (as test1)
curl http://localhost:5000/api/mentions/unread-count \
  -H "Authorization: Bearer $TOKEN1"
```

---

## Out of scope (not part of this migration)

These remain FK-less and could be hardened the same way in a separate change:

- `experts.created_by_user_id` — UUID but no FK
- `mention_notifications.recipient_user_id` — VARCHAR, no FK

Tell me if you want either of those done.