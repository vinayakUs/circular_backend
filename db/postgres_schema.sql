-- properties table
CREATE TABLE IF NOT EXISTS properties (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    archived_at TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_properties_type ON properties(type);
CREATE INDEX IF NOT EXISTS idx_properties_type_name ON properties(type, name) WHERE archived = FALSE;
-- C7: prevent duplicate active (name, type) pairs under concurrent admin writes.
-- Partial index: archived rows can keep their (name, type) for historical
-- reference, but only ONE active row per (name, type) is allowed.
CREATE UNIQUE INDEX IF NOT EXISTS uq_properties_name_type_active
ON properties (name, type)
WHERE archived = FALSE;
CREATE INDEX IF NOT EXISTS idx_properties_metadata_gin ON properties USING gin (metadata jsonb_path_ops);

INSERT INTO properties (id, name, type)
VALUES (
    '5283a54e-327a-477f-946d-72841e2fa442',
    'Exchange Compliance',
    'department'
);

-- scraper_checkpoints
CREATE TABLE IF NOT EXISTS scraper_checkpoints (
    source VARCHAR(20) PRIMARY KEY,
    last_run_date DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    es_bloom_filter BYTEA,
    es_last_run_at TIMESTAMPTZ,
    es_records_processed INT DEFAULT 0
);

-- circulars
CREATE TABLE IF NOT EXISTS circulars (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id VARCHAR(200) NOT NULL,
    source VARCHAR(20) NOT NULL,
    source_item_key TEXT,
    full_reference TEXT NOT NULL,
    department VARCHAR(200),
    title TEXT NOT NULL,
    issue_date DATE NOT NULL,
    effective_date DATE,
    applicable_to_nse BOOLEAN NOT NULL DEFAULT FALSE,
    url TEXT,
    pdf_url TEXT,
    content_hash VARCHAR(64),
    status VARCHAR(20) NOT NULL DEFAULT 'DISCOVERED',
    error_message TEXT,
    detected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    es_indexed_at TIMESTAMPTZ,
    es_chunk_count INT,
    es_index_name VARCHAR(100),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (source, source_item_key)
);
CREATE INDEX IF NOT EXISTS idx_circulars_status ON circulars(status);
CREATE INDEX IF NOT EXISTS idx_circulars_source ON circulars(source);
CREATE INDEX IF NOT EXISTS idx_circulars_issue_date ON circulars(issue_date DESC);
CREATE INDEX IF NOT EXISTS idx_circulars_department ON circulars(department);
CREATE INDEX IF NOT EXISTS idx_circulars_active ON circulars (source, is_active) WHERE is_active = TRUE;

-- circular_signatories
CREATE TABLE IF NOT EXISTS circular_signatories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    signatory_name VARCHAR(500) NOT NULL,
    signatory_designation VARCHAR(500) NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (circular_id, signatory_name, signatory_designation)
);
CREATE INDEX IF NOT EXISTS idx_circular_signatories_circular_id ON circular_signatories(circular_id);

-- circular_assets
CREATE TABLE IF NOT EXISTS circular_assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    asset_role VARCHAR(30) NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    content_hash VARCHAR(64),
    mime_type VARCHAR(100),
    archive_member_path TEXT,
    file_size_bytes BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_circular_assets_circular_id ON circular_assets(circular_id);
CREATE INDEX IF NOT EXISTS idx_circular_assets_circular_role ON circular_assets(circular_id, asset_role);

-- processing_tasks
CREATE TABLE IF NOT EXISTS processing_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    processor_name VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(circular_id, processor_name)
);

-- notification_logs (referenced by mention_notifications.notification_log_id)
CREATE TABLE IF NOT EXISTS notification_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template_name VARCHAR(100) NOT NULL,
    recipient_email VARCHAR(255) NOT NULL,
    subject VARCHAR(500) NOT NULL,
    variables JSONB DEFAULT '{}',
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    sent_at TIMESTAMPTZ,
    correlation_id VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_notification_logs_status ON notification_logs(status);
CREATE INDEX IF NOT EXISTS idx_notification_logs_recipient ON notification_logs(recipient_email);
CREATE INDEX IF NOT EXISTS idx_notification_logs_correlation ON notification_logs(correlation_id);

-- users (LDAP user to department mapping with audit)
-- MUST come before experts: experts.created_by_user_id references users(id)
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(255) NOT NULL UNIQUE,    -- LDAP uid (unique)
    department_id UUID NOT NULL REFERENCES properties(id),
    email VARCHAR(255),                      -- display/contact email (nullable for now)
    name VARCHAR(255),                       -- display name (nullable for now)
    phone_e164 VARCHAR(32),                  -- phone for SMS OTP (nullable; LDAP-provisioned)
    persona_slug TEXT,                       -- selected persona (null = not set; references config/personas.json)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by VARCHAR(255) NOT NULL,        -- LDAP uid of admin who added
    updated_at TIMESTAMPTZ,
    updated_by VARCHAR(255),
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_users_department ON users(department_id);
CREATE INDEX IF NOT EXISTS idx_users_is_deleted ON users(is_deleted) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_users_persona_slug ON users(persona_slug) WHERE persona_slug IS NOT NULL;

-- Idempotent migration for existing databases (no-op on fresh installs).
-- ALTER TABLE users ADD COLUMN IF NOT EXISTS email VARCHAR(255);
-- ALTER TABLE users ADD COLUMN IF NOT EXISTS name VARCHAR(255);
-- ALTER TABLE users ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE;
-- ALTER TABLE users ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
-- ALTER TABLE users ADD COLUMN IF NOT EXISTS persona_slug TEXT;

-- experts (current schema)
CREATE TABLE IF NOT EXISTS experts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    expert_name VARCHAR(255) NOT NULL,
    highlight_text VARCHAR(4000) NOT NULL,
    highlights JSONB DEFAULT '[]',
    status VARCHAR(20) DEFAULT 'open',
    created_by_user_id UUID REFERENCES users(id),
    created_by_dep_id UUID REFERENCES properties(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(circular_id, expert_name)
);
CREATE INDEX IF NOT EXISTS idx_experts_circular_id ON experts(circular_id);
CREATE INDEX IF NOT EXISTS idx_experts_status ON experts(status);

-- comments
CREATE TABLE IF NOT EXISTS comments (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    expert_id   UUID NOT NULL REFERENCES experts(id) ON DELETE CASCADE,
    user_db_id  UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    text        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_comments_expert_id  ON comments(expert_id);
CREATE INDEX IF NOT EXISTS idx_comments_user_db_id ON comments(user_db_id);

-- summaries
CREATE TABLE IF NOT EXISTS summaries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id     UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    summary_key     TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (circular_id)
);

-- circular_references: extracted reference chain graph
-- One row per outgoing reference from a source circular.
-- Stores the raw target_circular_number always; target_circular_id (FK) is
-- nullable because the referenced circular may not yet exist in `circulars`.
-- A backfill job later resolves target_circular_id against the current state
-- of `circulars`.
CREATE TABLE IF NOT EXISTS circular_references (
    id                    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    source_circular_id    UUID         NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    target_circular_id    UUID             NULL REFERENCES circulars(id) ON DELETE SET NULL,
    target_circular_number TEXT        NOT NULL,
    relationship_type     TEXT         NOT NULL,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_relationship CHECK (relationship_type IN (
        'references', 'amends', 'supersedes', 'clarifies',
        'implements', 'modifies', 'rescinds', 'enforces'
    ))
);
-- Prevent duplicate edges: one row per (source, target number, relationship)
CREATE UNIQUE INDEX IF NOT EXISTS uq_circular_references_edge
    ON circular_references (source_circular_id, target_circular_number, relationship_type);
-- Outgoing edges: "what does this circular reference?"
CREATE INDEX IF NOT EXISTS idx_circular_references_source
    ON circular_references (source_circular_id);
-- Incoming edges: "what references this circular?"
CREATE INDEX IF NOT EXISTS idx_circular_references_target
    ON circular_references (target_circular_id)
    WHERE target_circular_id IS NOT NULL;
-- Fuzzy lookups by raw text (for display joins before FK is resolved)
CREATE INDEX IF NOT EXISTS idx_circular_references_target_number
    ON circular_references (target_circular_number);

-- expert_departments_mapping: many-to-many junction (one expert → many departments)
CREATE TABLE IF NOT EXISTS expert_departments_mapping (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    expert_id UUID NOT NULL REFERENCES experts(id) ON DELETE CASCADE,
    department_id UUID NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(expert_id, department_id)
);
CREATE INDEX IF NOT EXISTS idx_expert_departments_mapping_expert
    ON expert_departments_mapping(expert_id);
CREATE INDEX IF NOT EXISTS idx_expert_departments_mapping_department
    ON expert_departments_mapping(department_id);

-- =====================================================================
-- @mention notification module
-- =====================================================================

-- comment_mentions: one row per @mention parsed from a comment
CREATE TABLE IF NOT EXISTS comment_mentions (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    comment_id              UUID NOT NULL REFERENCES comments(id) ON DELETE CASCADE,
    expert_id               UUID NOT NULL REFERENCES experts(id)  ON DELETE CASCADE,
    mentioned_by_user_db_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    target_type             VARCHAR(16)  NOT NULL,
    target_id               VARCHAR(255) NOT NULL,
    target_label            VARCHAR(255) NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_mention_target_type CHECK (target_type IN ('user','department')),
    UNIQUE (comment_id, target_type, target_id)
);
CREATE INDEX IF NOT EXISTS idx_comment_mentions_comment ON comment_mentions(comment_id);
CREATE INDEX IF NOT EXISTS idx_comment_mentions_expert  ON comment_mentions(expert_id);
CREATE INDEX IF NOT EXISTS idx_comment_mentions_target  ON comment_mentions(target_type, target_id);

-- mention_notifications: fan-out delivery queue (one row per recipient)
CREATE TABLE IF NOT EXISTS mention_notifications (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mention_id          UUID NOT NULL REFERENCES comment_mentions(id) ON DELETE CASCADE,
    recipient_user_id   VARCHAR(255) NOT NULL,
    recipient_email     VARCHAR(255) NOT NULL,
    status              VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    notification_log_id UUID REFERENCES notification_logs(id),
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at             TIMESTAMPTZ,
    read_at             TIMESTAMPTZ,
    CONSTRAINT chk_mention_notif_status CHECK (status IN ('PENDING','SENT','FAILED','SKIPPED')),
    UNIQUE (mention_id, recipient_user_id)
);
CREATE INDEX IF NOT EXISTS idx_mention_notif_status    ON mention_notifications(status, created_at);
CREATE INDEX IF NOT EXISTS idx_mention_notif_recipient ON mention_notifications(recipient_user_id, created_at DESC);

-- CAPTCHA tables (added 2026-09)
CREATE TABLE IF NOT EXISTS captcha_challenges (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    answer_hash CHAR(64)    NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL,
    consumed    BOOLEAN     NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_captcha_challenges_expires
    ON captcha_challenges (expires_at);

CREATE TABLE IF NOT EXISTS captcha_images (
    challenge_id UUID        PRIMARY KEY REFERENCES captcha_challenges(id) ON DELETE CASCADE,
    png_bytes    BYTEA       NOT NULL,
    content_type VARCHAR(50) NOT NULL DEFAULT 'image/png',
    byte_len     INTEGER     NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- MFA tables (added 2026-09)
CREATE TABLE IF NOT EXISTS mfa_challenges (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_db_id  UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel     VARCHAR(16) NOT NULL CHECK (channel IN ('email','sms')),
    code_hash   VARCHAR(64) NOT NULL,
    attempts    INT         NOT NULL DEFAULT 0,
    consumed    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mfa_challenge_user_active
    ON mfa_challenges (user_db_id, consumed, expires_at);

CREATE TABLE IF NOT EXISTS mfa_pending_tokens (
    token_id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash        VARCHAR(64) NOT NULL UNIQUE,
    user_db_id        UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    issued_ip         INET,
    issued_user_agent TEXT,
    consumed          BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at        TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mfa_pending_user_active
    ON mfa_pending_tokens (user_db_id, consumed, expires_at);

-- DDL for action items table (12-10-2026)

CREATE TABLE IF NOT EXISTS action_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    action_item TEXT NOT NULL,
    deadline DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_action_items_circular_id
    ON action_items(circular_id);

-- DDL for per circular_persona_matches Many-to-many join table (added 13-10-2026)

CREATE TABLE IF NOT EXISTS circular_persona_matches (
    circular_id  UUID        NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    persona_slug TEXT        NOT NULL,
    matched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (circular_id, persona_slug)
);
CREATE INDEX IF NOT EXISTS idx_cpm_lookup ON circular_persona_matches(persona_slug);
CREATE INDEX IF NOT EXISTS idx_cpm_circular ON circular_persona_matches(circular_id);

-- DDL for refresh_tokens table (22-10-2026)
-- Opaque server-side refresh credentials. Only the SHA-256 hash is stored;
-- the plaintext exists solely in the HttpOnly cookie.
-- family_id groups a rotation chain so that presenting an already-consumed
-- token can be detected as theft and revoke the whole family.

CREATE TABLE IF NOT EXISTS refresh_tokens (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_db_id       UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash       VARCHAR(64) NOT NULL UNIQUE,
    family_id        UUID        NOT NULL,
    issued_ip        INET,
    issued_user_agent TEXT,
    consumed         BOOLEAN     NOT NULL DEFAULT FALSE,
    consumed_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at       TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_active
    ON refresh_tokens (user_db_id, consumed, expires_at);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_family
    ON refresh_tokens (family_id);

-- DDL for notification_outbox table (16-11-2026)

CREATE TABLE IF NOT EXISTS notification_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Recipient (user_id NULL for anonymous users)
    user_id UUID REFERENCES users(id),
    recipient_email VARCHAR(255) NOT NULL,

    -- Notification details
    channel VARCHAR(30) NOT NULL,
    template VARCHAR(150) NOT NULL,
    params JSONB,

    -- Scheduling
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Processing status
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,

    -- Worker details
    locked_by VARCHAR(100),
    locked_at TIMESTAMPTZ,

    -- Duplicate prevention
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    last_error TEXT,

    -- Constraints
    CONSTRAINT chk_notification_status
        CHECK (status IN ('PENDING', 'PROCESSING', 'SENT', 'FAILED')),

    -- EMAIL only for now; widen to ('EMAIL','SMS','PUSH') when needed
    CONSTRAINT chk_notification_channel
        CHECK (channel IN ('EMAIL')),

    CONSTRAINT chk_attempt_count
        CHECK (attempt_count >= 0 AND max_attempts > 0)
);

-- Index for fetching pending notifications
CREATE INDEX IF NOT EXISTS idx_notification_outbox_pending
    ON notification_outbox (scheduled_at)
    WHERE status = 'PENDING';

-- Index for recovering stuck jobs
CREATE INDEX IF NOT EXISTS idx_notification_outbox_processing
    ON notification_outbox (locked_at)
    WHERE status = 'PROCESSING';

-- Index for user notification history
CREATE INDEX IF NOT EXISTS idx_notification_outbox_user
    ON notification_outbox (user_id);

-- DDL for user_topics table (17-11-2026)
-- User-created notification topics: name + keywords + sources + alert cadence.
-- Matching engine reads keywords/sources later; alert_level drives delivery.

CREATE TABLE IF NOT EXISTS user_topics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    topic_name VARCHAR(150) NOT NULL,
    keywords JSONB NOT NULL DEFAULT '[]',   -- ["circuit limit", "margin"]
    sources JSONB NOT NULL DEFAULT '[]',    -- ["NSE", "SEBI"]
    alert_level VARCHAR(20) NOT NULL DEFAULT 'DAILY',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_user_topic_alert_level
        CHECK (alert_level IN ('IMMEDIATE', 'DAILY', 'WEEKLY', 'OFF')),
    CONSTRAINT uq_user_topic_name UNIQUE (user_id, topic_name)
);

CREATE INDEX IF NOT EXISTS idx_user_topics_user ON user_topics (user_id);

-- DDL for user_notification_prefs table (17-11-2026)
-- Per-user global digest setting: cadence of the scheduled email digest.
-- Topic alert levels (user_topics) are separate and unaffected.

CREATE TABLE IF NOT EXISTS user_notification_prefs (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    digest_frequency VARCHAR(10) NOT NULL DEFAULT 'NONE',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_user_notification_prefs_digest
        CHECK (digest_frequency IN ('DAILY', 'WEEKLY', 'NONE'))
);

-- Every new user gets a default digest setting ('NONE' = opt-in) at
-- creation time, whatever code path inserts the row.
CREATE OR REPLACE FUNCTION fn_user_notification_prefs_default()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO user_notification_prefs (user_id, digest_frequency)
    VALUES (NEW.id, 'NONE')
    ON CONFLICT (user_id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_users_default_notification_prefs ON users;
CREATE TRIGGER trg_users_default_notification_prefs
    AFTER INSERT ON users
    FOR EACH ROW EXECUTE FUNCTION fn_user_notification_prefs_default();
