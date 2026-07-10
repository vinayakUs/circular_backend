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
CREATE INDEX IF NOT EXISTS idx_properties_metadata_gin ON properties USING gin (metadata jsonb_path_ops);

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
    circular_id VARCHAR(50) NOT NULL,
    source VARCHAR(20) NOT NULL,
    source_item_key TEXT,
    full_reference TEXT NOT NULL,
    department VARCHAR(50),
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
    UNIQUE (source, source_item_key)
);
CREATE INDEX IF NOT EXISTS idx_circulars_status ON circulars(status);
CREATE INDEX IF NOT EXISTS idx_circulars_source ON circulars(source);
CREATE INDEX IF NOT EXISTS idx_circulars_issue_date ON circulars(issue_date DESC);

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

-- notification_logs
CREATE TABLE IF NOT EXISTS notification_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template_name VARCHAR(100) NOT NULL,
    recipient_email VARCHAR(255) NOT NULL,
    subject VARCHAR(500) NOT NULL,
    variables JSONB DEFAULT '{}',
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_notification_logs_status ON notification_logs(status);
CREATE INDEX IF NOT EXISTS idx_notification_logs_recipient ON notification_logs(recipient_email);

-- experts (renamed from circular_department_mapping) old table one to one mapping
-- CREATE TABLE IF NOT EXISTS experts (
--     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
--     circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
--     department_id UUID NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
--     expert_name VARCHAR(255) NOT NULL,
--     highlight_text VARCHAR(4000) NOT NULL,
--     highlights JSONB DEFAULT '[]',
--     status VARCHAR(20) DEFAULT 'open',
--     created_by_user_id UUID REFERENCES users(id),
--     created_by_dep_id UUID REFERENCES properties(id),
--     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
--     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
--     UNIQUE(circular_id, department_id, expert_name)
-- );
-- CREATE INDEX IF NOT EXISTS idx_experts_circular_id ON experts(circular_id);
-- CREATE INDEX IF NOT EXISTS idx_experts_department_id ON experts(department_id);
-- CREATE INDEX IF NOT EXISTS idx_experts_status ON experts(status);


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
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    expert_id UUID NOT NULL REFERENCES experts(id) ON DELETE CASCADE,
    user_id VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_comments_expert_id ON comments(expert_id);


-- users (LDAP user to department mapping with audit)
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(255) NOT NULL UNIQUE,    -- LDAP uid (unique)
    department_id UUID NOT NULL REFERENCES properties(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by VARCHAR(255) NOT NULL,        -- LDAP uid of admin who added
    updated_at TIMESTAMPTZ,
    updated_by VARCHAR(255)
);
CREATE INDEX IF NOT EXISTS idx_users_department ON users(department_id);


-- summaries

CREATE TABLE IF NOT EXISTS summaries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id     UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    summary_key     TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (circular_id)
  )


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