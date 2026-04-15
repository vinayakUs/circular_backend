CREATE EXTENSION IF NOT EXISTS pgcrypto;

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
    url TEXT,
    pdf_url TEXT,
    file_path VARCHAR(500),
    content_hash VARCHAR(64),
    status VARCHAR(20) NOT NULL DEFAULT 'DISCOVERED',
    error_message TEXT,
    detected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    es_indexed_at TIMESTAMPTZ,
    es_chunk_count INT,
    es_index_name VARCHAR(100),
    UNIQUE (source, circular_id)
);

CREATE INDEX IF NOT EXISTS idx_circulars_status ON circulars(status);
CREATE INDEX IF NOT EXISTS idx_circulars_source ON circulars(source);
CREATE INDEX IF NOT EXISTS idx_circulars_issue_date ON circulars(issue_date DESC);
CREATE INDEX IF NOT EXISTS idx_circulars_es_pending ON circulars(status, es_indexed_at) WHERE file_path IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_circulars_source_item_key ON circulars(source, source_item_key);

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

CREATE INDEX IF NOT EXISTS idx_circular_assets_circular_id
    ON circular_assets(circular_id);
CREATE INDEX IF NOT EXISTS idx_circular_assets_circular_role
    ON circular_assets(circular_id, asset_role);
CREATE UNIQUE INDEX IF NOT EXISTS idx_circular_assets_identity
    ON circular_assets(circular_id, asset_role, COALESCE(archive_member_path, ''));

CREATE TABLE IF NOT EXISTS scraper_checkpoints (
    source VARCHAR(20) PRIMARY KEY,
    last_run_date DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    es_bloom_filter BYTEA,
    es_last_run_at TIMESTAMPTZ,
    es_records_processed INT DEFAULT 0
);
