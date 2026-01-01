-- Archive history tracking for document provenance verification
-- Enables verifying when documents existed via web archives (Wayback Machine, archive.today, etc.)

-- Snapshots captured by archive services
CREATE TABLE archive_snapshots (
    id SERIAL PRIMARY KEY,
    service TEXT NOT NULL,              -- wayback, archive_today, common_crawl, etc.
    original_url TEXT NOT NULL,         -- URL that was archived
    archive_url TEXT NOT NULL,          -- URL to retrieve from archive
    captured_at TIMESTAMPTZ NOT NULL,   -- When archive captured it
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    http_status INTEGER,                -- Status code from archive metadata
    mimetype TEXT,                      -- MIME type from archive metadata
    content_length BIGINT,              -- File size from archive metadata
    digest TEXT,                        -- Archive's content hash (e.g., Wayback SHA-1)
    metadata JSONB NOT NULL DEFAULT '{}'
);

-- Track archive checks to avoid redundant queries
CREATE TABLE archive_checks (
    id SERIAL PRIMARY KEY,
    document_version_id INTEGER NOT NULL REFERENCES document_versions(id),
    archive_source TEXT NOT NULL,       -- Which service we queried
    url_checked TEXT NOT NULL,          -- URL we searched for
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    snapshots_found INTEGER NOT NULL DEFAULT 0,
    matching_snapshots INTEGER NOT NULL DEFAULT 0,
    result TEXT NOT NULL,               -- verified, new_versions, no_snapshots, error
    error_message TEXT
);

-- Add archive-related fields to document_versions
ALTER TABLE document_versions ADD COLUMN archive_snapshot_id INTEGER REFERENCES archive_snapshots(id);
ALTER TABLE document_versions ADD COLUMN earliest_archived_at TIMESTAMPTZ;

-- Indexes for efficient queries
CREATE INDEX idx_archive_snapshots_service ON archive_snapshots(service);
CREATE INDEX idx_archive_snapshots_original_url ON archive_snapshots(original_url);
CREATE INDEX idx_archive_snapshots_captured_at ON archive_snapshots(captured_at);
CREATE INDEX idx_archive_snapshots_service_url ON archive_snapshots(service, original_url);

CREATE INDEX idx_archive_checks_version ON archive_checks(document_version_id);
CREATE INDEX idx_archive_checks_source ON archive_checks(archive_source);
CREATE INDEX idx_archive_checks_checked_at ON archive_checks(checked_at);
CREATE INDEX idx_archive_checks_result ON archive_checks(result);
CREATE INDEX idx_archive_checks_version_source ON archive_checks(document_version_id, archive_source);

CREATE INDEX idx_document_versions_archive_snapshot ON document_versions(archive_snapshot_id)
    WHERE archive_snapshot_id IS NOT NULL;
CREATE INDEX idx_document_versions_earliest_archived ON document_versions(earliest_archived_at)
    WHERE earliest_archived_at IS NOT NULL;
