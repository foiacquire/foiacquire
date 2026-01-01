-- Revert archive history tracking

DROP INDEX IF EXISTS idx_document_versions_earliest_archived;
DROP INDEX IF EXISTS idx_document_versions_archive_snapshot;
DROP INDEX IF EXISTS idx_archive_checks_version_source;
DROP INDEX IF EXISTS idx_archive_checks_result;
DROP INDEX IF EXISTS idx_archive_checks_checked_at;
DROP INDEX IF EXISTS idx_archive_checks_source;
DROP INDEX IF EXISTS idx_archive_checks_version;
DROP INDEX IF EXISTS idx_archive_snapshots_service_url;
DROP INDEX IF EXISTS idx_archive_snapshots_captured_at;
DROP INDEX IF EXISTS idx_archive_snapshots_original_url;
DROP INDEX IF EXISTS idx_archive_snapshots_service;

DROP TABLE IF EXISTS archive_checks;
DROP TABLE IF EXISTS archive_snapshots;

-- SQLite doesn't support DROP COLUMN, so we need to recreate the table
-- This is handled by creating a new table without the columns and copying data
CREATE TABLE document_versions_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    mime_type TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    source_url TEXT,
    original_filename TEXT,
    server_date TEXT,
    page_count INTEGER,
    content_hash_blake3 TEXT,
    FOREIGN KEY (document_id) REFERENCES documents(id)
);

INSERT INTO document_versions_new
SELECT id, document_id, content_hash, file_path, file_size, mime_type,
       acquired_at, source_url, original_filename, server_date, page_count, content_hash_blake3
FROM document_versions;

DROP TABLE document_versions;
ALTER TABLE document_versions_new RENAME TO document_versions;

-- Recreate indexes
CREATE INDEX idx_versions_document ON document_versions(document_id);
CREATE INDEX idx_versions_hash ON document_versions(content_hash);
CREATE INDEX idx_versions_mime_type ON document_versions(mime_type);
CREATE INDEX idx_versions_doc_mime ON document_versions(document_id, mime_type);
