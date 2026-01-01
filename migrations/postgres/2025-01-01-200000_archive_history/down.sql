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

ALTER TABLE document_versions DROP COLUMN IF EXISTS earliest_archived_at;
ALTER TABLE document_versions DROP COLUMN IF EXISTS archive_snapshot_id;

DROP TABLE IF EXISTS archive_checks;
DROP TABLE IF EXISTS archive_snapshots;
