-- Add missing unique constraint on document_pages
-- This constraint is needed for ON CONFLICT to work with (document_id, version_id, page_number)

-- SQLite: Create unique index if not exists
CREATE UNIQUE INDEX IF NOT EXISTS idx_document_pages_unique
ON document_pages(document_id, version_id, page_number);
