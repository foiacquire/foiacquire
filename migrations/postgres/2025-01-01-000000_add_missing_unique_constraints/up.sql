-- Add missing unique constraint on document_pages
-- This constraint is needed for ON CONFLICT to work with (document_id, version_id, page_number)

-- Drop the regular index if it exists (we'll replace with unique)
DROP INDEX IF EXISTS idx_pages_doc_version_page;

-- Add unique constraint (use CREATE UNIQUE INDEX for better control)
CREATE UNIQUE INDEX IF NOT EXISTS idx_document_pages_unique
ON document_pages(document_id, version_id, page_number);
