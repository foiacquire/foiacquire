//! Document repository for SQLite persistence.
//!
//! This module provides async database access for document operations using sqlx.

#![allow(dead_code)]

mod helpers;

use chrono::{DateTime, Utc};
use sqlx::sqlite::SqlitePool;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::{parse_datetime, parse_datetime_opt, Result};
use crate::models::{Document, DocumentStatus, DocumentVersion, VirtualFile};

// Re-export public types
pub use helpers::{
    extract_filename_parts, sanitize_filename, BrowseResult, DocumentNavigation, DocumentSummary,
    VersionSummary,
};

use helpers::DocumentPartial;

/// Row type for Document SQLx query mapping.
#[derive(sqlx::FromRow)]
struct DocumentRow {
    id: String,
    source_id: String,
    title: String,
    source_url: String,
    extracted_text: Option<String>,
    synopsis: Option<String>,
    tags: Option<String>,
    status: String,
    metadata: String,
    created_at: String,
    updated_at: String,
    discovery_method: String,
}

impl DocumentRow {
    fn into_partial(self) -> DocumentPartial {
        let tags: Vec<String> = self
            .tags
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        DocumentPartial {
            id: self.id,
            source_id: self.source_id,
            title: self.title,
            source_url: self.source_url,
            extracted_text: self.extracted_text,
            synopsis: self.synopsis,
            tags,
            status: DocumentStatus::from_str(&self.status).unwrap_or(DocumentStatus::Pending),
            metadata: serde_json::from_str(&self.metadata)
                .unwrap_or(serde_json::Value::Object(Default::default())),
            created_at: parse_datetime(&self.created_at),
            updated_at: parse_datetime(&self.updated_at),
            discovery_method: self.discovery_method,
        }
    }
}

/// Row type for DocumentVersion SQLx query mapping.
#[derive(sqlx::FromRow)]
struct VersionRow {
    id: i64,
    #[allow(dead_code)]
    document_id: String,
    content_hash: String,
    file_path: String,
    file_size: i64,
    mime_type: String,
    acquired_at: String,
    source_url: Option<String>,
    original_filename: Option<String>,
    server_date: Option<String>,
    page_count: Option<i64>,
}

impl From<VersionRow> for DocumentVersion {
    fn from(row: VersionRow) -> Self {
        DocumentVersion {
            id: row.id,
            content_hash: row.content_hash,
            file_path: PathBuf::from(row.file_path),
            file_size: row.file_size as u64,
            mime_type: row.mime_type,
            acquired_at: parse_datetime(&row.acquired_at),
            source_url: row.source_url,
            original_filename: row.original_filename,
            server_date: parse_datetime_opt(row.server_date),
            page_count: row.page_count.map(|c| c as u32),
        }
    }
}

/// Row type for VirtualFile SQLx query mapping.
#[derive(sqlx::FromRow)]
struct VirtualFileRow {
    id: String,
    document_id: String,
    version_id: i64,
    archive_path: String,
    filename: String,
    file_size: i64,
    mime_type: String,
    extracted_text: Option<String>,
    synopsis: Option<String>,
    tags: Option<String>,
    status: String,
    created_at: String,
    updated_at: String,
}

impl From<VirtualFileRow> for VirtualFile {
    fn from(row: VirtualFileRow) -> Self {
        use crate::models::VirtualFileStatus;
        VirtualFile {
            id: row.id,
            document_id: row.document_id,
            version_id: row.version_id,
            archive_path: row.archive_path,
            filename: row.filename,
            file_size: row.file_size as u64,
            mime_type: row.mime_type,
            extracted_text: row.extracted_text,
            synopsis: row.synopsis,
            tags: row
                .tags
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
            status: VirtualFileStatus::from_str(&row.status).unwrap_or(VirtualFileStatus::Pending),
            created_at: parse_datetime(&row.created_at),
            updated_at: parse_datetime(&row.updated_at),
        }
    }
}

/// Async SQLx-backed document repository.
#[derive(Clone)]
pub struct AsyncDocumentRepository {
    pool: SqlitePool,
    documents_dir: PathBuf,
}

impl AsyncDocumentRepository {
    /// Create a new async document repository with an existing pool.
    pub fn new(pool: SqlitePool, documents_dir: PathBuf) -> Self {
        Self {
            pool,
            documents_dir,
        }
    }

    /// Get the documents directory path.
    pub fn documents_dir(&self) -> &Path {
        &self.documents_dir
    }

    // ========================================================================
    // Core CRUD operations
    // ========================================================================

    /// Get a document by ID.
    pub async fn get(&self, id: &str) -> Result<Option<Document>> {
        let row = sqlx::query_as!(
            DocumentRow,
            r#"SELECT
                id as "id!",
                source_id as "source_id!",
                title as "title!",
                source_url as "source_url!",
                extracted_text,
                synopsis,
                tags,
                status as "status!",
                metadata as "metadata!",
                created_at as "created_at!",
                updated_at as "updated_at!",
                discovery_method as "discovery_method!"
               FROM documents WHERE id = ?"#,
            id
        )
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let partial = row.into_partial();
                let versions = self.load_versions(&partial.id).await?;
                Ok(Some(partial.with_versions(versions)))
            }
            None => Ok(None),
        }
    }

    /// Get a document by source URL.
    pub async fn get_by_url(&self, url: &str) -> Result<Option<Document>> {
        let row = sqlx::query_as!(
            DocumentRow,
            r#"SELECT
                id as "id!",
                source_id as "source_id!",
                title as "title!",
                source_url as "source_url!",
                extracted_text,
                synopsis,
                tags,
                status as "status!",
                metadata as "metadata!",
                created_at as "created_at!",
                updated_at as "updated_at!",
                discovery_method as "discovery_method!"
               FROM documents WHERE source_url = ?"#,
            url
        )
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let partial = row.into_partial();
                let versions = self.load_versions(&partial.id).await?;
                Ok(Some(partial.with_versions(versions)))
            }
            None => Ok(None),
        }
    }

    /// Get just the source URLs for a source (lightweight, for URL analysis).
    pub async fn get_urls_by_source(&self, source_id: &str) -> Result<Vec<String>> {
        let urls = sqlx::query_scalar!(
            r#"SELECT source_url as "source_url!" FROM documents WHERE source_id = ?"#,
            source_id
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(urls)
    }

    /// Get all source URLs as a HashSet for fast duplicate detection during import.
    pub async fn get_all_urls_set(&self) -> Result<HashSet<String>> {
        let urls = sqlx::query_scalar!(r#"SELECT source_url as "source_url!" FROM documents"#)
            .fetch_all(&self.pool)
            .await?;

        Ok(urls.into_iter().collect())
    }

    /// Get all content hashes as a HashSet for fast content deduplication during import.
    pub async fn get_all_content_hashes(&self) -> Result<HashSet<String>> {
        let hashes = sqlx::query_scalar!(
            r#"SELECT DISTINCT content_hash as "content_hash!" FROM document_versions"#
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(hashes.into_iter().collect())
    }

    /// Check if a document exists.
    pub async fn exists(&self, id: &str) -> Result<bool> {
        let count: i32 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i32" FROM documents WHERE id = ?"#,
            id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count > 0)
    }

    /// Check if content hash exists.
    pub async fn content_exists(&self, content_hash: &str) -> Result<bool> {
        let count: i32 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i32" FROM document_versions WHERE content_hash = ?"#,
            content_hash
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count > 0)
    }

    /// Save a document (insert or update).
    pub async fn save(&self, doc: &Document) -> Result<()> {
        let tags_json = serde_json::to_string(&doc.tags)?;
        let metadata_json = serde_json::to_string(&doc.metadata)?;
        let created_at = doc.created_at.to_rfc3339();
        let updated_at = doc.updated_at.to_rfc3339();
        let status = doc.status.as_str();

        sqlx::query!(
            r#"INSERT INTO documents (id, source_id, title, source_url, extracted_text, synopsis, tags, status, metadata, created_at, updated_at, discovery_method)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
               ON CONFLICT(id) DO UPDATE SET
                   title = excluded.title,
                   source_url = excluded.source_url,
                   extracted_text = excluded.extracted_text,
                   synopsis = excluded.synopsis,
                   tags = excluded.tags,
                   status = excluded.status,
                   metadata = excluded.metadata,
                   updated_at = excluded.updated_at"#,
            doc.id,
            doc.source_id,
            doc.title,
            doc.source_url,
            doc.extracted_text,
            doc.synopsis,
            tags_json,
            status,
            metadata_json,
            created_at,
            updated_at,
            doc.discovery_method
        )
        .execute(&self.pool)
        .await?;

        // Get existing version hashes
        let existing_hashes: Vec<String> = sqlx::query_scalar!(
            r#"SELECT content_hash as "content_hash!" FROM document_versions WHERE document_id = ?"#,
            doc.id
        )
        .fetch_all(&self.pool)
        .await?;

        // Insert new versions
        for version in &doc.versions {
            if !existing_hashes.contains(&version.content_hash) {
                let file_path = version.file_path.to_string_lossy().to_string();
                let file_size = version.file_size as i64;
                let acquired_at = version.acquired_at.to_rfc3339();
                let server_date = version.server_date.map(|d| d.to_rfc3339());
                let page_count = version.page_count.map(|c| c as i64);

                sqlx::query!(
                    r#"INSERT INTO document_versions
                        (document_id, content_hash, file_path, file_size, mime_type, acquired_at, source_url, original_filename, server_date, page_count)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"#,
                    doc.id,
                    version.content_hash,
                    file_path,
                    file_size,
                    version.mime_type,
                    acquired_at,
                    version.source_url,
                    version.original_filename,
                    server_date,
                    page_count
                )
                .execute(&self.pool)
                .await?;
            }
        }

        Ok(())
    }

    /// Delete a document and its versions.
    pub async fn delete(&self, id: &str) -> Result<bool> {
        sqlx::query!("DELETE FROM document_versions WHERE document_id = ?", id)
            .execute(&self.pool)
            .await?;

        let result = sqlx::query!("DELETE FROM documents WHERE id = ?", id)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Update the MIME type of a specific document version.
    pub async fn update_version_mime_type(
        &self,
        document_id: &str,
        version_id: i64,
        new_mime_type: &str,
    ) -> Result<()> {
        sqlx::query!(
            "UPDATE document_versions SET mime_type = ?1 WHERE document_id = ?2 AND id = ?3",
            new_mime_type,
            document_id,
            version_id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ========================================================================
    // Version loading helpers
    // ========================================================================

    /// Load versions for a document.
    async fn load_versions(&self, document_id: &str) -> Result<Vec<DocumentVersion>> {
        let rows = sqlx::query_as!(
            VersionRow,
            r#"SELECT
                id as "id!",
                document_id as "document_id!",
                content_hash as "content_hash!",
                file_path as "file_path!",
                file_size as "file_size!",
                mime_type as "mime_type!",
                acquired_at as "acquired_at!",
                source_url,
                original_filename,
                server_date,
                page_count
               FROM document_versions
               WHERE document_id = ?
               ORDER BY acquired_at DESC"#,
            document_id
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(DocumentVersion::from).collect())
    }

    /// Load versions for multiple documents in a single query.
    pub async fn load_versions_bulk(
        &self,
        document_ids: &[String],
    ) -> Result<HashMap<String, Vec<DocumentVersion>>> {
        if document_ids.is_empty() {
            return Ok(HashMap::new());
        }

        // SQLx doesn't support IN with dynamic arrays in query! macro,
        // so we use a different approach - fetch all then filter
        // For large datasets, this should be batched
        let mut versions_map: HashMap<String, Vec<DocumentVersion>> = HashMap::new();

        // Process in batches
        const BATCH_SIZE: usize = 100;
        for chunk in document_ids.chunks(BATCH_SIZE) {
            for doc_id in chunk {
                let versions = self.load_versions(doc_id).await?;
                versions_map.insert(doc_id.clone(), versions);
            }
        }

        Ok(versions_map)
    }

    // ========================================================================
    // Stats and counts
    // ========================================================================

    /// Count total documents.
    pub async fn count(&self) -> Result<u64> {
        let count: i32 = sqlx::query_scalar!(r#"SELECT COUNT(*) as "count!: i32" FROM documents"#)
            .fetch_one(&self.pool)
            .await?;

        Ok(count as u64)
    }

    /// Count documents by source.
    pub async fn count_by_source(&self, source_id: &str) -> Result<u64> {
        let count: i32 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i32" FROM documents WHERE source_id = ?"#,
            source_id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count as u64)
    }

    /// Count documents by status.
    pub async fn count_by_status(&self, status: DocumentStatus) -> Result<u64> {
        let status_str = status.as_str();
        let count: i32 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i32" FROM documents WHERE status = ?"#,
            status_str
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count as u64)
    }

    /// Update document status.
    pub async fn update_status(&self, id: &str, status: DocumentStatus) -> Result<()> {
        let status_str = status.as_str();
        let now = Utc::now().to_rfc3339();

        sqlx::query!(
            "UPDATE documents SET status = ?, updated_at = ? WHERE id = ?",
            status_str,
            now,
            id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Update document extracted text.
    pub async fn update_extracted_text(&self, id: &str, text: Option<&str>) -> Result<()> {
        let now = Utc::now().to_rfc3339();

        sqlx::query!(
            "UPDATE documents SET extracted_text = ?, updated_at = ? WHERE id = ?",
            text,
            now,
            id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Update document synopsis.
    pub async fn update_synopsis(&self, id: &str, synopsis: Option<&str>) -> Result<()> {
        let now = Utc::now().to_rfc3339();

        sqlx::query!(
            "UPDATE documents SET synopsis = ?, updated_at = ? WHERE id = ?",
            synopsis,
            now,
            id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get all documents by source.
    pub async fn get_by_source(&self, source_id: &str) -> Result<Vec<Document>> {
        let rows = sqlx::query_as!(
            DocumentRow,
            r#"SELECT
                id as "id!",
                source_id as "source_id!",
                title as "title!",
                source_url as "source_url!",
                extracted_text,
                synopsis,
                tags,
                status as "status!",
                metadata as "metadata!",
                created_at as "created_at!",
                updated_at as "updated_at!",
                discovery_method as "discovery_method!"
               FROM documents WHERE source_id = ?"#,
            source_id
        )
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions_map = self.load_versions_bulk(&doc_ids).await?;

        let docs = rows
            .into_iter()
            .map(|row| {
                let id = row.id.clone();
                let partial = row.into_partial();
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    /// Get documents by status.
    pub async fn get_by_status(&self, status: DocumentStatus) -> Result<Vec<Document>> {
        let status_str = status.as_str();

        let rows = sqlx::query_as!(
            DocumentRow,
            r#"SELECT
                id as "id!",
                source_id as "source_id!",
                title as "title!",
                source_url as "source_url!",
                extracted_text,
                synopsis,
                tags,
                status as "status!",
                metadata as "metadata!",
                created_at as "created_at!",
                updated_at as "updated_at!",
                discovery_method as "discovery_method!"
               FROM documents WHERE status = ?"#,
            status_str
        )
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions_map = self.load_versions_bulk(&doc_ids).await?;

        let docs = rows
            .into_iter()
            .map(|row| {
                let id = row.id.clone();
                let partial = row.into_partial();
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    /// Get all documents.
    pub async fn get_all(&self) -> Result<Vec<Document>> {
        let rows = sqlx::query_as!(
            DocumentRow,
            r#"SELECT
                id as "id!",
                source_id as "source_id!",
                title as "title!",
                source_url as "source_url!",
                extracted_text,
                synopsis,
                tags,
                status as "status!",
                metadata as "metadata!",
                created_at as "created_at!",
                updated_at as "updated_at!",
                discovery_method as "discovery_method!"
               FROM documents ORDER BY updated_at DESC"#
        )
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions_map = self.load_versions_bulk(&doc_ids).await?;

        let docs = rows
            .into_iter()
            .map(|row| {
                let id = row.id.clone();
                let partial = row.into_partial();
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    /// Get documents filtered by tag.
    pub async fn get_by_tag(&self, tag: &str, source_id: Option<&str>) -> Result<Vec<Document>> {
        let tag_pattern = format!("%\"{}%", tag.to_lowercase());

        let rows = match source_id {
            Some(sid) => {
                sqlx::query_as!(
                    DocumentRow,
                    r#"SELECT
                        id as "id!",
                        source_id as "source_id!",
                        title as "title!",
                        source_url as "source_url!",
                        extracted_text,
                        synopsis,
                        tags,
                        status as "status!",
                        metadata as "metadata!",
                        created_at as "created_at!",
                        updated_at as "updated_at!",
                        discovery_method as "discovery_method!"
                       FROM documents WHERE LOWER(tags) LIKE ?1 AND source_id = ?2 ORDER BY updated_at DESC"#,
                    tag_pattern,
                    sid
                )
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as!(
                    DocumentRow,
                    r#"SELECT
                        id as "id!",
                        source_id as "source_id!",
                        title as "title!",
                        source_url as "source_url!",
                        extracted_text,
                        synopsis,
                        tags,
                        status as "status!",
                        metadata as "metadata!",
                        created_at as "created_at!",
                        updated_at as "updated_at!",
                        discovery_method as "discovery_method!"
                       FROM documents WHERE LOWER(tags) LIKE ?1 ORDER BY updated_at DESC"#,
                    tag_pattern
                )
                .fetch_all(&self.pool)
                .await?
            }
        };

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions_map = self.load_versions_bulk(&doc_ids).await?;

        let docs = rows
            .into_iter()
            .map(|row| {
                let id = row.id.clone();
                let partial = row.into_partial();
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    /// Get documents filtered by type category (pdf, image, office, etc.).
    pub async fn get_by_type_category(
        &self,
        type_name: &str,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        use crate::utils::mime_type_sql_condition;

        let mime_condition = match mime_type_sql_condition(type_name) {
            Some(cond) => cond,
            None => return Ok(vec![]), // Unknown type category
        };
        let limit_val = limit.max(1) as i64;

        // Build dynamic query based on source_id
        let rows = match source_id {
            Some(sid) => {
                // Use raw query since mime_condition is dynamic
                sqlx::query_as::<_, DocumentRow>(&format!(
                    r#"SELECT d.id, d.source_id, d.title, d.source_url, d.extracted_text,
                              d.synopsis, d.tags, d.status, d.metadata, d.created_at,
                              d.updated_at, d.discovery_method
                       FROM documents d
                       JOIN document_versions dv ON d.id = dv.document_id
                       WHERE {} AND d.source_id = ?
                       GROUP BY d.id
                       ORDER BY d.updated_at DESC
                       LIMIT ?"#,
                    mime_condition
                ))
                .bind(sid)
                .bind(limit_val)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, DocumentRow>(&format!(
                    r#"SELECT d.id, d.source_id, d.title, d.source_url, d.extracted_text,
                              d.synopsis, d.tags, d.status, d.metadata, d.created_at,
                              d.updated_at, d.discovery_method
                       FROM documents d
                       JOIN document_versions dv ON d.id = dv.document_id
                       WHERE {}
                       GROUP BY d.id
                       ORDER BY d.updated_at DESC
                       LIMIT ?"#,
                    mime_condition
                ))
                .bind(limit_val)
                .fetch_all(&self.pool)
                .await?
            }
        };

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions_map = self.load_versions_bulk(&doc_ids).await?;

        let docs = rows
            .into_iter()
            .map(|row| {
                let id = row.id.clone();
                let partial = row.into_partial();
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    /// Get the version ID for a document's current version.
    pub async fn get_current_version_id(&self, document_id: &str) -> Result<Option<i64>> {
        let id: Option<i64> = sqlx::query_scalar!(
            r#"SELECT id as "id!" FROM document_versions WHERE document_id = ? ORDER BY acquired_at DESC LIMIT 1"#,
            document_id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(id)
    }

    /// Insert a new virtual file.
    pub async fn insert_virtual_file(&self, vf: &VirtualFile) -> Result<()> {
        let tags_json = serde_json::to_string(&vf.tags).unwrap_or_else(|_| "[]".to_string());
        let file_size = vf.file_size as i64;
        let created_at = vf.created_at.to_rfc3339();
        let updated_at = vf.updated_at.to_rfc3339();
        let status = vf.status.as_str();

        sqlx::query!(
            r#"INSERT INTO virtual_files (id, document_id, version_id, archive_path, filename, mime_type, file_size, extracted_text, synopsis, tags, status, created_at, updated_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"#,
            vf.id,
            vf.document_id,
            vf.version_id,
            vf.archive_path,
            vf.filename,
            vf.mime_type,
            file_size,
            vf.extracted_text,
            vf.synopsis,
            tags_json,
            status,
            created_at,
            updated_at
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Count archive documents that haven't been processed for virtual files.
    pub async fn count_unprocessed_archives(&self, source_id: Option<&str>) -> Result<u64> {
        let count: i32 = match source_id {
            Some(sid) => {
                sqlx::query_scalar!(
                    r#"SELECT COUNT(*) as "count!: i32" FROM documents d
                       JOIN document_versions dv ON d.id = dv.document_id
                       WHERE (dv.mime_type = 'application/zip' OR dv.mime_type = 'application/x-zip-compressed')
                       AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
                       AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)
                       AND d.source_id = ?"#,
                    sid
                )
                .fetch_one(&self.pool)
                .await?
            }
            None => {
                sqlx::query_scalar!(
                    r#"SELECT COUNT(*) as "count!: i32" FROM documents d
                       JOIN document_versions dv ON d.id = dv.document_id
                       WHERE (dv.mime_type = 'application/zip' OR dv.mime_type = 'application/x-zip-compressed')
                       AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
                       AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)"#
                )
                .fetch_one(&self.pool)
                .await?
            }
        };

        Ok(count as u64)
    }

    /// Count email documents that haven't been processed for attachments.
    pub async fn count_unprocessed_emails(&self, source_id: Option<&str>) -> Result<u64> {
        let count: i32 = match source_id {
            Some(sid) => {
                sqlx::query_scalar!(
                    r#"SELECT COUNT(*) as "count!: i32" FROM documents d
                       JOIN document_versions dv ON d.id = dv.document_id
                       WHERE dv.mime_type = 'message/rfc822'
                       AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
                       AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)
                       AND d.source_id = ?"#,
                    sid
                )
                .fetch_one(&self.pool)
                .await?
            }
            None => {
                sqlx::query_scalar!(
                    r#"SELECT COUNT(*) as "count!: i32" FROM documents d
                       JOIN document_versions dv ON d.id = dv.document_id
                       WHERE dv.mime_type = 'message/rfc822'
                       AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
                       AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)"#
                )
                .fetch_one(&self.pool)
                .await?
            }
        };

        Ok(count as u64)
    }

    /// Get archive documents that haven't been processed for virtual files.
    pub async fn get_unprocessed_archives(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let limit_val = limit.max(1) as i64;

        let rows = match source_id {
            Some(sid) => {
                sqlx::query_as!(
                    DocumentRow,
                    r#"SELECT d.id as "id!", d.source_id as "source_id!", d.title as "title!",
                              d.source_url as "source_url!", d.extracted_text, d.synopsis, d.tags,
                              d.status as "status!", d.metadata as "metadata!", d.created_at as "created_at!",
                              d.updated_at as "updated_at!", d.discovery_method as "discovery_method!"
                       FROM documents d
                       JOIN document_versions dv ON d.id = dv.document_id
                       WHERE (dv.mime_type = 'application/zip' OR dv.mime_type = 'application/x-zip-compressed')
                       AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
                       AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)
                       AND d.source_id = ?
                       ORDER BY d.updated_at DESC
                       LIMIT ?"#,
                    sid,
                    limit_val
                )
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as!(
                    DocumentRow,
                    r#"SELECT d.id as "id!", d.source_id as "source_id!", d.title as "title!",
                              d.source_url as "source_url!", d.extracted_text, d.synopsis, d.tags,
                              d.status as "status!", d.metadata as "metadata!", d.created_at as "created_at!",
                              d.updated_at as "updated_at!", d.discovery_method as "discovery_method!"
                       FROM documents d
                       JOIN document_versions dv ON d.id = dv.document_id
                       WHERE (dv.mime_type = 'application/zip' OR dv.mime_type = 'application/x-zip-compressed')
                       AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
                       AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)
                       ORDER BY d.updated_at DESC
                       LIMIT ?"#,
                    limit_val
                )
                .fetch_all(&self.pool)
                .await?
            }
        };

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions_map = self.load_versions_bulk(&doc_ids).await?;

        let docs = rows
            .into_iter()
            .map(|row| {
                let id = row.id.clone();
                let partial = row.into_partial();
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    /// Get email documents that haven't been processed for attachments.
    pub async fn get_unprocessed_emails(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let limit_val = limit.max(1) as i64;

        let rows = match source_id {
            Some(sid) => {
                sqlx::query_as!(
                    DocumentRow,
                    r#"SELECT d.id as "id!", d.source_id as "source_id!", d.title as "title!",
                              d.source_url as "source_url!", d.extracted_text, d.synopsis, d.tags,
                              d.status as "status!", d.metadata as "metadata!", d.created_at as "created_at!",
                              d.updated_at as "updated_at!", d.discovery_method as "discovery_method!"
                       FROM documents d
                       JOIN document_versions dv ON d.id = dv.document_id
                       WHERE dv.mime_type = 'message/rfc822'
                       AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
                       AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)
                       AND d.source_id = ?
                       ORDER BY d.updated_at DESC
                       LIMIT ?"#,
                    sid,
                    limit_val
                )
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as!(
                    DocumentRow,
                    r#"SELECT d.id as "id!", d.source_id as "source_id!", d.title as "title!",
                              d.source_url as "source_url!", d.extracted_text, d.synopsis, d.tags,
                              d.status as "status!", d.metadata as "metadata!", d.created_at as "created_at!",
                              d.updated_at as "updated_at!", d.discovery_method as "discovery_method!"
                       FROM documents d
                       JOIN document_versions dv ON d.id = dv.document_id
                       WHERE dv.mime_type = 'message/rfc822'
                       AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
                       AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)
                       ORDER BY d.updated_at DESC
                       LIMIT ?"#,
                    limit_val
                )
                .fetch_all(&self.pool)
                .await?
            }
        };

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions_map = self.load_versions_bulk(&doc_ids).await?;

        let docs = rows
            .into_iter()
            .map(|row| {
                let id = row.id.clone();
                let partial = row.into_partial();
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    // ========================================================================
    // Annotation tracking
    // ========================================================================

    /// Record that an annotation was completed for a document.
    pub async fn record_annotation(
        &self,
        document_id: &str,
        annotation_type: &str,
        version: i32,
        result: Option<&str>,
        error: Option<&str>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            r#"INSERT INTO document_annotations (document_id, annotation_type, completed_at, version, result, error)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6)
               ON CONFLICT(document_id, annotation_type) DO UPDATE SET
                   completed_at = excluded.completed_at,
                   version = excluded.version,
                   result = excluded.result,
                   error = excluded.error"#
        )
        .bind(document_id)
        .bind(annotation_type)
        .bind(&now)
        .bind(version)
        .bind(result)
        .bind(error)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Check if a specific annotation type has been completed for a document.
    pub async fn has_annotation(&self, document_id: &str, annotation_type: &str) -> Result<bool> {
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM document_annotations WHERE document_id = ? AND annotation_type = ?"
        )
        .bind(document_id)
        .bind(annotation_type)
        .fetch_one(&self.pool)
        .await?;

        Ok(count.0 > 0)
    }

    /// Get documents missing a specific annotation type.
    pub async fn get_documents_missing_annotation(
        &self,
        annotation_type: &str,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        let base_query = r#"
            SELECT d.id FROM documents d
            WHERE NOT EXISTS (
                SELECT 1 FROM document_annotations da
                WHERE da.document_id = d.id AND da.annotation_type = ?
            )
        "#;

        let ids: Vec<(String,)> = match source_id {
            Some(sid) => {
                let sql = format!("{} AND d.source_id = ? LIMIT ?", base_query);
                sqlx::query_as(&sql)
                    .bind(annotation_type)
                    .bind(sid)
                    .bind(limit as i64)
                    .fetch_all(&self.pool)
                    .await?
            }
            None => {
                let sql = format!("{} LIMIT ?", base_query);
                sqlx::query_as(&sql)
                    .bind(annotation_type)
                    .bind(limit as i64)
                    .fetch_all(&self.pool)
                    .await?
            }
        };

        Ok(ids.into_iter().map(|(id,)| id).collect())
    }

    // ========================================================================
    // Date estimation
    // ========================================================================

    /// Count documents needing date estimation.
    pub async fn count_documents_needing_date_estimation(
        &self,
        source_id: Option<&str>,
    ) -> Result<u64> {
        let base_query = r#"
            SELECT COUNT(*) FROM documents d
            JOIN document_versions dv ON d.id = dv.document_id
            WHERE d.estimated_date IS NULL
              AND d.manual_date IS NULL
              AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
              AND NOT EXISTS (
                  SELECT 1 FROM document_annotations da
                  WHERE da.document_id = d.id AND da.annotation_type = 'date_detection'
              )
        "#;

        let count: (i64,) = match source_id {
            Some(sid) => {
                let sql = format!("{} AND d.source_id = ?", base_query);
                sqlx::query_as(&sql)
                    .bind(sid)
                    .fetch_one(&self.pool)
                    .await?
            }
            None => {
                sqlx::query_as(base_query)
                    .fetch_one(&self.pool)
                    .await?
            }
        };

        Ok(count.0 as u64)
    }

    /// Get documents that need date estimation.
    /// Returns (doc_id, filename, server_date, acquired_at, source_url).
    #[allow(clippy::type_complexity)]
    pub async fn get_documents_needing_date_estimation(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Option<String>, Option<DateTime<Utc>>, DateTime<Utc>, Option<String>)>>
    {
        #[derive(sqlx::FromRow)]
        struct DateEstRow {
            id: String,
            original_filename: Option<String>,
            server_date: Option<String>,
            acquired_at: String,
            source_url: Option<String>,
        }

        let base_query = r#"
            SELECT d.id, dv.original_filename, dv.server_date, dv.acquired_at, d.source_url
            FROM documents d
            JOIN document_versions dv ON d.id = dv.document_id
            WHERE d.estimated_date IS NULL
              AND d.manual_date IS NULL
              AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
              AND NOT EXISTS (
                  SELECT 1 FROM document_annotations da
                  WHERE da.document_id = d.id AND da.annotation_type = 'date_detection'
              )
        "#;

        let rows: Vec<DateEstRow> = match source_id {
            Some(sid) => {
                let sql = format!("{} AND d.source_id = ? LIMIT ?", base_query);
                sqlx::query_as(&sql)
                    .bind(sid)
                    .bind(limit as i64)
                    .fetch_all(&self.pool)
                    .await?
            }
            None => {
                let sql = format!("{} LIMIT ?", base_query);
                sqlx::query_as(&sql)
                    .bind(limit as i64)
                    .fetch_all(&self.pool)
                    .await?
            }
        };

        let results = rows
            .into_iter()
            .map(|row| {
                let server_dt = parse_datetime_opt(row.server_date);
                let acquired_dt = parse_datetime(&row.acquired_at);

                (row.id, row.original_filename, server_dt, acquired_dt, row.source_url)
            })
            .collect();

        Ok(results)
    }

    /// Update estimated date for a document.
    pub async fn update_estimated_date(
        &self,
        document_id: &str,
        estimated_date: DateTime<Utc>,
        confidence: &str,
        source: &str,
    ) -> Result<()> {
        let estimated_date_str = estimated_date.to_rfc3339();
        let now = Utc::now().to_rfc3339();

        sqlx::query(
            "UPDATE documents SET estimated_date = ?, date_confidence = ?, date_source = ?, updated_at = ? WHERE id = ?"
        )
        .bind(&estimated_date_str)
        .bind(confidence)
        .bind(source)
        .bind(&now)
        .bind(document_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get document counts grouped by status.
    pub async fn count_all_by_status(&self) -> Result<HashMap<String, u64>> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT status, COUNT(*) FROM documents GROUP BY status"
        )
        .fetch_all(&self.pool)
        .await?;

        let mut counts = HashMap::new();
        for (status, count) in rows {
            counts.insert(status, count as u64);
        }

        Ok(counts)
    }

    /// Count documents needing OCR.
    pub async fn count_needing_ocr(&self, source_id: Option<&str>) -> Result<u64> {
        let base_query = r#"
            SELECT COUNT(DISTINCT d.id) FROM documents d
            JOIN document_versions dv ON dv.document_id = d.id
            WHERE d.status = 'downloaded'
              AND dv.mime_type IN ('application/pdf', 'image/png', 'image/jpeg', 'image/tiff', 'image/gif', 'image/bmp', 'text/plain', 'text/html')
        "#;

        let count: (i64,) = match source_id {
            Some(sid) => {
                let sql = format!("{} AND d.source_id = ?", base_query);
                sqlx::query_as(&sql)
                    .bind(sid)
                    .fetch_one(&self.pool)
                    .await?
            }
            None => {
                sqlx::query_as(base_query)
                    .fetch_one(&self.pool)
                    .await?
            }
        };

        Ok(count.0 as u64)
    }

    /// Count documents needing LLM summarization.
    pub async fn count_needing_summarization(&self, source_id: Option<&str>) -> Result<u64> {
        let base_query = r#"
            SELECT COUNT(DISTINCT d.id) FROM documents d
            JOIN document_pages dp ON dp.document_id = d.id
            WHERE d.synopsis IS NULL
              AND dp.final_text IS NOT NULL AND LENGTH(dp.final_text) > 0
        "#;

        let count: (i64,) = match source_id {
            Some(sid) => {
                let sql = format!("{} AND d.source_id = ?", base_query);
                sqlx::query_as(&sql)
                    .bind(sid)
                    .fetch_one(&self.pool)
                    .await?
            }
            None => {
                sqlx::query_as(base_query)
                    .fetch_one(&self.pool)
                    .await?
            }
        };

        Ok(count.0 as u64)
    }

    /// Get documents needing LLM summarization.
    pub async fn get_needing_summarization(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let limit_val = limit.max(1) as i64;

        let rows: Vec<DocumentRow> = match source_id {
            Some(sid) => {
                sqlx::query_as(
                    r#"SELECT DISTINCT d.id, d.source_id, d.title,
                              d.source_url, d.extracted_text, d.synopsis, d.tags,
                              d.status, d.metadata, d.created_at,
                              d.updated_at, d.discovery_method
                       FROM documents d
                       JOIN document_pages dp ON dp.document_id = d.id
                       WHERE d.synopsis IS NULL
                         AND d.source_id = ?
                         AND dp.final_text IS NOT NULL AND LENGTH(dp.final_text) > 0
                       LIMIT ?"#,
                )
                .bind(sid)
                .bind(limit_val)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as(
                    r#"SELECT DISTINCT d.id, d.source_id, d.title,
                              d.source_url, d.extracted_text, d.synopsis, d.tags,
                              d.status, d.metadata, d.created_at,
                              d.updated_at, d.discovery_method
                       FROM documents d
                       JOIN document_pages dp ON dp.document_id = d.id
                       WHERE d.synopsis IS NULL
                         AND dp.final_text IS NOT NULL AND LENGTH(dp.final_text) > 0
                       LIMIT ?"#,
                )
                .bind(limit_val)
                .fetch_all(&self.pool)
                .await?
            }
        };

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions_map = self.load_versions_bulk(&doc_ids).await?;

        let docs = rows
            .into_iter()
            .map(|row| {
                let id = row.id.clone();
                let partial = row.into_partial();
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    // ========================================================================
    // Page operations
    // ========================================================================

    /// Get all pages for a document version.
    pub async fn get_pages(
        &self,
        document_id: &str,
        version_id: i64,
    ) -> Result<Vec<crate::models::DocumentPage>> {
        use crate::models::PageOcrStatus;

        let rows: Vec<(i64, String, i64, i32, Option<String>, Option<String>, Option<String>, String, String, String)> =
            sqlx::query_as(
                "SELECT id, document_id, version_id, page_number, pdf_text, ocr_text, final_text, ocr_status, created_at, updated_at
                 FROM document_pages WHERE document_id = ? AND version_id = ? ORDER BY page_number"
            )
            .bind(document_id)
            .bind(version_id)
            .fetch_all(&self.pool)
            .await?;

        let pages = rows
            .into_iter()
            .map(|(id, doc_id, ver_id, page_num, pdf_text, ocr_text, final_text, ocr_status, created_at, updated_at)| {
                crate::models::DocumentPage {
                    id,
                    document_id: doc_id,
                    version_id: ver_id,
                    page_number: page_num as u32,
                    pdf_text,
                    ocr_text,
                    final_text,
                    ocr_status: PageOcrStatus::from_str(&ocr_status).unwrap_or(PageOcrStatus::Pending),
                    created_at: parse_datetime(&created_at),
                    updated_at: parse_datetime(&updated_at),
                }
            })
            .collect();

        Ok(pages)
    }

    /// Get combined final text for all pages of a document.
    pub async fn get_combined_page_text(
        &self,
        document_id: &str,
        version_id: i64,
    ) -> Result<Option<String>> {
        let pages = self.get_pages(document_id, version_id).await?;

        if pages.is_empty() {
            return Ok(None);
        }

        let combined: String = pages
            .into_iter()
            .filter_map(|p| p.final_text)
            .collect::<Vec<_>>()
            .join("\n\n");

        if combined.is_empty() {
            Ok(None)
        } else {
            Ok(Some(combined))
        }
    }

    // ========================================================================
    // OCR-related operations
    // ========================================================================

    /// Count pages needing OCR (status = 'text_extracted').
    pub async fn count_pages_needing_ocr(&self) -> Result<u64> {
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM document_pages WHERE ocr_status = 'text_extracted'"
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count.0 as u64)
    }

    /// Get pages needing OCR (status = 'text_extracted').
    pub async fn get_pages_needing_ocr(&self, limit: usize) -> Result<Vec<crate::models::DocumentPage>> {
        use crate::models::PageOcrStatus;
        let limit_val = limit.max(1) as i64;

        let rows: Vec<(i64, String, i64, i32, Option<String>, Option<String>, Option<String>, String, String, String)> =
            sqlx::query_as(
                r#"SELECT id, document_id, version_id, page_number, pdf_text, ocr_text, final_text, ocr_status, created_at, updated_at
                   FROM document_pages
                   WHERE ocr_status = 'text_extracted'
                   ORDER BY
                       CASE
                           WHEN pdf_text IS NULL OR pdf_text = '' THEN 0
                           WHEN LENGTH(pdf_text) < 100 THEN 1
                           ELSE 2
                       END,
                       created_at ASC
                   LIMIT ?"#
            )
            .bind(limit_val)
            .fetch_all(&self.pool)
            .await?;

        let pages = rows
            .into_iter()
            .map(|(id, doc_id, ver_id, page_num, pdf_text, ocr_text, final_text, ocr_status, created_at, updated_at)| {
                crate::models::DocumentPage {
                    id,
                    document_id: doc_id,
                    version_id: ver_id,
                    page_number: page_num as u32,
                    pdf_text,
                    ocr_text,
                    final_text,
                    ocr_status: PageOcrStatus::from_str(&ocr_status).unwrap_or(PageOcrStatus::Pending),
                    created_at: parse_datetime(&created_at),
                    updated_at: parse_datetime(&updated_at),
                }
            })
            .collect();

        Ok(pages)
    }

    /// Get documents needing OCR processing.
    pub async fn get_needing_ocr(&self, source_id: Option<&str>, limit: usize) -> Result<Vec<Document>> {
        let limit_val = limit.max(1) as i64;

        // OCR supported MIME types
        let mime_types = [
            "application/pdf", "image/png", "image/jpeg", "image/tiff",
            "image/gif", "image/bmp", "text/plain", "text/html"
        ];
        let placeholders: String = mime_types.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

        let rows: Vec<DocumentRow> = match source_id {
            Some(sid) => {
                let sql = format!(
                    r#"SELECT d.id, d.source_id, d.title, d.source_url, d.extracted_text,
                              d.synopsis, d.tags, d.status, d.metadata, d.created_at,
                              d.updated_at, d.discovery_method
                       FROM documents d
                       JOIN document_versions dv ON dv.document_id = d.id
                       WHERE d.status = 'downloaded'
                         AND dv.mime_type IN ({})
                         AND d.source_id = ?
                       GROUP BY d.id
                       LIMIT ?"#,
                    placeholders
                );
                let mut query = sqlx::query_as::<_, DocumentRow>(&sql);
                for mime in &mime_types {
                    query = query.bind(*mime);
                }
                query.bind(sid).bind(limit_val).fetch_all(&self.pool).await?
            }
            None => {
                let sql = format!(
                    r#"SELECT d.id, d.source_id, d.title, d.source_url, d.extracted_text,
                              d.synopsis, d.tags, d.status, d.metadata, d.created_at,
                              d.updated_at, d.discovery_method
                       FROM documents d
                       JOIN document_versions dv ON dv.document_id = d.id
                       WHERE d.status = 'downloaded'
                         AND dv.mime_type IN ({})
                       GROUP BY d.id
                       LIMIT ?"#,
                    placeholders
                );
                let mut query = sqlx::query_as::<_, DocumentRow>(&sql);
                for mime in &mime_types {
                    query = query.bind(*mime);
                }
                query.bind(limit_val).fetch_all(&self.pool).await?
            }
        };

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions_map = self.load_versions_bulk(&doc_ids).await?;

        let docs = rows
            .into_iter()
            .map(|row| {
                let id = row.id.clone();
                let partial = row.into_partial();
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    /// Save a document page (insert or update).
    pub async fn save_page(&self, page: &crate::models::DocumentPage) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        let ocr_status = page.ocr_status.as_str();

        let result = sqlx::query(
            r#"INSERT INTO document_pages
               (document_id, version_id, page_number, pdf_text, ocr_text, final_text, ocr_status, created_at, updated_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
               ON CONFLICT(document_id, version_id, page_number) DO UPDATE SET
                   pdf_text = COALESCE(?4, pdf_text),
                   ocr_text = COALESCE(?5, ocr_text),
                   final_text = COALESCE(?6, final_text),
                   ocr_status = ?7,
                   updated_at = ?8"#
        )
        .bind(&page.document_id)
        .bind(page.version_id)
        .bind(page.page_number as i32)
        .bind(&page.pdf_text)
        .bind(&page.ocr_text)
        .bind(&page.final_text)
        .bind(ocr_status)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Set the cached page count for a document version.
    pub async fn set_version_page_count(&self, version_id: i64, page_count: u32) -> Result<()> {
        sqlx::query("UPDATE document_versions SET page_count = ? WHERE id = ?")
            .bind(page_count as i64)
            .bind(version_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Delete all pages for a document version.
    pub async fn delete_pages(&self, document_id: &str, version_id: i64) -> Result<u64> {
        let result = sqlx::query("DELETE FROM document_pages WHERE document_id = ? AND version_id = ?")
            .bind(document_id)
            .bind(version_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    /// Store an OCR result for a page.
    pub async fn store_page_ocr_result(
        &self,
        page_id: i64,
        backend: &str,
        ocr_text: Option<&str>,
        confidence: Option<f64>,
        processing_time_ms: Option<u64>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let time_ms = processing_time_ms.map(|t| t as i64);

        sqlx::query(
            r#"INSERT INTO page_ocr_results (page_id, backend, ocr_text, confidence, processing_time_ms, created_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(page_id, backend) DO UPDATE SET
                   ocr_text = excluded.ocr_text,
                   confidence = excluded.confidence,
                   processing_time_ms = excluded.processing_time_ms,
                   created_at = excluded.created_at"#
        )
        .bind(page_id)
        .bind(backend)
        .bind(ocr_text)
        .bind(confidence)
        .bind(time_ms)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Check if all pages for a document version are done processing.
    pub async fn are_all_pages_complete(&self, document_id: &str, version_id: i64) -> Result<bool> {
        let row: (i64, i64) = sqlx::query_as(
            r#"SELECT COUNT(*), SUM(CASE WHEN ocr_status IN ('ocr_complete', 'failed', 'skipped') THEN 1 ELSE 0 END)
               FROM document_pages WHERE document_id = ? AND version_id = ?"#
        )
        .bind(document_id)
        .bind(version_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.0 > 0 && row.0 == row.1)
    }

    /// Finalize a document by combining page text and setting status to OcrComplete.
    pub async fn finalize_document(&self, document_id: &str) -> Result<bool> {
        let doc = match self.get(document_id).await? {
            Some(d) => d,
            None => return Ok(false),
        };

        let version = match doc.current_version() {
            Some(v) => v,
            None => return Ok(false),
        };

        let combined_text = match self.get_combined_page_text(document_id, version.id).await? {
            Some(t) if !t.is_empty() => t,
            _ => return Ok(false),
        };

        // Update document with combined text and status
        let mut updated_doc = doc.clone();
        updated_doc.extracted_text = Some(combined_text.clone());
        updated_doc.status = crate::models::DocumentStatus::OcrComplete;
        updated_doc.updated_at = Utc::now();
        self.save(&updated_doc).await?;

        // Write text file alongside the document
        let text_path = version.file_path.with_extension(format!(
            "{}.txt",
            version.file_path.extension().unwrap_or_default().to_string_lossy()
        ));
        let _ = std::fs::write(&text_path, &combined_text);

        Ok(true)
    }

    /// Find and finalize all documents that have all pages OCR complete.
    pub async fn finalize_pending_documents(&self, source_id: Option<&str>) -> Result<usize> {
        let sql = match source_id {
            Some(_) => {
                r#"SELECT DISTINCT d.id FROM documents d
                   JOIN document_versions dv ON dv.document_id = d.id
                   JOIN document_pages dp ON dp.document_id = d.id AND dp.version_id = dv.id
                   WHERE d.status != 'ocr_complete'
                     AND d.source_id = ?
                   GROUP BY d.id, dp.version_id
                   HAVING COUNT(*) = SUM(CASE WHEN dp.ocr_status = 'ocr_complete' THEN 1 ELSE 0 END)
                     AND COUNT(*) > 0"#
            }
            None => {
                r#"SELECT DISTINCT d.id FROM documents d
                   JOIN document_versions dv ON dv.document_id = d.id
                   JOIN document_pages dp ON dp.document_id = d.id AND dp.version_id = dv.id
                   WHERE d.status != 'ocr_complete'
                   GROUP BY d.id, dp.version_id
                   HAVING COUNT(*) = SUM(CASE WHEN dp.ocr_status = 'ocr_complete' THEN 1 ELSE 0 END)
                     AND COUNT(*) > 0"#
            }
        };

        let doc_ids: Vec<(String,)> = match source_id {
            Some(sid) => {
                sqlx::query_as(sql)
                    .bind(sid)
                    .fetch_all(&self.pool)
                    .await?
            }
            None => {
                sqlx::query_as(sql)
                    .fetch_all(&self.pool)
                    .await?
            }
        };

        let mut finalized = 0;
        for (doc_id,) in doc_ids {
            if self.finalize_document(&doc_id).await? {
                finalized += 1;
            }
        }

        Ok(finalized)
    }

    // ========== Stats and Aggregation Methods ==========

    /// Get all source document counts.
    pub async fn get_all_source_counts(&self) -> Result<HashMap<String, u64>> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT source_id, COUNT(*) as count FROM documents GROUP BY source_id",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(source_id, count)| (source_id, count as u64))
            .collect())
    }

    /// Get document type statistics (by MIME type).
    pub async fn get_type_stats(&self, source_id: Option<&str>) -> Result<Vec<(String, u64)>> {
        let rows: Vec<(String, i64)> = match source_id {
            Some(sid) => {
                sqlx::query_as(
                    r#"SELECT v.mime_type, COUNT(DISTINCT d.id) as count
                       FROM documents d
                       JOIN document_versions v ON v.document_id = d.id
                       WHERE d.source_id = ?
                       GROUP BY v.mime_type
                       ORDER BY count DESC"#,
                )
                .bind(sid)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as(
                    r#"SELECT v.mime_type, COUNT(DISTINCT d.id) as count
                       FROM documents d
                       JOIN document_versions v ON v.document_id = d.id
                       GROUP BY v.mime_type
                       ORDER BY count DESC"#,
                )
                .fetch_all(&self.pool)
                .await?
            }
        };

        Ok(rows
            .into_iter()
            .map(|(mime, count)| (mime, count as u64))
            .collect())
    }

    /// Get document category statistics (aggregated from MIME types).
    pub async fn get_category_stats(&self, source_id: Option<&str>) -> Result<Vec<(String, u64)>> {
        let type_stats = self.get_type_stats(source_id).await?;
        let mut cat_counts: HashMap<String, u64> = HashMap::new();

        for (mime, count) in type_stats {
            let category = helpers::mime_to_category(&mime).to_string();
            *cat_counts.entry(category).or_default() += count;
        }

        let mut result: Vec<_> = cat_counts.into_iter().collect();
        result.sort_by(|a, b| b.1.cmp(&a.1));
        Ok(result)
    }

    /// Get all tags with document counts.
    pub async fn get_all_tags(&self) -> Result<Vec<(String, usize)>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT tags FROM documents WHERE tags IS NOT NULL AND tags != '[]'",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut tag_counts: HashMap<String, usize> = HashMap::new();
        for (tags_json,) in rows {
            if let Ok(tags) = serde_json::from_str::<Vec<String>>(&tags_json) {
                for tag in tags {
                    *tag_counts.entry(tag).or_default() += 1;
                }
            }
        }

        let mut result: Vec<_> = tag_counts.into_iter().collect();
        result.sort_by(|a, b| b.1.cmp(&a.1));
        Ok(result)
    }

    /// Search tags by prefix.
    pub async fn search_tags(&self, query: &str, limit: usize) -> Result<Vec<(String, usize)>> {
        let all_tags = self.get_all_tags().await?;
        let query_lower = query.to_lowercase();

        let matching: Vec<_> = all_tags
            .into_iter()
            .filter(|(tag, _)| tag.to_lowercase().contains(&query_lower))
            .take(limit)
            .collect();

        Ok(matching)
    }

    // ========== Summaries and Navigation ==========

    /// Get all document summaries (lightweight, without extracted_text).
    pub async fn get_all_summaries(&self) -> Result<Vec<DocumentSummary>> {
        let rows: Vec<DocumentRow> = sqlx::query_as(
            r#"SELECT id, source_id, title, source_url, extracted_text, synopsis, tags,
                      status, metadata, created_at, updated_at, discovery_method
               FROM documents
               ORDER BY updated_at DESC"#,
        )
        .fetch_all(&self.pool)
        .await?;

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions = self.load_versions_bulk(&doc_ids).await?;

        let summaries = rows
            .into_iter()
            .map(|row| {
                let partial = row.into_partial();
                let version_list = versions.get(&partial.id).cloned().unwrap_or_default();
                let current_version = version_list.last().map(|v| VersionSummary {
                    content_hash: v.content_hash.clone(),
                    file_path: v.file_path.clone(),
                    file_size: v.file_size,
                    mime_type: v.mime_type.clone(),
                    acquired_at: v.acquired_at,
                    original_filename: v.original_filename.clone(),
                    server_date: v.server_date,
                });

                DocumentSummary {
                    id: partial.id,
                    source_id: partial.source_id,
                    title: partial.title,
                    source_url: partial.source_url,
                    synopsis: partial.synopsis,
                    tags: partial.tags,
                    status: partial.status,
                    created_at: partial.created_at,
                    updated_at: partial.updated_at,
                    current_version,
                }
            })
            .collect();

        Ok(summaries)
    }

    /// Get document summaries for a specific source.
    pub async fn get_summaries_by_source(&self, source_id: &str) -> Result<Vec<DocumentSummary>> {
        let rows: Vec<DocumentRow> = sqlx::query_as(
            r#"SELECT id, source_id, title, source_url, extracted_text, synopsis, tags,
                      status, metadata, created_at, updated_at, discovery_method
               FROM documents
               WHERE source_id = ?
               ORDER BY updated_at DESC"#,
        )
        .bind(source_id)
        .fetch_all(&self.pool)
        .await?;

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions = self.load_versions_bulk(&doc_ids).await?;

        let summaries = rows
            .into_iter()
            .map(|row| {
                let partial = row.into_partial();
                let version_list = versions.get(&partial.id).cloned().unwrap_or_default();
                let current_version = version_list.last().map(|v| VersionSummary {
                    content_hash: v.content_hash.clone(),
                    file_path: v.file_path.clone(),
                    file_size: v.file_size,
                    mime_type: v.mime_type.clone(),
                    acquired_at: v.acquired_at,
                    original_filename: v.original_filename.clone(),
                    server_date: v.server_date,
                });

                DocumentSummary {
                    id: partial.id,
                    source_id: partial.source_id,
                    title: partial.title,
                    source_url: partial.source_url,
                    synopsis: partial.synopsis,
                    tags: partial.tags,
                    status: partial.status,
                    created_at: partial.created_at,
                    updated_at: partial.updated_at,
                    current_version,
                }
            })
            .collect();

        Ok(summaries)
    }

    /// Get recent documents (lightweight summaries).
    pub async fn get_recent(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DocumentSummary>> {
        let rows: Vec<DocumentRow> = match source_id {
            Some(sid) => {
                sqlx::query_as(
                    r#"SELECT id, source_id, title, source_url, extracted_text, synopsis, tags,
                              status, metadata, created_at, updated_at, discovery_method
                       FROM documents
                       WHERE source_id = ?
                       ORDER BY updated_at DESC
                       LIMIT ?"#,
                )
                .bind(sid)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as(
                    r#"SELECT id, source_id, title, source_url, extracted_text, synopsis, tags,
                              status, metadata, created_at, updated_at, discovery_method
                       FROM documents
                       ORDER BY updated_at DESC
                       LIMIT ?"#,
                )
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
        };

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions = self.load_versions_bulk(&doc_ids).await?;

        let summaries = rows
            .into_iter()
            .map(|row| {
                let partial = row.into_partial();
                let version_list = versions.get(&partial.id).cloned().unwrap_or_default();
                let current_version = version_list.last().map(|v| VersionSummary {
                    content_hash: v.content_hash.clone(),
                    file_path: v.file_path.clone(),
                    file_size: v.file_size,
                    mime_type: v.mime_type.clone(),
                    acquired_at: v.acquired_at,
                    original_filename: v.original_filename.clone(),
                    server_date: v.server_date,
                });

                DocumentSummary {
                    id: partial.id,
                    source_id: partial.source_id,
                    title: partial.title,
                    source_url: partial.source_url,
                    synopsis: partial.synopsis,
                    tags: partial.tags,
                    status: partial.status,
                    created_at: partial.created_at,
                    updated_at: partial.updated_at,
                    current_version,
                }
            })
            .collect();

        Ok(summaries)
    }

    // ========== Content Hash Operations ==========

    /// Get all content hashes with document info.
    pub async fn get_content_hashes(&self) -> Result<Vec<(String, String, String, String)>> {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            r#"SELECT d.id, d.source_id, v.content_hash, d.title
               FROM documents d
               JOIN document_versions v ON v.document_id = d.id
               ORDER BY d.id"#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Find sources that have a document with the given content hash.
    pub async fn find_sources_by_hash(
        &self,
        content_hash: &str,
        exclude_source: Option<&str>,
    ) -> Result<Vec<(String, String, String)>> {
        let rows: Vec<(String, String, String)> = match exclude_source {
            Some(excl) => {
                sqlx::query_as(
                    r#"SELECT d.source_id, d.id, d.title
                       FROM documents d
                       JOIN document_versions v ON v.document_id = d.id
                       WHERE v.content_hash = ? AND d.source_id != ?"#,
                )
                .bind(content_hash)
                .bind(excl)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as(
                    r#"SELECT d.source_id, d.id, d.title
                       FROM documents d
                       JOIN document_versions v ON v.document_id = d.id
                       WHERE v.content_hash = ?"#,
                )
                .bind(content_hash)
                .fetch_all(&self.pool)
                .await?
            }
        };

        Ok(rows)
    }

    // ========== Virtual Files ==========

    /// Get virtual files for a document.
    pub async fn get_virtual_files(&self, document_id: &str) -> Result<Vec<VirtualFile>> {
        let rows: Vec<VirtualFileRow> = sqlx::query_as(
            r#"SELECT id, document_id, version_id, archive_path, filename,
                      file_size, mime_type, extracted_text, synopsis, tags,
                      status, created_at, updated_at
               FROM virtual_files
               WHERE document_id = ?
               ORDER BY filename"#,
        )
        .bind(document_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    // ========== Page Operations ==========

    /// Count pages for a document version.
    pub async fn count_pages(&self, document_id: &str, version_id: i64) -> Result<u32> {
        let (count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM document_pages WHERE document_id = ? AND version_id = ?",
        )
        .bind(document_id)
        .bind(version_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(count as u32)
    }

    /// Get document navigation (prev/next within filtered list).
    pub async fn get_document_navigation(
        &self,
        doc_id: &str,
        types: &[String],
        tags: &[String],
        source_id: Option<&str>,
        search_query: Option<&str>,
    ) -> Result<Option<DocumentNavigation>> {
        // Build dynamic WHERE clause
        let mut conditions = vec!["1=1".to_string()];
        let mut params: Vec<String> = Vec::new();

        if let Some(sid) = source_id {
            conditions.push("d.source_id = ?".to_string());
            params.push(sid.to_string());
        }

        if !types.is_empty() {
            let type_conditions: Vec<String> = types
                .iter()
                .filter_map(|t| helpers::mime_type_condition(t))
                .collect();
            if !type_conditions.is_empty() {
                conditions.push(format!("({})", type_conditions.join(" OR ")));
            }
        }

        for tag in tags {
            conditions.push("d.tags LIKE ?".to_string());
            params.push(format!("%\"{}%", tag));
        }

        if let Some(q) = search_query {
            conditions.push("(d.title LIKE ? OR d.extracted_text LIKE ?)".to_string());
            let like_pattern = format!("%{}%", q);
            params.push(like_pattern.clone());
            params.push(like_pattern);
        }

        let where_clause = conditions.join(" AND ");

        // Query with window functions to get position, prev, next
        let sql = format!(
            r#"WITH filtered AS (
                SELECT d.id, d.title,
                       ROW_NUMBER() OVER (ORDER BY d.updated_at DESC) as row_num
                FROM documents d
                JOIN document_versions v ON v.document_id = d.id
                WHERE {}
            ),
            current AS (
                SELECT row_num FROM filtered WHERE id = ?
            ),
            total AS (
                SELECT COUNT(*) as cnt FROM filtered
            )
            SELECT
                (SELECT id FROM filtered WHERE row_num = current.row_num - 1) as prev_id,
                (SELECT title FROM filtered WHERE row_num = current.row_num - 1) as prev_title,
                (SELECT id FROM filtered WHERE row_num = current.row_num + 1) as next_id,
                (SELECT title FROM filtered WHERE row_num = current.row_num + 1) as next_title,
                current.row_num as position,
                total.cnt as total
            FROM current, total"#,
            where_clause
        );

        // Build query dynamically
        let mut query = sqlx::query_as::<
            _,
            (
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                i64,
                i64,
            ),
        >(&sql);

        for param in &params {
            query = query.bind(param);
        }
        query = query.bind(doc_id);

        let result = query.fetch_optional(&self.pool).await?;

        Ok(result.map(
            |(prev_id, prev_title, next_id, next_title, position, total)| DocumentNavigation {
                prev_id,
                prev_title,
                next_id,
                next_title,
                position: position as u64,
                total: total as u64,
            },
        ))
    }

    // ========== Browse (Pagination) ==========

    /// Browse documents with filtering and pagination.
    pub async fn browse(
        &self,
        types: &[String],
        tags: &[String],
        source_id: Option<&str>,
        search_query: Option<&str>,
        page: usize,
        per_page: usize,
        cached_total: Option<u64>,
    ) -> Result<BrowseResult> {
        // Build dynamic WHERE clause
        let mut conditions = vec!["1=1".to_string()];
        let mut params: Vec<String> = Vec::new();

        if let Some(sid) = source_id {
            conditions.push("d.source_id = ?".to_string());
            params.push(sid.to_string());
        }

        if !types.is_empty() {
            let type_conditions: Vec<String> = types
                .iter()
                .filter_map(|t| helpers::mime_type_condition(t))
                .collect();
            if !type_conditions.is_empty() {
                conditions.push(format!("({})", type_conditions.join(" OR ")));
            }
        }

        for tag in tags {
            conditions.push("d.tags LIKE ?".to_string());
            params.push(format!("%\"{}%", tag));
        }

        if let Some(q) = search_query {
            conditions.push("(d.title LIKE ? OR d.extracted_text LIKE ?)".to_string());
            let like_pattern = format!("%{}%", q);
            params.push(like_pattern.clone());
            params.push(like_pattern);
        }

        let where_clause = conditions.join(" AND ");
        let offset = (page.saturating_sub(1)) * per_page;

        // Get total count
        let total = if let Some(cached) = cached_total {
            cached
        } else {
            let count_sql = format!(
                r#"SELECT COUNT(DISTINCT d.id)
                   FROM documents d
                   JOIN document_versions v ON v.document_id = d.id
                   WHERE {}"#,
                where_clause
            );

            let mut count_query = sqlx::query_as::<_, (i64,)>(&count_sql);
            for param in &params {
                count_query = count_query.bind(param);
            }

            let (count,) = count_query.fetch_one(&self.pool).await?;
            count as u64
        };

        // Get documents
        let select_sql = format!(
            r#"SELECT DISTINCT d.id, d.source_id, d.title, d.source_url, d.extracted_text,
                      d.synopsis, d.tags, d.status, d.metadata, d.created_at, d.updated_at,
                      d.discovery_method
               FROM documents d
               JOIN document_versions v ON v.document_id = d.id
               WHERE {}
               ORDER BY d.updated_at DESC
               LIMIT ? OFFSET ?"#,
            where_clause
        );

        let mut select_query = sqlx::query_as::<_, DocumentRow>(&select_sql);
        for param in &params {
            select_query = select_query.bind(param);
        }
        select_query = select_query.bind(per_page as i64).bind(offset as i64);

        let rows: Vec<DocumentRow> = select_query.fetch_all(&self.pool).await?;

        let doc_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let versions = self.load_versions_bulk(&doc_ids).await?;

        let documents: Vec<Document> = rows
            .into_iter()
            .map(|row| {
                let partial = row.into_partial();
                let doc_versions = versions.get(&partial.id).cloned().unwrap_or_default();
                partial.with_versions(doc_versions)
            })
            .collect();

        let start_position = offset as u64 + 1;
        let prev_cursor = if page > 1 {
            Some(format!("{}", page - 1))
        } else {
            None
        };
        let next_cursor = if (offset + per_page) < total as usize {
            Some(format!("{}", page + 1))
        } else {
            None
        };

        Ok(BrowseResult {
            documents,
            prev_cursor,
            next_cursor,
            start_position,
            total,
        })
    }

    /// Count documents matching browse filters.
    pub async fn browse_count(
        &self,
        types: &[String],
        tags: &[String],
        source_id: Option<&str>,
        search_query: Option<&str>,
    ) -> Result<u64> {
        let mut conditions = vec!["1=1".to_string()];
        let mut params: Vec<String> = Vec::new();

        if let Some(sid) = source_id {
            conditions.push("d.source_id = ?".to_string());
            params.push(sid.to_string());
        }

        if !types.is_empty() {
            let type_conditions: Vec<String> = types
                .iter()
                .filter_map(|t| helpers::mime_type_condition(t))
                .collect();
            if !type_conditions.is_empty() {
                conditions.push(format!("({})", type_conditions.join(" OR ")));
            }
        }

        for tag in tags {
            conditions.push("d.tags LIKE ?".to_string());
            params.push(format!("%\"{}%", tag));
        }

        if let Some(q) = search_query {
            conditions.push("(d.title LIKE ? OR d.extracted_text LIKE ?)".to_string());
            let like_pattern = format!("%{}%", q);
            params.push(like_pattern.clone());
            params.push(like_pattern);
        }

        let where_clause = conditions.join(" AND ");
        let sql = format!(
            r#"SELECT COUNT(DISTINCT d.id)
               FROM documents d
               JOIN document_versions v ON v.document_id = d.id
               WHERE {}"#,
            where_clause
        );

        let mut query = sqlx::query_as::<_, (i64,)>(&sql);
        for param in &params {
            query = query.bind(param);
        }

        let (count,) = query.fetch_one(&self.pool).await?;
        Ok(count as u64)
    }

    // ========== OCR Results ==========

    /// Get pages that don't have results from a specific backend.
    pub async fn get_pages_without_backend(
        &self,
        document_id: &str,
        backend: &str,
    ) -> Result<Vec<(i64, i64)>> {
        let rows: Vec<(i64, i64)> = sqlx::query_as(
            r#"SELECT dp.id, dp.page_number
               FROM document_pages dp
               LEFT JOIN ocr_results ocr ON ocr.page_id = dp.id AND ocr.backend = ?
               WHERE dp.document_id = ? AND ocr.id IS NULL
               ORDER BY dp.page_number"#,
        )
        .bind(backend)
        .bind(document_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Get OCR results for multiple pages in bulk.
    pub async fn get_pages_ocr_results_bulk(
        &self,
        page_ids: &[i64],
    ) -> Result<HashMap<i64, Vec<(String, Option<String>, Option<f64>, Option<i64>)>>> {
        if page_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let placeholders: String = page_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            r#"SELECT page_id, backend, text, confidence, processing_time_ms
               FROM ocr_results
               WHERE page_id IN ({})
               ORDER BY page_id, backend"#,
            placeholders
        );

        let mut query = sqlx::query_as::<_, (i64, String, Option<String>, Option<f64>, Option<i64>)>(&sql);
        for id in page_ids {
            query = query.bind(id);
        }

        let rows = query.fetch_all(&self.pool).await?;

        let mut result: HashMap<i64, Vec<(String, Option<String>, Option<f64>, Option<i64>)>> =
            HashMap::new();
        for (page_id, backend, text, confidence, processing_time) in rows {
            result
                .entry(page_id)
                .or_default()
                .push((backend, text, confidence, processing_time));
        }

        Ok(result)
    }
}
