//! OCR processing helper functions.

use crate::config::OcrConfig;
use crate::models::{Document, DocumentPage, PageOcrStatus};
use crate::ocr::{FallbackOcrBackend, OcrBackend, OcrConfig as OcrBackendConfig, TextExtractor};
use crate::repository::DieselDocumentRepository;

use super::types::PageOcrResult;

/// Extract text from a document per-page using pdftotext.
/// This function runs in a blocking context and uses the runtime handle to call async methods.
pub fn extract_document_text_per_page(
    doc: &Document,
    doc_repo: &DieselDocumentRepository,
    handle: &tokio::runtime::Handle,
) -> anyhow::Result<usize> {
    let extractor = TextExtractor::new();

    let version = doc
        .current_version()
        .ok_or_else(|| anyhow::anyhow!("Document has no versions"))?;

    // Only process PDFs with per-page extraction
    if version.mime_type != "application/pdf" {
        // For non-PDFs, use the old extraction method
        let result = extractor.extract(&version.file_path, &version.mime_type)?;

        // Create a single "page" for non-PDF documents
        let mut page = DocumentPage::new(doc.id.clone(), version.id, 1);
        page.pdf_text = Some(result.text.clone());
        page.final_text = Some(result.text);
        page.ocr_status = PageOcrStatus::OcrComplete;
        handle.block_on(doc_repo.save_page(&page))?;

        // Cache page count (1 for non-PDFs)
        let _ = handle.block_on(doc_repo.set_version_page_count(version.id, 1));

        // Non-PDFs are complete immediately - finalize the document
        let _ = handle.block_on(doc_repo.finalize_document(&doc.id));

        return Ok(1);
    }

    // Get page count (use cached value if available)
    let page_count = version.page_count.unwrap_or_else(|| {
        tracing::debug!(
            "Getting page count for document {}: {}",
            doc.id,
            version.file_path.display()
        );
        let count = extractor
            .get_pdf_page_count(&version.file_path)
            .unwrap_or(1);
        tracing::debug!("Document {} has {} pages", doc.id, count);
        count
    });

    // Cache page count if not already cached
    if version.page_count.is_none() {
        let _ = handle.block_on(doc_repo.set_version_page_count(version.id, page_count));
    }

    // Delete any existing pages for this document version (in case of re-processing)
    handle.block_on(doc_repo.delete_pages(&doc.id, version.id as i32))?;

    let mut pages_created = 0;

    for page_num in 1..=page_count {
        tracing::debug!(
            "Processing page {}/{} of document {}",
            page_num,
            page_count,
            doc.id
        );
        // Extract text using pdftotext
        let pdf_text = extractor
            .extract_pdf_page_text(&version.file_path, page_num)
            .unwrap_or_default();

        let mut page = DocumentPage::new(doc.id.clone(), version.id, page_num);
        page.pdf_text = Some(pdf_text.clone());
        page.ocr_status = PageOcrStatus::TextExtracted;

        tracing::debug!(
            "Saving page {}/{} to database for document {}",
            page_num,
            page_count,
            doc.id
        );
        let page_id = handle.block_on(doc_repo.save_page(&page))?;

        // Store pdftotext result in page_ocr_results for comparison
        if !pdf_text.is_empty() {
            let _ = handle.block_on(doc_repo.store_page_ocr_result(
                page_id,
                "pdftotext",
                None, // no model for pdftotext
                Some(&pdf_text),
                None, // no confidence score for pdftotext
                None, // no processing time tracked
                None, // no image hash for pdftotext (text extraction)
            ));
        }

        pages_created += 1;
    }

    Ok(pages_created)
}

/// Run OCR on a page and compare with existing text.
/// If all pages for this document are now complete, the document is finalized
/// (status set to OcrComplete, combined text saved).
/// This function runs in a blocking context and uses the runtime handle to call async methods.
///
/// Uses the default tesseract backend. For configurable fallback chains, use
/// `ocr_document_page_with_config`.
#[allow(dead_code)]
pub fn ocr_document_page(
    page: &DocumentPage,
    doc_repo: &DieselDocumentRepository,
    handle: &tokio::runtime::Handle,
) -> anyhow::Result<PageOcrResult> {
    ocr_document_page_with_config(page, doc_repo, handle, &OcrConfig::default())
}

/// Run OCR on a page using a configured fallback chain.
///
/// Tries each backend in the chain until one succeeds. On rate limit errors,
/// automatically falls back to the next backend.
pub fn ocr_document_page_with_config(
    page: &DocumentPage,
    doc_repo: &DieselDocumentRepository,
    handle: &tokio::runtime::Handle,
    ocr_config: &OcrConfig,
) -> anyhow::Result<PageOcrResult> {
    let extractor = TextExtractor::new();

    // Create fallback backend from config
    let fallback_backend = FallbackOcrBackend::from_config(
        &ocr_config.backends,
        ocr_config.always_tesseract,
        OcrBackendConfig::default(),
    );

    // Get the document to find the file path
    let doc = handle
        .block_on(doc_repo.get(&page.document_id))?
        .ok_or_else(|| anyhow::anyhow!("Document not found"))?;

    let version = doc
        .versions
        .iter()
        .find(|v| v.id == page.version_id)
        .ok_or_else(|| anyhow::anyhow!("Version not found"))?;

    // Run OCR on this page with image hash deduplication
    let mut updated_page = page.clone();
    let mut improved = false;

    // First, compute the image hash to check for existing OCR results
    let hash_result = extractor.get_pdf_page_hash(&version.file_path, page.page_number);

    // Check if we already have a result for this exact image from any backend in our chain
    let existing_result = if let Ok(ref image_hash) = hash_result {
        // Check each backend in the chain for existing results
        let mut found = None;
        for backend_name in &ocr_config.backends {
            if let Ok(Some(result)) =
                handle.block_on(doc_repo.find_ocr_result_by_image_hash(image_hash, backend_name))
            {
                found = Some((result, backend_name.clone()));
                break;
            }
        }
        found
    } else {
        None
    };

    if let Some((existing, backend_name)) = existing_result {
        // Reuse existing OCR result - skip expensive OCR call
        let ocr_text = existing.text.unwrap_or_default();
        let ocr_chars = ocr_text.chars().filter(|c| !c.is_whitespace()).count();
        let pdf_chars = page
            .pdf_text
            .as_ref()
            .map(|t| t.chars().filter(|c| !c.is_whitespace()).count())
            .unwrap_or(0);

        improved = ocr_chars > pdf_chars + (pdf_chars / 5);

        updated_page.ocr_text = Some(ocr_text.clone());
        updated_page.ocr_status = PageOcrStatus::OcrComplete;
        updated_page.final_text = if ocr_chars > 0 {
            Some(ocr_text.clone())
        } else {
            page.pdf_text.clone()
        };

        // Store reference to the deduplicated result
        let _ = handle.block_on(doc_repo.store_page_ocr_result(
            page.id,
            &backend_name,
            existing.model.as_deref(),
            Some(&ocr_text),
            existing.confidence,
            existing.processing_time_ms,
            hash_result.ok().as_deref(),
        ));

        tracing::debug!(
            "Reused existing {} OCR result for page {} (hash match)",
            backend_name,
            page.page_number
        );
    } else {
        // No existing result - run OCR with fallback chain
        // First get the image hash for storage
        let image_hash = hash_result.ok();

        match fallback_backend.ocr_pdf_page(&version.file_path, page.page_number) {
            Ok(result) => {
                let ocr_text = result.text;
                let backend_name = result.backend.as_str();
                let ocr_chars = ocr_text.chars().filter(|c| !c.is_whitespace()).count();
                let pdf_chars = page
                    .pdf_text
                    .as_ref()
                    .map(|t| t.chars().filter(|c| !c.is_whitespace()).count())
                    .unwrap_or(0);

                // Track if OCR provided more content (for reporting)
                improved = ocr_chars > pdf_chars + (pdf_chars / 5);

                updated_page.ocr_text = Some(ocr_text.clone());
                updated_page.ocr_status = PageOcrStatus::OcrComplete;

                // Prefer OCR over extracted text (unless OCR is empty)
                updated_page.final_text = if ocr_chars > 0 {
                    Some(ocr_text.clone())
                } else {
                    page.pdf_text.clone()
                };

                // Store result with actual backend name and image hash
                let _ = handle.block_on(doc_repo.store_page_ocr_result(
                    page.id,
                    backend_name,
                    result.model.as_deref(),
                    Some(&ocr_text),
                    result.confidence,
                    Some(result.processing_time_ms as i32),
                    image_hash.as_deref(),
                ));

                tracing::debug!(
                    "OCR completed for page {} using {} backend",
                    page.page_number,
                    backend_name
                );
            }
            Err(e) => {
                tracing::debug!(
                    "OCR failed for page {}, using PDF text: {}",
                    page.page_number,
                    e
                );
                // Mark as failed but still set final_text to PDF text so document can be finalized
                updated_page.ocr_status = PageOcrStatus::Failed;
                updated_page.final_text = page.pdf_text.clone();
            }
        }
    }

    handle.block_on(doc_repo.save_page(&updated_page))?;

    // Check if all pages for this document are now complete, and if so, finalize it
    let mut document_finalized = false;
    if handle
        .block_on(doc_repo.are_all_pages_complete(&page.document_id, page.version_id as i32))?
    {
        let _ = handle.block_on(doc_repo.finalize_document(&page.document_id));
        document_finalized = true;
        tracing::debug!(
            "Document {} finalized after page {} completed",
            page.document_id,
            page.page_number
        );
    }

    Ok(PageOcrResult {
        improved,
        document_finalized,
    })
}
