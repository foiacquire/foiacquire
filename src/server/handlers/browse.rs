//! Browse page handler.

use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse},
};
use serde::Deserialize;

use super::super::cache::StatsCache;
use super::super::templates;
use super::super::AppState;
use super::helpers::build_timeline_data;

/// Query params for the unified browse page.
#[derive(Debug, Clone, Deserialize)]
pub struct BrowseParams {
    pub types: Option<String>,
    pub tags: Option<String>,
    pub source: Option<String>,
    pub q: Option<String>,
    pub page: Option<usize>,
    pub per_page: Option<usize>,
}

/// Unified document browse page with filters.
pub async fn browse_documents(
    State(state): State<AppState>,
    Query(params): Query<BrowseParams>,
) -> impl IntoResponse {
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);
    let page = params.page.unwrap_or(1).clamp(1, 100_000);

    let types: Vec<String> = params
        .types
        .as_ref()
        .map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .take(20)
                .collect()
        })
        .unwrap_or_default();

    let tags: Vec<String> = params
        .tags
        .as_ref()
        .map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .take(50)
                .collect()
        })
        .unwrap_or_default();

    let (cached_total, skip_count) = if types.is_empty() && tags.is_empty() && params.q.is_none() {
        let count = if let Some(source_id) = params.source.as_deref() {
            state.doc_repo.count_by_source(source_id).await.ok()
        } else {
            state.doc_repo.count().await.ok()
        };
        (count, false)
    } else {
        let cache_key = StatsCache::browse_count_key(
            params.source.as_deref(),
            &types,
            &tags,
            params.q.as_deref(),
        );
        let cached = state.stats_cache.get_browse_count(&cache_key);
        (cached, cached.is_none())
    };

    let effective_total = if skip_count { Some(0) } else { cached_total };

    let has_filters = !types.is_empty() || !tags.is_empty() || params.q.is_some();

    // Run browse query
    let browse_result = match state
        .doc_repo
        .browse(
            &types,
            &tags,
            params.source.as_deref(),
            params.q.as_deref(),
            page,
            per_page,
            effective_total,
        )
        .await
    {
        Ok(result) => result,
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load documents: {}</p>", e),
                None,
            ));
        }
    };

    // Get type stats
    let type_stats: Vec<(String, u64)> = state
        .doc_repo
        .get_category_stats(None)
        .await
        .unwrap_or_default();

    // Get tags and sources if no filters are active
    let (all_tags, sources) = if has_filters {
        (Vec::new(), Vec::new())
    } else {
        let tags_result = state.doc_repo.get_all_tags().await.unwrap_or_default();
        let counts = state.doc_repo.get_all_source_counts().await.unwrap_or_default();
        let source_list = state.source_repo.get_all().await.unwrap_or_default();
        let sources_result: Vec<_> = source_list
            .into_iter()
            .map(|s| {
                let count = counts.get(&s.id).copied().unwrap_or(0);
                (s.id, s.name, count)
            })
            .collect();
        (tags_result, sources_result)
    };

    if skip_count {
        let state_for_count = state.clone();
        let types_bg = types.clone();
        let tags_bg = tags.clone();
        let source_bg = params.source.clone();
        let q_bg = params.q.clone();

        let cache_key = StatsCache::browse_count_key(
            source_bg.as_deref(),
            &types_bg,
            &tags_bg,
            q_bg.as_deref(),
        );

        tokio::spawn(async move {
            if let Ok(count) = state_for_count
                .doc_repo
                .browse_count(&types_bg, &tags_bg, source_bg.as_deref(), q_bg.as_deref())
                .await
            {
                state_for_count
                    .stats_cache
                    .set_browse_count(cache_key, count);
            }
        });
    }

    let timeline = build_timeline_data(&browse_result.documents);
    let timeline_json = serde_json::to_string(&timeline).unwrap_or_else(|_| "{}".to_string());

    let doc_data: Vec<_> = browse_result
        .documents
        .iter()
        .filter_map(|doc| {
            let version = doc.current_version()?;
            let display_name = version
                .original_filename
                .clone()
                .unwrap_or_else(|| doc.title.clone());

            Some((
                doc.id.clone(),
                display_name,
                doc.source_id.clone(),
                version.mime_type.clone(),
                version.file_size,
                version.acquired_at,
                doc.synopsis.clone(),
                doc.tags.clone(),
            ))
        })
        .collect();

    let content = templates::browse_page(
        &doc_data,
        &type_stats,
        &types,
        &tags,
        params.source.as_deref(),
        &all_tags,
        &sources,
        browse_result.prev_cursor.as_deref(),
        browse_result.next_cursor.as_deref(),
        browse_result.start_position,
        browse_result.total,
        per_page,
    );
    Html(templates::base_template(
        "Browse",
        &content,
        Some(&timeline_json),
    ))
}
