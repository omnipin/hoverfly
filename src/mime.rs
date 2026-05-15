//! Minimal extension-based Content-Type lookup, used by collection
//! uploads to populate per-file metadata in the manifest so the
//! gateway returns the correct `Content-Type` header on retrieval
//! (otherwise everything is served as `text/plain` and modern browsers
//! refuse to load stylesheets and ES modules with a MIME mismatch).
//!
//! Covers the common types a VitePress / Vite / Next.js static export
//! produces; anything not in this list falls back to `None`, which the
//! manifest encoder serializes as no `Content-Type` entry — the
//! gateway then defaults to `application/octet-stream`.

/// Map a file path's extension to a sensible Content-Type, or `None`
/// if the extension isn't in the table.
pub fn guess_from_path(path: &str) -> Option<&'static str> {
    let lower = path.to_ascii_lowercase();
    let ext = lower.rsplit('.').next()?;
    match ext {
        // images
        "jpg" | "jpeg" => Some("image/jpeg"),
        "png" => Some("image/png"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "avif" => Some("image/avif"),
        "svg" => Some("image/svg+xml"),
        "ico" => Some("image/x-icon"),
        // web
        "html" | "htm" => Some("text/html; charset=utf-8"),
        "css" => Some("text/css; charset=utf-8"),
        "js" | "mjs" => Some("application/javascript; charset=utf-8"),
        "json" => Some("application/json; charset=utf-8"),
        "map" => Some("application/json; charset=utf-8"),
        "xml" => Some("application/xml; charset=utf-8"),
        "txt" => Some("text/plain; charset=utf-8"),
        "wasm" => Some("application/wasm"),
        // fonts
        "woff" => Some("font/woff"),
        "woff2" => Some("font/woff2"),
        "ttf" => Some("font/ttf"),
        "otf" => Some("font/otf"),
        "eot" => Some("application/vnd.ms-fontobject"),
        // documents
        "pdf" => Some("application/pdf"),
        // archives
        "zip" => Some("application/zip"),
        "tar" => Some("application/x-tar"),
        "gz" => Some("application/gzip"),
        // av
        "mp4" => Some("video/mp4"),
        "webm" => Some("video/webm"),
        "mp3" => Some("audio/mpeg"),
        "wav" => Some("audio/wav"),
        "ogg" => Some("audio/ogg"),
        _ => None,
    }
}
