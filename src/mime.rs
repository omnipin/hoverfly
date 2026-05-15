//! Path → MIME lookup. Thin wrapper around `mime_guess` so callers
//! don't need to know about the underlying iter/`Mime` types.
//!
//! Used by collection uploads to populate per-file `Content-Type`
//! metadata in the manifest. Without it the gateway serves every
//! entry as `text/plain`, which modern browsers refuse for
//! stylesheets, ES modules, fonts, wasm, etc.

/// First `mime_guess` essence for the path's extension, with a
/// `charset=utf-8` parameter added for text types. Returns `None`
/// when no association is known (the manifest then stores no
/// metadata and the gateway falls back to `application/octet-stream`).
pub fn guess_from_path(path: &str) -> Option<String> {
    let m = mime_guess::from_path(path).first()?;
    let s = m.essence_str();
    if matches!(m.type_(), mime_guess::mime::TEXT)
        || s == "application/javascript"
        || s == "application/json"
        || s == "application/xml"
    {
        Some(format!("{s}; charset=utf-8"))
    } else {
        Some(s.to_string())
    }
}
