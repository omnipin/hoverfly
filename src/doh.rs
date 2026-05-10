//! Configurable DNS-over-HTTPS resolver.
//!
//! Uses the JSON API form (RFC 8484 dns-json) so the same client works on both
//! native (reqwest+rustls) and wasm32 (reqwest+fetch).
//!
//! Default endpoint: `https://cloudflare-dns.com/dns-query`. Override with [`Doh::with_url`].

use serde::Deserialize;
use thiserror::Error;

use crate::DEFAULT_DOH_URL;

#[derive(Debug, Error)]
pub enum DohError {
    #[error("http: {0}")]
    Http(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("nxdomain: {0}")]
    NxDomain(String),
}

/// DNS record types we care about for swarm.
#[derive(Copy, Clone, Debug)]
pub enum RecordType {
    A,
    Aaaa,
    Txt,
}

impl RecordType {
    const fn as_num(self) -> u16 {
        match self {
            Self::A => 1,
            Self::Aaaa => 28,
            Self::Txt => 16,
        }
    }
}

/// DoH JSON API response shape (subset).
#[derive(Debug, Deserialize)]
struct DohResponse {
    #[serde(rename = "Status")]
    status: u32,
    #[serde(rename = "Answer", default)]
    answer: Vec<DohAnswer>,
}

#[derive(Debug, Deserialize)]
struct DohAnswer {
    #[serde(rename = "type")]
    rtype: u16,
    data: String,
}

/// DoH client.
#[derive(Clone, Debug)]
pub struct Doh {
    url: String,
    client: reqwest::Client,
}

impl Default for Doh {
    fn default() -> Self {
        Self::with_url(DEFAULT_DOH_URL)
    }
}

impl Doh {
    pub fn with_url(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client: reqwest::Client::new(),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Query records of `rtype` for `name`. Returns the raw `data` strings from each answer
    /// whose type matches; TXT data already has the JSON-quoted form (we strip quotes).
    pub async fn query(&self, name: &str, rtype: RecordType) -> Result<Vec<String>, DohError> {
        let resp = self
            .client
            .get(&self.url)
            .query(&[("name", name), ("type", &rtype.as_num().to_string())])
            .header("accept", "application/dns-json")
            .send()
            .await
            .map_err(|e| DohError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(DohError::Http(format!("status {}", resp.status())));
        }

        let body: DohResponse = resp.json().await.map_err(|e| DohError::Decode(e.to_string()))?;
        if body.status != 0 {
            return Err(DohError::NxDomain(format!("status code {}", body.status)));
        }

        let want = rtype.as_num();
        let mut out = Vec::new();
        for a in body.answer {
            if a.rtype != want {
                continue;
            }
            let mut s = a.data;
            // TXT records come as JSON-quoted strings; multiple strings are concatenated by the resolver.
            if matches!(rtype, RecordType::Txt) {
                if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                    s = s[1..s.len() - 1].replace("\" \"", "");
                }
            }
            out.push(s);
        }
        Ok(out)
    }

    pub async fn txt(&self, name: &str) -> Result<Vec<String>, DohError> {
        self.query(name, RecordType::Txt).await
    }

    pub async fn a(&self, name: &str) -> Result<Vec<String>, DohError> {
        self.query(name, RecordType::A).await
    }

    pub async fn aaaa(&self, name: &str) -> Result<Vec<String>, DohError> {
        self.query(name, RecordType::Aaaa).await
    }
}
