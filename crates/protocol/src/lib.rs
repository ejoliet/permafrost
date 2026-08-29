//! Shared wire format between the collector, the browser node and any other client.
//!
//! Deliberately free of `iroh` dependencies so it compiles unchanged for
//! `wasm32-unknown-unknown`; the collector converts to/from `iroh` types at the edge.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// ALPN of the fallback blob-fetch protocol (see README §5).
pub const FETCH_ALPN: &[u8] = b"permafrost/fetch/1";

/// Current manifest schema version.
pub const MANIFEST_VERSION: u32 = 1;

/// Maximum log line we accept; longer lines are truncated and flagged.
pub const MAX_LINE_BYTES: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("ticket is not valid base32")]
    TicketEncoding,
    #[error("ticket payload is malformed: {0}")]
    TicketPayload(postcard::Error),
    #[error("hash must be 32 bytes encoded as 64 hex chars or 52 base32 chars")]
    HashEncoding,
}

// ---------------------------------------------------------------------------
// Hash
// ---------------------------------------------------------------------------

/// A blake3 blob hash. Serialized as bytes over the wire, as a lowercase
/// base32 string in JSON (matching `iroh_blobs::Hash`'s `Display`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobHash(pub [u8; 32]);

impl BlobHash {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            &data_encoding::BASE32_NOPAD
                .encode(&self.0)
                .to_ascii_lowercase(),
        )
    }
}

impl fmt::Debug for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobHash({self})")
    }
}

impl std::str::FromStr for BlobHash {
    type Err = ProtocolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // AIDEV-NOTE: accept both iroh's base32 Display form and plain hex, because
        // hashes get copy-pasted out of logs and JSON by humans during triage.
        let raw = if s.len() == 64 {
            data_encoding::HEXLOWER_PERMISSIVE.decode(s.as_bytes())
        } else {
            data_encoding::BASE32_NOPAD.decode(s.to_ascii_uppercase().as_bytes())
        }
        .map_err(|_| ProtocolError::HashEncoding)?;
        let bytes: [u8; 32] = raw.try_into().map_err(|_| ProtocolError::HashEncoding)?;
        Ok(BlobHash(bytes))
    }
}

impl Serialize for BlobHash {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.serialize_str(&self.to_string())
        } else {
            s.serialize_bytes(&self.0)
        }
    }
}

impl<'de> Deserialize<'de> for BlobHash {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        if d.is_human_readable() {
            let s = String::deserialize(d)?;
            s.parse().map_err(D::Error::custom)
        } else {
            let b = serde_bytes_32(d)?;
            Ok(BlobHash(b))
        }
    }
}

fn serde_bytes_32<'de, D: serde::Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
    use serde::de::Error as _;
    let v = <&[u8]>::deserialize(d)?;
    v.try_into()
        .map_err(|_| D::Error::custom("expected 32 bytes"))
}

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

/// Distinct values of promoted labels present in a segment.
pub type LabelIndex = BTreeMap<String, Vec<String>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentEntry {
    pub hash: BlobHash,
    pub start_ns: i64,
    pub end_ns: i64,
    pub rows: u64,
    pub bytes: u64,
    #[serde(default)]
    pub label_index: LabelIndex,
}

impl SegmentEntry {
    /// Does this segment overlap `[from_ns, to_ns]` and satisfy every label filter?
    ///
    /// A filter on a label the segment does not index is treated as a match: the
    /// index only covers promoted labels, so it can rule segments *out*, never in.
    pub fn matches(&self, from_ns: i64, to_ns: i64, labels: &[(String, String)]) -> bool {
        if self.end_ns < from_ns || self.start_ns > to_ns {
            return false;
        }
        labels.iter().all(|(k, v)| match self.label_index.get(k) {
            Some(values) => values.iter().any(|got| got == v),
            None => true,
        })
    }
}

/// The manifest *is* the index — there is no database (README §3).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub version: u32,
    pub node: String,
    pub segments: Vec<SegmentEntry>,
}

impl Manifest {
    pub fn new(node: String) -> Self {
        Self {
            version: MANIFEST_VERSION,
            node,
            segments: Vec::new(),
        }
    }

    /// Segments a query must fetch, oldest first.
    pub fn select(
        &self,
        from_ns: i64,
        to_ns: i64,
        labels: &[(String, String)],
    ) -> Vec<&SegmentEntry> {
        let mut hits: Vec<&SegmentEntry> = self
            .segments
            .iter()
            .filter(|s| s.matches(from_ns, to_ns, labels))
            .collect();
        hits.sort_by_key(|s| s.start_ns);
        hits
    }
}

// ---------------------------------------------------------------------------
// Ticket
// ---------------------------------------------------------------------------

/// Enough of an `iroh::NodeAddr` to dial, without depending on iroh.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct NodeAddrWire {
    pub node_id: [u8; 32],
    pub relay_url: Option<String>,
    #[serde(default)]
    pub direct_addrs: Vec<String>,
}

/// `base32(postcard(Ticket))` — the payload of the `#t=` link fragment.
///
/// Possession of a ticket is read access in v1 (README §Non-Goals).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ticket {
    pub node_addr: NodeAddrWire,
    pub manifest_hash_hint: BlobHash,
}

impl Ticket {
    pub fn encode(&self) -> String {
        let bytes = postcard::to_allocvec(self).expect("ticket is always serializable");
        data_encoding::BASE32_NOPAD
            .encode(&bytes)
            .to_ascii_lowercase()
    }

    pub fn decode(s: &str) -> Result<Self, ProtocolError> {
        let bytes = data_encoding::BASE32_NOPAD
            .decode(s.trim().to_ascii_uppercase().as_bytes())
            .map_err(|_| ProtocolError::TicketEncoding)?;
        postcard::from_bytes(&bytes).map_err(ProtocolError::TicketPayload)
    }
}

impl fmt::Display for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.encode())
    }
}

/// Build the browser deep link (README §4).
///
/// `ui_base` may or may not end in `/`; the ticket always lands in the fragment
/// so it is never sent to the page's HTTP server.
pub fn deep_link(
    ui_base: &str,
    ticket: &Ticket,
    from_ns: i64,
    to_ns: i64,
    labels: &[(String, String)],
) -> String {
    let base = ui_base.trim_end_matches('/');
    let mut url = format!("{base}/#t={}&from={from_ns}&to={to_ns}", ticket.encode());
    if !labels.is_empty() {
        let joined = labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        url.push_str("&labels=");
        url.push_str(&urlencode(&joined));
    }
    url
}

/// Percent-encode everything outside the unreserved set. Small enough that a
/// dependency would cost more than it saves.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Parse the `labels=` query value (`k=v,k=v`) back into pairs.
pub fn parse_labels(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .filter(|p| !p.is_empty())
        .filter_map(|p| p.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

// ---------------------------------------------------------------------------
// Blob fetch
// ---------------------------------------------------------------------------

/// Range read request of the fallback protocol; `len == None` means "to the end".
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchRequest {
    pub hash: BlobHash,
    pub offset: u64,
    pub len: Option<u64>,
}

/// The only transport surface the UI is allowed to know about, so the Phase 1
/// spike outcome (iroh-blobs vs `permafrost/fetch/1`) swaps in without touching
/// business logic.
pub trait BlobFetch {
    type Error;

    fn fetch_blob(
        &self,
        hash: BlobHash,
        range: std::ops::Range<u64>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, Self::Error>>;

    fn fetch_all(
        &self,
        hash: BlobHash,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, Self::Error>> {
        self.fetch_blob(hash, 0..u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(seed: u8) -> BlobHash {
        BlobHash([seed; 32])
    }

    #[test]
    fn hash_roundtrips_through_base32_and_hex() {
        let h = hash(7);
        assert_eq!(h.to_string().parse::<BlobHash>().unwrap(), h);
        let hex = data_encoding::HEXLOWER.encode(&h.0);
        assert_eq!(hex.parse::<BlobHash>().unwrap(), h);
        assert!("not-a-hash".parse::<BlobHash>().is_err());
    }

    #[test]
    fn ticket_roundtrips() {
        let t = Ticket {
            node_addr: NodeAddrWire {
                node_id: [3; 32],
                relay_url: Some("https://relay.example/".into()),
                direct_addrs: vec!["192.0.2.1:4433".into()],
            },
            manifest_hash_hint: hash(9),
        };
        assert_eq!(Ticket::decode(&t.encode()).unwrap(), t);
        assert!(Ticket::decode("!!!").is_err());
    }

    #[test]
    fn manifest_json_matches_documented_shape() {
        let m = Manifest {
            version: 1,
            node: "node".into(),
            segments: vec![SegmentEntry {
                hash: hash(1),
                start_ns: 0,
                end_ns: 10,
                rows: 2,
                bytes: 3,
                label_index: BTreeMap::from([("job".into(), vec!["api".into()])]),
            }],
        };
        let json = serde_json::to_string(&m).unwrap();
        assert!(
            json.contains("\"label_index\":{\"job\":[\"api\"]}"),
            "{json}"
        );
        assert_eq!(serde_json::from_str::<Manifest>(&json).unwrap(), m);
    }

    #[test]
    fn select_filters_by_time_and_promoted_labels() {
        let seg = |h, start, end, job: &str| SegmentEntry {
            hash: hash(h),
            start_ns: start,
            end_ns: end,
            rows: 1,
            bytes: 1,
            label_index: BTreeMap::from([("job".into(), vec![job.into()])]),
        };
        let m = Manifest {
            version: 1,
            node: "n".into(),
            segments: vec![
                seg(1, 100, 200, "api"),
                seg(2, 300, 400, "api"),
                seg(3, 150, 250, "worker"),
            ],
        };

        let all = m.select(0, i64::MAX, &[]);
        assert_eq!(all.len(), 3);
        assert!(
            all[0].start_ns <= all[1].start_ns,
            "select must be sorted by start_ns"
        );

        // Time window excludes the 300..400 segment.
        assert_eq!(m.select(0, 260, &[]).len(), 2);
        // Label filter excludes the worker segment.
        let api = m.select(0, i64::MAX, &[("job".into(), "api".into())]);
        assert_eq!(api.len(), 2);
        // Unknown label key cannot rule a segment out.
        assert_eq!(
            m.select(0, i64::MAX, &[("pod".into(), "x".into())]).len(),
            3
        );
        // Boundaries are inclusive.
        assert_eq!(m.select(200, 200, &[]).len(), 2);
    }

    #[test]
    fn deep_link_encodes_labels_and_keeps_ticket_in_fragment() {
        let t = Ticket {
            node_addr: NodeAddrWire::default(),
            manifest_hash_hint: hash(2),
        };
        let url = deep_link(
            "https://ui.example/",
            &t,
            1,
            2,
            &[("job".into(), "api b".into())],
        );
        assert!(url.starts_with("https://ui.example/#t="), "{url}");
        assert!(url.contains("&from=1&to=2"));
        assert!(url.contains("labels=job%3Dapi%20b"), "{url}");
        assert_eq!(
            parse_labels("job=api,level=error"),
            vec![
                ("job".into(), "api".into()),
                ("level".into(), "error".into())
            ]
        );
    }
}
