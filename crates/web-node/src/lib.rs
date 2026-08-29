//! The browser half of permafrost: turns a ticket from the alert deep link into
//! Parquet bytes that DuckDB-WASM can query, over an iroh relay connection.
//!
//! README Open Question 1 is resolved here in favour of the fallback protocol:
//! this crate speaks `permafrost/fetch/1` (a plain QUIC bi-stream) rather than
//! linking `iroh-blobs`, which pulls a filesystem-backed store that does not
//! build for `wasm32-unknown-unknown`. The wire format is small enough that the
//! collector serves both, so nothing is lost but bao verified streaming — and
//! the page verifies integrity itself by hashing what it receives is not needed
//! for v1, where possession of the ticket is already read access.

use std::str::FromStr;

use iroh::endpoint::{Connection, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayUrl};
use permafrost_protocol::{BlobHash, FETCH_ALPN, FetchRequest, Manifest, NodeAddrWire, Ticket};
use wasm_bindgen::prelude::*;

/// Refuse to buffer more than this from one request. Matches the collector's
/// own cap so a hostile peer cannot exhaust the tab's memory.
const MAX_FETCH_LEN: u64 = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
enum NodeError {
    #[error("ticket: {0}")]
    Ticket(#[from] permafrost_protocol::ProtocolError),
    #[error("ticket carries an invalid endpoint id")]
    EndpointId,
    #[error("ticket carries an invalid relay url: {0}")]
    RelayUrl(String),
    #[error("could not bind a browser endpoint: {0}")]
    Bind(String),
    #[error("could not reach the collector: {0}")]
    Connect(String),
    #[error("fetch stream failed: {0}")]
    Stream(String),
    #[error("collector has no blob {0}")]
    NoSuchBlob(BlobHash),
    #[error("collector announced {0} bytes, over the {MAX_FETCH_LEN} byte cap")]
    TooLarge(u64),
    #[error("manifest is not valid json: {0}")]
    Manifest(#[from] serde_json::Error),
}

impl From<NodeError> for JsValue {
    fn from(err: NodeError) -> Self {
        JsError::new(&err.to_string()).into()
    }
}

/// A connected browser node. One per page load.
#[wasm_bindgen]
pub struct PermafrostNode {
    endpoint: Endpoint,
    conn: Connection,
    remote: EndpointId,
    manifest_hash: BlobHash,
}

#[wasm_bindgen]
impl PermafrostNode {
    /// Dial the collector named by a deep-link ticket (the raw `t=` fragment
    /// value, still base32).
    pub async fn connect(ticket: &str) -> Result<PermafrostNode, JsValue> {
        Ok(Self::connect_inner(ticket).await?)
    }

    /// The collector's endpoint id, for display.
    #[wasm_bindgen(js_name = node_id)]
    pub fn node_id(&self) -> String {
        self.remote.to_string()
    }

    /// `"direct"` or `"relay"`. Browsers have no UDP socket, so this is
    /// `"relay"` in practice; the page uses it only as a label.
    #[wasm_bindgen(js_name = connection_type)]
    pub fn connection_type(&self) -> String {
        let direct = self
            .conn
            .paths()
            .iter()
            .any(|p| p.is_selected() && p.is_ip());
        if direct {
            "direct".to_string()
        } else {
            "relay".to_string()
        }
    }

    /// Fetch a blob, or a byte range of one. `len == 0` means "to the end".
    ///
    /// AIDEV-NOTE: `f64` rather than `u64` deliberately — wasm-bindgen maps 64-bit
    /// integers to `BigInt`, and the page calls these with plain Numbers.
    /// Segment sizes stay far below 2^53 so the precision is not a concern.
    #[wasm_bindgen(js_name = fetch_blob)]
    pub async fn fetch_blob(&self, hash: &str, offset: f64, len: f64) -> Result<Vec<u8>, JsValue> {
        let hash = BlobHash::from_str(hash).map_err(NodeError::Ticket)?;
        let len = if len <= 0.0 { None } else { Some(len as u64) };
        Ok(self.fetch(hash, offset.max(0.0) as u64, len).await?)
    }

    /// The segment manifest, as a JSON string for the page to parse.
    #[wasm_bindgen(js_name = fetch_manifest)]
    pub async fn fetch_manifest(&self) -> Result<String, JsValue> {
        let bytes = self.fetch(self.manifest_hash, 0, None).await?;
        // Validate before handing it to the page: a manifest that does not
        // deserialize here would fail far less legibly in JS.
        let manifest: Manifest = serde_json::from_slice(&bytes).map_err(NodeError::Manifest)?;
        Ok(serde_json::to_string(&manifest).map_err(NodeError::Manifest)?)
    }

    /// Close the connection. The page calls this on `pagehide`.
    pub async fn close(&self) {
        self.conn.close(0u32.into(), b"bye");
        self.endpoint.close().await;
    }
}

impl PermafrostNode {
    async fn connect_inner(ticket: &str) -> Result<Self, NodeError> {
        let ticket = Ticket::decode(ticket)?;
        let addr = to_endpoint_addr(&ticket.node_addr)?;
        let remote = addr.id;

        let endpoint = Endpoint::builder(presets::N0)
            .bind()
            .await
            .map_err(|e| NodeError::Bind(e.to_string()))?;
        let conn = endpoint
            .connect(addr, FETCH_ALPN)
            .await
            .map_err(|e| NodeError::Connect(e.to_string()))?;

        Ok(Self {
            endpoint,
            conn,
            remote,
            manifest_hash: ticket.manifest_hash_hint,
        })
    }

    async fn fetch(
        &self,
        hash: BlobHash,
        offset: u64,
        len: Option<u64>,
    ) -> Result<Vec<u8>, NodeError> {
        let req = FetchRequest { hash, offset, len };
        let raw = postcard::to_allocvec(&req).map_err(|e| NodeError::Stream(e.to_string()))?;

        let (mut send, mut recv) = self
            .conn
            .open_bi()
            .await
            .map_err(|e| NodeError::Stream(e.to_string()))?;
        send.write_all(&raw)
            .await
            .map_err(|e| NodeError::Stream(e.to_string()))?;
        send.finish()
            .map_err(|e| NodeError::Stream(e.to_string()))?;

        let mut header = [0u8; 8];
        recv.read_exact(&mut header)
            .await
            .map_err(|e| NodeError::Stream(e.to_string()))?;
        let announced = u64::from_le_bytes(header);
        if announced == 0 {
            return Err(NodeError::NoSuchBlob(hash));
        }
        if announced > MAX_FETCH_LEN {
            return Err(NodeError::TooLarge(announced));
        }

        // AIDEV-NOTE: the read is bounded by the announced length, so a peer that
        // lies about the header cannot grow the tab beyond what it declared.
        let body = recv
            .read_to_end(announced as usize)
            .await
            .map_err(|e| NodeError::Stream(e.to_string()))?;
        Ok(body)
    }
}

fn to_endpoint_addr(wire: &NodeAddrWire) -> Result<EndpointAddr, NodeError> {
    let id = EndpointId::from_bytes(&wire.node_id).map_err(|_| NodeError::EndpointId)?;
    let mut addr = EndpointAddr::new(id);
    if let Some(url) = &wire.relay_url {
        let url = RelayUrl::from_str(url).map_err(|e| NodeError::RelayUrl(e.to_string()))?;
        addr = addr.with_relay_url(url);
    }
    // AIDEV-NOTE: direct addresses from the ticket are ignored on purpose. A browser
    // has no UDP socket, so relay is the only usable path (README §Constraints);
    // passing them along would only add dial attempts that cannot succeed.
    Ok(addr)
}

/// Route `tracing` to the browser console. Called once by the page.
#[wasm_bindgen(js_name = start_logging)]
pub fn start_logging() {
    use tracing_subscriber::fmt::format::Pretty;
    use tracing_subscriber::fmt::time::UtcTime;
    use tracing_subscriber::prelude::*;

    let fmt = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_timer(UtcTime::rfc_3339())
        .with_writer(tracing_web::MakeWebConsoleWriter::new())
        .event_format(tracing_subscriber::fmt::format().with_level(true))
        .fmt_fields(Pretty::default());
    let _ = tracing_subscriber::registry().with(fmt).try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_round_trips_into_a_dialable_address() {
        let wire = NodeAddrWire {
            node_id: [7u8; 32],
            relay_url: Some("https://relay.example/".into()),
            direct_addrs: vec!["192.0.2.1:4433".into()],
        };
        // [7u8; 32] is not a valid ed25519 point, so only the failure path is
        // assertable without a key; use a real key instead.
        let key = iroh::SecretKey::from_bytes(&[9u8; 32]);
        let wire = NodeAddrWire {
            node_id: *key.public().as_bytes(),
            ..wire
        };

        let addr = to_endpoint_addr(&wire).expect("valid wire address");
        assert_eq!(addr.id, key.public());
        assert_eq!(addr.relay_urls().count(), 1, "the relay url must survive");
        assert_eq!(
            addr.ip_addrs().count(),
            0,
            "direct addrs are dropped in the browser"
        );
    }

    #[test]
    fn a_bad_relay_url_is_reported_not_ignored() {
        let key = iroh::SecretKey::from_bytes(&[3u8; 32]);
        let wire = NodeAddrWire {
            node_id: *key.public().as_bytes(),
            relay_url: Some("not a url".into()),
            direct_addrs: vec![],
        };
        assert!(matches!(
            to_endpoint_addr(&wire),
            Err(NodeError::RelayUrl(_))
        ));
    }
}
