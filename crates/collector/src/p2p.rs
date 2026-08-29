//! iroh endpoint, blob announce, manifest publication and the fallback fetch
//! protocol (README §Interface Contracts 3 and 5).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use iroh::endpoint::{Connection, presets};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, SecretKey, TransportAddr};
use iroh_blobs::api::Store;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::{BlobsProtocol, Hash};
use permafrost_protocol::{
    BlobHash, FETCH_ALPN, FetchRequest, Manifest, NodeAddrWire, SegmentEntry, Ticket,
};

use crate::config::Config;
use crate::segment::SegmentMeta;

/// Cap on a single fallback-protocol range read, so one client cannot ask the
/// collector to buffer an unbounded response.
const MAX_FETCH_LEN: u64 = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum P2pError {
    #[error("iroh endpoint: {0}")]
    Endpoint(String),
    #[error("blob store at {path}: {message}")]
    Store { path: PathBuf, message: String },
    #[error("secret key file {path}: {source}")]
    Key {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("announcing {path}: {message}")]
    Announce { path: PathBuf, message: String },
}

pub struct P2p {
    endpoint: Endpoint,
    store: FsStore,
    router: Router,
    manifest: Mutex<Manifest>,
    manifest_hash: RwLock<Option<BlobHash>>,
}

impl P2p {
    pub async fn spawn(cfg: &Config) -> Result<Arc<Self>, P2pError> {
        let secret = load_or_create_key(&cfg.secret_key_file)?;
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret)
            .bind()
            .await
            .map_err(|e| P2pError::Endpoint(e.to_string()))?;

        let blobs_dir = cfg.blobs_dir();
        let store = FsStore::load(&blobs_dir)
            .await
            .map_err(|e| P2pError::Store {
                path: blobs_dir.clone(),
                message: e.to_string(),
            })?;

        let blobs = BlobsProtocol::new(&store, None);
        let fetch = FetchProtocol {
            store: (*store).clone(),
        };
        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs)
            .accept(FETCH_ALPN, fetch)
            .spawn();

        let node = endpoint.id().to_string();
        tracing::info!(%node, "iroh endpoint listening");

        Ok(Arc::new(Self {
            endpoint,
            store,
            router,
            manifest: Mutex::new(Manifest::new(node)),
            manifest_hash: RwLock::new(None),
        }))
    }

    pub fn endpoint_id(&self) -> String {
        self.endpoint.id().to_string()
    }

    /// Wait until the relay connection is usable. Segments are still written
    /// and announced locally if this never completes.
    pub async fn wait_online(&self) {
        self.endpoint.online().await;
    }

    /// Add a flushed segment as a blob and republish the manifest.
    pub async fn announce(&self, meta: &SegmentMeta) -> Result<BlobHash, P2pError> {
        let tag = self
            .store
            .add_path(&meta.path)
            .await
            .map_err(|e| P2pError::Announce {
                path: meta.path.clone(),
                message: e.to_string(),
            })?;
        // AIDEV-NOTE: `add_path` returns a named (persistent) tag, so the blob
        // survives restarts and GC without the collector holding a TempTag.
        let hash = BlobHash(*tag.hash.as_bytes());

        if let Ok(mut manifest) = self.manifest.lock() {
            manifest.segments.push(SegmentEntry {
                hash,
                start_ns: meta.start_ns,
                end_ns: meta.end_ns,
                rows: meta.rows,
                bytes: meta.bytes,
                label_index: meta.label_index.clone(),
            });
        }
        self.publish_manifest().await?;
        tracing::info!(segment = %hash, rows = meta.rows, "segment announced");
        Ok(hash)
    }

    /// Drop segments the retention loop deleted from disk.
    pub async fn forget_segments(&self, paths: &[PathBuf]) -> Result<(), P2pError> {
        if paths.is_empty() {
            return Ok(());
        }
        // AIDEV-NOTE: the manifest is keyed by blob hash, not path, so retention
        // matches on the time range encoded in the filename instead.
        let dropped: Vec<i64> = paths.iter().filter_map(|p| segment_end_ns(p)).collect();
        if let Ok(mut manifest) = self.manifest.lock() {
            manifest.segments.retain(|s| !dropped.contains(&s.end_ns));
        }
        self.publish_manifest().await
    }

    /// Serialize the manifest as a blob; its hash goes into every new ticket.
    async fn publish_manifest(&self) -> Result<(), P2pError> {
        let json = {
            let Ok(manifest) = self.manifest.lock() else {
                return Ok(());
            };
            serde_json::to_vec(&*manifest).unwrap_or_default()
        };
        let tag = self
            .store
            .add_bytes(json)
            .await
            .map_err(|e| P2pError::Announce {
                path: PathBuf::from("<manifest>"),
                message: e.to_string(),
            })?;
        let hash = BlobHash(*tag.hash.as_bytes());
        if let Ok(mut slot) = self.manifest_hash.write() {
            *slot = Some(hash);
        }
        Ok(())
    }

    pub fn manifest(&self) -> Manifest {
        self.manifest
            .lock()
            .map(|m| m.clone())
            .unwrap_or_else(|_| Manifest::new(self.endpoint_id()))
    }

    /// The current deep-link ticket, once at least one manifest is published.
    pub fn ticket(&self) -> Option<Ticket> {
        let manifest_hash_hint = (*self.manifest_hash.read().ok()?)?;
        Some(Ticket {
            node_addr: to_wire(&self.endpoint.addr()),
            manifest_hash_hint,
        })
    }

    pub async fn shutdown(&self) {
        if let Err(err) = self.router.shutdown().await {
            tracing::warn!(%err, "iroh router shutdown");
        }
    }
}

fn to_wire(addr: &EndpointAddr) -> NodeAddrWire {
    NodeAddrWire {
        node_id: *addr.id.as_bytes(),
        relay_url: addr.addrs.iter().find_map(|a| match a {
            TransportAddr::Relay(url) => Some(url.to_string()),
            _ => None,
        }),
        direct_addrs: addr
            .addrs
            .iter()
            .filter_map(|a| match a {
                TransportAddr::Ip(sock) => Some(sock.to_string()),
                _ => None,
            })
            .collect(),
    }
}

fn segment_end_ns(path: &Path) -> Option<i64> {
    let name = path.file_name()?.to_str()?;
    name.strip_prefix("seg-")?
        .strip_suffix(".parquet")?
        .split_once('-')?
        .1
        .parse()
        .ok()
}

/// Load the node identity, generating and persisting one on first run.
fn load_or_create_key(path: &Path) -> Result<SecretKey, P2pError> {
    let err = |source| P2pError::Key {
        path: path.to_path_buf(),
        source,
    };
    if path.exists() {
        let raw = std::fs::read(path).map_err(err)?;
        let bytes: [u8; 32] = raw.as_slice().try_into().map_err(|_| P2pError::Key {
            path: path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected exactly 32 key bytes",
            ),
        })?;
        return Ok(SecretKey::from_bytes(&bytes));
    }

    let key = SecretKey::generate();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(err)?;
    }
    std::fs::write(path, key.to_bytes()).map_err(err)?;
    restrict_permissions(path)?;
    tracing::info!(path = %path.display(), "generated iroh node identity");
    Ok(key)
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> Result<(), P2pError> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|source| {
        P2pError::Key {
            path: path.to_path_buf(),
            source,
        }
    })
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) -> Result<(), P2pError> {
    Ok(())
}

// ---------------------------------------------------------------------------
// Fallback fetch protocol
// ---------------------------------------------------------------------------

/// `permafrost/fetch/1`: postcard request in, length-prefixed bytes out.
///
/// Exists because `iroh-blobs` in the browser is the project's biggest
/// technical risk (README §Recommended Stack); this path needs only a raw
/// bi-stream, which the wasm endpoint definitely has.
#[derive(Debug, Clone)]
struct FetchProtocol {
    store: Store,
}

impl ProtocolHandler for FetchProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        loop {
            let (mut send, mut recv) = match conn.accept_bi().await {
                Ok(pair) => pair,
                // The peer closing the connection is the normal exit path.
                Err(_) => return Ok(()),
            };
            let raw = recv
                .read_to_end(4096)
                .await
                .map_err(AcceptError::from_err)?;
            let bytes = match self.serve(&raw).await {
                Ok(bytes) => bytes,
                Err(err) => {
                    tracing::warn!(%err, "fetch request rejected");
                    // Zero length is the documented "no such blob" answer.
                    Vec::new()
                }
            };
            send.write_all(&(bytes.len() as u64).to_le_bytes())
                .await
                .map_err(AcceptError::from_err)?;
            send.write_all(&bytes)
                .await
                .map_err(AcceptError::from_err)?;
            send.finish().map_err(AcceptError::from_err)?;
        }
    }
}

impl FetchProtocol {
    async fn serve(&self, raw: &[u8]) -> Result<Vec<u8>, String> {
        let req: FetchRequest = postcard::from_bytes(raw).map_err(|e| e.to_string())?;
        // AIDEV-NOTE: whole-blob read then slice. Segments are capped at
        // PERMAFROST_SEGMENT_MAX_BYTES, so the server-side amplification is
        // bounded; the client still receives only the range it asked for.
        let all = self
            .store
            .get_bytes(Hash::from_bytes(*req.hash.as_bytes()))
            .await
            .map_err(|e| e.to_string())?;
        let start = (req.offset as usize).min(all.len());
        let want = req.len.unwrap_or(u64::MAX).min(MAX_FETCH_LEN) as usize;
        let end = start.saturating_add(want).min(all.len());
        Ok(all[start..end].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn key_is_generated_once_and_reloaded_verbatim() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("iroh.key");
        let first = load_or_create_key(&path).expect("generate");
        let second = load_or_create_key(&path).expect("reload");
        assert_eq!(
            first.to_bytes(),
            second.to_bytes(),
            "identity must be stable across restarts"
        );
        assert_eq!(std::fs::read(&path).expect("read").len(), 32);
    }

    #[cfg(unix)]
    #[test]
    fn key_file_is_not_world_readable() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("iroh.key");
        load_or_create_key(&path).expect("generate");
        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o077,
            0,
            "group/other must have no access, got {mode:o}"
        );
    }

    #[test]
    fn corrupt_key_file_is_reported_not_silently_replaced() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("iroh.key");
        std::fs::write(&path, b"too short").expect("write");
        assert!(load_or_create_key(&path).is_err());
    }

    #[test]
    fn wire_addr_splits_relay_from_direct_addresses() {
        let key = SecretKey::generate();
        let mut addr = EndpointAddr::from(key.public());
        addr.addrs
            .insert(TransportAddr::Ip("192.0.2.1:4433".parse().expect("addr")));
        addr.addrs.insert(TransportAddr::Relay(
            "https://relay.example./".parse().expect("relay url"),
        ));

        let wire = to_wire(&addr);
        assert_eq!(wire.node_id, *key.public().as_bytes());
        assert_eq!(wire.direct_addrs, vec!["192.0.2.1:4433".to_string()]);
        assert!(wire.relay_url.is_some());
    }

    #[test]
    fn retention_drops_the_matching_manifest_entries() {
        // Mirrors what forget_segments does to the manifest, without an endpoint.
        let mut manifest = Manifest::new("node".into());
        for end_ns in [100i64, 200, 300] {
            manifest.segments.push(SegmentEntry {
                hash: BlobHash([end_ns as u8; 32]),
                start_ns: 0,
                end_ns,
                rows: 1,
                bytes: 1,
                label_index: BTreeMap::new(),
            });
        }
        let expired = [PathBuf::from(format!("seg-{:020}-{:020}.parquet", 0, 100))];
        let dropped: Vec<i64> = expired.iter().filter_map(|p| segment_end_ns(p)).collect();
        assert_eq!(dropped, vec![100]);
        manifest.segments.retain(|s| !dropped.contains(&s.end_ns));
        assert_eq!(manifest.segments.len(), 2);
    }

    #[test]
    fn segment_end_ns_ignores_foreign_filenames() {
        assert_eq!(
            segment_end_ns(Path::new(
                "seg-00000000000000000001-00000000000000000009.parquet"
            )),
            Some(9)
        );
        assert_eq!(segment_end_ns(Path::new("iroh.key")), None);
        assert_eq!(segment_end_ns(Path::new("seg-nope.parquet")), None);
    }

    #[test]
    fn fetch_request_roundtrips_over_postcard() {
        let req = FetchRequest {
            hash: BlobHash([4; 32]),
            offset: 8,
            len: Some(1024),
        };
        let raw = postcard::to_allocvec(&req).expect("encode");
        assert_eq!(
            postcard::from_bytes::<FetchRequest>(&raw).expect("decode"),
            req
        );
    }
}
