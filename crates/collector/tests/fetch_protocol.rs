//! `permafrost/fetch/1` over two real iroh endpoints.
//!
//! This is the native half of the Phase 1 spike: the browser cannot be driven
//! from `cargo test`, but the wire format it speaks can be, and a regression
//! here would break the page silently.

use std::str::FromStr;

use clap::Parser as _;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayUrl};
use permafrost_collector::config::{Cli, Config};
use permafrost_collector::p2p::P2p;
use permafrost_collector::segment::SegmentMeta;
use permafrost_protocol::{BlobHash, FETCH_ALPN, FetchRequest, NodeAddrWire};

/// Rebuild a dialable address from the ticket's wire form, exactly as
/// `crates/web-node` does.
fn dial_addr(wire: &NodeAddrWire) -> EndpointAddr {
    let id = EndpointId::from_bytes(&wire.node_id).expect("valid endpoint id");
    let mut addr = EndpointAddr::new(id);
    if let Some(url) = &wire.relay_url {
        addr = addr.with_relay_url(RelayUrl::from_str(url).expect("valid relay url"));
    }
    for direct in &wire.direct_addrs {
        if let Ok(sock) = direct.parse() {
            addr = addr.with_ip_addr(sock);
        }
    }
    addr
}

async fn fetch(addr: &EndpointAddr, req: FetchRequest) -> Vec<u8> {
    let client = Endpoint::builder(presets::N0)
        .bind()
        .await
        .expect("bind client");
    let conn = client
        .connect(addr.clone(), FETCH_ALPN)
        .await
        .expect("connect");
    let (mut send, mut recv) = conn.open_bi().await.expect("open_bi");
    send.write_all(&postcard::to_allocvec(&req).expect("encode"))
        .await
        .expect("write");
    send.finish().expect("finish");

    let mut header = [0u8; 8];
    recv.read_exact(&mut header).await.expect("length header");
    let len = u64::from_le_bytes(header) as usize;
    let body = recv.read_to_end(len).await.expect("body");
    conn.close(0u32.into(), b"done");
    client.close().await;
    body
}

#[tokio::test]
async fn a_peer_fetches_an_announced_segment_whole_and_in_ranges() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = Config::load(Cli::parse_from([
        "permafrost",
        "--data-dir",
        dir.path().to_str().expect("utf8 path"),
    ]))
    .expect("config");
    let p2p = P2p::spawn(&cfg).await.expect("spawn p2p");

    // Stand in for a flushed segment; the fetch path does not parse Parquet.
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let path = dir
        .path()
        .join("seg-00000000000000000100-00000000000000000200.parquet");
    std::fs::write(&path, &payload).expect("write segment");

    let meta = SegmentMeta {
        path: path.clone(),
        start_ns: 100,
        end_ns: 200,
        rows: 1,
        bytes: payload.len() as u64,
        label_index: Default::default(),
    };
    let hash = p2p.announce(&meta).await.expect("announce");

    let ticket = p2p.ticket().expect("a manifest has been published");
    let addr = dial_addr(&ticket.node_addr);

    let whole = fetch(
        &addr,
        FetchRequest {
            hash,
            offset: 0,
            len: None,
        },
    )
    .await;
    assert_eq!(whole, payload, "whole-blob fetch must be byte-identical");

    // A range: this is how the page reads a Parquet footer without pulling the
    // entire segment.
    let tail = fetch(
        &addr,
        FetchRequest {
            hash,
            offset: 4000,
            len: Some(96),
        },
    )
    .await;
    assert_eq!(
        tail,
        payload[4000..],
        "range fetch returned the wrong slice"
    );

    // An unknown hash answers with a zero-length body rather than an error the
    // page would have to special-case.
    let missing = fetch(
        &addr,
        FetchRequest {
            hash: BlobHash([0u8; 32]),
            offset: 0,
            len: None,
        },
    )
    .await;
    assert!(missing.is_empty(), "unknown hash should return nothing");

    // The manifest the ticket points at is fetchable the same way, and lists
    // the segment we just announced.
    let manifest_bytes = fetch(
        &addr,
        FetchRequest {
            hash: ticket.manifest_hash_hint,
            offset: 0,
            len: None,
        },
    )
    .await;
    let manifest: permafrost_protocol::Manifest =
        serde_json::from_slice(&manifest_bytes).expect("manifest is json");
    assert_eq!(manifest.segments.len(), 1);
    assert_eq!(manifest.segments[0].hash, hash);
    assert_eq!(manifest.segments[0].rows, 1);

    p2p.shutdown().await;
}
