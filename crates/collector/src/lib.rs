//! permafrost collector: Loki-compatible ingest, Parquet segments, iroh serving.
//!
//! The binary in `main.rs` is thin wiring; everything testable lives here.

pub mod alert;
pub mod config;
pub mod ingest;
pub mod p2p;
pub mod rules;
pub mod segment;
