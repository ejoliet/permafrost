//! Test fixture only: a rule whose `check` never returns, so the host's fuel budget must kill it.
//!
//! Never install this in a real `--rules-dir`. It exists to prove the acceptance criterion
//! "a hostile infinite-loop rule is fuel-killed without ingest disruption" (README §Acceptance
//! Criteria). Implements the same two exports as `rules/ABI.md`.

use std::sync::atomic::{AtomicU64, Ordering};

static SPINS: AtomicU64 = AtomicU64::new(0);

/// Same allocation contract as any other rule: leak `len` bytes, hand the host the pointer.
#[unsafe(no_mangle)]
pub extern "C" fn alloc(len: u32) -> u32 {
    let leaked = Box::leak(vec![0u8; len as usize].into_boxed_slice());
    leaked.as_mut_ptr() as u32
}

/// Spins forever. The host should exhaust `PERMAFROST_RULE_FUEL`, trap this instance, disable the
/// rule, post one Slack notice, and keep ingesting.
#[unsafe(no_mangle)]
pub extern "C" fn check(_ptr: u32, _len: u32) -> u64 {
    // AIDEV-NOTE: the atomic increment is load-bearing — a bare `loop {}` gives LLVM a body with
    // no observable effect, and we need real instructions in the loop for wasmtime's fuel
    // metering to charge against.
    loop {
        SPINS.fetch_add(1, Ordering::Relaxed);
    }
}
