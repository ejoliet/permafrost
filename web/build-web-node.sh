#!/usr/bin/env bash
# Build crates/web-node into web/vendor/web_node/ for the static page.
#
# The one non-obvious part: iroh pulls in `ring`, whose build script compiles C
# for wasm32. Apple's bundled clang cannot target wasm, so point CC at a real
# LLVM (Homebrew's, by default) or the build fails with
# "No available targets are compatible with triple wasm32-unknown-unknown".
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(dirname "$here")"

llvm="${LLVM_PREFIX:-$(brew --prefix llvm 2>/dev/null || echo /usr)}"
export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-$llvm/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-$llvm/bin/llvm-ar}"

if ! "$CC_wasm32_unknown_unknown" --print-targets 2>/dev/null | grep -q wasm32; then
    echo "error: $CC_wasm32_unknown_unknown cannot target wasm32." >&2
    echo "       Install a wasm-capable LLVM (macOS: brew install llvm) or set" >&2
    echo "       LLVM_PREFIX / CC_wasm32_unknown_unknown to one." >&2
    exit 1
fi

exec wasm-pack build "$repo/crates/web-node" \
    --target web \
    --release \
    --out-dir "$here/vendor/web_node" \
    --out-name web_node \
    "$@"
