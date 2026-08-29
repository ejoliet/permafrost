#!/usr/bin/env bash
#
# Populate web/vendor/duckdb/ from the @duckdb/duckdb-wasm npm tarball.
#
# There is no bundler here on purpose (README §Constraints: "no framework, no
# bundler beyond wasm-pack output"). This script is the whole build step for the
# DuckDB half of web/vendor/; the iroh half comes from wasm-pack, see below.
#
# Usage:
#   ./web/fetch-vendor.sh              # no-op if already populated
#   ./web/fetch-vendor.sh --force      # re-download and overwrite
#   DUCKDB_WASM_VERSION=1.33.1-dev57.0 ./web/fetch-vendor.sh
#
# Safe to run from any cwd; idempotent.

set -euo pipefail

# --- config ------------------------------------------------------------------

# AIDEV-NOTE: @duckdb/duckdb-wasm 1.33.x is published ONLY as `-devN.0` prereleases —
# there is no plain `1.33.0`/`1.33.1` on npm. `latest` currently points at
# 1.33.1-dev57.0, which is what we pin. Bump deliberately, not automatically.
DUCKDB_WASM_VERSION="${DUCKDB_WASM_VERSION:-1.33.1-dev57.0}"

# Files app.js actually loads. The mvp worker ships without a sourcemap, hence
# the separate optional list.
REQUIRED_FILES=(
  duckdb-browser.mjs
  duckdb-mvp.wasm
  duckdb-browser-mvp.worker.js
  duckdb-eh.wasm
  duckdb-browser-eh.worker.js
)
OPTIONAL_FILES=(
  duckdb-browser.mjs.map
  duckdb-browser-eh.worker.js.map
)

# --- paths -------------------------------------------------------------------

# Resolve this script's directory even when invoked via a relative path or a
# symlink-free `bash web/fetch-vendor.sh` from the repo root.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
VENDOR_DIR="${SCRIPT_DIR}/vendor"
DEST_DIR="${VENDOR_DIR}/duckdb"

FORCE=0
for arg in "$@"; do
  case "$arg" in
    --force|-f) FORCE=1 ;;
    --help|-h)
      # Print the header comment block: everything after the shebang up to the
      # first non-comment line.
      awk 'NR==1 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "${BASH_SOURCE[0]}"
      exit 0
      ;;
    *)
      echo "fetch-vendor: unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

log() { printf '  %s\n' "$*"; }

# --- idempotency check -------------------------------------------------------

all_present() {
  local f
  for f in "${REQUIRED_FILES[@]}"; do
    [ -s "${DEST_DIR}/${f}" ] || return 1
  done
  # Version stamp guards against a stale vendor dir from an older pin.
  [ -f "${DEST_DIR}/.version" ] || return 1
  [ "$(cat "${DEST_DIR}/.version")" = "${DUCKDB_WASM_VERSION}" ] || return 1
  return 0
}

if [ "$FORCE" -eq 0 ] && all_present; then
  echo "duckdb-wasm ${DUCKDB_WASM_VERSION} already vendored in web/vendor/duckdb/ — nothing to do."
  echo "(re-run with --force to refresh)"
  exit 0
fi

command -v npm  >/dev/null 2>&1 || { echo "fetch-vendor: npm is required but not on PATH" >&2; exit 1; }
command -v tar  >/dev/null 2>&1 || { echo "fetch-vendor: tar is required but not on PATH" >&2; exit 1; }

# --- download ----------------------------------------------------------------

WORK_DIR="$(mktemp -d)"
cleanup() { rm -rf "${WORK_DIR}"; }
trap cleanup EXIT INT TERM

echo "Fetching @duckdb/duckdb-wasm@${DUCKDB_WASM_VERSION} …"
( cd "${WORK_DIR}" && npm pack "@duckdb/duckdb-wasm@${DUCKDB_WASM_VERSION}" --silent >/dev/null )

TARBALL="$(find "${WORK_DIR}" -maxdepth 1 -name '*.tgz' -print -quit)"
[ -n "${TARBALL}" ] || { echo "fetch-vendor: npm pack produced no tarball" >&2; exit 1; }

# Extract only the dist files we care about (the full tarball is ~30 MB packed
# and includes node targets, .d.ts trees and images we never serve).
EXTRACT_ARGS=()
for f in "${REQUIRED_FILES[@]}" "${OPTIONAL_FILES[@]}"; do
  EXTRACT_ARGS+=( "package/dist/${f}" )
done

# Optional members may be absent in some releases; ignore those failures but
# still fail loudly below if a REQUIRED file did not land.
tar -xzf "${TARBALL}" -C "${WORK_DIR}" "${EXTRACT_ARGS[@]}" 2>/dev/null || true

# --- install -----------------------------------------------------------------

mkdir -p "${DEST_DIR}"

for f in "${REQUIRED_FILES[@]}"; do
  src="${WORK_DIR}/package/dist/${f}"
  if [ ! -s "${src}" ]; then
    echo "fetch-vendor: required file missing from tarball: dist/${f}" >&2
    echo "  (did @duckdb/duckdb-wasm ${DUCKDB_WASM_VERSION} rename its dist layout?)" >&2
    exit 1
  fi
  cp -f "${src}" "${DEST_DIR}/${f}"
  log "vendor/duckdb/${f}"
done

for f in "${OPTIONAL_FILES[@]}"; do
  src="${WORK_DIR}/package/dist/${f}"
  if [ -s "${src}" ]; then
    cp -f "${src}" "${DEST_DIR}/${f}"
    log "vendor/duckdb/${f} (sourcemap)"
  fi
done

printf '%s\n' "${DUCKDB_WASM_VERSION}" > "${DEST_DIR}/.version"

echo
echo "duckdb-wasm ${DUCKDB_WASM_VERSION} vendored into web/vendor/duckdb/"

if [ ! -f "${VENDOR_DIR}/web_node/web_node.js" ]; then
  echo
  echo "Still missing: the iroh browser node. Build it with wasm-pack:"
  echo
  echo "  wasm-pack build --target web --out-dir ../../web/vendor/web_node crates/web-node"
  echo
fi
