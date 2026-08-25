#!/usr/bin/env bash
# Vendors mori's R-only pieces (the R-coupled half of mori.h, exact-size
# serialize streams, and the ALTREP layer) into src/vendor/, rewiring their
# region-layer references onto the vendored librei core. The region layer
# itself vendors from librei (tools/vendor-librei.sh) — mori is no longer
# its upstream (see the ipc plan, Phase 1 step 4).
#
# Usage: tools/vendor-mori.sh [ref]
#   ref        tag, branch, or full commit SHA to vendor (default: the pin below)
#   MORI_SRC   use a local mori checkout instead of cloning (must be at ref)
#   MORI_REPO  override the upstream clone URL
#
# Vendored files are never edited by hand (see AGENTS.md): changes go
# upstream to mori and are pulled by re-running this script. Idempotent.
# The script greps its own output for region-layer references left under
# mori_ names and fails non-zero on any hit.

set -euo pipefail

PIN="f55cd3ca8a7c4190ddd5501f53b1ab362d0aa87c"  # mori rei branch: Vendor librei at the rei.h/rei_ext.h surface split
REF="${1:-$PIN}"
REPO="${MORI_REPO:-https://github.com/shikokuchuo/mori}"
DEST="$(cd "$(dirname "$0")/.." && pwd)/src/vendor"
FILES="mori.h serialize.c altrep.c"

workdir=""
cleanup() { if [ -n "$workdir" ]; then rm -rf "$workdir"; fi; }
trap cleanup EXIT

if [ -n "${MORI_SRC:-}" ]; then
  src_root="$MORI_SRC"
else
  workdir="$(mktemp -d)"
  if printf '%s' "$REF" | grep -qE '^[0-9a-f]{40}$'; then
    # A commit SHA is not a ref: fetch it directly (GitHub serves reachable
    # SHAs) — clone --branch only takes branch/tag names.
    git init -q "$workdir/mori"
    git -C "$workdir/mori" remote add origin "$REPO"
    git -C "$workdir/mori" fetch -q --depth 1 origin "$REF"
    git -C "$workdir/mori" -c advice.detachedHead=false checkout -q FETCH_HEAD
  else
    git clone -q --depth 1 --branch "$REF" "$REPO" "$workdir/mori"
  fi
  src_root="$workdir/mori"
fi

commit="$(git -C "$src_root" rev-parse HEAD)"
commit_date="$(git -C "$src_root" log -1 --format=%cI HEAD)"
if [ -n "$(git -C "$src_root" status --porcelain -- src)" ]; then
  commit="$commit-dirty"   # MORI_SRC with uncommitted src/ changes
fi

mkdir -p "$DEST"
for f in $FILES; do
  cp "$src_root/src/$f" "$DEST/$f"
done
# The region layer vendors from librei now: drop the old vendored copies.
rm -f "$DEST/mori_region.h" "$DEST/shm.c"

# The enumerated substitution set — the only transformation performed.

# 1. Region-layer references rewire onto the vendored librei core (the
#    R-facing mori_shm_name / mori_shm_open_and_wrap and the wrap/finalizer
#    helpers are mori's own and keep their names). The bare struct type
#    matches only when not followed by an identifier character.
# 2. The region header is librei's: mori.h's include resolves to the
#    vendored internal header (the heap forms altrep.c calls are internal
#    there; internal.h includes rei.h).
sed -i.bak \
  -e 's|mori_shm_create_heap|rei_shm_create_heap|g' \
  -e 's|mori_shm_open_heap|rei_shm_open_heap|g' \
  -e 's|mori_shm_close_stack|rei_shm_close_stack|g' \
  -e 's|mori_shm_host_release|rei_shm_host_release|g' \
  -e 's|mori_shm_reap|rei_shm_reap|g' \
  -e 's|mori_err_describe|rei_err_describe|g' \
  -e 's|mori_shm_s|rei_shm_s|g' \
  -e 's|MORI_MAGIC_|REI_MAGIC_|g' \
  -e 's|MORI_HEADER_SIZE|REI_HEADER_SIZE|g' \
  -e 's|MORI_NAME_MAX|REI_NAME_MAX|g' \
  -e 's|MORI_PREFIX_LITERAL|REI_PREFIX_LITERAL|g' \
  -e 's|"mori_region\.h"|"librei/internal.h"|' \
  "$DEST/altrep.c" "$DEST/mori.h" "$DEST/serialize.c"

# 3. Extptr tag strings: installed symbols are process-global, so the
#    vendored view chain shares tag names with neither a loaded mori nor
#    the package's own region wraps (wrap.c's "rei_shm"). Must run before
#    the bare-type rule below, which would otherwise rewrite the
#    "mori_shm" literal to "rei_shm" first.
sed -i.bak \
  -e 's|"mori_shm"|"rei_mori_shm"|' \
  -e 's|"mori_host"|"rei_mori_host"|' \
  -e 's|"mori_owned"|"rei_mori_owned"|' \
  "$DEST/mori.h"

# 4. The bare struct type (not the R-facing mori_shm_* functions).
sed -i.bak -e 's|[[:<:]]mori_shm[[:>:]]|rei_shm|g' "$DEST/altrep.c" "$DEST/mori.h"

# 5. ALTREP class names + registering package: rei's classes stay
#    distinguishable from an installed mori's (class identity is name +
#    package + DllInfo; the first two are user-visible).
# 6. Error message prefix, package-consistent with the rest of rei.
sed -i.bak \
  -e 's|"mori_list"|"rei_list"|g' \
  -e 's|"mori_real"|"rei_real"|g' \
  -e 's|"mori_integer"|"rei_integer"|g' \
  -e 's|"mori_logical"|"rei_logical"|g' \
  -e 's|"mori_raw"|"rei_raw"|g' \
  -e 's|"mori_complex"|"rei_complex"|g' \
  -e 's|"mori_string"|"rei_string"|g' \
  -e 's|, "mori", dll)|, "rei", dll)|g' \
  -e 's|"mori: |"rei: |g' \
  "$DEST/altrep.c"

rm -f "$DEST"/*.bak

# The gate: no region-layer reference may remain under a mori_ name (a
# missed identifier fails loud at link time; a missed string forks the
# namespace silently).
if grep -nE 'mori_shm_create_heap|mori_shm_open_heap|mori_shm_close_stack|mori_shm_host_release|mori_shm_reap|mori_err_describe|mori_shm_s|MORI_MAGIC_|MORI_HEADER_SIZE|MORI_NAME_MAX|MORI_PREFIX_LITERAL|mori_region\.h|/mori_|Local..mori_' "$DEST/mori.h" "$DEST/altrep.c" "$DEST/serialize.c"; then
  echo "vendor-mori: FAIL — region-layer remnants above" >&2
  exit 1
fi
if grep -nE '[[:<:]]mori_shm[[:>:]]' "$DEST/mori.h" "$DEST/altrep.c" "$DEST/serialize.c"; then
  echo "vendor-mori: FAIL — bare mori_shm type remnants above" >&2
  exit 1
fi

{
  echo "Vendored mori R-only pieces — generated by tools/vendor-mori.sh; do not edit."
  echo
  echo "upstream: $REPO"
  echo "ref: $REF"
  echo "commit: $commit"
  echo "commit-date: $commit_date"
  echo "license: MIT (mori, same author) — see upstream LICENSE"
  echo
  (cd "$DEST" && shasum -a 256 $FILES)
} > "$DEST/VENDOR"

echo "vendored mori @ $REF ($commit) into $DEST"
