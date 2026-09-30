#ifndef MIZU_VIEW_H
#define MIZU_VIEW_H

#include <Rversion.h>
#include <Rinternals.h>
#include <R_ext/Altrep.h>
#include <string.h>

#include "vendor/libmizu/internal.h"

// Identifier grammar constants ------------------------------------------------

#define MIZU_VIEW_MAX_PATH        64                /* max indices in a path */
#define MIZU_VIEW_IDENTIFIER_MAX  1024              /* parser input length cap */
#define MIZU_VIEW_FORMAT_BUFLEN   1024              /* formatter stack buffer */

/* External-pointer tag strings (installed once at init). */
#define MIZU_VIEW_TAG_SHM   "mizu_view_shm"
#define MIZU_VIEW_TAG_HOST  "mizu_view_host"
#define MIZU_VIEW_TAG_OWNED "mizu_view_owned"

/* Region header flags word at byte offset 32 of the 64-byte header —
   bytes [24-31] remain embedder cross-process state, [36-63] reserved.
   Bit 0 records the S4 object bit, which the layouts otherwise cannot
   carry. */
#define MIZU_VIEW_FLAGS_OFF 32
#define MIZU_VIEW_FLAG_S4 0x1u

/* S4 flag riding an MIZL directory entry's sexptype: SEXPTYPEs are small
   positive values, so bit 30 is free. Set at write, masked off at read. */
#define MIZU_VIEW_ELEM_S4 0x40000000

/* int64 wire tag: outside SEXPTYPE space. The core's MIZU_TYPE_INT64 — the
   vendored unit cannot name the enum constant (mori carries no mizu_type_e);
   the embedder _Static_asserts the pin. */
#define MIZU_VIEW_TYPE_INT64 32

// Types -----------------------------------------------------------------------

typedef struct mizu_view_buf_s {
  unsigned char *buf;
  size_t len;
  size_t cur;
} mizu_view_buf;

/* Embedder release callback, fired exactly once per view — at COW
   materialization or at the view finalizer, whichever comes first. Embedded
   as the first member of every owned-metadata struct below, so the shared
   finalizer recovers it from any extptr addr (a struct pointer, suitably
   converted, points to its initial member). ALTLIST views fire at the
   finalizer only: extracted element views keep referencing the region, so a
   list is never fully detached before the whole view tree is finalized. */
typedef void (*mizu_view_release_fn)(void *);

typedef struct mizu_view_owned_s {
  mizu_view_release_fn release;
  void *release_arg;
} mizu_view_owned;

typedef struct mizu_view_vec_s {
  mizu_view_owned owned;
  const void *data;
  R_xlen_t length;
  int32_t index;   /* -1 = standalone, >= 0 = element of ALTLIST */
} mizu_view_vec;

typedef struct mizu_view_list_s {
  mizu_view_owned owned;
  unsigned char *base;       /* points to child MIZL start */
  int64_t region_size;       /* bounds all reads within this region */
  int32_t n_elements;
  int32_t index;             /* -1 = root, >= 0 = sub-list */
} mizu_view_list;

// serialize.c -----------------------------------------------------------------

size_t mizu_view_serialize_count(SEXP object);
size_t mizu_view_serialize_into(unsigned char *dst, SEXP object);
SEXP mizu_view_unserialize_from(unsigned char *src, size_t size);

static inline size_t mizu_view_sizeof_elt(int type) {
  switch (type) {
  case REALSXP:  return sizeof(double);
  case INTSXP:   return sizeof(int);
  case LGLSXP:   return sizeof(int);
  case RAWSXP:   return 1;
  case CPLXSXP:  return sizeof(Rcomplex);
  case MIZU_VIEW_TYPE_INT64: return sizeof(int64_t);  /* int64 bit patterns */
  default:       return 0;
  }
}

/* R's S4 data-part wrapper is an ALTREP that forwards every data method
   to the wrapped vector (data1): directly readable, no compression, no
   keeper chain. Recognize it by that shape — a non-ALTREP data1 of the
   same type whose data pointer the wrapper shares. Anything else foreign
   (a lazy ALTREP, a materialized compact sequence, an extptr-data1 view)
   is rejected: the layout write would materialize it or forfeit its
   compact wire form. R >= 4.6.1 patched / 4.7.0 (svn r90309): the
   wrapper's data-pointer request consolidates a shared data part in
   place (duplicate + swap into data1), so take the wrapper's pointer
   first, then re-read data1. */
static inline int mizu_view_altrep_readable(SEXP x) {
  SEXP inner = R_altrep_data1(x);
  if (ALTREP(inner) || TYPEOF(inner) != TYPEOF(x)) return 0;
  const void *px = DATAPTR_OR_NULL(x);
  if (px == NULL) return 0;
  inner = R_altrep_data1(x);   /* re-read: the pointer request may swap it */
  if (ALTREP(inner) || TYPEOF(inner) != TYPEOF(x)) return 0;
  const void *pi = DATAPTR_OR_NULL(inner);
  return pi != NULL && pi == px;
}

/* Apply a region header's S4 flag to a freshly wrapped view — after
   attributes land, so a read never consults a class definition
   (Rf_asS4 with complete = 0 sets the bit in place on a fresh object).
   Call on a validated region (>= MIZU_HEADER_SIZE bytes); embedders
   wrapping MIZH / MIZS roots through the raw constructors call this
   last. */
static inline SEXP mizu_view_apply_s4(SEXP x, const unsigned char *base) {
  uint32_t flags;
  memcpy(&flags, base + MIZU_VIEW_FLAGS_OFF, 4);
  return (flags & MIZU_VIEW_FLAG_S4) ? Rf_asS4(x, TRUE, 0) : x;
}

/* The [32-35] flags word is a format word: the S4 bit is the only assigned
   bit, and a set bit the reader does not know rejects the region as
   corrupt or newer. Mirrors the vendored core's layout checks. */
static inline int mizu_view_flags_known(const unsigned char *base) {
  uint32_t flags;
  memcpy(&flags, base + MIZU_VIEW_FLAGS_OFF, 4);
  return (flags & ~MIZU_VIEW_FLAG_S4) == 0;
}

// altrep.c --------------------------------------------------------------------

void mizu_view_altrep_init(DllInfo *dll);

/* bit64-compatible int64: the class singleton (constructed and preserved in
   mizu_view_altrep_init; its interned CHARSXP doubles as the probe's
   comparator) and the class-only gate — a REALSXP whose entire attribute
   set is class = "integer64". */
extern SEXP mizu_view_int64_class;
int mizu_view_is_int64(SEXP x);
int mizu_view_is_int64_any(SEXP x);

/* SHM extptr finalizers, defined alongside the wrap constructors that
   register them: mizu_view_shm_finalizer releases this side's mapping only;
   mizu_view_host_finalizer releases the SHM name/handle via
   mizu_shm_host_release. */
void mizu_view_shm_finalizer(SEXP ptr);
void mizu_view_host_finalizer(SEXP ptr);

/* Wrap constructors: the returned view's data1 pins `keeper` through its
   protected slot; release/release_arg ride the owned metadata and fire
   once (materialize or finalizer). Internal callers pass NULL, NULL. */
SEXP mizu_view_vec_wrap(const void *data, R_xlen_t length, int sexptype,
                   SEXP keeper, mizu_view_release_fn release, void *release_arg);
SEXP mizu_view_str_wrap(const unsigned char *region_base, R_xlen_t n,
                   int64_t data_size, SEXP keeper,
                   mizu_view_release_fn release, void *release_arg);
SEXP mizu_view_list_wrap(unsigned char *base, int64_t region_size, int32_t index,
                    SEXP keeper, mizu_view_release_fn release, void *release_arg);
void mizu_view_restore_attrs(SEXP result, unsigned char *buf, size_t size);

/* Layout oracle and writer for embedder-managed regions: the size pass
   walks the tree and returns 0 for anything the layout writer must not
   take: an ALTREP node is rejected unless it is a view (rides the
   wire hooks) or mizu_view_altrep_readable (R's S4 data-part wrappers
   qualify; a compact 1:1e8 would materialize through DATAPTR_RO at
   write). The write emits at most mizu_view_layout_size bytes (exactly
   when foreign == 0) and zeroes header reserved bytes.
   foreign: the cross-language staging mode — the oracle admits ALTREP
   atomic nodes (the write copies them through *_GET_REGION, never
   expanding the sender's vector) and the write builds the
   validity-bitmap section (mizu.h's [40-47]/[48-55] header words) fused
   after each atomic node's copy, returning the actual bytes used. */
size_t mizu_view_layout_size(SEXP x, int foreign);
size_t mizu_view_layout_write(unsigned char *base, SEXP x, int foreign);

/* View introspection: C-level is_shared, the identifier formatter, the
   identifier parser, and a path walk over an already-open region (keeper
   flows to the returned view's chain). */
int mizu_view_check(SEXP x);
SEXP mizu_view_shm_name(SEXP x);
int mizu_view_parse_id(const char *s, char *name_out, size_t name_out_size,
                  int32_t *path_out, int *path_len);
SEXP mizu_view_walk_path(unsigned char *base, int64_t region_size,
                    const int32_t *path, int path_len, SEXP keeper);

/* Embedder wire hooks (optional; set once at embedder load): `emit` fires
   from the Serialized_state methods when an identifier — not a
   materialization — is emitted for a view (the holder set then widens
   beyond the direct peer); `resolve` fires from the identifier resolve
   paths after the wrap, with the freshly opened consumer mapping. An
   embedder running a cross-process lifetime protocol uses the pair to
   flag regions on escape and to count remote references on arrival. */
typedef void (*mizu_view_emit_hook_fn)(SEXP view);
typedef void (*mizu_view_resolve_hook_fn)(SEXP view, mizu_shm *shm);
void mizu_view_set_wire_hooks(mizu_view_emit_hook_fn emit, mizu_view_resolve_hook_fn resolve);

/* Embedder open hook (optional; set once at embedder load): the identifier
   resolve paths dedupe consumer mappings through a process-global
   name-keyed cache whose miss branch opens through this hook instead of the
   default fully-RO mizu_shm_open_heap. An embedder whose cross-process
   protocol writes the region header (a refcount word on page 0) installs a
   page-0-RW open here; the hook returns a heap mizu_shm * the layer wraps
   and owns, same as the default open. The cache size is a view-layer
   constant on purpose: a bare MIZU_OPEN_CACHE_MAX would survive vendoring
   verbatim into mori, whose region layer defines only MORI_OPEN_CACHE_MAX —
   the MIZU_VIEW_ prefix renames to MORI_CACHE_MAX and cannot collide. */
#define MIZU_VIEW_CACHE_MAX 16
typedef mizu_shm *(*mizu_view_open_hook_fn)(const char *name);
void mizu_view_set_open_hook(mizu_view_open_hook_fn hook);

/* Embedder attribute-blob hooks (optional; set once at embedder load, as
   a triple). When set, the layout writer offers every non-empty
   attribute set to `size` first: a nonzero return is the blob's encoded
   size and `write` emits exactly those bytes for the same object (a
   decline there is a bug, never a fallback); a zero `size` return
   declines and the blob is an R_Serialize stream, the pre-hook form.
   Every blob read passes through mizu_view_restore_attrs, which hands
   the embedder's form to `read` (dispatched on the blob's first byte)
   and keeps R_Unserialize for anything else. An embedder that leaves
   the triple unset keeps R_Serialize both ways and never meets the
   embedder form on read. */
typedef size_t (*mizu_view_attrs_size_fn)(SEXP x);
typedef size_t (*mizu_view_attrs_write_fn)(unsigned char *dst, SEXP x);
typedef void (*mizu_view_attrs_read_fn)(SEXP result, const unsigned char *buf,
                                        size_t size);
void mizu_view_set_attrs_hooks(mizu_view_attrs_size_fn size,
                               mizu_view_attrs_write_fn write,
                               mizu_view_attrs_read_fn read);

// Alignment macro and the MIZS string block geometry -------------------------

#define MIZU_VIEW_ALIGN64(x) (((x) + 63) & ~(size_t)63)

/* The string block: the body of an MIZS region (at byte 64) and the form of
   an MIZL STRSXP leaf (at its directory entry's data_offset). For n strings,
   four sections, each 64-byte aligned from the block start:

     validity   ceil(n / 8) bytes — a bitmap, LSB first (bit i of byte i / 8):
                set = present, clear = NA_STRING
     offsets    (n + 1) int64 — offsets[0] = 0, non-decreasing; string i is
                data[offsets[i], offsets[i + 1]) and an NA spans zero bytes
     encoding   n uint8 — the cetype_t of each string (CE_NATIVE, CE_UTF8,
                CE_LATIN1 or CE_BYTES); 0 for an NA
     data       offsets[n] bytes — the packed string bytes, no terminators

   validity, offsets and data are Arrow large_utf8's three buffers verbatim,
   so a consumer that has checked every encoding byte is UTF-8-compatible can
   hand the block to an Arrow reader without a copy; the encoding section is
   the R addition (a per-CHARSXP mark Arrow has no home for). Every writer,
   reader and size oracle takes the section offsets from this one function. */
typedef struct mizu_view_str_geom_s {
  size_t validity;
  size_t offsets;
  size_t encoding;
  size_t data;      /* also the size of everything before the string bytes */
} mizu_view_str_geom;

/* The core's mizu_mizs_geometry owns the section arithmetic (vendored
   mizu_ext.h); this wrapper keeps the embedder-local types. */
static inline mizu_view_str_geom mizu_view_str_geometry(size_t n) {
  mizu_mizs_geom g = mizu_mizs_geometry((int64_t) n);
  mizu_view_str_geom v = {
    (size_t) g.validity, (size_t) g.offsets, (size_t) g.encoding,
    (size_t) g.data
  };
  return v;
}

static inline int mizu_view_str_valid(const unsigned char *validity, size_t i) {
  return (validity[i >> 3] >> (i & 7)) & 1;
}

#endif /* MIZU_VIEW_H */
