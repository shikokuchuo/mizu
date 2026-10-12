/* The R binding's Arrow paths. Send: the class gate (mizu_arrow_check) and
   the frame stage (mizu_arrow_stage_r) over the core's Arrow frame writer
   (vendor/libmizu/arrow.c). The R helpers eval inside the transactional
   guarantee — they complete before any shared-state mutation; the core
   stage's capability conjunction is the only caps home, and the size
   pre-gate (the region-span proxy) the binding's one tier choice. No pin:
   the frame region is self-contained (every byte copied off the Arrow
   buffers; no remote leaves from R). Receive (mizu_as_arrow): one fused
   decide-and-fill .Call — every column locates uniformly through its own
   view identifier (a bare root by magic, a path through
   mizu_view_path_entry), and one counted loan per distinct region holds
   the pages the exported arrays point into. */

#include "arrow.h"
#include <stdlib.h>
#include <string.h>

/* ANY_ATTRIB() joined the C API in R 4.5.0; equivalent fallback for earlier
   R, where ATTRIB() was still the sanctioned spelling. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

int mizu_arrow_check(SEXP x) {
  switch (TYPEOF(x)) {
  case ENVSXP:
    return Rf_inherits(x, "ArrowTabular") ||
      Rf_inherits(x, "RecordBatchReader");
  case EXTPTRSXP:
    return Rf_inherits(x, "nanoarrow_array_stream") ||
      Rf_inherits(x, "nanoarrow_array");
  }
  return 0;
}

/* fn(arg) in the mizu namespace, the fresh result returned UNPROTECTED. */
static SEXP arrow_call1(const char *fn, SEXP arg, SEXP ns) {
  SEXP f = Rf_findFun(Rf_install(fn), ns);
  SEXP call = PROTECT(Rf_lang2(f, arg));
  SEXP out = Rf_eval(call, ns);
  UNPROTECT(1);
  return out;
}

/* fn(arg1, arg2) in the mizu namespace, the fresh result UNPROTECTED. */
static SEXP arrow_call2(const char *fn, SEXP arg1, SEXP arg2, SEXP ns) {
  SEXP f = Rf_findFun(Rf_install(fn), ns);
  SEXP call = PROTECT(Rf_lang3(f, arg1, arg2));
  SEXP out = Rf_eval(call, ns);
  UNPROTECT(1);
  return out;
}

void mizu_arrow_stage_r(SEXP x, mizu_slot_hdr *hdr, unsigned char *payload,
                        uint32_t inline_max, mizu_handle *h, void *ctx) {
  mizu_r_handle *rh = (mizu_r_handle *) ctx;
  SEXP ns = PROTECT(R_FindNamespace(Rf_ScalarString(Rf_mkChar("mizu"))));
  SEXP res = PROTECT(arrow_call1("arrow_batches", x, ns));
  SEXP schema = VECTOR_ELT(res, 0);
  SEXP batches = VECTOR_ELT(res, 1);
  if (batches == R_NilValue) {
    /* a non-struct array: convert on the unconsumed stream and re-enter
       as an ordinary vector */
    SEXP value =
      PROTECT(arrow_call1("arrow_to_value", VECTOR_ELT(res, 2), ns));
    mizu_r_stage_channel((void *) value, hdr, payload, inline_max, h, ctx);
    UNPROTECT(3);
    return;
  }
  const R_xlen_t nb = XLENGTH(batches);
  const ArrowArray **bp = (const ArrowArray **) R_alloc(nb, sizeof *bp);
  for (R_xlen_t i = 0; i < nb; i++)
    bp[i] = (const ArrowArray *) R_ExternalPtrAddr(VECTOR_ELT(batches, i));
  mizu_arrow *a;
  if (mizu_arrow_open(&a, (const ArrowSchema *) R_ExternalPtrAddr(schema),
                      bp, (size_t) nb, rh->peer_caps) != MIZU_OK)
    mizu_stop_not_portable("the Arrow frame", mizu_last_error_message(), "");
  const size_t gate =
    inline_max > MIZU_ZC_FLOOR ? (size_t) inline_max : MIZU_ZC_FLOOR;
  mizu_arrow_warn warn = { 0, 0 };
  if (mizu_arrow_size(a) <= gate ||
      mizu_arrow_stage(h, a, hdr, payload, &warn) != MIZU_OK) {
    /* at or under the region tier, or declined by the stage's caps/churn
       gate or a failed reservation: the held batches convert (never a
       re-stream) and re-enter as an ordinary data.frame */
    mizu_arrow_close(a);
    SEXP value =
      PROTECT(arrow_call2("arrow_batches_to_value", schema, batches, ns));
    mizu_r_stage_channel((void *) value, hdr, payload, inline_max, h, ctx);
    UNPROTECT(3);
    return;
  }
  mizu_arrow_close(a);
  UNPROTECT(2);
  /* the conversion counts raise after the write half completed: a handler
     longjmp abandons the stage here with the unpublished reservation
     reclaimed by the next send's rollback */
  if (warn.n_range != 0)
    Rf_warning(
      "mizu: %llu integer value(s) beyond +/-2^53 convert to NA on the R side",
      (unsigned long long) warn.n_range
    );
  if (warn.n_intmin != 0)
    Rf_warning(
      "mizu: %llu int32 value(s) of -2147483648 read as NA_integer_ in R",
      (unsigned long long) warn.n_intmin
    );
}

// The receive-side export (mizu_as_arrow) -----------------------------------------

/* The borrow kinds: 0 copy / 1 double / 2 int32 / 3 int64 / 4 string. */
enum {
  ARROW_BORROW_REAL = 1,
  ARROW_BORROW_INT = 2,
  ARROW_BORROW_INT64 = 3,
  ARROW_BORROW_STR = 4
};

/* The MIZS leaf's encoding gate for the verbatim export, fused with span
   bounds: 0 when any element's mark cannot cross to Arrow (latin1 /
   bytes, or a native mark failing UTF-8 validation). Runs off the
   directory block — no per-element CHARSXP walk. */
static int arrow_str_enc_ok(const unsigned char *block, int64_t n,
                            int64_t body) {
  mizu_mizs_geom g = mizu_mizs_geometry(n);
  if (g.data > body) return 0;
  const int64_t str_bytes = body - g.data;
  const uint8_t *validity = block + g.validity;
  const int64_t *offs = (const int64_t *) (block + g.offsets);
  const uint8_t *enc = block + g.encoding;
  const uint8_t *data = block + g.data;
  if (offs[0] != 0 || offs[n] < 0 || offs[n] > str_bytes) return 0;
  for (int64_t i = 0; i < n; i++) {
    if ((validity[i >> 3] & (1u << (i & 7))) == 0) continue;
    const int64_t lo = offs[i], hi = offs[i + 1];
    if (hi < lo) return 0;
    const uint8_t m = enc[i];
    if (m == MIZU_CE_LATIN1 || m == MIZU_CE_BYTES) return 0;
    if (m == MIZU_CE_NATIVE &&
        !mizu_ix_utf8_valid(data + lo, (size_t) (hi - lo)))
      return 0;
  }
  return 1;
}

/* One borrowed column's located geometry: the decision records
   region-relative offsets (the regions are immutable post-publish, so the
   holder's fresh mapping at the acquire reads them byte-for-byte); the
   pre-fill resolves the validity pointer and the null count. */
typedef struct {
  R_xlen_t i;              /* the column index in x */
  mizu_shm *region;        /* resolved at the acquire; NULL = declined */
  char name[MIZU_NAME_MAX];
  int kind;                /* 1..4; zeroed by a failed acquire */
  int64_t data_off;        /* region-relative: element data / the MIZS block */
  int64_t length;
  int64_t voff, vcount;    /* the flattened validity pair (fixed width) */
  const void *valid;       /* resolved at the pre-fill */
  int64_t nulls;
} arrow_col_plan;

/* The candidate borrow kind off the live view alone: TYPEOF, the attr
   gates, the int64-any probe — no wire tag, no region read. */
static int arrow_col_kind(SEXP col) {
  switch (TYPEOF(col)) {
  case REALSXP:
    if (mizu_view_is_int64_any(col)) return ARROW_BORROW_INT64;
    return !ANY_ATTRIB(col) ? ARROW_BORROW_REAL : 0;
  case INTSXP:
    return !ANY_ATTRIB(col) ? ARROW_BORROW_INT : 0;
  case STRSXP:
    return !ANY_ATTRIB(col) ? ARROW_BORROW_STR : 0;
  }
  return 0;
}

static int32_t arrow_kind_tag(int kind) {
  switch (kind) {
  case ARROW_BORROW_REAL:  return MIZU_TYPE_REAL;
  case ARROW_BORROW_INT:   return MIZU_TYPE_INT;
  case ARROW_BORROW_INT64: return MIZU_TYPE_INT64;
  default:                 return MIZU_TYPE_STR;
  }
}

/* The uniform locate: a parsed identifier (rc/path) over an open mapping
   → the leaf's region-relative geometry in pl. rc 0 dispatches on the
   magic the way mizu_view_root_descriptor does (MIZH: the tag off the
   wire type, data at MIZU_HEADER_SIZE, the valid pair; MIZS: the block at
   MIZU_HEADER_SIZE, str_size the enc-gate body; MIZL declines — a list
   target wraps as VECSXP and never reaches here). rc 1 reads the terminal
   directory entry through mizu_view_path_entry, the offsets flattened to
   region-relative by the leaf-base delta (data_offset, and valid[0] when
   nonzero). The tag cross-checks against the kind; strings run the
   encoding gate on the located block. 0 on any rejection. */
static int arrow_locate(const unsigned char *base, size_t size, int rc,
                        const int32_t *path, int plen, int kind,
                        arrow_col_plan *pl) {
  const int32_t want = arrow_kind_tag(kind);
  if (rc == 0) {
    if (size < MIZU_HEADER_SIZE) return 0;
    uint32_t magic;
    memcpy(&magic, base, 4);
    if (magic == MIZU_MAGIC_VEC) {
      int wire;
      int64_t n, valid[2];
      if (mizu_mizh_check(base, size, &wire, &n, valid) != 0 ||
          wire != want)
        return 0;
      pl->data_off = MIZU_HEADER_SIZE;
      pl->length = n;
      pl->voff = valid[0];
      pl->vcount = valid[1];
      return 1;
    }
    if (magic == MIZU_MAGIC_STR) {
      int64_t n, str_size, attrs;
      if (mizu_mizs_check(base, size, &n, &str_size, &attrs) != 0 ||
          want != MIZU_TYPE_STR ||
          !arrow_str_enc_ok(base + MIZU_HEADER_SIZE, n, str_size))
        return 0;
      pl->data_off = MIZU_HEADER_SIZE;
      pl->length = n;
      pl->voff = 0;
      pl->vcount = 0;
      return 1;
    }
    return 0;
  }
  const unsigned char *leaf;
  mizu_mizl_entry e;
  if (mizu_view_path_entry(base, (int64_t) size, path, plen, &leaf,
                           &e) != 0)
    return 0;
  const int32_t tag = e.sexptype & ~(int32_t) MIZU_MIZL_S4;
  if (tag != want) return 0;
  const int64_t delta = leaf - base;
  if (kind == ARROW_BORROW_STR &&
      !arrow_str_enc_ok(leaf + e.data_offset, e.length,
                        e.data_size - e.attrs_size))
    return 0;
  pl->data_off = delta + e.data_offset;
  pl->length = e.length;
  pl->voff = e.valid[0] != 0 ? delta + e.valid[0] : 0;
  pl->vcount = e.valid[1];
  return 1;
}

// The export holder ---------------------------------------------------------------

/* One fresh view mapping + counted loan per distinct region backing a
   borrowed column (mizu_shm_open_view counts), shared by every column
   locating into that region; each filled ArrowArray takes one reference
   (private_data + the idempotent release below). The last release subs
   the loans and unmaps — pure C, callable from any thread (Arrow C++ may
   drop its last buffer reference off the R thread). */
typedef struct mizu_arrow_owned_s {
  uint8_t *bmp;
  struct mizu_arrow_owned_s *next;
} mizu_arrow_owned;

typedef struct {
  mizu_shm **regions;        /* open_view mappings + counted loans */
  size_t nregions, cap;
  long pid;
  _Atomic size_t refs;
  mizu_arrow_owned *owned;   /* sentinel-derived validity bitmaps */
  const void **bufs;         /* 3 pointers per borrowed column */
} mizu_arrow_holder;

/* The region named `name`, acquired once per holder: a scan of the
   acquired set, else a fresh open_view (the counted loan). NULL when the
   name went away (an orderly producer exit unlinked it). */
static mizu_shm *holder_acquire(mizu_arrow_holder *hd, const char *name) {
  for (size_t k = 0; k < hd->nregions; k++)
    if (strcmp(mizu_shm_name(hd->regions[k]), name) == 0)
      return hd->regions[k];
  mizu_shm *reg = NULL;
  if (mizu_shm_open_view(&reg, name) != MIZU_OK)
    return NULL;
  hd->regions[hd->nregions++] = reg;
  return reg;
}

static void mizu_arrow_holder_free(mizu_arrow_holder *hd) {
  for (size_t k = 0; k < hd->nregions; k++) {
    if (hd->pid == mizu_self_pid())
      mizu_zc_unref(hd->regions[k]); /* a forked child's sub would corrupt it */
    mizu_shm_close(hd->regions[k], 0);
  }
  while (hd->owned != NULL) {
    mizu_arrow_owned *nx = hd->owned->next;
    free(hd->owned->bmp);
    free(hd->owned);
    hd->owned = nx;
  }
  free(hd->bufs);
  free(hd->regions);
  free(hd);
}

static void mizu_arrow_array_release(struct ArrowArray *a) {
  a->release = NULL;                     /* idempotent */
  mizu_arrow_holder *hd = (mizu_arrow_holder *) a->private_data;
  if (atomic_fetch_sub_explicit(&hd->refs, 1, memory_order_acq_rel) != 1)
    return;
  mizu_arrow_holder_free(hd);
}

/* The fused decide-and-fill for mizu_as_arrow(): the per-column borrow
   decision (kinds, the return — all zero is the all-copy shape) and the
   zero-copy fill of the borrowed columns' pre-allocated ArrowArrays in
   one call, so the located geometry never leaves C. Every column locates
   uniformly through its own view identifier — a local frame leaf and a
   remote leaf alike ({validity, data} for fixed width: the tail bitmap
   when the region carries one, NULL when known-NA-free, else a
   sentinel-derived owned bitmap; {validity, offsets, data} for MIZS
   verbatim, the encoding gate at the decision). A region whose name went
   away declines only its own columns to the copy path. */
SEXP mizu_arrow_export_call(SEXP x, SEXP arrays) {
  if (TYPEOF(x) != VECSXP || TYPEOF(arrays) != VECSXP ||
      XLENGTH(x) != XLENGTH(arrays))
    Rf_error("mizu: corrupt arrow export call");
  const R_xlen_t n = XLENGTH(x);
  SEXP kinds = PROTECT(Rf_allocVector(INTSXP, n));
  memset(INTEGER(kinds), 0, (size_t) n * sizeof(int));
  arrow_col_plan *plans =
    (arrow_col_plan *) R_alloc(n != 0 ? n : 1, sizeof(arrow_col_plan));
  int nb = 0;

  /* Phase 1 — the decision (the only phase that allocates SEXPs; the
     holder does not exist yet, so a raise leaks nothing). */
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP col = PROTECT(VECTOR_ELT(x, i));
    int kind = mizu_view_check(col) && R_altrep_data2(col) == R_NilValue
      ? arrow_col_kind(col)
      : 0;
    if (kind == 0) {
      UNPROTECT(1);
      continue;
    }
    SEXP id = PROTECT(mizu_view_shm_name(col));
    if (id != R_NilValue) {
      char name[MIZU_NAME_MAX];
      int32_t path[MIZU_VIEW_MAX_PATH];
      int plen = 0;
      const int rc = mizu_view_parse_id(CHAR(STRING_ELT(id, 0)), name,
                                        sizeof name, path, &plen);
      mizu_shm *chain = rc >= 0 ? mizu_view_chain_shm(col) : NULL;
      if (chain != NULL &&
          arrow_locate((const unsigned char *) mizu_shm_addr(chain),
                       mizu_shm_size(chain), rc, path, plen, kind,
                       &plans[nb])) {
        plans[nb].i = i;
        plans[nb].region = NULL;
        memcpy(plans[nb].name, name, sizeof name);
        plans[nb].kind = kind;
        INTEGER(kinds)[i] = kind;
        nb++;
      }
    }
    UNPROTECT(2);
  }
  if (nb == 0) {
    UNPROTECT(1);
    return kinds;
  }

  /* Phase 2 — the acquire (no R API): one counted loan per distinct
     region; a failed open zeroes only that region's plans (the views
     still read off their own mappings — those columns copy). */
  mizu_arrow_holder *hd = (mizu_arrow_holder *) calloc(1, sizeof *hd);
  if (hd == NULL)
    Rf_error("mizu: allocation failure");
  hd->regions = (mizu_shm **) calloc((size_t) nb, sizeof(mizu_shm *));
  hd->bufs = (const void **) calloc((size_t) nb * 3, sizeof(void *));
  if (hd->regions == NULL || hd->bufs == NULL) {
    free(hd->regions);
    free(hd->bufs);
    free(hd);
    Rf_error("mizu: allocation failure");
  }
  atomic_init(&hd->refs, 0);
  hd->cap = (size_t) nb;
  hd->pid = mizu_self_pid();
  for (int j = 0; j < nb; j++) {
    if (plans[j].kind == 0) continue;
    mizu_shm *reg = holder_acquire(hd, plans[j].name);
    if (reg != NULL) {
      plans[j].region = reg;
      continue;
    }
    for (int k = j; k < nb; k++)
      if (plans[k].kind != 0 && strcmp(plans[k].name, plans[j].name) == 0) {
        plans[k].kind = 0;
        INTEGER(kinds)[plans[k].i] = 0;
      }
  }

  /* Phase 3a — the pre-fill (the sole fallible step): resolve every live
     plan's validity pointer and null count off its region mapping,
     building the sentinel-derived owned bitmaps (0xFF-prefilled — the
     unused high bits read valid), so the fill below cannot fail. */
  for (int j = 0; j < nb; j++) {
    arrow_col_plan *pl = &plans[j];
    if (pl->kind == 0) continue;
    if (R_ExternalPtrAddr(VECTOR_ELT(arrays, pl->i)) == NULL) {
      mizu_arrow_holder_free(hd);
      Rf_error("mizu: corrupt arrow export call");
    }
    const unsigned char *base =
      (const unsigned char *) mizu_shm_addr(pl->region);
    if (pl->kind == ARROW_BORROW_STR) {
      const unsigned char *block = base + pl->data_off;
      const mizu_mizs_geom g = mizu_mizs_geometry(pl->length);
      const uint8_t *validity = block + g.validity;
      int64_t nn = 0;
      for (int64_t k = 0; k < pl->length; k++)
        nn += (validity[k >> 3] & (1u << (k & 7))) == 0;
      pl->valid = nn != 0 ? (const void *) validity : NULL;
      pl->nulls = nn;
      continue;
    }
    if (pl->voff != 0) {                /* the tail bitmap, verbatim */
      pl->valid = base + pl->voff;
      pl->nulls = pl->vcount;
    } else if (pl->vcount == 0) {       /* absent: sentinel-derived */
      uint8_t *bmp = (uint8_t *) malloc(((size_t) pl->length + 7) / 8);
      if (bmp == NULL) {
        mizu_arrow_holder_free(hd);
        Rf_error("mizu: allocation failure");
      }
      memset(bmp, 0xFF, ((size_t) pl->length + 7) / 8);
      const int64_t nn = (int64_t) mizu_na_build(
        arrow_kind_tag(pl->kind), bmp, base + pl->data_off,
        (uint64_t) pl->length, 0
      );
      if (nn == 0) {
        free(bmp);
        pl->valid = NULL;
      } else {
        mizu_arrow_owned *ow = (mizu_arrow_owned *) malloc(sizeof *ow);
        if (ow == NULL) {
          free(bmp);
          mizu_arrow_holder_free(hd);
          Rf_error("mizu: allocation failure");
        }
        ow->bmp = bmp;
        ow->next = hd->owned;
        hd->owned = ow;
        pl->valid = bmp;
      }
      pl->nulls = nn;
    } else {                            /* {0,-1}: known-NA-free */
      pl->valid = NULL;
      pl->nulls = 0;
    }
  }

  /* Phase 3b — the infallible fill: the ArrowArray fields, 3 buffers for
     strings, 2 otherwise. */
  for (int j = 0; j < nb; j++) {
    arrow_col_plan *pl = &plans[j];
    if (pl->kind == 0) continue;
    struct ArrowArray *a =
      (struct ArrowArray *) R_ExternalPtrAddr(VECTOR_ELT(arrays, pl->i));
    const unsigned char *base =
      (const unsigned char *) mizu_shm_addr(pl->region);
    const void **bufs = hd->bufs + 3 * j;
    a->length = pl->length;
    a->null_count = pl->nulls;
    a->offset = 0;
    a->n_buffers = pl->kind == ARROW_BORROW_STR ? 3 : 2;
    a->n_children = 0;
    bufs[0] = pl->valid;
    if (pl->kind == ARROW_BORROW_STR) {
      const mizu_mizs_geom g = mizu_mizs_geometry(pl->length);
      const unsigned char *block = base + pl->data_off;
      bufs[1] = block + g.offsets;
      bufs[2] = block + g.data;
    } else {
      bufs[1] = base + pl->data_off;
    }
    a->buffers = bufs;
    a->children = NULL;
    a->dictionary = NULL;
    a->private_data = hd;
    a->release = mizu_arrow_array_release;
    atomic_fetch_add_explicit(&hd->refs, 1, memory_order_relaxed);
  }
  UNPROTECT(1);
  return kinds;
}
