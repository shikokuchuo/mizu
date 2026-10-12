/* The R binding's Arrow send path: the class gate (mizu_arrow_check) and
   the frame stage (mizu_arrow_stage_r) over the core's Arrow frame writer
   (vendor/libmizu/arrow.c). The R helpers eval inside the transactional
   guarantee — they complete before any shared-state mutation; the core
   stage's capability conjunction is the only caps home, and the size
   pre-gate (the region-span proxy) the binding's one tier choice. No pin:
   the frame region is self-contained (every byte copied off the Arrow
   buffers; no remote leaves from R). */

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

/* One column's borrow kind: the frame's own unmaterialized leaf view
   (the chain parse pins the region identity and the directory position),
   attribute-free or class-only integer64, the wire tag agreeing, strings
   past the encoding gate. A remote or foreign leaf fails the chain parse
   (it roots elsewhere) and takes the copy path. */
static int arrow_borrow_kind(SEXP col, const unsigned char *base,
                             const char *fname, int64_t i,
                             const mizu_mizl_entry *e) {
  if (!mizu_view_check(col) || TYPEOF(col) == VECSXP ||
      R_altrep_data2(col) != R_NilValue)
    return 0;
  SEXP id = mizu_view_shm_name(col);
  if (id == R_NilValue) return 0;
  char name[MIZU_NAME_MAX];
  int32_t path[MIZU_VIEW_MAX_PATH];
  int plen = 0;
  if (mizu_view_parse_id(CHAR(STRING_ELT(id, 0)), name, sizeof name,
                         path, &plen) != 1 ||
      plen != 1 || path[0] != i || strcmp(name, fname) != 0)
    return 0;
  const int32_t tag = e->sexptype & ~(int32_t) MIZU_MIZL_S4;
  switch (TYPEOF(col)) {
  case REALSXP:
    if (mizu_view_is_int64_any(col))
      return tag == MIZU_TYPE_INT64 ? ARROW_BORROW_INT64 : 0;
    return !ANY_ATTRIB(col) && tag == MIZU_TYPE_REAL ? ARROW_BORROW_REAL : 0;
  case INTSXP:
    return !ANY_ATTRIB(col) && tag == MIZU_TYPE_INT ? ARROW_BORROW_INT : 0;
  case STRSXP:
    return !ANY_ATTRIB(col) && tag == MIZU_TYPE_STR &&
      arrow_str_enc_ok(base + e->data_offset, e->length,
                       e->data_size - e->attrs_size) ? ARROW_BORROW_STR : 0;
  }
  return 0;
}

/* Per-column borrow kinds for mizu_as_arrow() (INTSXP(ncols); all zero —
   the whole-frame copy fallback — when x is not a region-backed frame). */
SEXP mizu_arrow_borrow_kinds_call(SEXP x) {
  const R_xlen_t ncols = XLENGTH(x);
  SEXP kinds = PROTECT(Rf_allocVector(INTSXP, ncols));
  memset(INTEGER(kinds), 0, (size_t) ncols * sizeof(int));
  if (!mizu_view_check(x) || TYPEOF(x) != VECSXP) {
    UNPROTECT(1);
    return kinds;
  }
  SEXP id = PROTECT(mizu_view_shm_name(x));
  char fname[MIZU_NAME_MAX];
  int32_t path[MIZU_VIEW_MAX_PATH];
  int plen = 0;
  mizu_shm *shm = NULL;
  int64_t n = 0, aoff = 0, asz = 0, hv[2];
  if (id == R_NilValue ||
      mizu_view_parse_id(CHAR(STRING_ELT(id, 0)), fname, sizeof fname,
                         path, &plen) != 0 ||
      mizu_shm_open(&shm, fname) != MIZU_OK ||
      mizu_mizl_check(mizu_shm_addr(shm), mizu_shm_size(shm), &n, &aoff,
                      &asz, hv) != 0 ||
      n != (int64_t) ncols) {
    if (shm != NULL) mizu_shm_close(shm, 0);
    UNPROTECT(2);
    return kinds;
  }
  const unsigned char *base = (const unsigned char *) mizu_shm_addr(shm);
  const size_t size = mizu_shm_size(shm);
  for (int64_t i = 0; i < n; i++) {
    mizu_mizl_entry e;
    if (mizu_mizl_elem(base, size, i, &e) != 0) break;
    SEXP col = PROTECT(VECTOR_ELT(x, (R_xlen_t) i));
    INTEGER(kinds)[i] = arrow_borrow_kind(col, base, fname, i, &e);
    UNPROTECT(1);
  }
  mizu_shm_close(shm, 0);
  UNPROTECT(2);
  return kinds;
}

// The export holder ---------------------------------------------------------------

/* One fresh view mapping + counted loan on the frame's region
   (mizu_shm_open_view counts), shared by every borrowed column of one
   mizu_as_arrow() call; each filled ArrowArray takes one reference
   (private_data + the idempotent release below). The last release subs
   the loan and unmaps — pure C, callable from any thread (Arrow C++ may
   drop its last buffer reference off the R thread). */
typedef struct mizu_arrow_owned_s {
  uint8_t *bmp;
  struct mizu_arrow_owned_s *next;
} mizu_arrow_owned;

typedef struct {
  mizu_shm *shm;
  long pid;
  _Atomic size_t refs;
  mizu_arrow_owned *owned;   /* sentinel-derived validity bitmaps */
  const void **bufs;         /* 3 pointers per borrowed column */
} mizu_arrow_holder;

static void mizu_arrow_holder_free(mizu_arrow_holder *hd) {
  if (hd->pid == mizu_self_pid())
    mizu_zc_unref(hd->shm);        /* a forked child's sub would corrupt it */
  mizu_shm_close(hd->shm, 0);
  while (hd->owned != NULL) {
    mizu_arrow_owned *nx = hd->owned->next;
    free(hd->owned->bmp);
    free(hd->owned);
    hd->owned = nx;
  }
  free(hd->bufs);
  free(hd);
}

static void mizu_arrow_array_release(struct ArrowArray *a) {
  a->release = NULL;                     /* idempotent */
  mizu_arrow_holder *hd = (mizu_arrow_holder *) a->private_data;
  if (atomic_fetch_sub_explicit(&hd->refs, 1, memory_order_acq_rel) != 1)
    return;
  mizu_arrow_holder_free(hd);
}

/* Fill every borrowed column's pre-allocated ArrowArray in place off the
   frame's MIZL directory: {validity, data} for fixed width (the tail
   bitmap when the region carries one, NULL when known-NA-free, else a
   sentinel-derived owned bitmap), {validity, offsets, data} for MIZS
   verbatim (the encoding gate already ran at the borrow decision). */
SEXP mizu_arrow_export_call(SEXP x, SEXP idx, SEXP arrays) {
  if (!mizu_view_check(x) || TYPEOF(x) != VECSXP ||
      TYPEOF(idx) != INTSXP || TYPEOF(arrays) != VECSXP ||
      XLENGTH(idx) != XLENGTH(arrays))
    Rf_error("mizu: corrupt arrow export call");
  SEXP id = PROTECT(mizu_view_shm_name(x));
  if (id == R_NilValue)
    Rf_error("mizu: the frame has no region");
  const char *fname = CHAR(STRING_ELT(id, 0));
  const R_xlen_t nb = XLENGTH(idx);
  mizu_arrow_holder *hd = (mizu_arrow_holder *) calloc(1, sizeof *hd);
  if (hd == NULL)
    Rf_error("mizu: allocation failure");
  hd->bufs = (const void **) calloc((size_t) (nb != 0 ? nb : 1) * 3,
                                    sizeof(void *));
  if (hd->bufs == NULL) {
    free(hd);
    Rf_error("mizu: allocation failure");
  }
  atomic_init(&hd->refs, 0);
  hd->owned = NULL;
  if (mizu_shm_open_view(&hd->shm, fname) != MIZU_OK) {
    /* the name went away (the producer exited and unlinked): FALSE sends
       the caller to the all-copy fallback — the views still read */
    free(hd->bufs);
    free(hd);
    UNPROTECT(1);
    return Rf_ScalarLogical(FALSE);
  }
  hd->pid = mizu_self_pid();
  /* the plan pass: validate every entry and allocate every owned bitmap
     (0xFF-prefilled — the unused high bits read valid), so the fill
     below cannot fail */
  unsigned char *base = (unsigned char *) mizu_shm_addr(hd->shm);
  const size_t size = mizu_shm_size(hd->shm);
  int64_t n, aoff, asz, hv[2];
  if (mizu_mizl_check(base, size, &n, &aoff, &asz, hv) != 0) {
    mizu_arrow_holder_free(hd);
    Rf_error("mizu: the frame region is corrupt or newer");
  }
  const void **datas = (const void **) R_alloc(nb, sizeof(void *));
  const void **valids = (const void **) R_alloc(nb, sizeof(void *));
  const void **offs = (const void **) R_alloc(nb, sizeof(void *));
  int64_t *lens = (int64_t *) R_alloc(nb, sizeof(int64_t));
  int64_t *nulls = (int64_t *) R_alloc(nb, sizeof(int64_t));
  int32_t *tags = (int32_t *) R_alloc(nb, sizeof(int32_t));
  for (R_xlen_t j = 0; j < nb; j++) {
    const int64_t col = (int64_t) INTEGER(idx)[j] - 1;
    if (col < 0 || col >= n ||
        R_ExternalPtrAddr(VECTOR_ELT(arrays, j)) == NULL) {
      mizu_arrow_holder_free(hd);
      Rf_error("mizu: corrupt arrow export call");
    }
    mizu_mizl_entry e;
    if (mizu_mizl_elem(base, size, col, &e) != 0) {
      mizu_arrow_holder_free(hd);
      Rf_error("mizu: the frame region is corrupt or newer");
    }
    const int32_t tag = e.sexptype & ~(int32_t) MIZU_MIZL_S4;
    lens[j] = e.length;
    tags[j] = tag;
    if (tag == MIZU_TYPE_STR) {
      mizu_mizs_geom g = mizu_mizs_geometry(e.length);
      const unsigned char *block = base + e.data_offset;
      const uint8_t *validity = block + g.validity;
      int64_t nn = 0;
      for (int64_t k = 0; k < e.length; k++)
        nn += (validity[k >> 3] & (1u << (k & 7))) == 0;
      datas[j] = block + g.data;
      offs[j] = block + g.offsets;
      valids[j] = nn != 0 ? (const void *) validity : NULL;
      nulls[j] = nn;
      continue;
    }
    if (tag != MIZU_TYPE_REAL && tag != MIZU_TYPE_INT &&
        tag != MIZU_TYPE_INT64) {
      mizu_arrow_holder_free(hd);
      Rf_error("mizu: column %lld is not borrowable",
               (long long) (col + 1));
    }
    datas[j] = base + e.data_offset;
    offs[j] = NULL;
    if (e.valid[0] != 0) {                /* the tail bitmap, verbatim */
      valids[j] = base + e.valid[0];
      nulls[j] = e.valid[1];
    } else if (e.valid[1] == 0) {         /* absent: sentinel-derived */
      uint8_t *bmp = (uint8_t *) malloc(((size_t) e.length + 7) / 8);
      if (bmp == NULL) {
        mizu_arrow_holder_free(hd);
        Rf_error("mizu: allocation failure");
      }
      memset(bmp, 0xFF, ((size_t) e.length + 7) / 8);
      int64_t nn = (int64_t) mizu_na_build(tag, bmp, datas[j],
                                           (uint64_t) e.length, 0);
      if (nn == 0) {
        free(bmp);
        valids[j] = NULL;
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
        valids[j] = bmp;
      }
      nulls[j] = nn;
    } else {                              /* {0,-1}: known-NA-free */
      valids[j] = NULL;
      nulls[j] = 0;
    }
  }
  for (R_xlen_t j = 0; j < nb; j++) {
    struct ArrowArray *a =
      (struct ArrowArray *) R_ExternalPtrAddr(VECTOR_ELT(arrays, j));
    const void **bufs = hd->bufs + 3 * j;
    a->length = lens[j];
    a->null_count = nulls[j];
    a->offset = 0;
    a->n_buffers = tags[j] == MIZU_TYPE_STR ? 3 : 2;
    a->n_children = 0;
    bufs[0] = valids[j];
    if (tags[j] == MIZU_TYPE_STR) {
      bufs[1] = offs[j];
      bufs[2] = datas[j];
    } else {
      bufs[1] = datas[j];
    }
    a->buffers = bufs;
    a->children = NULL;
    a->dictionary = NULL;
    a->private_data = hd;
    a->release = mizu_arrow_array_release;
    atomic_fetch_add_explicit(&hd->refs, 1, memory_order_relaxed);
  }
  UNPROTECT(1);
  return Rf_ScalarLogical(TRUE);
}
