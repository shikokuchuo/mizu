/* The R binding's Arrow paths. Send: the class gate (mizu_arrow_check) and
   the frame stage (mizu_arrow_stage_r) over the core's Arrow frame writer
   (vendor/libmizu/arrow.c), serving the channel and pool stage hooks
   alike — the re-entry tier dispatch arrives as a parameter, and the pool
   passes this build's own capability word (a task frame is same-language
   by construction). The R helpers eval inside the transactional
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

/* The attributes, if any: the (name, value) pairs, class last through
   Rf_classgets — the object bit follows the class as R's own setAttrib
   maintains it (view.c's apply discipline). */
static void arrow_copy_attrs(SEXP out, SEXP x) {
  SEXP attrs = PROTECT(R_getAttributes(x));
  SEXP classv = R_NilValue;
  if (TYPEOF(attrs) == VECSXP) {
    SEXP names = PROTECT(Rf_getAttrib(attrs, R_NamesSymbol));
    for (R_xlen_t i = 0, n = XLENGTH(attrs); i < n; i++) {
      SEXP sym = Rf_installChar(STRING_ELT(names, i));
      if (sym == R_ClassSymbol) { classv = VECTOR_ELT(attrs, i); continue; }
      Rf_setAttrib(out, sym, VECTOR_ELT(attrs, i));
    }
    UNPROTECT(1);                /* names */
  } else {
    for (SEXP a = attrs; a != R_NilValue; a = CDR(a)) {
      if (TAG(a) == R_ClassSymbol) { classv = CAR(a); continue; }
      Rf_setAttrib(out, TAG(a), CAR(a));
    }
  }
  if (classv != R_NilValue) Rf_classgets(out, classv);
  UNPROTECT(1);
}

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

/* The mizu namespace and the R helpers, cached on first use on the precious
   root (they do not exist when R_init_mizu runs — the namespace's R code
   loads after the DLL — and R_FindNamespace + the lookups otherwise repeat
   per send). The nanoarrow namespace and its entry points sit behind the
   requireNamespace gate (mizu:::arrow_ns), cached by nano_gate on the first
   pull. */
static void arrow_ns(void) {
  if (mizu_priv(MIZU_PRIV_MIZU_NS) != R_NilValue) return;
  SEXP mizu_ns =
    PROTECT(R_FindNamespace(Rf_ScalarString(Rf_mkChar("mizu"))));
  SEXP fn_arrow_ns = PROTECT(Rf_findFun(Rf_install("arrow_ns"), mizu_ns));
  SEXP fn_arrow_to_value =
    PROTECT(Rf_findFun(Rf_install("arrow_to_value"), mizu_ns));
  SEXP fn_batches_to_value =
    PROTECT(Rf_findFun(Rf_install("arrow_batches_to_value"), mizu_ns));
  mizu_priv_set(MIZU_PRIV_MIZU_NS, mizu_ns);
  mizu_priv_set(MIZU_PRIV_FN_ARROW_NS, fn_arrow_ns);
  mizu_priv_set(MIZU_PRIV_FN_ARROW_TO_VALUE, fn_arrow_to_value);
  mizu_priv_set(MIZU_PRIV_FN_BATCHES_TO_VALUE, fn_batches_to_value);
  UNPROTECT(4);
}

/* fn() in the mizu namespace, the fresh result returned UNPROTECTED. */
static SEXP arrow_call0(SEXP fn) {
  SEXP call = PROTECT(Rf_lang1(fn));
  SEXP out = Rf_eval(call, mizu_priv(MIZU_PRIV_MIZU_NS));
  UNPROTECT(1);
  return out;
}

/* fn(arg) in the mizu namespace, the fresh result returned UNPROTECTED. */
static SEXP arrow_call1(SEXP fn, SEXP arg) {
  SEXP call = PROTECT(Rf_lang2(fn, arg));
  SEXP out = Rf_eval(call, mizu_priv(MIZU_PRIV_MIZU_NS));
  UNPROTECT(1);
  return out;
}

/* fn(arg1, arg2) in the mizu namespace, the fresh result UNPROTECTED. */
static SEXP arrow_call2(SEXP fn, SEXP arg1, SEXP arg2) {
  SEXP call = PROTECT(Rf_lang3(fn, arg1, arg2));
  SEXP out = Rf_eval(call, mizu_priv(MIZU_PRIV_MIZU_NS));
  UNPROTECT(1);
  return out;
}

/* The requireNamespace gate: the first pull evals mizu:::arrow_ns() (its
   error is the "nanoarrow required" text) and caches the namespace and
   entry points it returns. */
static void nano_gate(void) {
  if (mizu_priv(MIZU_PRIV_NANO_NS) != R_NilValue) return;
  arrow_ns();
  SEXP nano_ns = PROTECT(arrow_call0(mizu_priv(MIZU_PRIV_FN_ARROW_NS)));
  SEXP fn_as_stream =
    PROTECT(Rf_findFun(Rf_install("as_nanoarrow_array_stream"), nano_ns));
  SEXP fn_alloc_schema =
    PROTECT(Rf_findFun(Rf_install("nanoarrow_allocate_schema"), nano_ns));
  SEXP fn_alloc_array =
    PROTECT(Rf_findFun(Rf_install("nanoarrow_allocate_array"), nano_ns));
  mizu_priv_set(MIZU_PRIV_NANO_NS, nano_ns);
  mizu_priv_set(MIZU_PRIV_FN_AS_STREAM, fn_as_stream);
  mizu_priv_set(MIZU_PRIV_FN_ALLOC_SCHEMA, fn_alloc_schema);
  mizu_priv_set(MIZU_PRIV_FN_ALLOC_ARRAY, fn_alloc_array);
  UNPROTECT(4);
}

// The held pulled state ------------------------------------------------------

/* A send candidate, pulled: the schema and every batch owned as bare ABI
   structs (pymizu's ixs ownership model), the core handle and the
   batch-pointer array it borrows joining at the open. Behind an extptr
   whose finalizer releases everything — the longjmp backstop for every R
   call that may run while the structs are held (the conversion bails). The
   prot is the unconsumed stream on a non-struct pull, the only state its
   bail needs. The scratch slot header and name buffer serve
   mizu_arrow_stage's (hdr, payload) form — a top-level stage copies the
   SHM_VEC stamp off them, the codec's frame leaf reads only the name. */
typedef struct {
  mizu_arrow *a;              /* NULL until open / once closed */
  ArrowSchema schema;         /* owned; released at the probe on a
                                 non-struct pull (only its format gates) */
  ArrowArray *batches;        /* owned pulled batches */
  size_t nbatches;
  int nonstruct;              /* the schema probe declined a struct */
  const ArrowArray **bp;      /* the pointer array the core reads (at open) */
  mizu_slot_hdr hdr;
  unsigned char name[MIZU_NAME_MAX];
} mizu_arrow_held;

/* The pulled structs released (idempotent): every batch through its own
   release, the schema likewise. A successful stage reclaims them early —
   the frame is self-contained off the Arrow buffers; the bail's ABI move
   marks its structs with a NULL release; the finalizer takes the rest. */
static void arrow_release_pulled(mizu_arrow_held *hd) {
  for (size_t i = 0; i < hd->nbatches; i++)
    if (hd->batches[i].release != NULL) {
      hd->batches[i].release(&hd->batches[i]);
      hd->batches[i].release = NULL;
    }
  free(hd->batches);
  hd->batches = NULL;
  hd->nbatches = 0;
  if (hd->schema.release != NULL) {
    hd->schema.release(&hd->schema);
    hd->schema.release = NULL;
  }
}

static void mizu_arrow_held_finalize(SEXP xp) {
  mizu_arrow_held *hd = (mizu_arrow_held *) R_ExternalPtrAddr(xp);
  if (hd == NULL) return;
  if (hd->a != NULL) mizu_arrow_close(hd->a);
  arrow_release_pulled(hd);
  free(hd->bp);
  free(hd);
  R_ClearExternalPtr(xp);
}

/* A stream pull failure, get_last_error when the producer offers it. */
NORET static void arrow_stream_raise(ArrowArrayStream *st) {
  const char *e = st->get_last_error != NULL ? st->get_last_error(st) : NULL;
  Rf_error(
    "mizu: the Arrow stream failed: %s", e != NULL ? e : "unknown error"
  );
}

/* The pull: one R call (as_nanoarrow_array_stream(x) — arrow's own S3
   export is the bulk of the pull cost and stays), then the schema probe
   and every batch pull in C off the extptr's ArrowArrayStream. Sending a
   stream consumes it: a streaming RecordBatchReader is fully materialized
   at send, source bytes and region resident at once (inherent to the
   one-region design). The fresh extptr is returned UNPROTECTED. */
SEXP mizu_arrow_pull(SEXP x) {
  nano_gate();
  SEXP stream = PROTECT(arrow_call1(mizu_priv(MIZU_PRIV_FN_AS_STREAM), x));
  ArrowArrayStream *st = (ArrowArrayStream *) R_ExternalPtrAddr(stream);
  mizu_arrow_held *hd = (mizu_arrow_held *) calloc(1, sizeof *hd);
  if (hd == NULL) Rf_error("mizu: allocation failure");
  SEXP xp = PROTECT(
    R_MakeExternalPtr(hd, Rf_install("mizu_arrow_held"), stream)
  );
  R_RegisterCFinalizerEx(xp, mizu_arrow_held_finalize, TRUE);
  if (st->get_schema(st, &hd->schema) != 0) arrow_stream_raise(st);
  if (hd->schema.format == NULL || strcmp(hd->schema.format, "+s") != 0) {
    /* a non-struct stream: the bail converts on the unconsumed stream */
    if (hd->schema.release != NULL) {
      hd->schema.release(&hd->schema);
      hd->schema.release = NULL;
    }
    hd->nonstruct = 1;
    UNPROTECT(2);
    return xp;
  }
  size_t cap = 0;
  for (;;) {
    if (hd->nbatches == cap) {
      const size_t ncap = cap != 0 ? 2 * cap : 4;
      ArrowArray *nb =
        (ArrowArray *) realloc(hd->batches, ncap * sizeof(ArrowArray));
      if (nb == NULL) Rf_error("mizu: allocation failure");
      hd->batches = nb;
      cap = ncap;
    }
    ArrowArray *a = &hd->batches[hd->nbatches];
    memset(a, 0, sizeof *a);
    if (st->get_next(st, a) != 0) arrow_stream_raise(st);
    if (a->release == NULL) break;    /* the end of the stream */
    hd->nbatches++;
  }
  /* the stream is consumed: release it C-side (nanoarrow's finalizer
     no-ops on a NULL release) and drop the husk from the prot */
  if (st->release != NULL) {
    st->release(st);
    st->release = NULL;
  }
  R_SetExternalPtrProtected(xp, R_NilValue);
  UNPROTECT(2);
  return xp;
}

int mizu_arrow_nonstruct(SEXP xp) {
  return ((mizu_arrow_held *) R_ExternalPtrAddr(xp))->nonstruct;
}

/* A plain copy of an atomic ALTREP vector, filled through the read-only
   accessors — nanoarrow's string conversions are lazy ALTREP, which the
   codec's plain walk declines; below the frame gate the data is small,
   so the conversion bail trades one bounded copy for the codec's framing.
   The CHARSXP conversions themselves are paid either way (the write walk
   would force them element by element); the copy adds the pointer array. */
static SEXP arrow_plain(SEXP x) {
  const SEXPTYPE ty = TYPEOF(x);
  const R_xlen_t n = XLENGTH(x);
  SEXP out = PROTECT(Rf_allocVector(ty, n));
  if (ty == STRSXP) {
    for (R_xlen_t i = 0; i < n; i++)
      SET_STRING_ELT(out, i, STRING_ELT(x, i));
  } else {
    const size_t elt = mizu_view_sizeof_elt((int) ty);
    unsigned char *dst = (unsigned char *) mizu_vec_ptr(out);
    unsigned char buf[8192];
    for (R_xlen_t i = 0; i < n; ) {
      R_xlen_t m = (R_xlen_t) (sizeof buf / elt);
      if (m > n - i) m = n - i;
      switch (ty) {
      case LGLSXP: LOGICAL_GET_REGION(x, i, m, (int *) buf); break;
      case INTSXP: INTEGER_GET_REGION(x, i, m, (int *) buf); break;
      case REALSXP: REAL_GET_REGION(x, i, m, (double *) buf); break;
      case CPLXSXP: COMPLEX_GET_REGION(x, i, m, (Rcomplex *) buf); break;
      default:     RAW_GET_REGION(x, i, m, (Rbyte *) buf); break;
      }
      memcpy(dst + (size_t) i * elt, buf, (size_t) m * elt);
      i += m;
    }
  }
  if (ANY_ATTRIB(x)) arrow_copy_attrs(out, x);
  UNPROTECT(1);
  return out;
}

/* The conversion bail's lazies materialized: the bare vector itself, or
   each ALTREP column of the converted frame (a list column's elements
   can still arrive lazy — the codec's frame-tier write walk serves them
   through the region accessors). */
static SEXP arrow_materialize(SEXP x) {
  const SEXPTYPE ty = TYPEOF(x);
  if (ALTREP(x)) {
    if (ty == STRSXP || ty == LGLSXP || ty == INTSXP || ty == REALSXP ||
        ty == CPLXSXP || ty == RAWSXP)
      return arrow_plain(x);
    return x;
  }
  if (ty == VECSXP) {
    for (R_xlen_t i = 0, n = XLENGTH(x); i < n; i++) {
      SEXP col = VECTOR_ELT(x, i);
      const SEXPTYPE cty = TYPEOF(col);
      if (ALTREP(col) &&
          (cty == STRSXP || cty == LGLSXP || cty == INTSXP ||
           cty == REALSXP || cty == CPLXSXP || cty == RAWSXP)) {
        SEXP plain = PROTECT(arrow_plain(col));
        SET_VECTOR_ELT(x, i, plain);
        UNPROTECT(1);
      }
    }
  }
  return x;
}

/* The by-value form of a pulled candidate: a non-struct stream converts on
   the unconsumed stream (arrow_to_value); the held schema and batches move
   into fresh nanoarrow extptrs (the ABI move: memcpy, then the source's
   release NULLed) and stream back as a struct (arrow_batches_to_value,
   which re-associates the schema per batch). At most once per pull, with
   the core handle already closed. */
SEXP mizu_arrow_value(SEXP xp) {
  nano_gate();
  mizu_arrow_held *hd = (mizu_arrow_held *) R_ExternalPtrAddr(xp);
  if (hd->nonstruct)
    return arrow_call1(mizu_priv(MIZU_PRIV_FN_ARROW_TO_VALUE),
                       R_ExternalPtrProtected(xp));
  SEXP schema = PROTECT(arrow_call0(mizu_priv(MIZU_PRIV_FN_ALLOC_SCHEMA)));
  memcpy(R_ExternalPtrAddr(schema), &hd->schema, sizeof(ArrowSchema));
  hd->schema.release = NULL;
  SEXP batches = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) hd->nbatches));
  for (size_t i = 0; i < hd->nbatches; i++) {
    SEXP arr = PROTECT(arrow_call0(mizu_priv(MIZU_PRIV_FN_ALLOC_ARRAY)));
    memcpy(R_ExternalPtrAddr(arr), &hd->batches[i], sizeof(ArrowArray));
    hd->batches[i].release = NULL;
    SET_VECTOR_ELT(batches, (R_xlen_t) i, arr);
    UNPROTECT(1);
  }
  SEXP out = PROTECT(
    arrow_call2(mizu_priv(MIZU_PRIV_FN_BATCHES_TO_VALUE), schema, batches));
  SEXP plain = arrow_materialize(out);
  UNPROTECT(3);
  return plain;
}

// The open over a pulled candidate ----------------------------------------------

/* The open: the core borrows the schema, the batch structs, and the
   pointer array — all owned by the held struct, outliving the handle.
   0 opened, -1 the core's decline, -2 the allocation failure — the caller
   raises. */
static int arrow_open_into(mizu_arrow_held *hd, uint32_t caps,
                           size_t *fsize) {
  const size_t nb = hd->nbatches;
  hd->a = NULL;
  hd->bp = (const ArrowArray **)
    malloc((nb > 0 ? nb : 1) * sizeof(ArrowArray *));
  if (hd->bp == NULL) return -2;
  for (size_t i = 0; i < nb; i++) hd->bp[i] = &hd->batches[i];
  if (mizu_arrow_open(&hd->a, &hd->schema, hd->bp, nb, caps) != MIZU_OK) {
    free(hd->bp);
    hd->bp = NULL;
    return -1;
  }
  *fsize = mizu_arrow_size(hd->a);
  return 0;
}

NORET static void arrow_open_raise(int rc) {
  if (rc == -2) Rf_error("mizu: allocation failure");
  mizu_stop_not_portable("the Arrow frame", mizu_last_error_message(), "");
}

void mizu_arrow_open_r(SEXP xp, uint32_t caps, size_t *fsize) {
  const int rc =
    arrow_open_into((mizu_arrow_held *) R_ExternalPtrAddr(xp), caps, fsize);
  if (rc != 0) arrow_open_raise(rc);
}

void mizu_arrow_close_held(SEXP xp) {
  mizu_arrow_held *hd = (mizu_arrow_held *) R_ExternalPtrAddr(xp);
  if (hd->a != NULL) {
    mizu_arrow_close(hd->a);
    hd->a = NULL;
  }
  free(hd->bp);
  hd->bp = NULL;
}

/* The stage over a held handle, closed either way: 0 staged, the pulled
   structs released with the close (the frame is self-contained off the
   Arrow buffers), the (hdr, payload) stamp left in the struct's scratch;
   -1 declined (the gate or the reservation), the pull kept for the
   caller's conversion bail. */
static int arrow_stage_held(mizu_arrow_held *hd, mizu_handle *h,
                            unsigned char *name, uint32_t *name_len,
                            mizu_arrow_warn *warn) {
  const int staged =
    mizu_arrow_stage(h, hd->a, &hd->hdr, hd->name, warn) == MIZU_OK;
  mizu_arrow_close(hd->a);
  hd->a = NULL;
  free(hd->bp);
  hd->bp = NULL;
  if (!staged) return -1;
  arrow_release_pulled(hd);
  if (name != NULL) memcpy(name, hd->name, hd->hdr.len);
  if (name_len != NULL) *name_len = (uint32_t) hd->hdr.len;
  return 0;
}

int mizu_arrow_stage_held(SEXP xp, mizu_handle *h, unsigned char *name,
                          uint32_t *name_len, mizu_arrow_warn *warn) {
  return arrow_stage_held(
    (mizu_arrow_held *) R_ExternalPtrAddr(xp), h, name, name_len, warn
  );
}

void mizu_arrow_warn_raise(const mizu_arrow_warn *warn) {
  if (warn->n_range != 0)
    Rf_warning(
      "mizu: %llu uint64 value(s) beyond 2^63 - 1 convert to NA",
      (unsigned long long) warn->n_range
    );
  if (warn->n_intmin != 0)
    Rf_warning(
      "mizu: %llu int32 value(s) of -2147483648 read as NA_integer_ in R",
      (unsigned long long) warn->n_intmin
    );
  if (warn->n_int64 != 0)
    Rf_warning(
      "mizu: %llu int64 value(s) of -9223372036854775808 read as NA_integer64_ in R",
      (unsigned long long) warn->n_int64
    );
}

void mizu_arrow_stage_r(SEXP x, mizu_slot_hdr *hdr, unsigned char *payload,
                        uint32_t inline_max, mizu_handle *h, void *ctx,
                        uint32_t caps, mizu_stage_fn reenter) {
  SEXP xp = PROTECT(mizu_arrow_pull(x));
  mizu_arrow_held *hd = (mizu_arrow_held *) R_ExternalPtrAddr(xp);
  if (hd->nonstruct) {
    /* a non-struct stream: convert on the unconsumed stream and re-enter
       as an ordinary vector */
    SEXP value = PROTECT(mizu_arrow_value(xp));
    mizu_qc_scope temp;
    mizu_stage_temp_begin(&temp);
    reenter((void *) value, hdr, payload, inline_max, h, ctx);
    mizu_stage_temp_end(&temp);
    UNPROTECT(2);
    return;
  }
  const size_t gate =
    inline_max > MIZU_ZC_FLOOR ? (size_t) inline_max : MIZU_ZC_FLOOR;
  /* the pulled structs ride the extptr: the conversion bail's R calls run
     while batches are C-owned, so the finalizer is the longjmp backstop
     on this path too */
  size_t fsize;
  const int orc = arrow_open_into(hd, caps, &fsize);
  if (orc != 0) arrow_open_raise(orc);
  if (fsize <= gate) {
    /* at or under the region tier: one 'I' stream straight off the Arrow
       buffers (the core's inline frame form, dst NULL sizing) — inline
       within the budget with the keeperless claim (the stream commits no
       retain-table entry of its own), else the arena copy or a SHM_RAW
       spill per the tier machinery. No convert back to a value, no
       serialize pass; the same-language read paths dispatch the stream on
       its 0x49 first byte (mizu_stream_read), the foreign reader always
       did. */
    mizu_arrow_warn warn = { 0, 0, 0 };
    const size_t n = mizu_arrow_write_ix(hd->a, NULL, &warn);
    if (n <= (size_t) inline_max) {
      mizu_arrow_write_ix(hd->a, payload, &warn);
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = MIZU_AUX_F_KEEPERLESS;
    } else {
      uint64_t off;
      unsigned char *chunk =
        mizu_stage_arena_alloc(h, MIZU_ALIGN64(n), &off);
      if (chunk != NULL) {
        mizu_arrow_write_ix(hd->a, chunk, &warn);
        hdr->kind = MIZU_KIND_ARENA;
        hdr->len = 0;
        hdr->aux = off;
        uint64_t n64 = (uint64_t) n;
        memcpy(payload, &n64, sizeof(n64));
      } else {
        /* the pool has no arena (and a full channel arena declines): the
           SHM_RAW spill, the region alone retained — the stream is
           self-contained */
        mizu_stage_reap(h);
        mizu_shm *shm = mizu_spill_get_raise(h, n);
        mizu_arrow_write_ix(hd->a, (unsigned char *) shm->addr, &warn);
        hdr->kind = MIZU_KIND_SHM_RAW;
        hdr->len = (uint32_t) shm->name_len;
        hdr->aux = (uint64_t) n;
        memcpy(payload, shm->name, shm->name_len);
        mizu_stage_retain(h, shm);
      }
    }
    mizu_arrow_close(hd->a);
    hd->a = NULL;
    free(hd->bp);
    hd->bp = NULL;
    arrow_release_pulled(hd);
    UNPROTECT(1);
    /* the conversion counts raise after the write half completed (the
       MIZL path's discipline below) */
    mizu_arrow_warn_raise(&warn);
    return;
  }
  mizu_arrow_warn warn = { 0, 0, 0 };
  if (arrow_stage_held(hd, h, NULL, NULL, &warn) != 0) {
    /* declined by the stage's caps/churn gate or a failed reservation:
       the held batches convert and re-enter as an ordinary data.frame */
    SEXP value = PROTECT(mizu_arrow_value(xp));
    mizu_qc_scope temp;
    mizu_stage_temp_begin(&temp);
    reenter((void *) value, hdr, payload, inline_max, h, ctx);
    mizu_stage_temp_end(&temp);
    UNPROTECT(2);
    return;
  }
  *hdr = hd->hdr;
  memcpy(payload, hd->name, hd->hdr.len);
  UNPROTECT(1);
  /* the conversion counts raise after the write half completed: a handler
     longjmp abandons the stage here with the unpublished reservation
     reclaimed by the next send's rollback */
  mizu_arrow_warn_raise(&warn);
}

// The receive-side export (mizu_as_arrow) -----------------------------------------

/* The borrow kinds: 0 copy / 1 double / 2 int32 / 3 int64 / 4 string /
   5 uint64. */
enum {
  ARROW_BORROW_REAL = 1,
  ARROW_BORROW_INT = 2,
  ARROW_BORROW_INT64 = 3,
  ARROW_BORROW_STR = 4,
  ARROW_BORROW_UINT64 = 5
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
   gates, the int64-any probe — no wire tag, no region read. The u64
   class identity runs first: the view is an attribute-free REALSXP, so
   it would otherwise classify REAL and fail locate's tag cross-check
   against the entry's INT64. */
static int arrow_col_kind(SEXP col) {
  switch (TYPEOF(col)) {
  case REALSXP:
    if (R_altrep_inherits(col, mizu_view_u64_class))
      return ARROW_BORROW_UINT64;
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
  case ARROW_BORROW_REAL:   return MIZU_TYPE_REAL;
  case ARROW_BORROW_INT:    return MIZU_TYPE_INT;
  case ARROW_BORROW_INT64:  return MIZU_TYPE_INT64;
  case ARROW_BORROW_UINT64: return MIZU_TYPE_INT64;
  default:                  return MIZU_TYPE_STR;
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
      /* the null count is a popcount over the validity bytes, the tail
         byte masked so the unused-high-bits convention never enters */
      int64_t ones = 0;
      const int64_t nb = pl->length / 8;
      for (int64_t k = 0; k < nb; k++)
        ones += __builtin_popcount(validity[k]);
      if ((pl->length & 7) != 0)
        ones += __builtin_popcount(
          validity[nb] & (0xFFu >> (8 - (pl->length & 7)))
        );
      const int64_t nn = pl->length - ones;
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
  /* every acquire failed: no array references the holder, so no release
     callback ever frees it — free it here */
  if (atomic_load_explicit(&hd->refs, memory_order_acquire) == 0)
    mizu_arrow_holder_free(hd);
  UNPROTECT(1);
  return kinds;
}
