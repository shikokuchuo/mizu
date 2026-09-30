/* mizu_map staging and worker-side context — one fresh mizu region per map
   call, holding a 128-byte header, ONE serialized descriptor stream
   (list(f, dots, x), or list(f, dots) when x rides the RAWVEC section), an
   optional RAWVEC x section (bare bytes of a names-tolerant raw-eligible x,
   sliced per chunk on the workers so x is never deserialized and never
   fully materialized per worker), and an optional output area for the
   template path (vapply semantics: n × m results written at disjoint
   element offsets by the chunk loops — the result-slot OK publish ordered
   before the collector's read is the happens-before — then gathered
   submitter-side in one memcpy). Chunk tasks are ordinary pool tasks and
   this file touches no pool internals: the worker context cache lives on
   the worker handle (pool.c) and the chunk loop in R/map.R. */

#include <stdlib.h>
#include "mizu.h"

static SEXP mizu_map_tag;
static SEXP mizu_rs_sym;
SEXP mizu_srcref_sym;
static SEXP mizu_srcfile_sym;
static SEXP mizu_wholesrcref_sym;
static SEXP mizu_function_sym;

void mizu_map_init(void) {
  mizu_map_tag = Rf_install("mizu_map");
  mizu_rs_sym = Rf_install(".Random.seed");
  mizu_srcref_sym = Rf_install("srcref");
  mizu_srcfile_sym = Rf_install("srcfile");
  mizu_wholesrcref_sym = Rf_install("wholeSrcref");
  mizu_function_sym = Rf_install("function");
}

/* The map region's protocol half — the 128-byte header, the morsel-state
   words, the claim CAS, the AIMD batch sizing, reset/trim, and the lost-set
   scan — is the core's morsel module (vendor/libmizu/morsel.c,
   mizu_morsel_*): the struct is the layout. This file keeps the language-
   coupled half: the descriptor stream, the x-section slices, the batch
   eval loop, and the result assembly. */

/* The map-local RAWVEC gate, deliberately looser than mizu_raw_type: no
   size cap, and attributes are the R side's to check (names-only is
   admissible there — names stay submitter-side for assembly and the chunk
   loop's [[ drops them anyway; class-only integer64 is admitted there and
   stamped at the section write). ALTREP still disqualifies — a
   mori::share()d x must reduce to its identifier inside the descriptor
   stream via the hooks, not be copied wholesale into a second region — as
   does S4. Returns the section byte length, or -1 when x must ride the
   descriptor. */
SEXP mizu_map_eligible(SEXP x) {
  switch (TYPEOF(x)) {
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
    break;
  default:
    return Rf_ScalarReal(-1);
  }
  if (ALTREP(x) || Rf_isS4(x)) return Rf_ScalarReal(-1);
  return Rf_ScalarReal((double) XLENGTH(x) *
                       (double) mizu_view_sizeof_elt(TYPEOF(x)));
}

/* The batch element class test: a REALSXP chunk whose class is exactly
   "integer64" (a raw-section slice, a codec blob slice, or a resolved view)
   hands f classed integer64 scalars. The interned CHARSXP compare is exact;
   class-only is not required — per-element scalars drop other attributes
   either way. */
static int map_is_int64_classed(SEXP x) {
  SEXP cls = Rf_getAttrib(x, R_ClassSymbol);
  return TYPEOF(cls) == STRSXP && XLENGTH(cls) == 1 &&
    STRING_ELT(cls, 0) == STRING_ELT(mizu_view_int64_class, 0);
}

static const char *map_type_name(int type) {
  switch (type) {
  case LGLSXP:  return "logical";
  case INTSXP:  return "integer";
  case REALSXP: return "double";
  case CPLXSXP: return "complex";
  case RAWSXP:  return "raw";
  case MIZU_TYPE_INT64: return "integer64";
  }
  return "?";
}

// Map handle -------------------------------------------------------------------

/* The header is validated exactly once per mapping — at open, or authored
   at stage — and cached process-local behind this external pointer, so the
   per-call primitives pay a tag check + bounds instead of a 128-byte
   memcpy + full re-validation (per element on the template write path).
   Strictly safer, too: the local copy is immune to concurrent scribbling
   over shm that per-call re-reads would re-trust. prot pins the region
   wrap, so the mapping outlives the handle. */
typedef struct mizu_map_h_s {
  mizu_shm *shm;
  mizu_morsel_hdr h;
  /* Batch sizing state (mizu_morsel_next), process-private and never wire
     state, reset at each run's first-call CLAIM CAS. A doorbell help
     that claims a queued runner of the *same* map through this ctx
     aliases it; the cost is a mis-sized batch or a re-ramp on resume —
     harmless. */
  mizu_morsel_sizer sizer;
} mizu_map_h;

static void map_h_finalizer(SEXP xp) {
  free(R_ExternalPtrAddr(xp));
  R_ClearExternalPtr(xp);
}

static SEXP map_h_make(mizu_shm *shm, const mizu_morsel_hdr *h, SEXP wrap) {
  mizu_map_h *mh = calloc(1, sizeof(*mh));
  if (mh == NULL) Rf_error("mizu: allocation failure");
  mh->shm = shm;
  mh->h = *h;
  mizu_morsel_sizer_init(&mh->sizer);
  SEXP xp = PROTECT(R_MakeExternalPtr(mh, mizu_map_tag, wrap));
  R_RegisterCFinalizerEx(xp, map_h_finalizer, TRUE);
  UNPROTECT(1);
  return xp;
}

static mizu_map_h *map_h_get(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_map_tag)
    Rf_error("mizu: not a map handle");
  mizu_map_h *mh = (mizu_map_h *) R_ExternalPtrAddr(xp);
  if (mh == NULL) Rf_error("mizu: map handle is closed");
  return mh;
}

static uint32_t map_ordinal(mizu_map_h *mh, SEXP r_sexp) {
  int r = Rf_asInteger(r_sexp);
  if (r < 0 || (uint32_t) r >= mh->h.claim_n)
    Rf_error("mizu: runner ordinal out of range");
  return (uint32_t) r;
}

/* Stage one map call into a fresh region. desc is the single descriptor
   stream's object; x the RAWVEC-section vector or NULL; desc_len the exact
   stream size when the R side already counted it (the region-less probe's
   bounded pass), or NULL to count here — so the descriptor costs one count
   pass and one write pass total, never two counts. The write re-verifies
   the count: mizu_view_serialize_into checks no bounds, and a mismatch here
   means heap corruption, not a recoverable condition. morsel_size fixes
   the region's morsel geometry (the R side derives it; prepared re-runs
   inherit it). Returns list(name, map handle pinning the producer wrap);
   the caller pins the handle for the map's duration. */
SEXP mizu_map_stage(SEXP desc, SEXP x, SEXP desc_len_sexp, SEXP n_sexp,
                   SEXP template_sexp, SEXP morsel_sexp) {
  double nd = Rf_asReal(n_sexp);
  if (!(nd >= 1) || nd > 9.007199254740992e15)
    Rf_error("mizu: invalid map length");
  uint64_t n = (uint64_t) nd;
  double msd = Rf_asReal(morsel_sexp);
  if (!(msd >= 1) || msd > nd)
    Rf_error("mizu: invalid map morsel size");
  uint64_t morsel_size = (uint64_t) msd;
  /* a RAWSXP descriptor is already framed bytes (the 'I' interchange form
     of a spec map's descriptor) — memcpy, no serialize pass */
  const int desc_raw = TYPEOF(desc) == RAWSXP;
  size_t desc_len = desc_raw ? (size_t) XLENGTH(desc) :
    desc_len_sexp == R_NilValue ?
    mizu_view_serialize_count(desc) : (size_t) Rf_asReal(desc_len_sexp);
  if (desc_len == 0)
    Rf_error("mizu: invalid map descriptor size");

  uint32_t x_type = 0, out_type = 0;
  uint64_t x_len = 0, out_m = 0;
  if (x != R_NilValue) {
    /* class-only integer64 rides the x section as bare int64 bytes — the
       wire tag carries the class; slices re-apply it (mizu_wire_alloc) */
    int wtype = mizu_view_is_int64(x) ? MIZU_TYPE_INT64 : (int) TYPEOF(x);
    size_t elt = mizu_view_sizeof_elt(wtype);
    if (elt == 0 || mizu_vec_ptr(x) == NULL ||
        (uint64_t) XLENGTH(x) != n)
      Rf_error("mizu: x is not eligible for the map raw section");
    x_type = (uint32_t) wtype;
    x_len = n * elt;
  }
  if (template_sexp != R_NilValue) {
    /* an integer64 template stamps the output area int64 — the wire tag
       carries the class; collect re-applies it (mizu_wire_alloc / the vec
       wrap) */
    int ot = TYPEOF(template_sexp) == REALSXP &&
      map_is_int64_classed(template_sexp) ?
      MIZU_TYPE_INT64 : (int) TYPEOF(template_sexp);
    out_m = (uint64_t) XLENGTH(template_sexp);
    if (mizu_view_sizeof_elt(ot) == 0 || out_m == 0)
      Rf_error("mizu: invalid map template");
    out_type = (uint32_t) ot;
  }

  mizu_morsel_hdr h;
  uint64_t size = mizu_morsel_layout(&h, n, morsel_size,
                                    desc_len, x_type, x_len, out_type, out_m,
                                    MIZU_MAX_WORKERS);
  /* the R-facing checks above pre-validate the geometry, so a layout
     refusal is always a size overflow (incl. n past the header's 2^48) */
  if (size == 0)
    Rf_error("mizu: map region too large");

  mizu_shm *shm;
  if (mizu_shm_create(&shm, (size_t) size) != MIZU_OK) {
    const char *summary, *hint;
    mizu_err_describe(mizu_last_error_category(), &summary, &hint);
    mizu_stop_shm((double) size,
                 "mizu: cannot create map region (%llu bytes): %s%s%s",
                 (unsigned long long) size, summary,
                 hint[0] != '\0' ? ". " : "", hint);
  }
  SEXP wrap = PROTECT(mizu_shm_wrap_producer(shm));
  unsigned char *b = (unsigned char *) shm->addr;
  memcpy(b, &h, sizeof(h));
  if (desc_raw) {
    if ((size_t) XLENGTH(desc) != desc_len)
      Rf_error("mizu: invalid map descriptor size");
    memcpy(b + h.desc_off, RAW(desc), desc_len);
  } else if (mizu_serialize_bounded(b + h.desc_off, desc_len, desc) !=
             desc_len) {
    Rf_error("mizu: map descriptor size changed between count and write");
  }
  if (x != R_NilValue)
    memcpy(b + h.x_off, mizu_vec_ptr(x), (size_t) h.x_len);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(out, 0, Rf_mkString(shm->name));
  SET_VECTOR_ELT(out, 1, map_h_make(shm, &h, wrap));
  UNPROTECT(2);
  return out;
}

// Worker-side context -----------------------------------------------------------

/* Attach a map region. Runners attach writable — every runner CASes the
   shared morsel state, not just the template path's output-area stores —
   and no-populate, so a large RAWVEC x still demand-pages per worker; the
   read-only consumer open remains for passive readers. Both failure modes
   raise ordinary R errors — they happen inside the task eval, so they
   publish as the runner's ERR result (or the CANCEL drop absorbs them on
   the timeout path, where the submitter unlinked the region under a
   straggler) — never the fatal infrastructure path. */
SEXP mizu_map_open(SEXP name_sexp, SEXP writable_sexp) {
  if (TYPEOF(name_sexp) != STRSXP || XLENGTH(name_sexp) != 1)
    Rf_error("mizu: expected a map region name");
  const char *name = CHAR(STRING_ELT(name_sexp, 0));
  mizu_shm *shm;
  mizu_status st = Rf_asLogical(writable_sexp) == TRUE ?
    mizu_shm_open_rw(&shm, name, 0) : mizu_shm_open(&shm, name);
  if (st != MIZU_OK)
    mizu_stop_shm(NA_REAL, "mizu: cannot open map region '%s' — its "
                 "submitter died or the map ended", name);
  mizu_morsel_hdr h;
  const char *err = mizu_morsel_hdr_check(shm->addr, shm->size, &h);
  if (err != NULL) {
    mizu_shm_close(shm, 0);
    Rf_error("mizu: invalid map region: %s", err);
  }
  SEXP wrap = PROTECT(mizu_shm_wrap_consumer(shm));
  SEXP out = map_h_make(shm, &h, wrap);
  UNPROTECT(1);
  return out;
}

SEXP mizu_map_desc(SEXP xp) {
  mizu_map_h *mh = map_h_get(xp);
  const unsigned char *desc =
    (const unsigned char *) mh->shm->addr + mh->h.desc_off;
  /* the descriptor's codec identity rides its first byte: the 'I'
     interchange form (a spec map) or this binding's private stream — the
     one branch the Phase 5 reader dispatch needs */
  if (mh->h.desc_len >= 1 && desc[0] == MIZU_INTEROP_MAGIC)
    return mizu_interop_read_map_desc(desc, (size_t) mh->h.desc_len,
                                      R_GlobalEnv);
  return mizu_view_unserialize_from(desc, (size_t) mh->h.desc_len);
}

/* The template-path flag off the cached header (a spec map's runner reads
   it here — the kind-2 stream carries no per-runner flag). */
SEXP mizu_map_is_template(SEXP xp) {
  return Rf_ScalarLogical(map_h_get(xp)->h.out_type != 0);
}

/* RAWVEC x slice [lo, hi]: one allocVector + memcpy straight from the
   mapping — per chunk, not per map, so a worker never holds more than a
   chunk of a huge x. */
static SEXP map_slice_copy(mizu_map_h *mh, uint64_t lo, uint64_t hi) {
  size_t elt = mizu_view_sizeof_elt((int) mh->h.x_type);
  R_xlen_t len = (R_xlen_t) (hi - lo + 1);
  SEXP out = mizu_wire_alloc((int) mh->h.x_type, len);
  memcpy(mizu_vec_ptr(out),
         (unsigned char *) mh->shm->addr + mh->h.x_off +
         (size_t) (lo - 1) * elt,
         (size_t) len * elt);
  return out;
}

SEXP mizu_map_slice(SEXP xp, SEXP lo_sexp, SEXP hi_sexp) {
  mizu_map_h *mh = map_h_get(xp);
  if (mh->h.x_kind != MIZU_MORSEL_X_RAW)
    Rf_error("mizu: map region has no x section");
  double lo = Rf_asReal(lo_sexp), hi = Rf_asReal(hi_sexp);
  if (!(lo >= 1) || !(hi >= lo) || hi > (double) mh->h.n)
    Rf_error("mizu: map slice out of range");
  return map_slice_copy(mh, (uint64_t) lo, (uint64_t) hi);
}

/* Template-path write of element e's value at its disjoint output-area
   offset. "Like vapply" means exactly vapply, coercions included: exact
   type memcpys, an upward coercion (logical -> integer -> double ->
   complex) goes through R's own coerceVector so NA semantics match.
   Shared by mizu_map_write and the batch loop; protects value itself, so
   an unprotected Rf_eval result can ride in. */
static void map_write_value(mizu_map_h *mh, double e, SEXP value) {
  if (!(e >= 1) || e > (double) mh->h.n)
    Rf_error("mizu: map element index out of range");
  int vt = TYPEOF(value), ot = (int) mh->h.out_type;
  /* int64 joins no coercion lattice: the value must be integer64 already,
     and the write stays a memcpy (a coerceVector to tag 32 is not a
     thing) */
  int widens = ot == MIZU_TYPE_INT64 ?
    (vt == REALSXP && map_is_int64_classed(value)) :
    vt == ot ||
    (ot == INTSXP  && vt == LGLSXP) ||
    (ot == REALSXP && (vt == LGLSXP || vt == INTSXP)) ||
    (ot == CPLXSXP && (vt == LGLSXP || vt == INTSXP || vt == REALSXP));
  if (!widens || Rf_xlength(value) != (R_xlen_t) mh->h.out_m)
    Rf_error("mizu: map values must be type '%s' and length %llu",
             map_type_name(ot), (unsigned long long) mh->h.out_m);
  unsigned char *dst = (unsigned char *) mh->shm->addr + mh->h.out_off +
    (size_t) (e - 1) * (mh->h.out_m * mh->h.out_elt);
  size_t nbytes = (size_t) (mh->h.out_m * mh->h.out_elt);
  if (vt != ot && ot != MIZU_TYPE_INT64) {
    PROTECT(value);
    value = PROTECT(Rf_coerceVector(value, (SEXPTYPE) ot));
    memcpy(dst, mizu_vec_ptr(value), nbytes);
    UNPROTECT(2);
  } else {
    memcpy(dst, mizu_vec_ptr(value), nbytes);
  }
}

SEXP mizu_map_write(SEXP xp, SEXP e_sexp, SEXP value) {
  mizu_map_h *mh = map_h_get(xp);
  if (mh->h.out_type == 0)
    Rf_error("mizu: map region has no output area");
  map_write_value(mh, Rf_asReal(e_sexp), value);
  return R_NilValue;
}

/* One batch's whole element loop, in C — what lapply does: the call
   f(elt, ...) is built once with the constant dots spliced in (names
   preserved), and only the element cell's CAR is swapped per iteration
   under Rf_eval in the caller's frame (rho), matching do.call's
   parent.frame() semantics. Absorbs the two other per-element .Calls:
   template writes go straight to the output area (xp NULL — the blob
   path — is always generic), and the seeded path installs and jumps the
   CMRG state inline, exactly mizu_map_rng_install's per-element step.
   ei_sexp is an R-allocated REALSXP(1) cell the loop stamps with the
   in-flight element index before each eval, so the R side's one
   tryCatch per batch annotates an escaping error with the failing
   element — the "first by element index" contract. x is the batch's
   source vector (the RAWVEC slice, or the descriptor x) and base its
   0-based offset of element lo. Returns the batch's value list, or NULL
   on the template path. */
SEXP mizu_map_batch(SEXP xp, SEXP f, SEXP dots, SEXP x, SEXP base_sexp,
                   SEXP lo_sexp, SEXP hi_sexp, SEXP sr, SEXP ei_sexp,
                   SEXP rho) {
  mizu_map_h *mh = xp == R_NilValue ? NULL : map_h_get(xp);
  int tmpl = mh != NULL && mh->h.out_type != 0;
  double lo = Rf_asReal(lo_sexp), hi = Rf_asReal(hi_sexp);
  double base = Rf_asReal(base_sexp);
  if (!(lo >= 1) || !(hi >= lo) || !(base >= 0) ||
      base + (hi - lo + 1) > (double) XLENGTH(x) ||
      (tmpl && hi > (double) mh->h.n))
    Rf_error("mizu: invalid map batch range");
  if (dots != R_NilValue && TYPEOF(dots) != VECSXP)
    Rf_error("mizu: invalid map dots");
  if (TYPEOF(ei_sexp) != REALSXP || XLENGTH(ei_sexp) != 1)
    Rf_error("mizu: invalid element-index cell");
  if (TYPEOF(rho) != ENVSXP)
    Rf_error("mizu: invalid evaluation environment");
  double *ei = REAL(ei_sexp);
  R_xlen_t len = (R_xlen_t) (hi - lo + 1);
  R_xlen_t base0 = (R_xlen_t) base;

  int state[6];
  int seeded = sr != R_NilValue;
  if (seeded) {
    if (TYPEOF(sr) != INTSXP || XLENGTH(sr) != 6)
      Rf_error("mizu: invalid RNG stream state");
    memcpy(state, INTEGER(sr), 6 * sizeof(int));
  }

  R_xlen_t ndots = dots == R_NilValue ? 0 : XLENGTH(dots);
  SEXP args = PROTECT(Rf_allocList((int) (1 + ndots)));
  SEXP elt_cell = args;
  SEXP tail = elt_cell;
  SEXP dnames = ndots > 0 ? Rf_getAttrib(dots, R_NamesSymbol) : R_NilValue;
  for (R_xlen_t j = 0; j < ndots; j++) {
    tail = CDR(tail);
    SETCAR(tail, VECTOR_ELT(dots, j));
    if (dnames != R_NilValue) {
      SEXP nm = STRING_ELT(dnames, j);
      if (nm != NA_STRING && CHAR(nm)[0] != '\0')
        SET_TAG(tail, Rf_install(CHAR(nm)));
    }
  }
  SEXP call = PROTECT(Rf_lcons(f, args));

  SEXP out = R_NilValue;
  int np = 2;
  if (!tmpl) {
    out = PROTECT(Rf_allocVector(VECSXP, len));
    np++;
  }
  const int xt = TYPEOF(x);
  /* An int64 chunk (raw-section slice, codec blob slice, or resolved int64
     view) hands f classed integer64 scalars — a bare double element would
     reinterpret the bit pattern, not convert it */
  const int xi64 = xt == REALSXP && map_is_int64_classed(x);
  /* Atomic element reads go through one hoisted data pointer: for a
     non-ALTREP x its own block; for an ALTREP with data behind it (a
     mizu view, a foreign shared vector, a materialized one) the shared
     pages — the per-element writable accessors would COW-materialize the
     whole vector per worker. NULL when an ALTREP has no data block (a
     compact 1:n), where the standard accessors stand. */
  const void *xd = NULL;
  if (mizu_view_sizeof_elt(xt) != 0)
    xd = ALTREP(x) ? DATAPTR_OR_NULL(x) : mizu_vec_ptr(x);
  for (R_xlen_t i = 0; i < len; i++) {
    if ((i & 63) == 0) R_CheckUserInterrupt();
    double e = lo + (double) i;
    *ei = e;
    if (seeded) {
      SEXP seedv = PROTECT(Rf_allocVector(INTSXP, 7));
      INTEGER(seedv)[0] = 10407;
      memcpy(INTEGER(seedv) + 1, state, 6 * sizeof(int));
      Rf_defineVar(mizu_rs_sym, seedv, R_GlobalEnv);
      UNPROTECT(1);
      mizu_rng_jump(state);
    }
    R_xlen_t idx = base0 + i;
    SEXP elt;
    switch (xt) {
    case VECSXP: case EXPRSXP:
      elt = VECTOR_ELT(x, idx);
      break;
    case LGLSXP:
      elt = Rf_ScalarLogical(xd == NULL ? LOGICAL(x)[idx]
                                        : ((const int *) xd)[idx]);
      break;
    case INTSXP:
      elt = Rf_ScalarInteger(xd == NULL ? INTEGER(x)[idx]
                                        : ((const int *) xd)[idx]);
      break;
    case REALSXP:
      elt = Rf_ScalarReal(xd == NULL ? REAL(x)[idx]
                                     : ((const double *) xd)[idx]);
      break;
    case CPLXSXP:
      elt = Rf_ScalarComplex(xd == NULL ? COMPLEX(x)[idx]
                                        : ((const Rcomplex *) xd)[idx]);
      break;
    case RAWSXP:
      elt = Rf_ScalarRaw(xd == NULL ? RAW(x)[idx]
                                    : ((const Rbyte *) xd)[idx]);
      break;
    case STRSXP:  elt = Rf_ScalarString(STRING_ELT(x, idx)); break;
    default:      Rf_error("mizu: unsupported map element type");
    }
    SETCAR(elt_cell, elt);
    if (xi64) {
      /* anchored by the SETCAR above: classgets allocates */
      Rf_classgets(elt, mizu_view_int64_class);
    }
    SEXP v = PROTECT(Rf_eval(call, rho));
    if (tmpl) map_write_value(mh, e, v);
    else SET_VECTOR_ELT(out, i, v);
    UNPROTECT(1);
  }
  UNPROTECT(np);
  return out;
}

/* Submitter-side assembly: n × m results move cross-process exactly once,
   unserialized — one mizu_wire_alloc + one memcpy (an int64 area arrives
   classed). Names and dim are the R side's. */
SEXP mizu_map_gather(SEXP xp) {
  mizu_map_h *mh = map_h_get(xp);
  if (mh->h.out_type == 0)
    Rf_error("mizu: map region has no output area");
  R_xlen_t len = (R_xlen_t) (mh->h.n * mh->h.out_m);
  SEXP out = mizu_wire_alloc((int) mh->h.out_type, len);
  memcpy(mizu_vec_ptr(out), (unsigned char *) mh->shm->addr + mh->h.out_off,
         (size_t) len * mh->h.out_elt);
  return out;
}

/* `.collect = "view"`: the output area wrapped as an ALTREP view over the
   map region's own pages — no gather memcpy. Names / dim / dimnames are
   applied here, not R-side: the R setters see a shared object and
   duplicate, and the default ALTREP duplicate materializes (the vec
   classes register no Duplicate method). The keeper is the map handle
   itself: its prot chain pins the producer wrap, so the region lives
   until the view is released (the R side marks the map state consumed, so
   a prepared re-run restages instead of overwriting the pages). With no
   mori-shm hop in that chain the view never crosses by reference — a
   re-send degrades to a materializing copy, as the region is not an MIZH
   layout and REF resolution would misread it. */
SEXP mizu_map_gather_view(SEXP xp, SEXP nms, SEXP tn) {
  mizu_map_h *mh = map_h_get(xp);
  if (mh->h.out_type == 0)
    Rf_error("mizu: map region has no output area");
  uint64_t n = mh->h.n, m = mh->h.out_m;
  SEXP view = PROTECT(mizu_view_vec_wrap(
    (unsigned char *) mh->shm->addr + mh->h.out_off, (R_xlen_t) (n * m),
    (int) mh->h.out_type, xp, NULL, NULL));
  if (m == 1) {
    if (nms != R_NilValue) Rf_setAttrib(view, R_NamesSymbol, nms);
    UNPROTECT(1);
    return view;
  }
  SEXP dim;
  if (n * m > (uint64_t) INT_MAX) {   /* a long vector: double dim */
    dim = PROTECT(Rf_allocVector(REALSXP, 2));
    REAL(dim)[0] = (double) m;
    REAL(dim)[1] = (double) n;
  } else {
    dim = PROTECT(Rf_allocVector(INTSXP, 2));
    INTEGER(dim)[0] = (int) m;
    INTEGER(dim)[1] = (int) n;
  }
  Rf_setAttrib(view, R_DimSymbol, dim);
  if (tn != R_NilValue || nms != R_NilValue) {
    SEXP dn = PROTECT(Rf_allocVector(VECSXP, 2));
    SET_VECTOR_ELT(dn, 0, tn);
    SET_VECTOR_ELT(dn, 1, nms);
    Rf_setAttrib(view, R_DimNamesSymbol, dn);
    UNPROTECT(1);
  }
  UNPROTECT(2);
  return view;
}

/* Generic-path assembly: one pass splices every collected runner
   result's batch value lists into out by element position — the R side
   per batch allocated a seq.int index as long as the batch and paid the
   subassignment dispatch; here the batch ranges recompute from the
   morsel geometry exactly as mizu_map_next issued them (the final grant
   clamps to n) and only the pointer copies remain. A range or length
   mismatch against a published batch means protocol corruption, not a
   recoverable condition. */
SEXP mizu_map_splice(SEXP out, SEXP results, SEXP ms_sexp) {
  if (TYPEOF(out) != VECSXP || TYPEOF(results) != VECSXP)
    Rf_error("mizu: invalid map splice arguments");
  double ms = Rf_asReal(ms_sexp);
  if (!(ms >= 1))
    Rf_error("mizu: invalid morsel size");
  R_xlen_t n = XLENGTH(out);
  for (R_xlen_t r = 0; r < XLENGTH(results); r++) {
    SEXP res = VECTOR_ELT(results, r);
    if (TYPEOF(res) != VECSXP || XLENGTH(res) < 3)
      Rf_error("mizu: invalid map runner result");
    SEXP hm = VECTOR_ELT(res, 0), hk = VECTOR_ELT(res, 1),
         vals = VECTOR_ELT(res, 2);
    if (TYPEOF(hm) != REALSXP || TYPEOF(hk) != REALSXP ||
        TYPEOF(vals) != VECSXP || XLENGTH(hk) != XLENGTH(hm) ||
        XLENGTH(vals) != XLENGTH(hm))
      Rf_error("mizu: invalid map runner result");
    const double *m = REAL(hm), *k = REAL(hk);
    for (R_xlen_t b = 0; b < XLENGTH(hm); b++) {
      if (!(m[b] >= 0) || !(k[b] >= 1))
        Rf_error("mizu: invalid map batch range");
      double lo_d = m[b] * ms + 1, hi_d = (m[b] + k[b]) * ms;
      if (hi_d > (double) n) hi_d = (double) n;
      if (!(lo_d >= 1) || lo_d > (double) n || hi_d < lo_d)
        Rf_error("mizu: invalid map batch range");
      R_xlen_t lo = (R_xlen_t) lo_d, hi = (R_xlen_t) hi_d;
      SEXP bv = VECTOR_ELT(vals, b);
      if (TYPEOF(bv) != VECSXP || XLENGTH(bv) != hi - lo + 1)
        Rf_error("mizu: map batch result length mismatch");
      for (R_xlen_t i = 0; i <= hi - lo; i++)
        SET_VECTOR_ELT(out, lo - 1 + i, VECTOR_ELT(bv, i));
    }
  }
  return R_NilValue;
}

/* Worker-death lost set: issued = [0, cursor), lost = issued minus the
   union of the collected batch histories — a batch in no history was
   issued but never completed (its claimant died, or f errored
   mid-batch); a dead runner's whole history lands here too — it
   publishes only at exhaustion — and durably written template elements
   report conservatively as lost, never wrong. runs is the R side's list
   of runner results and error-carried history pairs; only the first two
   elements of each (batch starts, batch sizes, in morsels) are read.
   Returns the lost element ranges as a two-column double matrix of
   inclusive 1-based [lo, hi]. */
SEXP mizu_map_lost(SEXP xp, SEXP runs) {
  mizu_map_h *mh = map_h_get(xp);
  const uint64_t msz = mh->h.morsel_size, n = mh->h.n;
  /* the issued bound, in elements */
  uint64_t bound = mizu_morsel_cursor(mh->shm->addr, &mh->h) * msz;
  if (bound > n) bound = n;
  if (TYPEOF(runs) != VECSXP)
    Rf_error("mizu: invalid map batch history");
  R_xlen_t nh = XLENGTH(runs), total = 0;
  for (R_xlen_t i = 0; i < nh; i++) {
    SEXP pr = VECTOR_ELT(runs, i);
    if (TYPEOF(pr) != VECSXP || XLENGTH(pr) < 2 ||
        TYPEOF(VECTOR_ELT(pr, 0)) != REALSXP ||
        TYPEOF(VECTOR_ELT(pr, 1)) != REALSXP ||
        XLENGTH(VECTOR_ELT(pr, 0)) != XLENGTH(VECTOR_ELT(pr, 1)))
      Rf_error("mizu: invalid map batch history");
    total += XLENGTH(VECTOR_ELT(pr, 0));
  }
  /* marshal the (morsel start, morsel count) histories into element spans */
  mizu_morsel_span *b =
    (mizu_morsel_span *) R_alloc((size_t) (total > 0 ? total : 1),
                                 sizeof(*b));
  R_xlen_t at = 0;
  for (R_xlen_t i = 0; i < nh; i++) {
    SEXP pr = VECTOR_ELT(runs, i);
    SEXP hm = VECTOR_ELT(pr, 0), hk = VECTOR_ELT(pr, 1);
    const double *m = REAL(hm), *k = REAL(hk);
    for (R_xlen_t j = 0; j < XLENGTH(hm); j++, at++) {
      if (!(m[j] >= 0) || !(k[j] >= 1) ||
          m[j] > (double) mh->h.n_morsels ||
          k[j] > (double) mh->h.n_morsels)
        Rf_error("mizu: invalid map batch history");
      uint64_t lo = (uint64_t) m[j] * msz;
      uint64_t hi = ((uint64_t) m[j] + (uint64_t) k[j]) * msz;
      b[at].lo = lo > n ? n : lo;
      b[at].hi = hi > n ? n : hi;
    }
  }
  mizu_morsel_span *gaps =
    (mizu_morsel_span *) R_alloc((size_t) total + 1, sizeof(*gaps));
  size_t ngap = mizu_morsel_lost(b, (size_t) total, bound, gaps);
  SEXP out = PROTECT(Rf_allocMatrix(REALSXP, (R_xlen_t) ngap, 2));
  double *lo = REAL(out), *hi = lo + ngap;
  for (size_t g = 0; g < ngap; g++) {
    lo[g] = (double) gaps[g].lo + 1;   /* 0-based half-open -> 1-based incl. */
    hi[g] = (double) gaps[g].hi;
  }
  SEXP cn = PROTECT(Rf_allocVector(STRSXP, 2));
  SET_STRING_ELT(cn, 0, Rf_mkChar("lo"));
  SET_STRING_ELT(cn, 1, Rf_mkChar("hi"));
  SEXP dn = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(dn, 1, cn);
  Rf_setAttrib(out, R_DimNamesSymbol, dn);
  UNPROTECT(3);
  return out;
}

// Morsel protocol ---------------------------------------------------------------

/* One whole batch transition — generation-fenced lane claim, cancel and
   pool-signal checks, sized cursor issue, x slice — in a single .Call
   against the cached-header handle. NULL means stop: the lane was lost to
   the trim or a reset re-armed it (before any issue), the cancel word
   fired, the pool is stopping, the owner died, or the cursor is
   exhausted. Otherwise list(m, k, lo, hi, x slice | NULL, help flag): m
   the 0-based first morsel of the batch, k its morsel count after the
   final partial grant, [lo, hi] its 1-based element range.

   sig is the opaque address trio from mizu_pool_signals (NULL skips the
   loads — the in-process protocol tests). pin bypasses the sizing policy
   with a fixed k; now overrides the mizu_now() read — both test entries,
   NULL in production. */
SEXP mizu_map_next(SEXP xp, SEXP r_sexp, SEXP gen_sexp, SEXP sig,
                  SEXP pin_sexp, SEXP now_sexp) {
  mizu_map_h *mh = map_h_get(xp);
  uint32_t r = map_ordinal(mh, r_sexp);
  uint32_t gen = (uint32_t) Rf_asReal(gen_sexp);
  mizu_pool_sig *s = sig == R_NilValue ? NULL : mizu_pool_sig_get(sig);
  uint64_t pin_k = 0;
  if (pin_sexp != R_NilValue) {
    double pk = Rf_asReal(pin_sexp);
    if (!(pk >= 1)) Rf_error("mizu: invalid pinned batch size");
    pin_k = (uint64_t) pk;
  }
  double now = now_sexp == R_NilValue ? mizu_now() : Rf_asReal(now_sexp);

  /* the core's batch transition: generation-fenced lane claim, cancel and
     pool-signal checks, AIMD sizing, cursor issue */
  uint64_t m, k;
  int help;
  if (!mizu_morsel_next(mh->shm->addr, &mh->h, &mh->sizer, r, gen, s, pin_k,
                       now, &m, &k, &help))
    return R_NilValue;

  uint64_t lo, hi;
  mizu_morsel_span_of(&mh->h, m, k, &lo, &hi);
  lo++;   /* 0-based half-open to 1-based inclusive */
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 6));
  SET_VECTOR_ELT(out, 0, Rf_ScalarReal((double) m));
  SET_VECTOR_ELT(out, 1, Rf_ScalarReal((double) k));
  SET_VECTOR_ELT(out, 2, Rf_ScalarReal((double) lo));
  SET_VECTOR_ELT(out, 3, Rf_ScalarReal((double) hi));
  if (mh->h.x_kind == MIZU_MORSEL_X_RAW)
    SET_VECTOR_ELT(out, 4, map_slice_copy(mh, lo, hi));
  SET_VECTOR_ELT(out, 5, Rf_ScalarLogical(help));
  UNPROTECT(1);
  return out;
}

/* The exhausted-runner trim's CAS, folding its own trigger: a no-op
   ("idle" refusal) unless the cursor is exhausted or the cancel word is
   set. A won IDLE -> ABANDONED CAS at the current generation proves that
   runner never started and never will do work — mizu_pool_cancel alone
   cannot carry the trim, being advisory and discard-only while the
   trigger condition is the routine end state of every map. Returns the
   verdict as the morsel-state code itself: MIZU_MORSEL_ABANDONED (won,
   or already trimmed), MIZU_MORSEL_RUNNING (the runner is executing or
   already published — collect it), MIZU_MORSEL_IDLE (trigger unarmed:
   collect defers this handle rather than parking on it). */
SEXP mizu_map_abandon(SEXP xp, SEXP r_sexp) {
  mizu_map_h *mh = map_h_get(xp);
  uint32_t r = map_ordinal(mh, r_sexp);
  uint32_t gen = mizu_morsel_generation(mh->shm->addr, &mh->h);
  return Rf_ScalarInteger(
    mizu_morsel_abandon(mh->shm->addr, &mh->h, r, gen));
}

/* The cancel word: set by the submitter on timeout / cancel / death, and
   by an erroring runner itself before its ERR publish — the fail-fast
   store that stops every peer within ~a batch. Idempotent, and total —
   it runs from unwind paths (map_cancel under on.exit, a runner's error
   handler), so a closed or foreign handle no-ops. */
SEXP mizu_map_cancel_set(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_map_tag)
    return R_NilValue;
  mizu_map_h *mh = (mizu_map_h *) R_ExternalPtrAddr(xp);
  if (mh == NULL) return R_NilValue;
  mizu_morsel_cancel_set(mh->shm->addr, &mh->h);
  return R_NilValue;
}

SEXP mizu_map_cancel_get(SEXP xp) {
  mizu_map_h *mh = map_h_get(xp);
  return Rf_ScalarLogical(mizu_morsel_cancel_get(mh->shm->addr, &mh->h));
}

/* Prepared-run re-arm, O(1) in n (no per-morsel state exists to clear):
   bump the generation, stamp (new_gen << 2) | IDLE over the CLAIM array,
   zero the cursor, clear the cancel word. The stamped generation is the
   fence against a stale trimmed runner from the prior run: its
   first-call CAS expects the old generation and fails against the
   re-armed word however the reset interleaves. Returns the new
   generation — the value the next run's payloads must carry. */
SEXP mizu_map_reset(SEXP xp) {
  mizu_map_h *mh = map_h_get(xp);
  return Rf_ScalarReal((double) mizu_morsel_reset(mh->shm->addr, &mh->h));
}

/* Geometry and state snapshot: the stage-time constants plus single reads
   of the mutable words. The submit-time generation read and the death
   path's lost-set bound (the cursor, clamped to n_morsels) both ride
   here. */
SEXP mizu_map_info(SEXP xp) {
  mizu_map_h *mh = map_h_get(xp);
  const char *names[] = {"n", "morsel_size", "n_morsels", "claim_n",
                         "generation", "cursor", "cancel", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_ScalarReal((double) mh->h.n));
  SET_VECTOR_ELT(out, 1, Rf_ScalarReal((double) mh->h.morsel_size));
  SET_VECTOR_ELT(out, 2, Rf_ScalarReal((double) mh->h.n_morsels));
  SET_VECTOR_ELT(out, 3, Rf_ScalarInteger((int) mh->h.claim_n));
  SET_VECTOR_ELT(out, 4, Rf_ScalarReal((double)
    mizu_morsel_generation(mh->shm->addr, &mh->h)));
  SET_VECTOR_ELT(out, 5, Rf_ScalarReal((double)
    mizu_morsel_cursor(mh->shm->addr, &mh->h)));
  SET_VECTOR_ELT(out, 6, Rf_ScalarLogical(
    mizu_morsel_cancel_get(mh->shm->addr, &mh->h)));
  UNPROTECT(1);
  return out;
}

/* Prepared-map in-place x swap: memcpy a RAWVEC-eligible replacement of
   identical type and length over the region's x section (the producer
   mapping is writable). Safe because a RAWVEC x is sliced from the
   mapping per batch and never cached worker-side. Errors on any
   mismatch — the R side restages instead of swapping. */
SEXP mizu_map_swap_x(SEXP xp, SEXP x) {
  mizu_map_h *mh = map_h_get(xp);
  if (mh->h.x_kind != MIZU_MORSEL_X_RAW)
    Rf_error("mizu: map region has no x section");
  /* the staged code is a wire type: class-only integer64 compares as
     MIZU_TYPE_INT64, not REALSXP */
  uint32_t wtype =
    (uint32_t) (mizu_view_is_int64(x) ? MIZU_TYPE_INT64 : (int) TYPEOF(x));
  if (wtype != mh->h.x_type ||
      (uint64_t) XLENGTH(x) != mh->h.n ||
      ALTREP(x) || Rf_isS4(x) || mizu_vec_ptr(x) == NULL)
    Rf_error("mizu: replacement x must match the staged type and length");
  memcpy((unsigned char *) mh->shm->addr + mh->h.x_off, mizu_vec_ptr(x),
         (size_t) mh->h.x_len);
  return R_NilValue;
}

/* One CLAIM word decoded — the protocol tests' view of the handshake. */
SEXP mizu_map_claim_state(SEXP xp, SEXP r_sexp) {
  mizu_map_h *mh = map_h_get(xp);
  uint32_t r = map_ordinal(mh, r_sexp);
  uint32_t w = mizu_morsel_claim(mh->shm->addr, &mh->h, r);
  const char *names[] = {"state", "generation", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(
    (w & 3u) == MIZU_MORSEL_IDLE ? "idle" :
    (w & 3u) == MIZU_MORSEL_RUNNING ? "running" : "abandoned"));
  SET_VECTOR_ELT(out, 1, Rf_ScalarReal((double) (w >> 2)));
  UNPROTECT(1);
  return out;
}

/* The shared timeout sentinel, for mizu_map's own deadline returns —
   `.timeout` follows the sentinel discipline (returned, never raised). */
SEXP mizu_map_timeout_call(void) {
  return mizu_sent_timeout;
}

/* ANY_ATTRIB(), the closure accessors and R_mkClosure() joined the C API
   in R 4.5.0; backports for earlier R per Writing R Extensions, "Moving
   into C API compliance" (the closure constructor is the allocSExp +
   setters idiom — no mkClosure entry point existed before 4.5.0). */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif
#if R_VERSION < R_Version(4, 5, 0)
#define R_ClosureFormals(x) FORMALS(x)
#define R_ClosureBody(x)    BODY(x)
#define R_ClosureEnv(x)     CLOENV(x)

SEXP R_mkClosure(SEXP formals, SEXP body, SEXP env) {
  SEXP fun = Rf_allocSExp(CLOSXP);
  SET_FORMALS(fun, formals);
  SET_BODY(fun, body);
  SET_CLOENV(fun, env);
  return fun;
}
#endif

/* In-place strip of a private tree: the three source-reference attributes
   on every call / pairlist / expression node, plus the srcref object the
   parser stores as the fourth element of a `function` call literal — the
   nested-closure case, truncated rather than copied out. Backbone cells
   are walked iteratively; only child recursion grows the stack, so depth
   stays the expression nesting the parser already bounded. */
static void mizu_strip_walk(SEXP x) {
  switch (TYPEOF(x)) {
  case LANGSXP:
  case LISTSXP:
    if (ANY_ATTRIB(x)) {
      Rf_setAttrib(x, mizu_srcref_sym, R_NilValue);
      Rf_setAttrib(x, mizu_srcfile_sym, R_NilValue);
      Rf_setAttrib(x, mizu_wholesrcref_sym, R_NilValue);
    }
    if (CAR(x) == mizu_function_sym && CDDR(x) != R_NilValue) {
      SETCDR(CDDR(x), R_NilValue);
    }
    mizu_strip_walk(CAR(x));
    for (SEXP node = CDR(x); node != R_NilValue; node = CDR(node)) {
      mizu_strip_walk(CAR(node));
    }
    break;
  case EXPRSXP:
    if (ANY_ATTRIB(x)) {
      Rf_setAttrib(x, mizu_srcref_sym, R_NilValue);
      Rf_setAttrib(x, mizu_srcfile_sym, R_NilValue);
      Rf_setAttrib(x, mizu_wholesrcref_sym, R_NilValue);
    }
    for (R_xlen_t i = 0; i < XLENGTH(x); i++) {
      mizu_strip_walk(VECTOR_ELT(x, i));
    }
    break;
  default:
    break;
  }
}

/* removeSource for the staging path: deep-duplicate formals (and a
   language body — pairlist/language duplicates are deep), strip the
   private trees in place, and rebuild the closure with R_mkClosure (the
   slots have no public setters), cloning the original's attributes and
   zapping srcref. Constants, symbols and bytecode bodies are shared
   untouched. Non-closures pass through. */
SEXP mizu_strip_srcref(SEXP f) {
  if (TYPEOF(f) != CLOSXP) {
    return f;
  }
  SEXP formals = PROTECT(Rf_duplicate(R_ClosureFormals(f)));
  mizu_strip_walk(formals);
  SEXP body = R_ClosureBody(f);
  if (TYPEOF(body) == LANGSXP || TYPEOF(body) == LISTSXP ||
      TYPEOF(body) == EXPRSXP) {
    body = PROTECT(Rf_duplicate(body));
    mizu_strip_walk(body);
  } else {
    PROTECT(body);
  }
  SEXP out = PROTECT(R_mkClosure(formals, body, R_ClosureEnv(f)));
  DUPLICATE_ATTRIB(out, f);
  Rf_setAttrib(out, mizu_srcref_sym, R_NilValue);
  UNPROTECT(3);
  return out;
}

/* removeSource for a language tree (a task expression): deep-duplicate —
   pairlist/language duplicates are deep — and strip the private copy in
   place. LANGSXP/LISTSXP only: an EXPRSXP duplicate is shallow and the
   walk would strip shared elements in place. */
SEXP mizu_strip_lang(SEXP x) {
  SEXP out = PROTECT(Rf_duplicate(x));
  mizu_strip_walk(out);
  UNPROTECT(1);
  return out;
}
