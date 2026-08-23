/* sora_map staging and worker-side context — one fresh sora region per map
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
#include "sora.h"

#define REI_MAP_MAGIC 0x534F524Du   /* "SORM" */

static SEXP sora_map_tag;
static SEXP sora_rs_sym;
SEXP sora_srcref_sym;
static SEXP sora_srcfile_sym;
static SEXP sora_wholesrcref_sym;
static SEXP sora_function_sym;

void sora_map_init(void) {
  sora_map_tag = Rf_install("sora_map");
  sora_rs_sym = Rf_install(".Random.seed");
  sora_srcref_sym = Rf_install("srcref");
  sora_srcfile_sym = Rf_install("srcfile");
  sora_wholesrcref_sym = Rf_install("wholeSrcref");
  sora_function_sym = Rf_install("function");
}

enum { REI_MAP_X_DESC = 0, REI_MAP_X_RAWVEC };

/* Map-descriptor region header. Not pool wire format — it rides its own
   region, keyed by the same ABI version — but the same rules apply: the
   struct is the layout, 64-byte-aligned sections follow it. */
typedef struct sora_map_hdr_s {
  uint32_t magic;
  uint32_t version;
  uint32_t flags;            /* reserved, 0 */
  uint32_t x_kind;           /* REI_MAP_X_DESC / REI_MAP_X_RAWVEC */
  uint32_t x_sexptype;       /* RAWVEC section element type */
  uint32_t out_sexptype;     /* template element type; 0 = no output area */
  uint32_t out_elt_size;
  uint32_t pad0;
  uint64_t n;                /* map elements */
  uint64_t desc_off, desc_len;
  uint64_t x_off, x_len;
  uint64_t out_off;
  uint64_t out_m;            /* template length: values per element */
  uint64_t morsel_size;      /* elements per morsel */
  uint64_t n_morsels;        /* ceiling(n / morsel_size) */
  uint64_t state_off;        /* morsel state section offset */
  uint32_t claim_n;          /* CLAIM word count (runner ordinal bound) */
  uint8_t  pad[12];
} sora_map_hdr;

typedef char sora_map_hdr_assert[(sizeof(sora_map_hdr) == 128) ? 1 : -1];

/* Morsel state section: one cache line for the cancel word and run
   generation counter (read-mostly), one for the shared cursor (the ticket
   dispenser, alone so runner RMW traffic never touches the cancel line),
   then the CLAIM array — one word per runner *ordinal*, packing
   (generation << 2) | state so the lane claim and the generation fence are
   one atomic: a check-then-CAS would leave a TOCTOU window against
   sora_map_reset's CLAIM re-arm. Generation comparisons mask to the word's
   30 bits (wrap takes 2^30 resets of one handle: harmless). Issue is a
   plain relaxed fetch_add — atomicity is all the shared state provides;
   ordering rides the task claim/publish chain. Completion is never
   recorded here: runners publish their batch histories through their
   ordinary results, and the lost set on death is arithmetic over them. */
#define REI_MAP_CANCEL_OFF ((uint64_t) 0)
#define REI_MAP_GEN_OFF    ((uint64_t) 4)
#define REI_MAP_CURSOR_OFF ((uint64_t) 64)
#define REI_MAP_CLAIM_OFF  ((uint64_t) 128)
#define REI_MAP_GEN_MASK   ((uint32_t) 0x3FFFFFFF)

enum { REI_MORSEL_IDLE = 0, REI_MORSEL_RUNNING, REI_MORSEL_ABANDONED };

/* Batch sizing policy constants (see sora_map_next): k targets a batch
   duration, growing at most 2x per step and shrinking immediately on
   overshoot, clamped to the cap — which bounds lost-set coarseness and
   the ramp worst case (a cost jump right after a ramp runs one cap-sized
   batch to completion). Frozen by the gate sweep (2026-08-04, M4 Pro,
   W = 4): T_target from {25, 50, 100, 200} us — trivial-f overhead falls
   monotonically with T (0.343 -> 0.296 us/elt generic, 0.482 -> 0.409
   seeded) while cancellation latency stays ~0.1 ms at every setting, so
   the largest candidate wins; the cap from {64, 256} — within noise on
   every overhead row, so the tighter ramp / lost-set bound wins. */
#define REI_MAP_T_TARGET  200e-6
#define REI_MAP_BATCH_CAP 64

/* The map-local RAWVEC gate, deliberately looser than sora_raw_eligible:
   no size cap, and attributes are the R side's to check (names-only is
   admissible there — names stay submitter-side for assembly and the chunk
   loop's [[ drops them anyway). ALTREP still disqualifies — a
   mori::share()d x must reduce to its identifier inside the descriptor
   stream via the hooks, not be copied wholesale into a second region — as
   does S4. Returns the section byte length, or -1 when x must ride the
   descriptor. */
SEXP sora_map_eligible(SEXP x) {
  switch (TYPEOF(x)) {
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
    break;
  default:
    return Rf_ScalarReal(-1);
  }
  if (ALTREP(x) || Rf_isS4(x)) return Rf_ScalarReal(-1);
  return Rf_ScalarReal((double) XLENGTH(x) *
                       (double) mori_sizeof_elt(TYPEOF(x)));
}

static const char *map_type_name(int type) {
  switch (type) {
  case LGLSXP:  return "logical";
  case INTSXP:  return "integer";
  case REALSXP: return "double";
  case CPLXSXP: return "complex";
  case RAWSXP:  return "raw";
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
typedef struct sora_map_h_s {
  rei_shm *shm;
  sora_map_hdr h;
  /* Batch sizing state (sora_map_next), process-private and never wire
     state, reset at each run's first-call CLAIM CAS. A doorbell help
     that claims a queued runner of the *same* map through this ctx
     aliases it; the cost is a mis-sized batch or a re-ramp on resume —
     harmless. */
  int32_t  run_r;            /* ordinal whose ramp this is (-1 = none) */
  uint32_t run_gen;
  uint64_t k;                /* current batch size, morsels */
  uint64_t k_last;           /* morsels issued last transition */
  double   t_last;           /* rei_now() at the last issue */
  double   cost;             /* est. seconds per morsel (0 = unknown) */
  int      skip;             /* last interval contained a help: no update */
} sora_map_h;

static void map_h_finalizer(SEXP xp) {
  free(R_ExternalPtrAddr(xp));
  R_ClearExternalPtr(xp);
}

static SEXP map_h_make(rei_shm *shm, const sora_map_hdr *h, SEXP wrap) {
  sora_map_h *mh = calloc(1, sizeof(*mh));
  if (mh == NULL) Rf_error("sora: allocation failure");
  mh->shm = shm;
  mh->h = *h;
  mh->run_r = -1;
  mh->k = 1;
  SEXP xp = PROTECT(R_MakeExternalPtr(mh, sora_map_tag, wrap));
  R_RegisterCFinalizerEx(xp, map_h_finalizer, TRUE);
  UNPROTECT(1);
  return xp;
}

static sora_map_h *map_h_get(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != sora_map_tag)
    Rf_error("sora: not a map handle");
  sora_map_h *mh = (sora_map_h *) R_ExternalPtrAddr(xp);
  if (mh == NULL) Rf_error("sora: map handle is closed");
  return mh;
}

static _Atomic uint32_t *map_cancel_word(sora_map_h *mh) {
  return (_Atomic uint32_t *)
    ((unsigned char *) mh->shm->addr + mh->h.state_off + REI_MAP_CANCEL_OFF);
}

static _Atomic uint32_t *map_gen_word(sora_map_h *mh) {
  return (_Atomic uint32_t *)
    ((unsigned char *) mh->shm->addr + mh->h.state_off + REI_MAP_GEN_OFF);
}

static _Atomic uint64_t *map_cursor_word(sora_map_h *mh) {
  return (_Atomic uint64_t *)
    ((unsigned char *) mh->shm->addr + mh->h.state_off + REI_MAP_CURSOR_OFF);
}

static _Atomic uint32_t *map_claim_word(sora_map_h *mh, uint32_t r) {
  return (_Atomic uint32_t *)
    ((unsigned char *) mh->shm->addr + mh->h.state_off + REI_MAP_CLAIM_OFF +
     (uint64_t) r * 4);
}

static uint32_t map_ordinal(sora_map_h *mh, SEXP r_sexp) {
  int r = Rf_asInteger(r_sexp);
  if (r < 0 || (uint32_t) r >= mh->h.claim_n)
    Rf_error("sora: runner ordinal out of range");
  return (uint32_t) r;
}

/* Stage one map call into a fresh region. desc is the single descriptor
   stream's object; x the RAWVEC-section vector or NULL; desc_len the exact
   stream size when the R side already counted it (the region-less probe's
   bounded pass), or NULL to count here — so the descriptor costs one count
   pass and one write pass total, never two counts. The write re-verifies
   the count: mori_serialize_into checks no bounds, and a mismatch here
   means heap corruption, not a recoverable condition. morsel_size fixes
   the region's morsel geometry (the R side derives it; prepared re-runs
   inherit it). Returns list(name, map handle pinning the producer wrap);
   the caller pins the handle for the map's duration. */
SEXP sora_map_stage(SEXP desc, SEXP x, SEXP desc_len_sexp, SEXP n_sexp,
                   SEXP template_sexp, SEXP morsel_sexp) {
  double nd = Rf_asReal(n_sexp);
  if (!(nd >= 1) || nd > 9.007199254740992e15)
    Rf_error("sora: invalid map length");
  uint64_t n = (uint64_t) nd;
  double msd = Rf_asReal(morsel_sexp);
  if (!(msd >= 1) || msd > nd)
    Rf_error("sora: invalid map morsel size");
  uint64_t morsel_size = (uint64_t) msd;
  size_t desc_len = desc_len_sexp == R_NilValue ?
    mori_serialize_count(desc) : (size_t) Rf_asReal(desc_len_sexp);
  if (desc_len == 0)
    Rf_error("sora: invalid map descriptor size");

  sora_map_hdr h = {
    .magic = REI_MAP_MAGIC,
    .version = REI_ABI_VERSION,
    .n = n,
    .desc_off = sizeof(sora_map_hdr),
    .desc_len = desc_len,
    .morsel_size = morsel_size,
    .n_morsels = (n + morsel_size - 1) / morsel_size,
    .claim_n = REI_MAX_WORKERS,
  };
  uint64_t off = REI_ALIGN64(sizeof(sora_map_hdr) + desc_len);
  if (x != R_NilValue) {
    size_t elt = mori_sizeof_elt(TYPEOF(x));
    if (elt == 0 || sora_vec_ptr(x) == NULL ||
        (uint64_t) XLENGTH(x) != n)
      Rf_error("sora: x is not eligible for the map raw section");
    h.x_kind = REI_MAP_X_RAWVEC;
    h.x_sexptype = (uint32_t) TYPEOF(x);
    h.x_off = off;
    h.x_len = n * elt;
    off = REI_ALIGN64(off + h.x_len);
  }
  /* morsel state between the descriptor / x sections and the output area;
     a fresh region is zero-filled, so cancel, generation, cursor and every
     CLAIM word ((0 << 2) | IDLE) start armed for generation 0 */
  h.state_off = off;
  off = REI_ALIGN64(off + REI_MAP_CLAIM_OFF + (uint64_t) h.claim_n * 4);
  if (template_sexp != R_NilValue) {
    size_t elt = mori_sizeof_elt(TYPEOF(template_sexp));
    uint64_t m = (uint64_t) XLENGTH(template_sexp);
    if (elt == 0 || m == 0)
      Rf_error("sora: invalid map template");
    if (n > (((uint64_t) 1 << 46) - off) / (m * elt))
      Rf_error("sora: map region too large");
    h.out_sexptype = (uint32_t) TYPEOF(template_sexp);
    h.out_elt_size = (uint32_t) elt;
    h.out_m = m;
    h.out_off = off;
    off += n * m * elt;
  }
  if (off > ((uint64_t) 1 << 46))
    Rf_error("sora: map region too large");

  rei_shm *shm;
  int rc = rei_shm_create_heap(&shm, (size_t) off);
  if (rc != REI_ERRCAT_NONE) {
    const char *summary, *hint;
    rei_err_describe(rc, &summary, &hint);
    sora_stop_shm((double) off,
                 "sora: cannot create map region (%llu bytes): %s%s%s",
                 (unsigned long long) off, summary,
                 hint[0] != '\0' ? ". " : "", hint);
  }
  SEXP wrap = PROTECT(sora_shm_wrap_producer(shm));
  unsigned char *b = (unsigned char *) shm->addr;
  memcpy(b, &h, sizeof(h));
  if (sora_serialize_bounded(b + h.desc_off, desc_len, desc) != desc_len)
    Rf_error("sora: map descriptor size changed between count and write");
  if (x != R_NilValue)
    memcpy(b + h.x_off, sora_vec_ptr(x), (size_t) h.x_len);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(out, 0, Rf_mkString(shm->name));
  SET_VECTOR_ELT(out, 1, map_h_make(shm, &h, wrap));
  UNPROTECT(2);
  return out;
}

// Worker-side context -----------------------------------------------------------

static const char *map_hdr_validate(const rei_shm *shm, sora_map_hdr *out) {
  if (shm->size < sizeof(sora_map_hdr))
    return "region is smaller than a map header";
  sora_map_hdr h;
  memcpy(&h, shm->addr, sizeof(h));
  if (h.magic != REI_MAP_MAGIC)
    return "bad magic: not a sora map region";
  if (h.version != REI_ABI_VERSION)
    return "ABI version mismatch: worker and submitter were built against "
           "different sora wire formats";
  if (h.n == 0 || h.n > ((uint64_t) 1 << 48))
    return "element count out of range";
  if (h.desc_off < sizeof(sora_map_hdr) || h.desc_off > shm->size ||
      h.desc_len == 0 || h.desc_len > shm->size - h.desc_off)
    return "descriptor lies outside the region";
  if (h.morsel_size == 0 ||
      h.n_morsels != (h.n + h.morsel_size - 1) / h.morsel_size)
    return "morsel geometry is inconsistent";
  if (h.claim_n == 0 || h.claim_n > (1u << 16) ||
      h.state_off < sizeof(sora_map_hdr) || (h.state_off & 63) != 0 ||
      h.state_off > shm->size ||
      REI_MAP_CLAIM_OFF + (uint64_t) h.claim_n * 4 > shm->size - h.state_off)
    return "morsel state section lies outside the region";
  if (h.x_kind == REI_MAP_X_RAWVEC) {
    size_t elt = mori_sizeof_elt((int) h.x_sexptype);
    if (elt == 0 || h.x_off > shm->size || h.x_len > shm->size - h.x_off ||
        h.x_len != h.n * elt)
      return "x section lies outside the region";
  } else if (h.x_kind != REI_MAP_X_DESC) {
    return "unknown x section kind";
  }
  if (h.out_sexptype != 0) {
    size_t elt = mori_sizeof_elt((int) h.out_sexptype);
    if (elt == 0 || elt != h.out_elt_size || h.out_m == 0 ||
        h.out_m > ((uint64_t) 1 << 32) || h.out_off > shm->size ||
        h.n > (shm->size - h.out_off) / (h.out_m * elt))
      return "output area lies outside the region";
  }
  if (out != NULL) *out = h;
  return NULL;
}

/* Attach a map region. Runners attach writable — every runner CASes the
   shared morsel state, not just the template path's output-area stores —
   and no-populate, so a large RAWVEC x still demand-pages per worker; the
   read-only consumer open remains for passive readers. Both failure modes
   raise ordinary R errors — they happen inside the task eval, so they
   publish as the runner's ERR result (or the CANCEL drop absorbs them on
   the timeout path, where the submitter unlinked the region under a
   straggler) — never the fatal infrastructure path. */
SEXP sora_map_open(SEXP name_sexp, SEXP writable_sexp) {
  if (TYPEOF(name_sexp) != STRSXP || XLENGTH(name_sexp) != 1)
    Rf_error("sora: expected a map region name");
  const char *name = CHAR(STRING_ELT(name_sexp, 0));
  rei_shm *shm = Rf_asLogical(writable_sexp) == TRUE ?
    rei_shm_open_rw_heap(name, 0) : rei_shm_open_heap(name);
  if (shm == NULL)
    sora_stop_shm(NA_REAL, "sora: cannot open map region '%s' — its "
                 "submitter died or the map ended", name);
  sora_map_hdr h;
  const char *err = map_hdr_validate(shm, &h);
  if (err != NULL) {
    rei_shm_close(shm, 0);
    Rf_error("sora: invalid map region: %s", err);
  }
  SEXP wrap = PROTECT(sora_shm_wrap_consumer(shm));
  SEXP out = map_h_make(shm, &h, wrap);
  UNPROTECT(1);
  return out;
}

SEXP sora_map_desc(SEXP xp) {
  sora_map_h *mh = map_h_get(xp);
  return mori_unserialize_from((unsigned char *) mh->shm->addr +
                               mh->h.desc_off, (size_t) mh->h.desc_len);
}

/* RAWVEC x slice [lo, hi]: one allocVector + memcpy straight from the
   mapping — per chunk, not per map, so a worker never holds more than a
   chunk of a huge x. */
static SEXP map_slice_copy(sora_map_h *mh, uint64_t lo, uint64_t hi) {
  size_t elt = mori_sizeof_elt((int) mh->h.x_sexptype);
  R_xlen_t len = (R_xlen_t) (hi - lo + 1);
  SEXP out = Rf_allocVector((SEXPTYPE) mh->h.x_sexptype, len);
  memcpy(sora_vec_ptr(out),
         (unsigned char *) mh->shm->addr + mh->h.x_off +
         (size_t) (lo - 1) * elt,
         (size_t) len * elt);
  return out;
}

SEXP sora_map_slice(SEXP xp, SEXP lo_sexp, SEXP hi_sexp) {
  sora_map_h *mh = map_h_get(xp);
  if (mh->h.x_kind != REI_MAP_X_RAWVEC)
    Rf_error("sora: map region has no x section");
  double lo = Rf_asReal(lo_sexp), hi = Rf_asReal(hi_sexp);
  if (!(lo >= 1) || !(hi >= lo) || hi > (double) mh->h.n)
    Rf_error("sora: map slice out of range");
  return map_slice_copy(mh, (uint64_t) lo, (uint64_t) hi);
}

/* Template-path write of element e's value at its disjoint output-area
   offset. "Like vapply" means exactly vapply, coercions included: exact
   type memcpys, an upward coercion (logical -> integer -> double ->
   complex) goes through R's own coerceVector so NA semantics match.
   Shared by sora_map_write and the batch loop; protects value itself, so
   an unprotected Rf_eval result can ride in. */
static void map_write_value(sora_map_h *mh, double e, SEXP value) {
  if (!(e >= 1) || e > (double) mh->h.n)
    Rf_error("sora: map element index out of range");
  int vt = TYPEOF(value), ot = (int) mh->h.out_sexptype;
  int widens = vt == ot ||
    (ot == INTSXP  && vt == LGLSXP) ||
    (ot == REALSXP && (vt == LGLSXP || vt == INTSXP)) ||
    (ot == CPLXSXP && (vt == LGLSXP || vt == INTSXP || vt == REALSXP));
  if (!widens || Rf_xlength(value) != (R_xlen_t) mh->h.out_m)
    Rf_error("sora: map values must be type '%s' and length %llu",
             map_type_name(ot), (unsigned long long) mh->h.out_m);
  unsigned char *dst = (unsigned char *) mh->shm->addr + mh->h.out_off +
    (size_t) (e - 1) * (mh->h.out_m * mh->h.out_elt_size);
  size_t nbytes = (size_t) (mh->h.out_m * mh->h.out_elt_size);
  if (vt != ot) {
    PROTECT(value);
    value = PROTECT(Rf_coerceVector(value, (SEXPTYPE) ot));
    memcpy(dst, sora_vec_ptr(value), nbytes);
    UNPROTECT(2);
  } else {
    memcpy(dst, sora_vec_ptr(value), nbytes);
  }
}

SEXP sora_map_write(SEXP xp, SEXP e_sexp, SEXP value) {
  sora_map_h *mh = map_h_get(xp);
  if (mh->h.out_sexptype == 0)
    Rf_error("sora: map region has no output area");
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
   CMRG state inline, exactly sora_map_rng_install's per-element step.
   ei_sexp is an R-allocated REALSXP(1) cell the loop stamps with the
   in-flight element index before each eval, so the R side's one
   tryCatch per batch annotates an escaping error with the failing
   element — the "first by element index" contract. x is the batch's
   source vector (the RAWVEC slice, or the descriptor x) and base its
   0-based offset of element lo. Returns the batch's value list, or NULL
   on the template path. */
SEXP sora_map_batch(SEXP xp, SEXP f, SEXP dots, SEXP x, SEXP base_sexp,
                   SEXP lo_sexp, SEXP hi_sexp, SEXP sr, SEXP ei_sexp,
                   SEXP rho) {
  sora_map_h *mh = xp == R_NilValue ? NULL : map_h_get(xp);
  int tmpl = mh != NULL && mh->h.out_sexptype != 0;
  double lo = Rf_asReal(lo_sexp), hi = Rf_asReal(hi_sexp);
  double base = Rf_asReal(base_sexp);
  if (!(lo >= 1) || !(hi >= lo) || !(base >= 0) ||
      base + (hi - lo + 1) > (double) XLENGTH(x) ||
      (tmpl && hi > (double) mh->h.n))
    Rf_error("sora: invalid map batch range");
  if (dots != R_NilValue && TYPEOF(dots) != VECSXP)
    Rf_error("sora: invalid map dots");
  if (TYPEOF(ei_sexp) != REALSXP || XLENGTH(ei_sexp) != 1)
    Rf_error("sora: invalid element-index cell");
  if (TYPEOF(rho) != ENVSXP)
    Rf_error("sora: invalid evaluation environment");
  double *ei = REAL(ei_sexp);
  R_xlen_t len = (R_xlen_t) (hi - lo + 1);
  R_xlen_t base0 = (R_xlen_t) base;

  int state[6];
  int seeded = sr != R_NilValue;
  if (seeded) {
    if (TYPEOF(sr) != INTSXP || XLENGTH(sr) != 6)
      Rf_error("sora: invalid RNG stream state");
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
  /* Atomic element reads go through one hoisted data pointer: for a
     non-ALTREP x its own block; for an ALTREP with data behind it (a
     sora view, a foreign shared vector, a materialized one) the shared
     pages — the per-element writable accessors would COW-materialize the
     whole vector per worker. NULL when an ALTREP has no data block (a
     compact 1:n), where the standard accessors stand. */
  const void *xd = NULL;
  if (mori_sizeof_elt(xt) != 0)
    xd = ALTREP(x) ? DATAPTR_OR_NULL(x) : sora_vec_ptr(x);
  for (R_xlen_t i = 0; i < len; i++) {
    if ((i & 63) == 0) R_CheckUserInterrupt();
    double e = lo + (double) i;
    *ei = e;
    if (seeded) {
      SEXP seedv = PROTECT(Rf_allocVector(INTSXP, 7));
      INTEGER(seedv)[0] = 10407;
      memcpy(INTEGER(seedv) + 1, state, 6 * sizeof(int));
      Rf_defineVar(sora_rs_sym, seedv, R_GlobalEnv);
      UNPROTECT(1);
      rei_rng_jump(state);
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
    default:      Rf_error("sora: unsupported map element type");
    }
    SETCAR(elt_cell, elt);
    SEXP v = PROTECT(Rf_eval(call, rho));
    if (tmpl) map_write_value(mh, e, v);
    else SET_VECTOR_ELT(out, i, v);
    UNPROTECT(1);
  }
  UNPROTECT(np);
  return out;
}

/* Submitter-side assembly: n × m results move cross-process exactly once,
   unserialized — one allocVector + one memcpy. Names and dim are the R
   side's. */
SEXP sora_map_gather(SEXP xp) {
  sora_map_h *mh = map_h_get(xp);
  if (mh->h.out_sexptype == 0)
    Rf_error("sora: map region has no output area");
  R_xlen_t len = (R_xlen_t) (mh->h.n * mh->h.out_m);
  SEXP out = Rf_allocVector((SEXPTYPE) mh->h.out_sexptype, len);
  memcpy(sora_vec_ptr(out), (unsigned char *) mh->shm->addr + mh->h.out_off,
         (size_t) len * mh->h.out_elt_size);
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
   re-send degrades to a materializing copy, as the region is not a MORH
   layout and REF resolution would misread it. */
SEXP sora_map_gather_view(SEXP xp, SEXP nms, SEXP tn) {
  sora_map_h *mh = map_h_get(xp);
  if (mh->h.out_sexptype == 0)
    Rf_error("sora: map region has no output area");
  uint64_t n = mh->h.n, m = mh->h.out_m;
  SEXP view = PROTECT(mori_vec_wrap(
    (unsigned char *) mh->shm->addr + mh->h.out_off, (R_xlen_t) (n * m),
    (int) mh->h.out_sexptype, xp, NULL, NULL));
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
   morsel geometry exactly as sora_map_next issued them (the final grant
   clamps to n) and only the pointer copies remain. A range or length
   mismatch against a published batch means protocol corruption, not a
   recoverable condition. */
SEXP sora_map_splice(SEXP out, SEXP results, SEXP ms_sexp) {
  if (TYPEOF(out) != VECSXP || TYPEOF(results) != VECSXP)
    Rf_error("sora: invalid map splice arguments");
  double ms = Rf_asReal(ms_sexp);
  if (!(ms >= 1))
    Rf_error("sora: invalid morsel size");
  R_xlen_t n = XLENGTH(out);
  for (R_xlen_t r = 0; r < XLENGTH(results); r++) {
    SEXP res = VECTOR_ELT(results, r);
    if (TYPEOF(res) != VECSXP || XLENGTH(res) < 3)
      Rf_error("sora: invalid map runner result");
    SEXP hm = VECTOR_ELT(res, 0), hk = VECTOR_ELT(res, 1),
         vals = VECTOR_ELT(res, 2);
    if (TYPEOF(hm) != REALSXP || TYPEOF(hk) != REALSXP ||
        TYPEOF(vals) != VECSXP || XLENGTH(hk) != XLENGTH(hm) ||
        XLENGTH(vals) != XLENGTH(hm))
      Rf_error("sora: invalid map runner result");
    const double *m = REAL(hm), *k = REAL(hk);
    for (R_xlen_t b = 0; b < XLENGTH(hm); b++) {
      if (!(m[b] >= 0) || !(k[b] >= 1))
        Rf_error("sora: invalid map batch range");
      double lo_d = m[b] * ms + 1, hi_d = (m[b] + k[b]) * ms;
      if (hi_d > (double) n) hi_d = (double) n;
      if (!(lo_d >= 1) || lo_d > (double) n || hi_d < lo_d)
        Rf_error("sora: invalid map batch range");
      R_xlen_t lo = (R_xlen_t) lo_d, hi = (R_xlen_t) hi_d;
      SEXP bv = VECTOR_ELT(vals, b);
      if (TYPEOF(bv) != VECSXP || XLENGTH(bv) != hi - lo + 1)
        Rf_error("sora: map batch result length mismatch");
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
typedef struct sora_mbatch_s { uint64_t m, k; } sora_mbatch;

static int sora_mbatch_cmp(const void *a, const void *b) {
  uint64_t x = ((const sora_mbatch *) a)->m;
  uint64_t y = ((const sora_mbatch *) b)->m;
  return (x > y) - (x < y);
}

SEXP sora_map_lost(SEXP xp, SEXP runs) {
  sora_map_h *mh = map_h_get(xp);
  uint64_t cur = atomic_load_explicit(map_cursor_word(mh),
                                      memory_order_acquire);
  if (cur > mh->h.n_morsels) cur = mh->h.n_morsels;
  if (TYPEOF(runs) != VECSXP)
    Rf_error("sora: invalid map batch history");
  R_xlen_t nh = XLENGTH(runs), total = 0;
  for (R_xlen_t i = 0; i < nh; i++) {
    SEXP pr = VECTOR_ELT(runs, i);
    if (TYPEOF(pr) != VECSXP || XLENGTH(pr) < 2 ||
        TYPEOF(VECTOR_ELT(pr, 0)) != REALSXP ||
        TYPEOF(VECTOR_ELT(pr, 1)) != REALSXP ||
        XLENGTH(VECTOR_ELT(pr, 0)) != XLENGTH(VECTOR_ELT(pr, 1)))
      Rf_error("sora: invalid map batch history");
    total += XLENGTH(VECTOR_ELT(pr, 0));
  }
  sora_mbatch *b =
    (sora_mbatch *) R_alloc((size_t) (total > 0 ? total : 1),
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
        Rf_error("sora: invalid map batch history");
      b[at].m = (uint64_t) m[j];
      b[at].k = (uint64_t) k[j];
    }
  }
  qsort(b, (size_t) total, sizeof(*b), sora_mbatch_cmp);
  const uint64_t msz = mh->h.morsel_size, n = mh->h.n;
  uint64_t pos = 0, ngap = 0;
  for (R_xlen_t i = 0; i < total; i++) {
    if (b[i].m > pos) ngap++;
    if (b[i].m + b[i].k > pos) pos = b[i].m + b[i].k;
  }
  if (pos < cur) ngap++;
  SEXP out = PROTECT(Rf_allocMatrix(REALSXP, (R_xlen_t) ngap, 2));
  double *lo = REAL(out), *hi = lo + ngap;
  pos = 0;
  uint64_t g = 0;
  for (R_xlen_t i = 0; i < total; i++) {
    if (b[i].m > pos) {
      uint64_t e = b[i].m * msz;
      lo[g] = (double) (pos * msz) + 1;
      hi[g] = (double) (e > n ? n : e);
      g++;
    }
    if (b[i].m + b[i].k > pos) pos = b[i].m + b[i].k;
  }
  if (pos < cur) {
    uint64_t e = cur * msz;
    lo[g] = (double) (pos * msz) + 1;
    hi[g] = (double) (e > n ? n : e);
    g++;
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

   sig is the opaque address trio from rei_pool_signals (NULL skips the
   loads — the in-process protocol tests). pin bypasses the sizing policy
   with a fixed k; now overrides the rei_now() read — both test entries,
   NULL in production. */
SEXP sora_map_next(SEXP xp, SEXP r_sexp, SEXP gen_sexp, SEXP sig,
                  SEXP pin_sexp, SEXP now_sexp) {
  sora_map_h *mh = map_h_get(xp);
  uint32_t r = map_ordinal(mh, r_sexp);
  uint32_t gen = ((uint32_t) Rf_asReal(gen_sexp)) & REI_MAP_GEN_MASK;

  /* first transition: CAS (gen << 2)|IDLE -> RUNNING — the one atomic
     that both claims the lane and fences the generation. It fails alike
     against ABANDONED (lost to the trim) and against a word re-armed
     with a newer generation; RUNNING at our generation means this very
     task already claimed it (each ordinal rides exactly one payload per
     generation), so later transitions — and a run resumed through an
     aliased ctx — fall straight through. */
  _Atomic uint32_t *cw = map_claim_word(mh, r);
  uint32_t running = (gen << 2) | REI_MORSEL_RUNNING;
  uint32_t w = atomic_load_explicit(cw, memory_order_acquire);
  if (w == ((gen << 2) | REI_MORSEL_IDLE) &&
      atomic_compare_exchange_strong_explicit(cw, &w, running,
                                              memory_order_seq_cst,
                                              memory_order_acquire))
    w = running;
  if (w != running) return R_NilValue;

  if (mh->run_r != (int32_t) r || mh->run_gen != gen) {
    /* run boundary through this ctx: relearn over a fresh ramp */
    mh->run_r = (int32_t) r;
    mh->run_gen = gen;
    mh->k = 1;
    mh->k_last = 0;
    mh->cost = 0;
    mh->skip = 0;
  }

  if (atomic_load_explicit(map_cancel_word(mh), memory_order_acquire) != 0)
    return R_NilValue;

  int help = 0;
  if (sig != R_NilValue) {
    rei_pool_sig *s = sora_pool_sig_get(sig);
    /* a runner is the one place a worker sits for a whole map without
       touching its step loop, where these words are consumed: NULL
       unwinds it there within ~a batch instead of at cursor exhaustion */
    if (atomic_load_explicit(s->shutdown, memory_order_relaxed) != 0 ||
        atomic_load_explicit(s->owner_dead, memory_order_relaxed) != 0)
      return R_NilValue;
    help = atomic_load_explicit(s->help_wanted, memory_order_relaxed) != 0;
  }

  double now = now_sexp == R_NilValue ? rei_now() : Rf_asReal(now_sexp);
  uint64_t k;
  if (pin_sexp != R_NilValue) {
    double pk = Rf_asReal(pin_sexp);
    if (!(pk >= 1)) Rf_error("sora: invalid pinned batch size");
    k = (uint64_t) pk;
  } else {
    if (mh->k_last > 0) {
      if (mh->skip) {
        mh->skip = 0;   /* interval contained a helped foreign task */
      } else {
        double per = (now - mh->t_last) / (double) mh->k_last;
        mh->cost = per > 1e-9 ? per : 1e-9;   /* clock-floor trivial f */
      }
      if (mh->cost > 0) {
        double want = REI_MAP_T_TARGET / mh->cost;
        uint64_t wk = want >= 1 ? (uint64_t) want : 1;
        /* grow at most 2x per step toward the target; shrink immediately
           on overshoot; clamp to the batch cap */
        mh->k = wk >= mh->k * 2 ? mh->k * 2 : wk;
        if (mh->k > REI_MAP_BATCH_CAP) mh->k = REI_MAP_BATCH_CAP;
      }
    }
    k = mh->k;
  }

  /* relaxed issue: atomicity (unique claim) is all the shared state
     provides; ordering rides the task claim/publish chain. Overshoot of
     up to k is harmless — a runner stops at its first exhausted issue. */
  uint64_t m = atomic_fetch_add_explicit(map_cursor_word(mh), k,
                                         memory_order_relaxed);
  if (m >= mh->h.n_morsels) return R_NilValue;
  if (k > mh->h.n_morsels - m) k = mh->h.n_morsels - m;   /* final grant */
  mh->k_last = k;
  mh->t_last = now;
  if (help) mh->skip = 1;

  uint64_t lo = m * mh->h.morsel_size + 1;
  uint64_t hi = (m + k) * mh->h.morsel_size;
  if (hi > mh->h.n) hi = mh->h.n;
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 6));
  SET_VECTOR_ELT(out, 0, Rf_ScalarReal((double) m));
  SET_VECTOR_ELT(out, 1, Rf_ScalarReal((double) k));
  SET_VECTOR_ELT(out, 2, Rf_ScalarReal((double) lo));
  SET_VECTOR_ELT(out, 3, Rf_ScalarReal((double) hi));
  if (mh->h.x_kind == REI_MAP_X_RAWVEC)
    SET_VECTOR_ELT(out, 4, map_slice_copy(mh, lo, hi));
  SET_VECTOR_ELT(out, 5, Rf_ScalarLogical(help));
  UNPROTECT(1);
  return out;
}

/* The exhausted-runner trim's CAS, folding its own trigger: a no-op
   ("idle" refusal) unless the cursor is exhausted or the cancel word is
   set. A won IDLE -> ABANDONED CAS at the current generation proves that
   runner never started and never will do work — rei_pool_cancel alone
   cannot carry the trim, being advisory and discard-only while the
   trigger condition is the routine end state of every map. Returns the
   verdict as the morsel-state code itself: REI_MORSEL_ABANDONED (won,
   or already trimmed), REI_MORSEL_RUNNING (the runner is executing or
   already published — collect it), REI_MORSEL_IDLE (trigger unarmed:
   collect defers this handle rather than parking on it). */
SEXP sora_map_abandon(SEXP xp, SEXP r_sexp) {
  sora_map_h *mh = map_h_get(xp);
  uint32_t r = map_ordinal(mh, r_sexp);
  _Atomic uint32_t *cw = map_claim_word(mh, r);
  uint32_t gen = atomic_load_explicit(map_gen_word(mh),
                                      memory_order_acquire) &
    REI_MAP_GEN_MASK;
  int armed =
    atomic_load_explicit(map_cursor_word(mh), memory_order_acquire) >=
      mh->h.n_morsels ||
    atomic_load_explicit(map_cancel_word(mh), memory_order_acquire) != 0;
  uint32_t w = atomic_load_explicit(cw, memory_order_acquire);
  if (armed)
    while (w == ((gen << 2) | REI_MORSEL_IDLE))
      if (atomic_compare_exchange_strong_explicit(
            cw, &w, (gen << 2) | REI_MORSEL_ABANDONED,
            memory_order_seq_cst, memory_order_acquire))
        return Rf_ScalarInteger(REI_MORSEL_ABANDONED);
  return Rf_ScalarInteger((int) (w & 3u));
}

/* The cancel word: set by the submitter on timeout / cancel / death, and
   by an erroring runner itself before its ERR publish — the fail-fast
   store that stops every peer within ~a batch. Idempotent, and total —
   it runs from unwind paths (map_cancel under on.exit, a runner's error
   handler), so a closed or foreign handle no-ops. */
SEXP sora_map_cancel_set(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != sora_map_tag)
    return R_NilValue;
  sora_map_h *mh = (sora_map_h *) R_ExternalPtrAddr(xp);
  if (mh == NULL) return R_NilValue;
  atomic_store_explicit(map_cancel_word(mh), 1u, memory_order_seq_cst);
  return R_NilValue;
}

SEXP sora_map_cancel_get(SEXP xp) {
  return Rf_ScalarLogical(
    atomic_load_explicit(map_cancel_word(map_h_get(xp)),
                         memory_order_acquire) != 0);
}

/* Prepared-run re-arm, O(1) in n (no per-morsel state exists to clear):
   bump the generation, stamp (new_gen << 2) | IDLE over the CLAIM array,
   zero the cursor, clear the cancel word. The stamped generation is the
   fence against a stale trimmed runner from the prior run: its
   first-call CAS expects the old generation and fails against the
   re-armed word however the reset interleaves. Returns the new
   generation — the value the next run's payloads must carry. */
SEXP sora_map_reset(SEXP xp) {
  sora_map_h *mh = map_h_get(xp);
  uint32_t gen = (atomic_fetch_add_explicit(map_gen_word(mh), 1u,
                                            memory_order_seq_cst) + 1) &
    REI_MAP_GEN_MASK;
  for (uint32_t r = 0; r < mh->h.claim_n; r++)
    atomic_store_explicit(map_claim_word(mh, r),
                          (gen << 2) | REI_MORSEL_IDLE,
                          memory_order_seq_cst);
  atomic_store_explicit(map_cursor_word(mh), 0, memory_order_seq_cst);
  atomic_store_explicit(map_cancel_word(mh), 0u, memory_order_seq_cst);
  return Rf_ScalarReal((double) gen);
}

/* Geometry and state snapshot: the stage-time constants plus single reads
   of the mutable words. The submit-time generation read and the death
   path's lost-set bound (the cursor, clamped to n_morsels) both ride
   here. */
SEXP sora_map_info(SEXP xp) {
  sora_map_h *mh = map_h_get(xp);
  const char *names[] = {"n", "morsel_size", "n_morsels", "claim_n",
                         "generation", "cursor", "cancel", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_ScalarReal((double) mh->h.n));
  SET_VECTOR_ELT(out, 1, Rf_ScalarReal((double) mh->h.morsel_size));
  SET_VECTOR_ELT(out, 2, Rf_ScalarReal((double) mh->h.n_morsels));
  SET_VECTOR_ELT(out, 3, Rf_ScalarInteger((int) mh->h.claim_n));
  SET_VECTOR_ELT(out, 4, Rf_ScalarReal((double)
    (atomic_load_explicit(map_gen_word(mh), memory_order_acquire) &
     REI_MAP_GEN_MASK)));
  uint64_t cur = atomic_load_explicit(map_cursor_word(mh),
                                      memory_order_acquire);
  if (cur > mh->h.n_morsels) cur = mh->h.n_morsels;
  SET_VECTOR_ELT(out, 5, Rf_ScalarReal((double) cur));
  SET_VECTOR_ELT(out, 6, Rf_ScalarLogical(
    atomic_load_explicit(map_cancel_word(mh), memory_order_acquire) != 0));
  UNPROTECT(1);
  return out;
}

/* Prepared-map in-place x swap: memcpy a RAWVEC-eligible replacement of
   identical type and length over the region's x section (the producer
   mapping is writable). Safe because a RAWVEC x is sliced from the
   mapping per batch and never cached worker-side. Errors on any
   mismatch — the R side restages instead of swapping. */
SEXP sora_map_swap_x(SEXP xp, SEXP x) {
  sora_map_h *mh = map_h_get(xp);
  if (mh->h.x_kind != REI_MAP_X_RAWVEC)
    Rf_error("sora: map region has no x section");
  if ((uint32_t) TYPEOF(x) != mh->h.x_sexptype ||
      (uint64_t) XLENGTH(x) != mh->h.n ||
      ALTREP(x) || Rf_isS4(x) || sora_vec_ptr(x) == NULL)
    Rf_error("sora: replacement x must match the staged type and length");
  memcpy((unsigned char *) mh->shm->addr + mh->h.x_off, sora_vec_ptr(x),
         (size_t) mh->h.x_len);
  return R_NilValue;
}

/* One CLAIM word decoded — the protocol tests' view of the handshake. */
SEXP sora_map_claim_state(SEXP xp, SEXP r_sexp) {
  sora_map_h *mh = map_h_get(xp);
  uint32_t r = map_ordinal(mh, r_sexp);
  uint32_t w = atomic_load_explicit(map_claim_word(mh, r),
                                    memory_order_acquire);
  const char *names[] = {"state", "generation", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(
    (w & 3u) == REI_MORSEL_IDLE ? "idle" :
    (w & 3u) == REI_MORSEL_RUNNING ? "running" : "abandoned"));
  SET_VECTOR_ELT(out, 1, Rf_ScalarReal((double) (w >> 2)));
  UNPROTECT(1);
  return out;
}

/* The shared timeout sentinel, for sora_map's own deadline returns —
   `.timeout` follows the sentinel discipline (returned, never raised). */
SEXP sora_map_timeout_call(void) {
  return sora_sent_timeout;
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
static void sora_strip_walk(SEXP x) {
  switch (TYPEOF(x)) {
  case LANGSXP:
  case LISTSXP:
    if (ANY_ATTRIB(x)) {
      Rf_setAttrib(x, sora_srcref_sym, R_NilValue);
      Rf_setAttrib(x, sora_srcfile_sym, R_NilValue);
      Rf_setAttrib(x, sora_wholesrcref_sym, R_NilValue);
    }
    if (CAR(x) == sora_function_sym && CDDR(x) != R_NilValue) {
      SETCDR(CDDR(x), R_NilValue);
    }
    sora_strip_walk(CAR(x));
    for (SEXP node = CDR(x); node != R_NilValue; node = CDR(node)) {
      sora_strip_walk(CAR(node));
    }
    break;
  case EXPRSXP:
    if (ANY_ATTRIB(x)) {
      Rf_setAttrib(x, sora_srcref_sym, R_NilValue);
      Rf_setAttrib(x, sora_srcfile_sym, R_NilValue);
      Rf_setAttrib(x, sora_wholesrcref_sym, R_NilValue);
    }
    for (R_xlen_t i = 0; i < XLENGTH(x); i++) {
      sora_strip_walk(VECTOR_ELT(x, i));
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
SEXP sora_strip_srcref(SEXP f) {
  if (TYPEOF(f) != CLOSXP) {
    return f;
  }
  SEXP formals = PROTECT(Rf_duplicate(R_ClosureFormals(f)));
  sora_strip_walk(formals);
  SEXP body = R_ClosureBody(f);
  if (TYPEOF(body) == LANGSXP || TYPEOF(body) == LISTSXP ||
      TYPEOF(body) == EXPRSXP) {
    body = PROTECT(Rf_duplicate(body));
    sora_strip_walk(body);
  } else {
    PROTECT(body);
  }
  SEXP out = PROTECT(R_mkClosure(formals, body, R_ClosureEnv(f)));
  DUPLICATE_ATTRIB(out, f);
  Rf_setAttrib(out, sora_srcref_sym, R_NilValue);
  UNPROTECT(3);
  return out;
}

/* removeSource for a language tree (a task expression): deep-duplicate —
   pairlist/language duplicates are deep — and strip the private copy in
   place. LANGSXP/LISTSXP only: an EXPRSXP duplicate is shallow and the
   walk would strip shared elements in place. */
SEXP sora_strip_lang(SEXP x) {
  SEXP out = PROTECT(Rf_duplicate(x));
  sora_strip_walk(out);
  UNPROTECT(1);
  return out;
}
