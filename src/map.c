/* kio_map staging and worker-side context — one fresh kioto region per map
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
#include "kioto.h"

#define KIO_MAP_MAGIC 0x4B494F4Du   /* "KIOM" */

enum { KIO_MAP_X_DESC = 0, KIO_MAP_X_RAWVEC };

/* Map-descriptor region header. Not pool wire format — it rides its own
   region, keyed by the same ABI version — but the same rules apply: the
   struct is the layout, 64-byte-aligned sections follow it. */
typedef struct kio_map_hdr_s {
  uint32_t magic;
  uint32_t version;
  uint32_t flags;            /* reserved, 0 */
  uint32_t x_kind;           /* KIO_MAP_X_DESC / KIO_MAP_X_RAWVEC */
  uint32_t x_sexptype;       /* RAWVEC section element type */
  uint32_t out_sexptype;     /* template element type; 0 = no output area */
  uint32_t out_elt_size;
  uint32_t pad0;
  uint64_t n;                /* map elements */
  uint64_t desc_off, desc_len;
  uint64_t x_off, x_len;
  uint64_t out_off;
  uint64_t out_m;            /* template length: values per element */
  uint8_t  pad[40];
} kio_map_hdr;

typedef char kio_map_hdr_assert[(sizeof(kio_map_hdr) == 128) ? 1 : -1];

/* The map-local RAWVEC gate, deliberately looser than kio_raw_eligible:
   no size cap, and attributes are the R side's to check (names-only is
   admissible there — names stay submitter-side for assembly and the chunk
   loop's [[ drops them anyway). ALTREP still disqualifies — a
   mori::share()d x must reduce to its identifier inside the descriptor
   stream via the hooks, not be copied wholesale into a second region — as
   does S4. Returns the section byte length, or -1 when x must ride the
   descriptor. */
SEXP kio_map_eligible(SEXP x) {
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

/* Stage one map call into a fresh region. desc is the single descriptor
   stream's object; x the RAWVEC-section vector or NULL; desc_len the exact
   stream size when the R side already counted it (the region-less probe's
   bounded pass), or NULL to count here — so the descriptor costs one count
   pass and one write pass total, never two counts. The write re-verifies
   the count: mori_serialize_into checks no bounds, and a mismatch here
   means heap corruption, not a recoverable condition. Returns list(name,
   producer wrap); the caller pins the wrap for the map's duration. */
SEXP kio_map_stage(SEXP desc, SEXP x, SEXP desc_len_sexp, SEXP n_sexp,
                   SEXP template_sexp) {
  double nd = Rf_asReal(n_sexp);
  if (!(nd >= 1) || nd > 9.007199254740992e15)
    Rf_error("kioto: invalid map length");
  uint64_t n = (uint64_t) nd;
  size_t desc_len = desc_len_sexp == R_NilValue ?
    mori_serialize_count(desc) : (size_t) Rf_asReal(desc_len_sexp);
  if (desc_len == 0)
    Rf_error("kioto: invalid map descriptor size");

  kio_map_hdr h = {
    .magic = KIO_MAP_MAGIC,
    .version = KIO_ABI_VERSION,
    .n = n,
    .desc_off = sizeof(kio_map_hdr),
    .desc_len = desc_len,
  };
  uint64_t off = MORI_ALIGN64(sizeof(kio_map_hdr) + desc_len);
  if (x != R_NilValue) {
    size_t elt = mori_sizeof_elt(TYPEOF(x));
    if (elt == 0 || kio_vec_ptr(x) == NULL ||
        (uint64_t) XLENGTH(x) != n)
      Rf_error("kioto: x is not eligible for the map raw section");
    h.x_kind = KIO_MAP_X_RAWVEC;
    h.x_sexptype = (uint32_t) TYPEOF(x);
    h.x_off = off;
    h.x_len = n * elt;
    off = MORI_ALIGN64(off + h.x_len);
  }
  if (template_sexp != R_NilValue) {
    size_t elt = mori_sizeof_elt(TYPEOF(template_sexp));
    uint64_t m = (uint64_t) XLENGTH(template_sexp);
    if (elt == 0 || m == 0)
      Rf_error("kioto: invalid map template");
    if (n > (((uint64_t) 1 << 46) - off) / (m * elt))
      Rf_error("kioto: map region too large");
    h.out_sexptype = (uint32_t) TYPEOF(template_sexp);
    h.out_elt_size = (uint32_t) elt;
    h.out_m = m;
    h.out_off = off;
    off += n * m * elt;
  }
  if (off > ((uint64_t) 1 << 46))
    Rf_error("kioto: map region too large");

  mori_shm *shm;
  int rc = mori_shm_create_heap(&shm, (size_t) off);
  if (rc != MORI_OK) {
    const char *summary, *hint;
    mori_err_describe(rc, &summary, &hint);
    kio_stop_shm((double) off,
                 "kioto: cannot create map region (%llu bytes): %s%s%s",
                 (unsigned long long) off, summary,
                 hint[0] != '\0' ? ". " : "", hint);
  }
  SEXP wrap = PROTECT(kio_shm_wrap_producer(shm));
  unsigned char *b = (unsigned char *) shm->addr;
  memcpy(b, &h, sizeof(h));
  if (kio_serialize_bounded(b + h.desc_off, desc_len, desc) != desc_len)
    Rf_error("kioto: map descriptor size changed between count and write");
  if (x != R_NilValue)
    memcpy(b + h.x_off, kio_vec_ptr(x), (size_t) h.x_len);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(out, 0, Rf_mkString(shm->name));
  SET_VECTOR_ELT(out, 1, wrap);
  UNPROTECT(2);
  return out;
}

// Worker-side context -----------------------------------------------------------

static const char *map_hdr_validate(const mori_shm *shm, kio_map_hdr *out) {
  if (shm->size < sizeof(kio_map_hdr))
    return "region is smaller than a map header";
  kio_map_hdr h;
  memcpy(&h, shm->addr, sizeof(h));
  if (h.magic != KIO_MAP_MAGIC)
    return "bad magic: not a kioto map region";
  if (h.version != KIO_ABI_VERSION)
    return "ABI version mismatch: worker and submitter were built against "
           "different kioto wire formats";
  if (h.n == 0 || h.n > ((uint64_t) 1 << 48))
    return "element count out of range";
  if (h.desc_off < sizeof(kio_map_hdr) || h.desc_off > shm->size ||
      h.desc_len == 0 || h.desc_len > shm->size - h.desc_off)
    return "descriptor lies outside the region";
  if (h.x_kind == KIO_MAP_X_RAWVEC) {
    size_t elt = mori_sizeof_elt((int) h.x_sexptype);
    if (elt == 0 || h.x_off > shm->size || h.x_len > shm->size - h.x_off ||
        h.x_len != h.n * elt)
      return "x section lies outside the region";
  } else if (h.x_kind != KIO_MAP_X_DESC) {
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

/* Attach a map region. Generic path: the vendored read-only consumer open,
   which never populates. Template path: writable for the output-area
   stores, no-populate so a large RAWVEC x still demand-pages per worker.
   Both failure modes raise ordinary R errors — they happen inside the task
   eval, so they publish as the chunk's ERR result (or the CANCEL drop
   absorbs them on the timeout path, where the submitter unlinked the
   region under a straggler) — never the fatal infrastructure path. */
SEXP kio_map_open(SEXP name_sexp, SEXP writable_sexp) {
  if (TYPEOF(name_sexp) != STRSXP || XLENGTH(name_sexp) != 1)
    Rf_error("kioto: expected a map region name");
  const char *name = CHAR(STRING_ELT(name_sexp, 0));
  mori_shm *shm = Rf_asLogical(writable_sexp) == TRUE ?
    kio_shm_open_rw_heap(name, 0) : mori_shm_open_heap(name);
  if (shm == NULL)
    kio_stop_shm(NA_REAL, "kioto: cannot open map region '%s' — its "
                 "submitter died or the map ended", name);
  const char *err = map_hdr_validate(shm, NULL);
  if (err != NULL) {
    mori_shm_close(shm, 0);
    free(shm);
    Rf_error("kioto: invalid map region: %s", err);
  }
  return kio_shm_wrap_consumer(shm);
}

/* Header fetch for the accessors below: one memcpy plus the full validation
   — cheap, and it makes every accessor total against a stray handle. */
static kio_map_hdr map_hdr_get(SEXP xp, mori_shm **shm_out) {
  mori_shm *shm = kio_region(xp);
  kio_map_hdr h;
  const char *err = map_hdr_validate(shm, &h);
  if (err != NULL) Rf_error("kioto: invalid map region: %s", err);
  *shm_out = shm;
  return h;
}

SEXP kio_map_desc(SEXP xp) {
  mori_shm *shm;
  kio_map_hdr h = map_hdr_get(xp, &shm);
  return mori_unserialize_from((unsigned char *) shm->addr + h.desc_off,
                               (size_t) h.desc_len);
}

/* RAWVEC x slice [lo, hi]: one allocVector + memcpy straight from the
   mapping — per chunk, not per map, so a worker never holds more than a
   chunk of a huge x. */
SEXP kio_map_slice(SEXP xp, SEXP lo_sexp, SEXP hi_sexp) {
  mori_shm *shm;
  kio_map_hdr h = map_hdr_get(xp, &shm);
  if (h.x_kind != KIO_MAP_X_RAWVEC)
    Rf_error("kioto: map region has no x section");
  double lo = Rf_asReal(lo_sexp), hi = Rf_asReal(hi_sexp);
  if (!(lo >= 1) || !(hi >= lo) || hi > (double) h.n)
    Rf_error("kioto: map slice out of range");
  size_t elt = mori_sizeof_elt((int) h.x_sexptype);
  R_xlen_t len = (R_xlen_t) (hi - lo + 1);
  SEXP out = Rf_allocVector((SEXPTYPE) h.x_sexptype, len);
  memcpy(kio_vec_ptr(out),
         (unsigned char *) shm->addr + h.x_off + (size_t) (lo - 1) * elt,
         (size_t) len * elt);
  return out;
}

/* Template-path write of element e's value at its disjoint output-area
   offset. "Like vapply" means exactly vapply, coercions included: exact
   type memcpys, an upward coercion (logical -> integer -> double ->
   complex) goes through R's own coerceVector so NA semantics match. */
SEXP kio_map_write(SEXP xp, SEXP e_sexp, SEXP value) {
  mori_shm *shm;
  kio_map_hdr h = map_hdr_get(xp, &shm);
  if (h.out_sexptype == 0)
    Rf_error("kioto: map region has no output area");
  double e = Rf_asReal(e_sexp);
  if (!(e >= 1) || e > (double) h.n)
    Rf_error("kioto: map element index out of range");
  int vt = TYPEOF(value), ot = (int) h.out_sexptype;
  int widens = vt == ot ||
    (ot == INTSXP  && vt == LGLSXP) ||
    (ot == REALSXP && (vt == LGLSXP || vt == INTSXP)) ||
    (ot == CPLXSXP && (vt == LGLSXP || vt == INTSXP || vt == REALSXP));
  if (!widens || Rf_xlength(value) != (R_xlen_t) h.out_m)
    Rf_error("kioto: map values must be type '%s' and length %llu",
             map_type_name(ot), (unsigned long long) h.out_m);
  if (vt != ot) value = Rf_coerceVector(value, (SEXPTYPE) ot);
  PROTECT(value);
  memcpy((unsigned char *) shm->addr + h.out_off +
         (size_t) (e - 1) * (h.out_m * h.out_elt_size),
         kio_vec_ptr(value), (size_t) (h.out_m * h.out_elt_size));
  UNPROTECT(1);
  return R_NilValue;
}

/* Submitter-side assembly: n × m results move cross-process exactly once,
   unserialized — one allocVector + one memcpy. Names and dim are the R
   side's. */
SEXP kio_map_gather(SEXP xp) {
  mori_shm *shm;
  kio_map_hdr h = map_hdr_get(xp, &shm);
  if (h.out_sexptype == 0)
    Rf_error("kioto: map region has no output area");
  R_xlen_t len = (R_xlen_t) (h.n * h.out_m);
  SEXP out = Rf_allocVector((SEXPTYPE) h.out_sexptype, len);
  memcpy(kio_vec_ptr(out), (unsigned char *) shm->addr + h.out_off,
         (size_t) len * h.out_elt_size);
  return out;
}

/* The shared timeout sentinel, for kio_map's own deadline returns —
   `.timeout` follows the sentinel discipline (returned, never raised). */
SEXP kio_map_timeout_call(void) {
  return kio_sent_timeout;
}
