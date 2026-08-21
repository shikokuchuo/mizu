/* Shared payload framing over the sora_slot_hdr wire form — the staging and
   materializing code common to Part I channel slots and Part II pool entries
   / result slots. The channel adds its arena tier around these; the pool has
   no arena (its payloads release at collect or slot reuse — unordered — so a
   producer-local FIFO allocator does not apply) and stages through
   sora_payload_stage directly. */

#include "sora.h"

/* ANY_ATTRIB() joined the C API in R 4.5.0; equivalent fallback for earlier
   R, where ATTRIB() was still the sanctioned spelling. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

void *sora_vec_ptr(SEXP x) {
  switch (TYPEOF(x)) {
  case LGLSXP:  return LOGICAL(x);
  case INTSXP:  return INTEGER(x);
  case REALSXP: return REAL(x);
  case CPLXSXP: return COMPLEX(x);
  case RAWSXP:  return RAW(x);
  }
  return NULL;
}

int sora_raw_type(SEXP x, size_t *out_len) {
  switch (TYPEOF(x)) {
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
    break;
  default:
    return 0;
  }
  /* The ALTREP exclusion is the linkage-free gate keeping mori-shared
     vectors on the hook path; the S4 bit is recorded by serialize, so it
     disqualifies alongside attributes. */
  if (ALTREP(x) || ANY_ATTRIB(x) || Rf_isS4(x)) return 0;
  *out_len = (size_t) XLENGTH(x) * mori_sizeof_elt(TYPEOF(x));
  return 1;
}

int sora_raw_eligible(SEXP x, uint32_t inline_max, size_t *out_len) {
  size_t n;
  if (!sora_raw_type(x, &n) || n > inline_max) return 0;
  *out_len = n;
  return 1;
}

int sora_str1_stage(sora_slot_hdr *hdr, unsigned char *payload,
                   uint32_t inline_max, SEXP x) {
  /* The ALTREP exclusion keeps foreign ALTSTRINGs on the serialize path
     (their Elt may materialize); sora's own string views never reach here
     (the REF check upstream claims them first) */
  if (TYPEOF(x) != STRSXP || XLENGTH(x) != 1 || ALTREP(x) ||
      ANY_ATTRIB(x) || Rf_isS4(x))
    return 0;
  SEXP s = STRING_ELT(x, 0);
  if (s == NA_STRING) {
    hdr->kind = SORA_KIND_STR1;
    hdr->len = 0;
    hdr->aux = SORA_STR1_NA;
    return 1;
  }
  size_t n = (size_t) LENGTH(s);
  if (n > (size_t) inline_max) return 0;
  hdr->kind = SORA_KIND_STR1;
  hdr->len = (uint32_t) n;
  hdr->aux = (uint64_t) Rf_getCharCE(s);
  memcpy(payload, CHAR(s), n);
  return 1;
}

/* The shared empty args list of a no-argument task: one preserved vector
   serves both the submitter (sora_submit's capture) and the worker (the
   task-frame read), so a constant task allocates no VECSXP(0) on either
   side. Read-only everywhere it appears — marked not-mutable, so a stray
   write fails loudly instead of corrupting every task. */
static SEXP empty_args;

SEXP sora_empty_args(void) {
  return empty_args;
}

void sora_payload_init(void) {
  empty_args = Rf_allocVector(VECSXP, 0);
  R_PreserveObject(empty_args);
#if R_VERSION >= R_Version(4, 5, 0)
  MARK_NOT_MUTABLE(empty_args);
#endif
}

void sora_payload_fini(void) {
  R_ReleaseObject(empty_args);
}

// Spill staging ------------------------------------------------------------

/* Each fills the retain entry: the region (checked out of fl, committed by
   the caller) plus, for the serialize stream, the pin of x. */

void sora_payload_spill_shm(sora_slot_hdr *hdr, unsigned char *payload, SEXP x,
                           size_t n, sora_spill_fl *fl, sora_keeper *out) {
  mori_shm *shm = sora_spill_region_get(fl, n);
  mori_serialize_into((unsigned char *) shm->addr, x);
  hdr->kind = SORA_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;
  memcpy(payload, shm->name, shm->name_len);
  out->region = shm;
  out->pin = x;
  out->kind = SORA_KEEP_SPILL;
}

void sora_payload_spill_raw(sora_slot_hdr *hdr, unsigned char *payload, SEXP x,
                           size_t n, sora_spill_fl *fl, sora_keeper *out) {
  mori_shm *shm = sora_spill_region_get(fl, n);
  memcpy(shm->addr, sora_vec_ptr(x), n);
  hdr->kind = SORA_KIND_RAWSPILL;
  hdr->len = (uint32_t) n;
  hdr->aux = (uint64_t) TYPEOF(x) | ((uint64_t) shm->name_len << 8);
  memcpy(payload, shm->name, shm->name_len);
  out->region = shm;
  out->kind = SORA_KEEP_SPILL;
}

/* The SHM_RAW spill of a codec stream (n from the counting first pass):
   the region alone is pinned — the writer rejected ALTREP, so no
   hook-emitted identifier can ride along. */
void sora_payload_spill_codec(sora_slot_hdr *hdr, unsigned char *payload,
                             SEXP x, size_t n, sora_spill_fl *fl,
                             sora_keeper *out) {
  mori_shm *shm = sora_spill_region_get(fl, n);
  if (sora_codec_write((unsigned char *) shm->addr, shm->size, x) != n)
    Rf_error("sora: codec write mismatch");   /* the walk is deterministic */
  hdr->kind = SORA_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;
  memcpy(payload, shm->name, shm->name_len);
  out->region = shm;
  out->kind = SORA_KEEP_SPILL;
}

/* The NIL, RAWVEC, and STR1 kinds retain nothing: their slot bytes are
   self-contained (RAWVEC and STR1 exclude ALTREP, attributes, and S4, so
   no hook-emitted identifier can ride along), unlike the serialize tiers,
   where a stream may carry mori identifiers whose views the pin keeps
   alive until consumer-done. */
void sora_payload_stage(sora_slot_hdr *hdr, unsigned char *payload,
                       uint32_t inline_max, SEXP x, sora_spill_fl *fl,
                       sora_keeper *out) {
  out->region = NULL;
  out->pin = R_NilValue;
  out->key = -1;
  out->kind = SORA_KEEP_FREE;
  size_t rawlen, total;
  /* NULL stages as the immediate kind: the canonical empty result / ACK
     pays no serialize pass and no receive-side allocation */
  if (x == R_NilValue) {
    hdr->kind = SORA_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
    return;
  }
  /* a sora-native view crosses by reference (REF) at any size — required
     once SHM_VEC views exist: the serialize-hook fallback resolves
     uncounted, and the producer could recycle under the far side's view.
     The pin keeps the view (and with it the region) until consumer-done. */
  if (sora_zc_ref_stage(hdr, payload, inline_max, x)) {
    out->pin = x;
    out->kind = SORA_KEEP_PIN;
    return;
  }
  if (sora_raw_eligible(x, inline_max, &rawlen)) {
    memcpy(payload, sora_vec_ptr(x), rawlen);
    hdr->kind = SORA_KIND_RAWVEC;
    hdr->len = (uint32_t) rawlen;
    hdr->aux = (uint64_t) TYPEOF(x);
    return;
  }
  if (sora_str1_stage(hdr, payload, inline_max, x))
    return;
  /* SHM_VEC: mori-layout-eligible objects (atomic vectors, strings, list
     trees) past the budget and the zc floor — cheap probes keep the
     layout-size walk off the inline path (zc.c). Under churn (the last
     spill miss swept the lent ledger and reclaimed nothing — a Linux-
     only signal, see spill_fl_pop) the fresh region per SHM_VEC payload
     is dearer than the serialize copy: fall to SHM_RAW, whose region
     surrenders deterministically at consumer-done. */
  if ((fl == NULL || !fl->churn) && sora_zc_eligible(x, inline_max, &total)) {
    sora_zc_stage(hdr, payload, x, total, fl, out);
    return;
  }
  /* Raw-bytes spill: the vectors RAWVEC takes inline, past the inline
     budget — the layout tier above already passed (under the zc floor, or
     churn-gated), and bare bytes skip both the serialize pass here and
     the parse at the far end. */
  size_t n;
  if (sora_raw_type(x, &n) && n <= UINT32_MAX) {
    sora_payload_spill_raw(hdr, payload, x, n, fl, out);
    return;
  }
  /* the compact codec ahead of R_Serialize: no per-call ref-table
     allocation on either side, and a self-contained stream (the writer
     rejects ALTREP, so no mori identifier can ride along) that pins
     nothing — the NIL/RAWVEC/STR1 discipline */
  n = sora_codec_write(payload, inline_max, x);
  if (n != 0) {
    if (n <= inline_max) {
      hdr->kind = SORA_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = 0;
      return;
    }
    sora_payload_spill_codec(hdr, payload, x, n, fl, out);
    return;
  }
  n = sora_serialize_bounded(payload, inline_max, x);
  if (n <= inline_max) {
    hdr->kind = SORA_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = 0;
    out->pin = x;
    out->kind = SORA_KEEP_PIN;
    return;
  }
  sora_payload_spill_shm(hdr, payload, x, n, fl, out);
}

// Read -----------------------------------------------------------------------

/* Open the named payload region read-only through the cache, returning the
   mapping (cache-owned: copy the payload out before consumer-done — the
   discipline the RAWSPILL/SHM_RAW tiers already assume). A vanished region
   reports through *gone (NULL return) when the caller can absorb it, else
   raises. */
static mori_shm *payload_region_open(const unsigned char *name_bytes,
                                    uint32_t name_len, int *gone,
                                    sora_open_cache *oc) {
  mori_shm *shm = sora_oc_lookup(oc, name_bytes, name_len);
  if (shm == NULL) {
    char name[MORI_NAME_MAX];
    memcpy(name, name_bytes, name_len);
    name[name_len] = '\0';
    shm = sora_shm_open_ro_heap(name);
    if (shm == NULL) {
      /* the region died with its creator (Win32 mappings cannot outlive
         theirs): report rather than raise when the caller can absorb it */
      if (gone != NULL) {
        *gone = 1;
        return NULL;
      }
      sora_stop_shm(NA_REAL, "sora: cannot open payload region '%s'", name);
    }
    sora_oc_store(oc, shm);
  }
  return shm;
}

SEXP sora_payload_read(const sora_slot_hdr *hdr, const unsigned char *payload,
                      uint32_t inline_max, int *gone, sora_open_cache *oc,
                      sora_zc_cache *zoc) {
  switch (hdr->kind) {
  case SORA_KIND_NIL:
    return R_NilValue;
  case SORA_KIND_STR1: {
    if (hdr->aux == SORA_STR1_NA) {
      if (hdr->len != 0) Rf_error("sora: corrupt payload slot");
      SEXP y = Rf_allocVector(STRSXP, 1);
      SET_STRING_ELT(y, 0, NA_STRING);
      return y;
    }
    if (hdr->len > inline_max || hdr->aux > CE_BYTES)
      Rf_error("sora: corrupt payload slot");
    SEXP y = PROTECT(Rf_allocVector(STRSXP, 1));
    SET_STRING_ELT(y, 0, Rf_mkCharLenCE((const char *) payload,
                                        (int) hdr->len,
                                        (cetype_t) hdr->aux));
    UNPROTECT(1);
    return y;
  }
  case SORA_KIND_INLINE:
    if (hdr->len > inline_max || hdr->len == 0)
      Rf_error("sora: corrupt payload slot");
    return payload[0] == SORA_CODEC_MAGIC ?
      sora_codec_read(payload, hdr->len) :
      mori_unserialize_from((unsigned char *) payload, hdr->len);
  case SORA_KIND_RAWVEC: {
    int type = (int) hdr->aux;
    size_t elt = mori_sizeof_elt(type);
    if (elt == 0 || hdr->len > inline_max || hdr->len % elt != 0)
      Rf_error("sora: corrupt payload slot");
    SEXP y = Rf_allocVector((SEXPTYPE) type, (R_xlen_t) (hdr->len / elt));
    memcpy(sora_vec_ptr(y), payload, hdr->len);
    return y;
  }
  case SORA_KIND_SHM_VEC:
    return sora_zc_read(hdr, payload, gone, zoc);
  case SORA_KIND_REF:
    return sora_zc_ref_read(hdr, payload, gone, zoc);
  case SORA_KIND_RAWSPILL: {
    /* pool framing: the region name in the payload, its length and the
       SEXPTYPE packed in aux (the channel's arena framing of the same
       kind is resolved in chan_materialize, never reaching here) */
    int type = (int) (hdr->aux & 0xff);
    uint32_t name_len = (uint32_t) (hdr->aux >> 8);
    size_t elt = mori_sizeof_elt(type);
    if (elt == 0 || hdr->len % elt != 0 ||
        name_len == 0 || name_len >= MORI_NAME_MAX)
      Rf_error("sora: corrupt payload slot");
    mori_shm *shm = payload_region_open(payload, name_len, gone, oc);
    if (shm == NULL) return R_NilValue;         /* *gone set */
    if (hdr->len > shm->size) Rf_error("sora: corrupt payload slot");
    SEXP y = Rf_allocVector((SEXPTYPE) type, (R_xlen_t) (hdr->len / elt));
    memcpy(sora_vec_ptr(y), shm->addr, hdr->len);
    return y;
  }
  case SORA_KIND_SHM_RAW: {
    if (hdr->len == 0 || hdr->len >= MORI_NAME_MAX)
      Rf_error("sora: corrupt payload slot");
    mori_shm *shm = payload_region_open(payload, hdr->len, gone, oc);
    if (shm == NULL) return R_NilValue;         /* *gone set */
    /* aux is the exact stream length: a recycled region is larger than the
       stream it carries, and the slack bytes are a previous payload's */
    size_t len = hdr->aux != 0 && hdr->aux <= (uint64_t) shm->size ?
      (size_t) hdr->aux : shm->size;
    unsigned char *stream = (unsigned char *) shm->addr;
    return stream[0] == SORA_CODEC_MAGIC ?
      sora_codec_read(stream, len) : mori_unserialize_from(stream, len);
  }
  }
  Rf_error("sora: corrupt payload slot");
}
