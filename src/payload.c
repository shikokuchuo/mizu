/* Shared payload framing over the mov_slot_hdr wire form — the staging and
   materializing code common to Part I channel slots and Part II pool entries
   / result slots. The channel adds its arena tier around these; the pool has
   no arena (its payloads release at collect or slot reuse — unordered — so a
   producer-local FIFO allocator does not apply) and stages through
   mov_payload_stage directly. */

#include "mov.h"

/* ANY_ATTRIB() joined the C API in R 4.5.0; equivalent fallback for earlier
   R, where ATTRIB() was still the sanctioned spelling. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

void *mov_vec_ptr(SEXP x) {
  switch (TYPEOF(x)) {
  case LGLSXP:  return LOGICAL(x);
  case INTSXP:  return INTEGER(x);
  case REALSXP: return REAL(x);
  case CPLXSXP: return COMPLEX(x);
  case RAWSXP:  return RAW(x);
  }
  return NULL;
}

int mov_raw_eligible(SEXP x, uint32_t inline_max, size_t *out_len) {
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
  size_t n = (size_t) XLENGTH(x) * mori_sizeof_elt(TYPEOF(x));
  if (n > inline_max) return 0;
  *out_len = n;
  return 1;
}

SEXP mov_payload_spill_shm(mov_slot_hdr *hdr, unsigned char *payload, SEXP x,
                           size_t n) {
  mori_shm *shm;
  int rc = mori_shm_create_heap(&shm, n);
  if (rc != MORI_OK) {
    const char *summary, *hint;
    mori_err_describe(rc, &summary, &hint);
    Rf_error("mov: cannot create payload region (%llu bytes): %s%s%s",
             (unsigned long long) n, summary,
             hint[0] != '\0' ? ". " : "", hint);
  }
  SEXP wrap = PROTECT(mov_shm_wrap_producer(shm));
  mori_serialize_into((unsigned char *) shm->addr, n, x);
  hdr->kind = MOV_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = 0;
  memcpy(payload, shm->name, shm->name_len);
  SEXP keep = Rf_allocVector(VECSXP, 2);
  SET_VECTOR_ELT(keep, 0, x);
  SET_VECTOR_ELT(keep, 1, wrap);
  UNPROTECT(1);
  return keep;
}

SEXP mov_payload_stage(mov_slot_hdr *hdr, unsigned char *payload,
                       uint32_t inline_max, SEXP x) {
  size_t rawlen;
  if (mov_raw_eligible(x, inline_max, &rawlen)) {
    memcpy(payload, mov_vec_ptr(x), rawlen);
    hdr->kind = MOV_KIND_RAWVEC;
    hdr->len = (uint32_t) rawlen;
    hdr->aux = (uint64_t) TYPEOF(x);
    return x;
  }
  size_t n = mov_serialize_bounded(payload, inline_max, x);
  if (n <= inline_max) {
    hdr->kind = MOV_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = 0;
    return x;
  }
  return mov_payload_spill_shm(hdr, payload, x, n);
}

SEXP mov_payload_read(const mov_slot_hdr *hdr, const unsigned char *payload,
                      uint32_t inline_max, int *gone) {
  switch (hdr->kind) {
  case MOV_KIND_INLINE:
    if (hdr->len > inline_max)
      Rf_error("mov: corrupt payload slot");
    return mori_unserialize_from((unsigned char *) payload, hdr->len);
  case MOV_KIND_RAWVEC: {
    int type = (int) hdr->aux;
    size_t elt = mori_sizeof_elt(type);
    if (elt == 0 || hdr->len > inline_max || hdr->len % elt != 0)
      Rf_error("mov: corrupt payload slot");
    SEXP y = Rf_allocVector((SEXPTYPE) type, (R_xlen_t) (hdr->len / elt));
    memcpy(mov_vec_ptr(y), payload, hdr->len);
    return y;
  }
  case MOV_KIND_SHM_RAW: {
    char name[MORI_NAME_MAX];
    if (hdr->len == 0 || hdr->len >= MORI_NAME_MAX)
      Rf_error("mov: corrupt payload slot");
    memcpy(name, payload, hdr->len);
    name[hdr->len] = '\0';
    mori_shm *shm = mori_shm_open_heap(name);
    if (shm == NULL) {
      /* the region died with its creator (Win32 mappings cannot outlive
         theirs): report rather than raise when the caller can absorb it */
      if (gone != NULL) {
        *gone = 1;
        return R_NilValue;
      }
      Rf_error("mov: cannot open payload region '%s'", name);
    }
    PROTECT(mov_shm_wrap_consumer(shm));
    SEXP y = mori_unserialize_from((unsigned char *) shm->addr, shm->size);
    UNPROTECT(1);                /* mapping drops with the wrapper's GC */
    return y;
  }
  }
  Rf_error("mov: corrupt payload slot");
}
