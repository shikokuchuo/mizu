/* Shared payload framing over the rei_slot_hdr wire form — the staging and
   materializing code common to Part I channel slots and Part II pool entries
   / result slots. The channel adds its arena tier around these; the pool has
   no arena (its payloads release at collect or slot reuse — unordered — so a
   producer-local FIFO allocator does not apply) and stages through
   rei_payload_stage directly. */

#include "rei.h"

/* ANY_ATTRIB() joined the C API in R 4.5.0; equivalent fallback for earlier
   R, where ATTRIB() was still the sanctioned spelling. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

void *rei_vec_ptr(SEXP x) {
  switch (TYPEOF(x)) {
  case LGLSXP:  return LOGICAL(x);
  case INTSXP:  return INTEGER(x);
  case REALSXP: return REAL(x);
  case CPLXSXP: return COMPLEX(x);
  case RAWSXP:  return RAW(x);
  }
  return NULL;
}

int rei_raw_type(SEXP x, size_t *out_len) {
  switch (TYPEOF(x)) {
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
    break;
  default:
    return 0;
  }
  /* The ALTREP exclusion is the linkage-free gate keeping shared
     vectors on the hook path; the S4 bit is recorded by serialize, so it
     disqualifies alongside attributes. */
  if (ALTREP(x) || ANY_ATTRIB(x) || Rf_isS4(x)) return 0;
  *out_len = (size_t) XLENGTH(x) * rei_view_sizeof_elt(TYPEOF(x));
  return 1;
}

int rei_raw_eligible(SEXP x, uint32_t inline_max, size_t *out_len) {
  size_t n;
  if (!rei_raw_type(x, &n) || n > inline_max) return 0;
  *out_len = n;
  return 1;
}

int rei_str1_stage(rei_slot_hdr *hdr, unsigned char *payload,
                   uint32_t inline_max, SEXP x) {
  /* The ALTREP exclusion keeps foreign ALTSTRINGs on the serialize path
     (their Elt may materialize); rei's own string views never reach here
     (the REF check upstream claims them first) */
  if (TYPEOF(x) != STRSXP || XLENGTH(x) != 1 || ALTREP(x) ||
      ANY_ATTRIB(x) || Rf_isS4(x))
    return 0;
  SEXP s = STRING_ELT(x, 0);
  if (s == NA_STRING) {
    hdr->kind = REI_KIND_STR1;
    hdr->len = 0;
    hdr->aux = REI_STR1_NA;
    return 1;
  }
  size_t n = (size_t) LENGTH(s);
  if (n > (size_t) inline_max) return 0;
  hdr->kind = REI_KIND_STR1;
  hdr->len = (uint32_t) n;
  hdr->aux = (uint64_t) Rf_getCharCE(s);
  memcpy(payload, CHAR(s), n);
  return 1;
}

/* The shared empty args list of a no-argument task: one preserved vector
   serves both the submitter (rei_submit's capture) and the worker (the
   task-frame read), so a constant task allocates no VECSXP(0) on either
   side. Read-only everywhere it appears — marked not-mutable, so a stray
   write fails loudly instead of corrupting every task. */
static SEXP empty_args;

SEXP rei_empty_args(void) {
  return empty_args;
}

void rei_payload_init(void) {
  empty_args = Rf_allocVector(VECSXP, 0);
  R_PreserveObject(empty_args);
#if R_VERSION >= R_Version(4, 5, 0)
  MARK_NOT_MUTABLE(empty_args);
#endif
}

void rei_payload_fini(void) {
  R_ReleaseObject(empty_args);
}

// Spill staging ------------------------------------------------------------

/* The service-form checkout: rei_stage_spill_get, raising rei_error_shm on
   create failure (the stager's raise-on-failure discipline). */
rei_shm *rei_spill_get_raise(rei_handle *h, size_t n) {
  rei_shm *shm;
  if (rei_stage_spill_get(h, n, &shm) != REI_OK) {
    const char *summary, *hint;
    rei_err_describe(rei_last_error_category(), &summary, &hint);
    rei_stop_shm((double) n,
                 "rei: cannot create payload region (%llu bytes): %s%s%s",
                 (unsigned long long) n, summary,
                 hint[0] != '\0' ? ". " : "", hint);
  }
  return shm;
}

/* Each stages through the handle's services: the region checkout
   (rei_stage_spill_get), the retain (rei_stage_retain), and for the
   serialize stream the pin of x (R_PreserveObject + rei_stage_pin, released
   through the binding's drop hook). PreserveObject precedes retain: it can
   longjmp, and an uncommitted checkout rolls back with nothing pinned. */

void rei_payload_spill_shm(rei_slot_hdr *hdr, unsigned char *payload, SEXP x,
                            size_t n, rei_handle *h) {
  rei_shm *shm = rei_spill_get_raise(h, n);
  rei_view_serialize_into((unsigned char *) shm->addr, x);
  hdr->kind = REI_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;
  memcpy(payload, shm->name, shm->name_len);
  R_PreserveObject(x);
  rei_stage_retain(h, shm);
  rei_stage_pin(h, (void *) x);
}

void rei_payload_spill_raw(rei_slot_hdr *hdr, unsigned char *payload, SEXP x,
                            size_t n, rei_handle *h) {
  rei_shm *shm = rei_spill_get_raise(h, n);
  memcpy(shm->addr, rei_vec_ptr(x), n);
  hdr->kind = REI_KIND_RAWSPILL;
  hdr->len = (uint32_t) n;
  hdr->aux = (uint64_t) TYPEOF(x) | ((uint64_t) shm->name_len << 8);
  memcpy(payload, shm->name, shm->name_len);
  rei_stage_retain(h, shm);   /* bare bytes carry no identifier: no pin */
}

/* The SHM_RAW spill of a codec stream (n from the counting first pass):
   the region alone is retained — the writer rejected ALTREP, so no
   hook-emitted identifier can ride along. */
void rei_payload_spill_codec(rei_slot_hdr *hdr, unsigned char *payload,
                              SEXP x, size_t n, rei_handle *h) {
  rei_shm *shm = rei_spill_get_raise(h, n);
  if (rei_codec_write((unsigned char *) shm->addr, shm->size, x) != n)
    Rf_error("rei: codec write mismatch");   /* the walk is deterministic */
  hdr->kind = REI_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;
  memcpy(payload, shm->name, shm->name_len);
  rei_stage_retain(h, shm);
}

/* The NIL, RAWVEC, and STR1 kinds retain nothing: their slot bytes are
   self-contained (RAWVEC and STR1 exclude ALTREP, attributes, and S4, so
   no hook-emitted identifier can ride along), unlike the serialize tiers,
   where a stream may carry view identifiers whose views the pin keeps
   alive until consumer-done. */
void rei_payload_stage(rei_slot_hdr *hdr, unsigned char *payload,
                        uint32_t inline_max, SEXP x, rei_handle *h) {
  size_t rawlen, total;
  /* NULL stages as the immediate kind: the canonical empty result / ACK
     pays no serialize pass and no receive-side allocation */
  if (x == R_NilValue) {
    hdr->kind = REI_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
    return;
  }
  /* a rei-native view crosses by reference (REF) at any size — required
     once SHM_VEC views exist: the serialize-hook fallback resolves
     uncounted, and the producer could recycle under the far side's view.
     The pin keeps the view (and with it the region) until consumer-done. */
  if (rei_zc_ref_stage(hdr, payload, inline_max, x)) {
    R_PreserveObject(x);
    rei_stage_pin(h, (void *) x);
    return;
  }
  if (rei_raw_eligible(x, inline_max, &rawlen)) {
    memcpy(payload, rei_vec_ptr(x), rawlen);
    hdr->kind = REI_KIND_RAWVEC;
    hdr->len = (uint32_t) rawlen;
    hdr->aux = (uint64_t) TYPEOF(x);
    return;
  }
  if (rei_str1_stage(hdr, payload, inline_max, x))
    return;
  /* SHM_VEC: view-layout-eligible objects (atomic vectors, strings, list
     trees) past the budget and the zc floor — cheap probes keep the
     layout-size walk off the inline path (zc.c). Under churn (the last
     spill miss swept the lent ledger and reclaimed nothing — a Linux-
     only signal) the fresh region per SHM_VEC payload is dearer than the
     serialize copy: fall to SHM_RAW, whose region surrenders
     deterministically at consumer-done. */
  if (rei_zc_eligible(x, inline_max, &total) &&
      !rei_handle_churn(h)) {
    rei_zc_stage(hdr, payload, x, total, h);
    return;
  }
  /* Raw-bytes spill: the vectors RAWVEC takes inline, past the inline
     budget — the layout tier above already passed (under the zc floor, or
     churn-gated), and bare bytes skip both the serialize pass here and
     the parse at the far end. */
  size_t n;
  if (rei_raw_type(x, &n) && n <= UINT32_MAX) {
    rei_payload_spill_raw(hdr, payload, x, n, h);
    return;
  }
  /* the compact codec ahead of R_Serialize: no per-call ref-table
     allocation on either side, and a self-contained stream (the writer
     rejects ALTREP, so no view identifier can ride along) that pins
     nothing — the NIL/RAWVEC/STR1 discipline */
  n = rei_codec_write(payload, inline_max, x);
  if (n != 0) {
    if (n <= inline_max) {
      hdr->kind = REI_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = 0;
      return;
    }
    rei_payload_spill_codec(hdr, payload, x, n, h);
    return;
  }
  n = rei_serialize_bounded(payload, inline_max, x);
  if (n <= inline_max) {
    hdr->kind = REI_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = 0;
    R_PreserveObject(x);
    rei_stage_pin(h, (void *) x);
    return;
  }
  rei_payload_spill_shm(hdr, payload, x, n, h);
}

// Read -----------------------------------------------------------------------

/* Materialize an INLINE / RAWVEC / SHM_RAW payload, or wrap a SHM_VEC / REF
   payload as an ALTREP view. Region opens ride the handle's open cache
   through rei_read_region (which sets ctx->gone on a vanished region — the
   read_fn then propagates by returning NULL); the view tiers open their own
   split mappings through the R-side cache zoc (ctx->binding_ctx). */
SEXP rei_payload_read(const rei_slot_hdr *hdr, const unsigned char *payload,
                       uint32_t inline_max, rei_read_ctx *ctx, SEXP foreign) {
  rei_zc_cache *zoc = &((rei_r_handle *) ctx->binding_ctx)->zoc;
  switch (hdr->kind) {
  case REI_KIND_NIL:
    return R_NilValue;
  case REI_KIND_STR1: {
    if (hdr->aux == REI_STR1_NA) {
      if (hdr->len != 0) Rf_error("rei: corrupt payload slot");
      SEXP y = Rf_allocVector(STRSXP, 1);
      SET_STRING_ELT(y, 0, NA_STRING);
      return y;
    }
    if (hdr->len > inline_max || hdr->aux > CE_BYTES)
      Rf_error("rei: corrupt payload slot");
    SEXP y = PROTECT(Rf_allocVector(STRSXP, 1));
    SET_STRING_ELT(y, 0, Rf_mkCharLenCE((const char *) payload,
                                        (int) hdr->len,
                                        (cetype_t) hdr->aux));
    UNPROTECT(1);
    return y;
  }
  case REI_KIND_INLINE:
    if (hdr->len > inline_max || hdr->len == 0)
      Rf_error("rei: corrupt payload slot");
    if (payload[0] == REI_CODEC_MAGIC)
      return rei_codec_read(payload, hdr->len);
    if (rei_is_python_payload(payload, hdr->len)) {
      if (foreign != NULL) {
        ((rei_r_handle *) ctx->binding_ctx)->saw_foreign = 1;
        return foreign;
      }
      rei_stop_python_payload();
    }
    return rei_view_unserialize_from((unsigned char *) payload, hdr->len);
  case REI_KIND_RAWVEC: {
    int type = (int) hdr->aux;
    size_t elt = rei_view_sizeof_elt(type);
    if (elt == 0 || hdr->len > inline_max || hdr->len % elt != 0)
      Rf_error("rei: corrupt payload slot");
    SEXP y = Rf_allocVector((SEXPTYPE) type, (R_xlen_t) (hdr->len / elt));
    memcpy(rei_vec_ptr(y), payload, hdr->len);
    return y;
  }
  case REI_KIND_SHM_VEC: {
    SEXP v = rei_zc_read(hdr, payload, &ctx->gone, zoc);
    return ctx->gone ? NULL : v;
  }
  case REI_KIND_REF: {
    SEXP v = rei_zc_ref_read(hdr, payload, &ctx->gone, zoc);
    return ctx->gone ? NULL : v;
  }
  case REI_KIND_RAWSPILL: {
    /* pool framing: the region name in the payload, its length and the
       SEXPTYPE packed in aux (the channel's arena framing of the same
       kind is resolved by the transport, never reaching here) */
    int type = (int) (hdr->aux & 0xff);
    uint32_t name_len = (uint32_t) (hdr->aux >> 8);
    size_t elt = rei_view_sizeof_elt(type);
    if (elt == 0 || hdr->len % elt != 0 ||
        name_len == 0 || name_len >= REI_NAME_MAX)
      Rf_error("rei: corrupt payload slot");
    rei_shm *shm = rei_read_region(ctx, payload, name_len);
    if (shm == NULL) return NULL;         /* ctx->gone set */
    if (hdr->len > shm->size) Rf_error("rei: corrupt payload slot");
    SEXP y = Rf_allocVector((SEXPTYPE) type, (R_xlen_t) (hdr->len / elt));
    memcpy(rei_vec_ptr(y), shm->addr, hdr->len);
    return y;
  }
  case REI_KIND_SHM_RAW: {
    if (hdr->len == 0 || hdr->len >= REI_NAME_MAX)
      Rf_error("rei: corrupt payload slot");
    rei_shm *shm = rei_read_region(ctx, payload, hdr->len);
    if (shm == NULL) return NULL;         /* ctx->gone set */
    /* aux is the exact stream length: a recycled region is larger than the
       stream it carries, and the slack bytes are a previous payload's */
    size_t len = hdr->aux != 0 && hdr->aux <= (uint64_t) shm->size ?
      (size_t) hdr->aux : shm->size;
    unsigned char *stream = (unsigned char *) shm->addr;
    if (stream[0] == REI_CODEC_MAGIC)
      return rei_codec_read(stream, len);
    if (rei_is_python_payload(stream, len)) {
      if (foreign != NULL) {
        ((rei_r_handle *) ctx->binding_ctx)->saw_foreign = 1;
        return foreign;
      }
      rei_stop_python_payload();
    }
    return rei_view_unserialize_from(stream, len);
  }
  }
  Rf_error("rei: corrupt payload slot");
}
