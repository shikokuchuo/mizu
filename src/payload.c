/* Shared payload framing over the mizu_slot_hdr wire form — the staging and
   materializing code common to Part I channel slots and Part II pool entries
   / result slots. The channel adds its arena tier around these; the pool has
   no arena (its payloads release at collect or slot reuse — unordered — so a
   producer-local FIFO allocator does not apply) and stages through
   mizu_payload_stage directly. */

#include "mizu.h"

/* ANY_ATTRIB() joined the C API in R 4.5.0; equivalent fallback for earlier
   R, where ATTRIB() was still the sanctioned spelling. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

/* The vendored view layer owns the macro spellings (mori renames them); pin
   them to the core's wire-format numbers here, where both are visible. */
_Static_assert(MIZU_VIEW_TYPE_INT64 == MIZU_TYPE_INT64,
               "view/core int64 tag drift");
_Static_assert(MIZU_VIEW_FLAGS_OFF == MIZU_HDR_FLAGS_OFF,
               "view/core flags-word offset drift");
_Static_assert(MIZU_VIEW_FLAG_S4 == MIZU_HDR_FLAG_S4,
               "view/core S4 flag drift");
_Static_assert(MIZU_VIEW_ELEM_S4 == MIZU_MIZL_S4,
               "view/core directory S4 bit drift");
/* The MIZU_CE_* encoding constants are R's cetype_t, hoisted to the wire
   contract. */
_Static_assert(MIZU_CE_NATIVE == CE_NATIVE, "CE_NATIVE drift");
_Static_assert(MIZU_CE_UTF8 == CE_UTF8, "CE_UTF8 drift");
_Static_assert(MIZU_CE_LATIN1 == CE_LATIN1, "CE_LATIN1 drift");
_Static_assert(MIZU_CE_BYTES == CE_BYTES, "CE_BYTES drift");

void *mizu_vec_ptr(SEXP x) {
  switch (TYPEOF(x)) {
  case LGLSXP:  return LOGICAL(x);
  case INTSXP:  return INTEGER(x);
  case REALSXP: return REAL(x);
  case CPLXSXP: return COMPLEX(x);
  case RAWSXP:  return RAW(x);
  }
  return NULL;
}

/* The single raw gate of a stage: the wire type code (0 = ineligible) with
   the byte length; each caller applies its own size gate. Ordering keeps the
   hot path free — only an attributed REALSXP pays the mizu_view_is_int64
   probe: a class-only integer64's class is consumed by the wire tag. */
int mizu_raw_type(SEXP x, size_t *out_len) {
  switch (TYPEOF(x)) {
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
    break;
  default:
    return 0;
  }
  /* The ALTREP exclusion is the linkage-free gate keeping shared
     vectors on the hook path; the S4 bit is recorded by serialize, so it
     disqualifies alongside attributes. */
  if (ALTREP(x) || Rf_isS4(x)) return 0;
  int code = TYPEOF(x);
  if (ANY_ATTRIB(x)) {
    if (!mizu_view_is_int64(x)) return 0;
    code = MIZU_TYPE_INT64;
  }
  *out_len = (size_t) XLENGTH(x) * mizu_view_sizeof_elt(code);
  return code;
}

/* Wire type -> fresh vector for the raw read paths: an int64 payload lands
   as bit64's exact layout (REALSXP + class "integer64", constructed
   directly — bit64 stays in Suggests); every other code is a SEXPTYPE. */
SEXP mizu_wire_alloc(int type, R_xlen_t n) {
  if (type == MIZU_TYPE_INT64) {
    SEXP y = PROTECT(Rf_allocVector(REALSXP, n));
    Rf_classgets(y, mizu_view_int64_class);
    UNPROTECT(1);
    return y;
  }
  return Rf_allocVector((SEXPTYPE) type, n);
}

int mizu_str1_stage(mizu_slot_hdr *hdr, unsigned char *payload,
                   uint32_t inline_max, SEXP x) {
  /* The ALTREP exclusion keeps foreign ALTSTRINGs on the serialize path
     (their Elt may materialize); mizu's own string views never reach here
     (the REF check upstream claims them first) */
  if (TYPEOF(x) != STRSXP || XLENGTH(x) != 1 || ALTREP(x) ||
      ANY_ATTRIB(x) || Rf_isS4(x))
    return 0;
  SEXP s = STRING_ELT(x, 0);
  if (s == NA_STRING) {
    hdr->kind = MIZU_KIND_STR1;
    hdr->len = 0;
    hdr->aux = MIZU_STR1_NA;
    return 1;
  }
  size_t n = (size_t) LENGTH(s);
  if (n > (size_t) inline_max) return 0;
  hdr->kind = MIZU_KIND_STR1;
  hdr->len = (uint32_t) n;
  hdr->aux = (uint64_t) Rf_getCharCE(s);
  memcpy(payload, CHAR(s), n);
  return 1;
}

/* The shared empty args list of a no-argument task: one preserved vector
   serves both the submitter (mizu_submit's capture) and the worker (the
   task-frame read), so a constant task allocates no VECSXP(0) on either
   side. Read-only everywhere it appears — marked not-mutable, so a stray
   write fails loudly instead of corrupting every task. */
static SEXP empty_args;

SEXP mizu_empty_args(void) {
  return empty_args;
}

void mizu_payload_init(void) {
  empty_args = Rf_allocVector(VECSXP, 0);
  R_PreserveObject(empty_args);
#if R_VERSION >= R_Version(4, 5, 0)
  MARK_NOT_MUTABLE(empty_args);
#endif
}

void mizu_payload_fini(void) {
  R_ReleaseObject(empty_args);
}

// Spill staging ------------------------------------------------------------

/* The service-form checkout: mizu_stage_spill_get, raising mizu_error_shm on
   create failure (the stager's raise-on-failure discipline). */
mizu_shm *mizu_spill_get_raise(mizu_handle *h, size_t n) {
  mizu_shm *shm;
  if (mizu_stage_spill_get(h, n, &shm) != MIZU_OK) {
    const char *summary, *hint;
    mizu_err_describe(mizu_last_error_category(), &summary, &hint);
    mizu_stop_shm((double) n,
                 "mizu: cannot create payload region (%llu bytes): %s%s%s",
                 (unsigned long long) n, summary,
                 hint[0] != '\0' ? ". " : "", hint);
  }
  return shm;
}

/* Each stages through the handle's services: the region checkout
   (mizu_stage_spill_get), the retain (mizu_stage_retain), and for the
   serialize stream the pin of x (mizu_r_pin, released through the binding's
   drop hook) — taken only when the REF-used flag fired during the serialize
   passes (a view crosses by reference inside the stream; SHM_RAW has no
   free aux bit, so the skip is the whole win here). The pin precedes
   retain: its cons-cell push can longjmp, and an uncommitted checkout
   rolls back with nothing pinned. */

void mizu_payload_spill_shm(mizu_slot_hdr *hdr, unsigned char *payload, SEXP x,
                            size_t n, mizu_handle *h, void *ctx) {
  mizu_shm *shm = mizu_spill_get_raise(h, n);
  mizu_view_serialize_into((unsigned char *) shm->addr, x);
  hdr->kind = MIZU_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;
  memcpy(payload, shm->name, shm->name_len);
  if (mizu_zc_ref_fired())
    mizu_r_pin(h, ctx, x);
  mizu_stage_retain(h, shm);
}

/* The SHM_RAW spill of a codec stream (n from the counting first pass):
   the region alone is retained — the writer rejected ALTREP, so no
   hook-emitted identifier can ride along. */
void mizu_payload_spill_codec(mizu_slot_hdr *hdr, unsigned char *payload,
                              SEXP x, size_t n, mizu_handle *h) {
  mizu_shm *shm = mizu_spill_get_raise(h, n);
  if (mizu_codec_write((unsigned char *) shm->addr, shm->size, x) != n)
    Rf_error("mizu: codec write mismatch");   /* the walk is deterministic */
  hdr->kind = MIZU_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;
  memcpy(payload, shm->name, shm->name_len);
  mizu_stage_retain(h, shm);
}

/* The SHM_RAW spill of an interop stream (n from the counting first
   pass): the region alone is retained — the stream is self-contained
   (no view identifier can ride along, the codec discipline). */
void mizu_payload_spill_interop(mizu_slot_hdr *hdr, unsigned char *payload,
                                SEXP x, size_t n, mizu_handle *h) {
  mizu_shm *shm = mizu_spill_get_raise(h, n);
  if (mizu_interop_write((unsigned char *) shm->addr, shm->size, x, NULL) != n)
    Rf_error("mizu: interop write mismatch");   /* the walk is deterministic */
  hdr->kind = MIZU_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;
  memcpy(payload, shm->name, shm->name_len);
  mizu_stage_retain(h, shm);
}

/* The SHM_RAW spill of a task stream past the inline budget (n from the
   counting first pass): the ordinary SHM_RAW retain, no keeperless claim
   — a task entry pins nothing the collect-side gate would read (§4.0).
   Refs stay on with no_zc forced (a REF leaf needs no checkout, and the
   single-checkout rule keeps a spilled stream free of SHM_VEC checkouts). */
void mizu_payload_spill_task(mizu_slot_hdr *hdr, unsigned char *payload,
                             SEXP spec, uint32_t target, uint64_t ident,
                             size_t n, mizu_handle *h, uint32_t caps,
                             uint32_t inline_max) {
  mizu_shm *shm = mizu_spill_get_raise(h, n);
  if (mizu_interop_write_task((unsigned char *) shm->addr, shm->size, spec,
                              target, ident, NULL, h, caps, inline_max,
                              R_NilValue, 1) != n)
    Rf_error("mizu: task write mismatch");   /* the walk is deterministic */
  hdr->kind = MIZU_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;
  memcpy(payload, shm->name, shm->name_len);
  mizu_stage_retain(h, shm);
}

/* The NIL, RAWVEC, and STR1 kinds retain nothing: their slot bytes are
   self-contained (RAWVEC and STR1 exclude ALTREP, attributes, and S4, so
   no hook-emitted identifier can ride along), unlike the serialize tiers,
   where a stream may carry view identifiers whose views the pin keeps
   alive until consumer-done. ctx is the stage hook's binding ctx. */
void mizu_payload_stage(mizu_slot_hdr *hdr, unsigned char *payload,
                        uint32_t inline_max, SEXP x, mizu_handle *h,
                        void *ctx) {
  mizu_r_handle *rh = (mizu_r_handle *) ctx;
  /* the reader-language state (one predicted branch per stage): a foreign
     pool handle is the §4.2 foreign result, its zero-copy admission the
     submitter's capability mask; same-language handles run unchanged */
  const int foreign = rh->peer_lang != 0 && rh->peer_lang != MIZU_LANG_R;
  size_t rawlen, total;
  /* NULL stages as the immediate kind: the canonical empty result / ACK
     pays no serialize pass and no receive-side allocation */
  if (x == R_NilValue) {
    hdr->kind = MIZU_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
    return;
  }
  /* a mizu-native view crosses by reference (REF) at any size — required
     once SHM_VEC views exist: the serialize-hook fallback resolves
     uncounted, and the producer could recycle under the far side's view.
     The pin keeps the view (and with it the region) until consumer-done.
     The foreign filter runs ahead of the claim: a view whose layout the
     submitter cannot wrap takes the interop writer below as a value copy
     off the shared pages. */
  if ((!foreign || mizu_zc_ref_foreign_ok(x, rh->peer_caps)) &&
      mizu_zc_ref_stage(hdr, payload, inline_max, x)) {
    mizu_r_pin(h, ctx, x);
    return;
  }
  /* one raw probe per stage: the code drives the core's raw-tier
     reservation below (0 on the STR1 / SHM_VEC-layout / codec paths) */
  int rawtype = mizu_raw_type(x, &rawlen);
  if (rawtype != 0) {
    /* the raw tiers (mizu_stage_raw, the core's policy): RAWVEC inline, the
       flat SHM_VEC layout past the zc floor, else a RAWSPILL region — bare
       bytes skip both the serialize pass here and the parse at the far
       end, and nothing is pinned (no identifier can ride along). A NULL
       reservation falls to the serialized tiers below. */
    unsigned char *dst = mizu_stage_raw(h, rawlen, rawtype, hdr, payload,
                                       inline_max);
    if (dst != NULL) {
      memcpy(dst, mizu_vec_ptr(x), rawlen);
      return;
    }
  } else if (foreign) {
    /* the foreign STR1: a length-1 string crosses normalized to UTF-8 */
    mizu_ix_decline rec = { 0, "", "", "" };
    int s1 = mizu_interop_str1_foreign(hdr, payload, inline_max, x, &rec);
    if (s1 > 0) return;
    if (s1 < 0) mizu_stop_not_portable(rec.path, rec.reason, rec.remedy);
  } else if (mizu_str1_stage(hdr, payload, inline_max, x)) {
    return;
  } else if (mizu_zc_eligible(x, inline_max, &total, 0) &&
             !mizu_handle_churn(h)) {
    /* SHM_VEC: view-layout-eligible objects (strings, list trees) past
       the budget and the zc floor — cheap probes keep the layout-size
       walk off the inline path (zc.c). Under churn (the last spill miss
       swept the lent ledger and reclaimed nothing — a Linux-only signal)
       the fresh region per SHM_VEC payload is dearer than the serialize
       copy: fall to SHM_RAW, whose region surrenders deterministically at
       consumer-done. */
    mizu_zc_stage(hdr, payload, x, total, h, ctx, 0);
    return;
  }
  /* the reader-language branch: a foreign result writes interop only, the
     zero-copy admission gated on the submitter's capability mask — a
     decline raises at publish, preflighted by the exec's gate (§4.2) */
  if (foreign) {
    if (rawtype == 0 &&
        mizu_zc_eligible_foreign(x, inline_max, &total, rh->peer_caps) &&
        !mizu_handle_churn(h)) {
      /* the foreign zero-copy gate: the baseline layouts plus whatever
         the submitter's mask advertises; the layout write builds the
         validity-bitmap section for the foreign reader */
      mizu_zc_stage(hdr, payload, x, total, h, ctx, 1);
      return;
    }
    mizu_ix_decline rec;
    size_t n = mizu_interop_write(payload, inline_max, x, &rec);
    if (n == 0) mizu_stop_not_portable(rec.path, rec.reason, rec.remedy);
    if (n <= inline_max) {
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = MIZU_AUX_F_KEEPERLESS;
      return;
    }
    mizu_payload_spill_interop(hdr, payload, x, n, h);
    return;
  }
  /* the compact codec ahead of R_Serialize: no per-call ref-table
     allocation on either side, and a self-contained stream (the writer
     rejects ALTREP, so no view identifier can ride along) that pins
     nothing — the NIL/RAWVEC/STR1 discipline, claimed on the wire */
  size_t n = mizu_codec_write(payload, inline_max, x);
  if (n != 0) {
    if (n <= inline_max) {
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = MIZU_AUX_F_KEEPERLESS;
      return;
    }
    mizu_payload_spill_codec(hdr, payload, x, n, h);
    return;
  }
  /* R_Serialize: the stream may carry a view by reference, so pin only
     when the emit hook fired during the pass — an unpinned stream is
     self-contained and claims keeperless. The flag stays live for the
     spill path's pin decision below. */
  mizu_zc_ref_reset();
  n = mizu_serialize_bounded(payload, inline_max, x);
  if (n <= inline_max) {
    hdr->kind = MIZU_KIND_INLINE;
    hdr->len = (uint32_t) n;
    if (mizu_zc_ref_fired()) {
      hdr->aux = 0;
      mizu_r_pin(h, ctx, x);
    } else {
      hdr->aux = MIZU_AUX_F_KEEPERLESS;
    }
    return;
  }
  mizu_payload_spill_shm(hdr, payload, x, n, h, ctx);
}

// Read -----------------------------------------------------------------------

/* Materialize an INLINE / RAWVEC / SHM_RAW payload, or wrap a SHM_VEC / REF
   payload as an ALTREP view. Region opens ride the handle's open cache
   through mizu_read_region (which sets ctx->gone on a vanished region — the
   read_fn then propagates by returning NULL); the view tiers open their own
   split mappings through the R-side cache zoc (ctx->binding_ctx). A foreign
   (Python) stream on a serialize tier: with consume_foreign (the channel)
   stash the interned decline condition on the handle and fail the read with
   MIZU_READ_CONSUME, so the slot is consumed before the veneer signals it;
   without it (the pool) raise in place — a pool is R-only, so a foreign
   stream there is corruption. */
SEXP mizu_payload_read(const mizu_slot_hdr *hdr, const unsigned char *payload,
                       uint32_t inline_max, mizu_read_ctx *ctx,
                       int consume_foreign) {
  mizu_zc_cache *zoc = &((mizu_r_handle *) ctx->binding_ctx)->zoc;
  switch (hdr->kind) {
  case MIZU_KIND_NIL:
    return R_NilValue;
  case MIZU_KIND_STR1: {
    if (hdr->aux == MIZU_STR1_NA) {
      if (hdr->len != 0) Rf_error("mizu: corrupt payload slot");
      SEXP y = Rf_allocVector(STRSXP, 1);
      SET_STRING_ELT(y, 0, NA_STRING);
      return y;
    }
    if (hdr->len > inline_max || hdr->aux > CE_BYTES)
      Rf_error("mizu: corrupt payload slot");
    SEXP y = PROTECT(Rf_allocVector(STRSXP, 1));
    SET_STRING_ELT(y, 0, Rf_mkCharLenCE((const char *) payload,
                                        (int) hdr->len,
                                        (cetype_t) hdr->aux));
    UNPROTECT(1);
    return y;
  }
  case MIZU_KIND_INLINE:
    if (hdr->len > inline_max || hdr->len == 0)
      Rf_error("mizu: corrupt payload slot");
    return mizu_stream_read(payload, hdr->len, ctx, consume_foreign);
  case MIZU_KIND_RAWVEC: {
    int type = (int) hdr->aux;
    size_t elt = mizu_view_sizeof_elt(type);
    if (elt == 0 || hdr->len > inline_max || hdr->len % elt != 0)
      Rf_error("mizu: corrupt payload slot");
    SEXP y = mizu_wire_alloc(type, (R_xlen_t) (hdr->len / elt));
    memcpy(mizu_vec_ptr(y), payload, hdr->len);
    return y;
  }
  case MIZU_KIND_SHM_VEC: {
    SEXP v = mizu_zc_read(hdr, payload, &ctx->gone, zoc);
    return ctx->gone ? NULL : v;
  }
  case MIZU_KIND_REF: {
    SEXP v = mizu_zc_ref_read(hdr, payload, &ctx->gone, zoc);
    return ctx->gone ? NULL : v;
  }
  case MIZU_KIND_RAWSPILL: {
    /* pool framing: the region name in the payload, its length and the
       SEXPTYPE packed in aux (the channel's arena framing of the same
       kind is resolved by the transport, never reaching here) */
    int type = mizu_aux_type(hdr->aux);
    uint32_t name_len = (uint32_t) mizu_aux_hi(hdr->aux);
    size_t elt = mizu_view_sizeof_elt(type);
    if (elt == 0 || hdr->len % elt != 0 ||
        name_len == 0 || name_len >= MIZU_NAME_MAX)
      Rf_error("mizu: corrupt payload slot");
    mizu_shm *shm = mizu_read_region(ctx, payload, name_len);
    if (shm == NULL) return NULL;         /* ctx->gone set */
    if (hdr->len > shm->size) Rf_error("mizu: corrupt payload slot");
    SEXP y = mizu_wire_alloc(type, (R_xlen_t) (hdr->len / elt));
    memcpy(mizu_vec_ptr(y), shm->addr, hdr->len);
    return y;
  }
  case MIZU_KIND_SHM_RAW: {
    if (hdr->len == 0 || hdr->len >= MIZU_NAME_MAX)
      Rf_error("mizu: corrupt payload slot");
    mizu_shm *shm = mizu_read_region(ctx, payload, hdr->len);
    if (shm == NULL) return NULL;         /* ctx->gone set */
    /* aux is the exact stream length: a recycled region is larger than the
       stream it carries, and the slack bytes are a previous payload's */
    size_t len = hdr->aux != 0 && hdr->aux <= (uint64_t) shm->size ?
      (size_t) hdr->aux : shm->size;
    return mizu_stream_read((unsigned char *) shm->addr, len, ctx,
                            consume_foreign);
  }
  }
  Rf_error("mizu: corrupt payload slot");
}

// The stream dispatch ---------------------------------------------------------------

/* The one first-byte dispatch of the serialize tiers (DESIGN.md's codec
   registry): 'I' the interchange stream ahead of 'R' the compact codec,
   then 'B'/'X'/'A' the R native streams. Anything else — 'P', pickle, or
   an unlisted byte — is the informative decline: consumed with the
   interned condition on the channel (a plain failure would wedge the
   ring behind the slot), raised in place on the pool (a pool is R-only,
   so a foreign stream there is corruption). */
SEXP mizu_stream_read(const unsigned char *buf, size_t len,
                      mizu_read_ctx *ctx, int consume_foreign) {
  if (len == 0) Rf_error("mizu: corrupt payload stream");
  switch (buf[0]) {
  case MIZU_INTEROP_MAGIC:
    /* the pool's collect-side result reader resolves a 0x13 leaf (a
       foreign result may carry one); the channel value reader declines */
    return mizu_interop_read_mode(buf, len, !consume_foreign);
  case MIZU_CODEC_MAGIC:
    return mizu_codec_read(buf, len);
  case 'B': case 'X': case 'A':
    return mizu_view_unserialize_from((unsigned char *) buf, len);
  default:
    if (consume_foreign) {
      mizu_decline_foreign(ctx);
      return NULL;
    }
    mizu_stop_python_payload();
  }
}
