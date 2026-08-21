/* The R binding's channel staging half of the language seam (sora.h's
   sora_binding): the SEXP <-> framed-bytes tier dispatch for sends
   (sora_r_stage_channel) and the materialize for receives
   (sora_r_read_channel), registered on every channel handle at
   create/attach and invoked by the transport (channel.c) through
   binding.stage / binding.read. The transport owns the ring, the arena,
   the wakes, and the retain-table commit; this file owns the payload
   framing policy — which tier an object takes. The pool stages through
   sora_payload_stage (payload.c) until its own seam lands. */

#include "sora.h"

// Stage ---------------------------------------------------------------------------

/* chan_send1's tier dispatch: frame x as (hdr, payload) — payload capacity
   inline_max — filling out with what the tier retains (stager-initialized,
   the sora_payload_stage discipline). Arena chunks come from
   sora_stage_arena_alloc, the arena base staying core-private; region
   checkouts ride the zc/payload spill helpers on the handle's free list.
   Raises on failure, never returns nonzero. */
int sora_r_stage_channel(void *obj, sora_slot_hdr *hdr,
                         unsigned char *payload, uint32_t inline_max,
                         void *handle, sora_keeper *out) {
  SEXP x = (SEXP) obj;
  sora_chan *c = (sora_chan *) handle;
  size_t rawlen, total;
  uint64_t off;
  unsigned char *chunk;

  out->region = NULL;
  out->pin = R_NilValue;
  out->key = -1;
  out->kind = SORA_KEEP_FREE;

  /* NULL is the immediate kind — no serialize pass, no receive alloc */
  if (x == R_NilValue) {
    hdr->kind = SORA_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
  } else if (sora_zc_ref_stage(hdr, payload, inline_max, x)) {
    /* a sora-native view crosses by reference (REF) at any size — required
       once SHM_VEC views exist: the serialize-hook fallback resolves
       uncounted, and the producer could recycle under the far side's view;
       the pin is the view itself */
    out->pin = x;
    out->kind = SORA_KEEP_PIN;
  } else if (sora_raw_eligible(x, inline_max, &rawlen)) {
    memcpy(payload, sora_vec_ptr(x), rawlen);
    hdr->kind = SORA_KIND_RAWVEC;
    hdr->len = (uint32_t) rawlen;
    hdr->aux = (uint64_t) TYPEOF(x);
  } else if (sora_str1_stage(hdr, payload, inline_max, x)) {
    /* a length-1 string's bytes are self-contained: pin nothing */
  } else if (sora_raw_type(x, &rawlen) && rawlen > inline_max &&
             rawlen <= UINT32_MAX &&
             (rawlen <= SORA_ZC_FLOOR_RAW || c->fl.churn) &&
             (chunk = sora_stage_arena_alloc(c, MORI_ALIGN64(rawlen),
                                             &off)) != NULL) {
    /* Raw-bytes arena spill: the vectors RAWVEC takes inline, past the
       inline budget. Bare bytes skip the serialize pass here and the
       parse at the far end; the chunk's lifetime tracks ring advance like
       any arena payload, and nothing is pinned (no identifier can ride
       along). Sits ahead of the zc tier up to SORA_ZC_FLOOR_RAW (the
       arena copy beats the view there) and serves as the churn-immune
       fallback past it; an arena miss falls through to zc/serialize. */
    memcpy(chunk, sora_vec_ptr(x), rawlen);
    hdr->kind = SORA_KIND_RAWSPILL;
    hdr->len = (uint32_t) rawlen;
    hdr->aux = (uint64_t) TYPEOF(x);
    memcpy(payload, &off, sizeof(off));
  } else if (!c->fl.churn && sora_zc_eligible(x, inline_max, &total)) {
    /* eligible objects past the budget go straight to SHM_VEC, skipping
       the arena: arena receive pays a full unserialize and a chunk can
       never hold a view (chunk lifetime tracks ring advance). The churn
       gate (Linux-only, spill.c): while lent regions prove consumer
       views outlive their traffic, the copy tiers below are cheaper —
       the arena and SHM_RAW surrender deterministically, where a fresh
       SHM_VEC region per message would pile up in the ledger */
    sora_chan_reap(c, 1);
    sora_zc_stage(hdr, payload, x, total, &c->fl, out);
  } else {
    /* the compact codec ahead of R_Serialize (payload.c): a codec stream
       is self-contained — the writer rejects ALTREP, so no hook-emitted
       mori identifier can ride along — and pins nothing */
    size_t n = sora_codec_write(payload, inline_max, x);
    int self_contained = n != 0;
    if (!self_contained)
      n = sora_serialize_bounded(payload, inline_max, x);
    if (n <= inline_max) {
      hdr->kind = SORA_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = 0;
      if (!self_contained) {
        out->pin = x;
        out->kind = SORA_KEEP_PIN;
      }
    } else {
      if ((chunk = sora_stage_arena_alloc(c, MORI_ALIGN64(n), &off)) != NULL) {
        if (self_contained) {
          if (sora_codec_write(chunk, n, x) != n)
            Rf_error("sora: codec write mismatch");
        } else {
          mori_serialize_into(chunk, x);
        }
        hdr->kind = SORA_KIND_ARENA;
        hdr->len = 0;
        hdr->aux = off;
        uint64_t n64 = (uint64_t) n;
        memcpy(payload, &n64, sizeof(n64));
        if (!self_contained) {
          out->pin = x;
          out->kind = SORA_KEEP_PIN;
        }
      } else {
        /* reap before staging: the consumer's latest head publish may
           have released a fitting region for this very spill to pop */
        sora_chan_reap(c, 1);
        if (self_contained)
          sora_payload_spill_codec(hdr, payload, x, n, &c->fl, out);
        else
          sora_payload_spill_shm(hdr, payload, x, n, &c->fl, out);
      }
    }
  }
  return 0;
}

// Read ----------------------------------------------------------------------------

/* chan_materialize's read half: the transport has already resolved an
   arena-referencing frame to its byte range (payload, limit), so the
   ARENA / channel RAWSPILL kinds below read resolved bytes — the arena
   base never leaves the transport. Everything else defers to the shared
   payload reader on the handle's caches. */
SEXP sora_r_read_channel(const sora_slot_hdr *hdr,
                         const unsigned char *payload, size_t limit,
                         int *gone, void *handle) {
  sora_chan *c = (sora_chan *) handle;

  if (hdr->kind == SORA_KIND_ARENA) {
    /* resolved stream bytes; limit is the arena-validated length */
    return payload[0] == SORA_CODEC_MAGIC ?
      sora_codec_read(payload, limit) :
      mori_unserialize_from((unsigned char *) payload, limit);
  }
  if (hdr->kind == SORA_KIND_RAWSPILL) {
    /* resolved RAWVEC bytes (the pool's region framing of this kind is
       read in sora_payload_read) */
    int type = (int) hdr->aux;
    size_t elt = mori_sizeof_elt(type);
    if (elt == 0 || hdr->len % elt != 0)
      Rf_error("sora: corrupt payload slot");
    SEXP y = Rf_allocVector((SEXPTYPE) type, (R_xlen_t) (hdr->len / elt));
    memcpy(sora_vec_ptr(y), payload, hdr->len);
    return y;
  }
  return sora_payload_read(hdr, payload, (uint32_t) limit, gone, &c->oc,
                           &c->zoc);
}
