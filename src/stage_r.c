/* The R binding's half of the language seam (mizu.h's mizu_binding). For the
   channel: the SEXP <-> framed-bytes tier dispatch for sends
   (mizu_r_stage_channel) and the materialize for receives
   (mizu_r_read_channel). For the pool: the stage_fn every handle registers
   (mizu_r_stage_pool), the collect read_fn (mizu_r_read_pool), the exec_fn a
   worker handle registers (mizu_r_exec_pool — task frame decode, the eval,
   and the result publish through the sink), the ERR envelope framing shared
   by exec and the unwind path (mizu_r_publish_err), and the trace thunk
   (mizu_r_trace). The core owns the ring/deque mechanics, the wakes, and the
   retain-table commits; this file owns the payload framing policy and the
   task evaluation. The check/drop/sweep hooks bridge R's interrupt, GC, and
   per-worker cache lifecycle. */

#include <stdio.h>
#include <string.h>
#include "mizu.h"
#include "arrow.h"
#include <R_ext/Utils.h>

/* ANY_ATTRIB() joined the C API in R 4.5.0; equivalent fallback for
   earlier R, where ATTRIB() was still the sanctioned spelling. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

// The stage guard ----------------------------------------------------------------

/* Wraps each stage hook: arms the attr-qualification verdict cache's
   fresh generation at entry (interop.c) and voids it at exit, the raise
   paths covered by the unwind cleanup so an armed generation never
   outlives the staged tree's anchoring. Re-entrant by construction — the
   Arrow decline path re-enters the stage hook, whose arm simply starts
   another generation. Also resets the by-reference mark list at entry:
   the list is one stage's emission record (the pin-and-no-keeperless
   signal, the worker's emitted-set for the argument-loan release), so a
   view marked by an earlier send must not stay referenced by it — the
   serialize-fallback reset alone left every REF-tier / remote-leaf mark
   pinned across all later stages. */
typedef int (*mizu_stage_impl_fn)(void *, mizu_slot_hdr *, unsigned char *,
                                  uint32_t, mizu_handle *, void *);
typedef struct {
  mizu_stage_impl_fn fn;
  void *obj;
  mizu_slot_hdr *hdr;
  unsigned char *payload;
  uint32_t inline_max;
  mizu_handle *h;
  void *ctx;
  int rc;
} mizu_stage_guard_s;

static SEXP mizu_stage_guard_run(void *d) {
  mizu_stage_guard_s *g = (mizu_stage_guard_s *) d;
  g->rc = g->fn(g->obj, g->hdr, g->payload, g->inline_max, g->h, g->ctx);
  return R_NilValue;
}

static void mizu_stage_guard_clean(void *d) {
  mizu_interop_stage_disarm();
}

static int mizu_stage_guard(mizu_stage_impl_fn fn, void *obj,
                            mizu_slot_hdr *hdr, unsigned char *payload,
                            uint32_t inline_max, mizu_handle *h, void *ctx) {
  mizu_stage_guard_s g = { fn, obj, hdr, payload, inline_max, h, ctx, 0 };
  mizu_zc_ref_reset();
  mizu_interop_stage_arm();
  /* cleanup runs on the normal and the raise exits alike */
  R_ExecWithCleanup(mizu_stage_guard_run, &g, mizu_stage_guard_clean, &g);
  return g.rc;
}

// Channel stage -------------------------------------------------------------------

/* The channel's tier dispatch: frame x as (hdr, payload) — payload capacity
   inline_max — retaining through the handle's services (the core commits the
   staging entry on success). Arena chunks come from mizu_stage_arena_alloc,
   the arena base staying core-private; region checkouts ride the spill
   helpers on the handle's free list. Raises on failure, never returns
   nonzero. */
static int mizu_r_stage_channel_impl(void *obj, mizu_slot_hdr *hdr,
                                     unsigned char *payload,
                                     uint32_t inline_max,
                                     mizu_handle *h, void *ctx) {
  SEXP x = (SEXP) obj;
  mizu_r_handle *rh = (mizu_r_handle *) ctx;
  if (x == rh->err_cond) {
    /* the peer shim's err send (mizu_channel_send_error): frame the
       condition as an 'I' err stream INLINE, whatever the peer's language
       — the pointer match (one compare, cleared as it matches) bypasses
       the value codec choice below. Bounded by construction; pins
       nothing. §4.1's spec submit reuses this pattern for task streams. */
    rh->err_cond = NULL;
    size_t en = mizu_interop_write_err(payload, inline_max, x);
    hdr->kind = MIZU_KIND_INLINE;
    hdr->len = (uint32_t) en;
    hdr->aux = MIZU_AUX_F_KEEPERLESS;
    return 0;
  }
  /* the reader-language state (one predicted branch per stage): foreign
     handles write interop only, filter the zero-copy tiers by the peer's
     capability mask, and stage top-level length-1 atomics as the 'I'
     scalar tags; same-language handles run today's path unchanged */
  const int foreign = rh->peer_lang != 0 && rh->peer_lang != MIZU_LANG_R;
  size_t total;
  uint64_t off;
  unsigned char *chunk;
  /* one raw probe per stage: the returned code drives the core's raw-tier
     reservation below (0 on the NIL / REF / STR1 / codec paths) */
  size_t rawlen;
  int rawtype = mizu_raw_type(x, &rawlen);

  /* NULL is the immediate kind — no serialize pass, no receive alloc */
  if (x == R_NilValue) {
    hdr->kind = MIZU_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
    return 0;
  }
  if (foreign && !ANY_ATTRIB(x) && !Rf_isS4(x) &&
      (TYPEOF(x) == LGLSXP || TYPEOF(x) == INTSXP ||
       TYPEOF(x) == REALSXP || TYPEOF(x) == CPLXSXP) && XLENGTH(x) == 1) {
    /* a top-level length-1 attribute-free atomic stages as the 'I'
       scalar tag (inline at RAWVEC's cost) — R scalars reach Python as
       scalars, top level and nested alike */
    size_t n = mizu_interop_write(payload, inline_max, x, NULL);
    if (n == 0 || n > inline_max)
      Rf_error("mizu: interop scalar staging failed");
    hdr->kind = MIZU_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = MIZU_AUX_F_KEEPERLESS;
    return 0;
  }
  if (mizu_arrow_check(x)) {
    /* an Arrow frame stages off the Arrow buffers via the core writer —
       one MIZL region past the size gate, one 'I' stream at or under it;
       the frame grammar's own path, ahead of the value tiers (an
       ArrowTabular environment would otherwise serialize) */
    mizu_arrow_stage_r(x, hdr, payload, inline_max, h, ctx, rh->peer_caps,
                       mizu_r_stage_channel);
    return 0;
  }
  if ((!foreign || mizu_zc_ref_foreign_ok(x, rh->peer_caps)) &&
      mizu_zc_ref_stage(hdr, payload, inline_max, x)) {
    /* a mizu-native view crosses by reference (REF) at any size — required
       once SHM_VEC views exist: the serialize-hook fallback resolves
       uncounted, and the producer could recycle under the far side's view;
       the pin is the view itself. The foreign filter runs ahead of the
       claim (the claim marks the region REFHELD): a view whose layout the
       peer cannot wrap takes the interop writer below as a value copy
       off the shared pages. */
    mizu_r_pin(h, ctx, x);
    return 0;
  }
  if (rawtype != 0) {
    /* the raw tiers (mizu_stage_raw, the core's policy): RAWVEC inline, the
       arena copy, or the flat SHM_VEC layout. Bare bytes skip the serialize
       pass here and the parse at the far end, and nothing is pinned (no
       identifier can ride along). A NULL reservation falls to the
       serialized tiers below. */
    unsigned char *dst = mizu_stage_raw(h, rawlen, rawtype, hdr, payload,
                                       inline_max);
    if (dst != NULL) {
      memcpy(dst, mizu_vec_ptr(x), rawlen);
      return 0;
    }
  } else if (foreign) {
    mizu_ix_decline rec = { 0, "", "", "" };
    int s1 = mizu_interop_str1_foreign(hdr, payload, inline_max, x, &rec);
    if (s1 > 0) return 0;
    if (s1 < 0) mizu_stop_not_portable(rec.path, rec.reason, rec.remedy);
  }
  if (!foreign && mizu_str1_stage(hdr, payload, inline_max, x)) {
    /* a length-1 string's bytes are self-contained: pin nothing */
    return 0;
  } else if (!foreign && mizu_zc_eligible(x, inline_max, &total, 0) &&
             !mizu_handle_churn(h)) {
    /* eligible objects past the budget go straight to SHM_VEC, skipping
       the arena: arena receive pays a full unserialize and a chunk can
       never hold a view (chunk lifetime tracks ring advance). The churn
       gate (Linux-only): while lent regions prove consumer views outlive
       their traffic, the copy tiers below are cheaper — the arena and
       SHM_RAW surrender deterministically, where a fresh SHM_VEC region
       per message would pile up in the ledger */
    mizu_stage_reap(h);
    mizu_zc_stage(hdr, payload, x, total, h, ctx, 0);
    return 0;
  } else if (foreign &&
             mizu_zc_eligible_foreign(x, inline_max, &total, rh->peer_caps) &&
             !mizu_handle_churn(h)) {
    /* the foreign zero-copy gate: the baseline layouts plus whatever the
       peer's capability mask advertises; the layout write builds the
       validity-bitmap section for the foreign reader's Arrow exports */
    mizu_stage_reap(h);
    mizu_zc_stage(hdr, payload, x, total, h, ctx, 1);
    return 0;
  }
  if (foreign) {
    /* interop only on a foreign handle: a decline raises at send with
       the walk's record (the private streams are unreadable by that
       peer by definition). Within the inline budget the stream pins
       nothing and claims keeperless; past it, the arena then a region
       retain like any codec stream. */
    mizu_ix_decline rec;
    size_t n = mizu_interop_write(payload, inline_max, x, &rec);
    if (n == 0) mizu_stop_not_portable(rec.path, rec.reason, rec.remedy);
    if (n <= inline_max) {
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = MIZU_AUX_F_KEEPERLESS;
      return 0;
    }
    if ((chunk = mizu_stage_arena_alloc(h, MIZU_ALIGN64(n), &off)) != NULL) {
      if (mizu_interop_write(chunk, n, x, NULL) != n)
        Rf_error("mizu: interop write mismatch");
      hdr->kind = MIZU_KIND_ARENA;
      hdr->len = 0;
      hdr->aux = off;
      uint64_t n64 = (uint64_t) n;
      memcpy(payload, &n64, sizeof(n64));
      return 0;
    }
    mizu_stage_reap(h);
    mizu_payload_spill_interop(hdr, payload, x, n, h);
    return 0;
  }
  /* the compact codec ahead of R_Serialize (payload.c): a codec stream
     is self-contained — the writer rejects ALTREP, so no hook-emitted
     view identifier can ride along — and pins nothing. A serialize stream
     pins only when the emit hook fired during the pass (a nested view
     rides by reference); unpinned, it claims keeperless inline. */
  size_t n = mizu_codec_write(payload, inline_max, x);
  int self_contained = n != 0;
  if (!self_contained) {
    /* a nested ArrowTabular crosses as the codec's frame leaf (same
       language only — the foreign branch returned above) */
    if (mizu_frame_tier(hdr, payload, inline_max, x, h)) return 0;
    mizu_zc_ref_reset();
    n = mizu_serialize_bounded(payload, inline_max, x);
  }
  if (n <= inline_max) {
    hdr->kind = MIZU_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = MIZU_AUX_F_KEEPERLESS;
    if (!self_contained && mizu_zc_ref_fired()) {
      hdr->aux = 0;
      mizu_r_pin(h, ctx, x);
    }
  } else {
    if ((chunk = mizu_stage_arena_alloc(h, MIZU_ALIGN64(n), &off)) != NULL) {
      if (self_contained) {
        if (mizu_codec_write(chunk, n, x) != n)
          Rf_error("mizu: codec write mismatch");
      } else {
        mizu_view_serialize_into(chunk, x);
      }
      hdr->kind = MIZU_KIND_ARENA;
      hdr->len = 0;
      hdr->aux = off;
      uint64_t n64 = (uint64_t) n;
      memcpy(payload, &n64, sizeof(n64));
      if (!self_contained && mizu_zc_ref_fired())
        mizu_r_pin(h, ctx, x);
    } else {
      /* reap before staging: the consumer's latest head publish may
         have released a fitting region for this very spill to pop */
      mizu_stage_reap(h);
      if (self_contained)
        mizu_payload_spill_codec(hdr, payload, x, n, h);
      else
        mizu_payload_spill_shm(hdr, payload, x, n, h, ctx);
    }
  }
  return 0;
}

int mizu_r_stage_channel(void *obj, mizu_slot_hdr *hdr,
                         unsigned char *payload, uint32_t inline_max,
                         mizu_handle *h, void *ctx) {
  return mizu_stage_guard(mizu_r_stage_channel_impl, obj, hdr, payload,
                          inline_max, h, ctx);
}

/* The batch-verb sink (recv_batch_fn / collect_all_fn): anchor each read
   product in a pre-PROTECTed VECSXP as it is delivered, so no SEXP sits
   unprotected across the next read's allocations. SET_VECTOR_ELT does
   not allocate. */
void mizu_vec_sink(void *ctx, size_t i, void *obj) {
  SET_VECTOR_ELT((SEXP) ctx, (R_xlen_t) i, (SEXP) obj);
}

// Read-hook containment -----------------------------------------------------------

/* R's longjmp is the only out-of-band escape a read hook has (a corrupt-
   payload guard, an R_Unserialize resource raise): the core learns the
   consume intent only from a returned read, so an escaping raise leaves the
   slot at the ring head and every later receive re-raises — the wedge the
   foreign-decline arm already handles in-band. Contain the hook body and
   convert: the channel consumes the slot and stashes the caught condition
   on the handle's decline slot (the mizu_decline_foreign shape generalized;
   the recv veneer signals it ahead of chan_raise). The pool returns NULL
   without CONSUME — pymizu's deliberate trade-off ("a freed result slot is
   a separate decision"), the result slot staying published-but-unread for
   the task finalizer — with the same stash for the collect veneers to
   signal in place of the core's generic read-failure record. */
typedef void *(*mizu_read_impl_fn)(const mizu_slot_hdr *,
                                   const unsigned char *, size_t,
                                   mizu_read_ctx *);
typedef struct {
  mizu_read_impl_fn fn;
  const mizu_slot_hdr *hdr;
  const unsigned char *payload;
  size_t limit;
  mizu_read_ctx *ctx;
  void *out;
  int failed;
  SEXP cond;
} mizu_read_catch_s;

static SEXP mizu_read_catch_body(void *d) {
  mizu_read_catch_s *c = (mizu_read_catch_s *) d;
  c->out = c->fn(c->hdr, c->payload, c->limit, c->ctx);
  return R_NilValue;
}

static SEXP mizu_read_catch_handler(SEXP cond, void *d) {
  mizu_read_catch_s *c = (mizu_read_catch_s *) d;
  c->failed = 1;
  c->cond = cond;
  return R_NilValue;
}

static void mizu_read_toplevel_run(void *d) {
  (void) mizu_read_catch_body(d);
}

/* consume is the channel's answer (a ring slot cannot be retried — leaving
   it wedges the head); the pool passes 0. The stash is a SET_VECTOR_ELT
   only: nothing allocates between the catch and the store. */
static void *mizu_read_guard(mizu_read_impl_fn fn,
                             const mizu_slot_hdr *hdr,
                             const unsigned char *payload, size_t limit,
                             mizu_read_ctx *ctx, int consume) {
  mizu_read_catch_s c = { fn, hdr, payload, limit, ctx, NULL, 0, R_NilValue };
  /* the catch is R_ToplevelExec, not R_tryCatchError: it restores the
     protect stack across the jump at context cost only (~0.1 us against
     ~6-9 us for the handler form — this is a hot verb) */
  if (R_ToplevelExec(mizu_read_toplevel_run, &c)) return c.out;
  /* the cold path alone captures the condition: re-run under the handler
     form — a deterministic guard or resource raise re-raises identically
     (the read's partial side effects are idempotent: a cached open, the
     GC-balanced wire loans), a transient one simply succeeds now */
  R_tryCatchError(mizu_read_catch_body, &c, mizu_read_catch_handler, &c);
  if (!c.failed) return c.out;
  mizu_r_handle *rh = (mizu_r_handle *) ctx->binding_ctx;
  SET_VECTOR_ELT(rh->prot, rh->decline_slot, c.cond);
  if (consume) ctx->flags |= MIZU_READ_CONSUME;
  return NULL;
}

// Channel read --------------------------------------------------------------------

/* The transport has already resolved an arena-referencing frame to its byte
   range (payload, limit), so the ARENA / channel RAWSPILL kinds below read
   resolved bytes — the arena base never leaves the transport. Everything
   else defers to the shared payload reader (ctx carries the handle's open
   cache and the R-side view cache). Returns the object, or NULL with
   ctx->gone set on a vanished out-of-line region. A foreign (Python) stream
   stashes the interned decline condition on the handle and fails the read
   with MIZU_READ_CONSUME, so the core consumes the slot before the recv
   veneer signals the record — a plain failure here would leave the slot in
   place and wedge the ring behind it. */
static void *mizu_r_read_channel_impl(const mizu_slot_hdr *hdr,
                                      const unsigned char *payload,
                                      size_t limit,
                                      mizu_read_ctx *ctx) {
  if (hdr->kind == MIZU_KIND_ARENA) {
    /* resolved stream bytes; limit is the arena-validated length */
    return (void *) mizu_stream_read(payload, limit, ctx, 1);
  }
  if (hdr->kind == MIZU_KIND_RAWSPILL) {
    /* resolved RAWVEC bytes (the pool's region framing of this kind is
       read in mizu_payload_read) */
    int type = (int) hdr->aux;
    size_t elt = mizu_view_sizeof_elt(type);
    /* one read: the slot is peer-writable, so the bound and the limit
       must be one snapshot (chan_read's discipline) */
    uint32_t len = hdr->len;
    if (elt == 0 || len > limit || len % elt != 0)
      Rf_error("mizu: corrupt payload slot");
    SEXP y = mizu_wire_alloc(type, (R_xlen_t) (len / elt));
    memcpy(mizu_vec_ptr(y), payload, len);
    return (void *) y;
  }
  return mizu_payload_read(hdr, payload, (uint32_t) limit, ctx, 1);
}

/* The registered read_fn: the contained form of the above (CONSUME on a
   caught raise — a channel slot cannot be left to wedge the ring). The
   trivial tiers stay unguarded: NIL / STR1 / RAWVEC / RAWSPILL have no
   unserialize and no layout walk — only a catastrophic allocation failure
   could raise there, and that stays the unhandled case (pymizu's
   MemoryError carve-out alike) — so the hot verbs pay no context cost. */
void *mizu_r_read_channel(const mizu_slot_hdr *hdr,
                          const unsigned char *payload, size_t limit,
                          mizu_read_ctx *ctx) {
  switch (hdr->kind) {
  case MIZU_KIND_NIL:
  case MIZU_KIND_STR1:
  case MIZU_KIND_RAWVEC:
  case MIZU_KIND_RAWSPILL:
    return mizu_r_read_channel_impl(hdr, payload, limit, ctx);
  default:
    return mizu_read_guard(mizu_r_read_channel_impl, hdr, payload, limit,
                           ctx, 1);
  }
}

// Pool stage --------------------------------------------------------------------

/* The pool's stage_fn: the payload.c tier dispatch on the handle's services,
   registered at create/join/attach. Serves task payloads at submit and
   result payloads at publish — a pool has no arena, so out-of-line frames
   are always named regions. Raises on failure, never returns nonzero. */
static int mizu_r_stage_pool_impl(void *obj, mizu_slot_hdr *hdr,
                                  unsigned char *payload,
                                  uint32_t inline_max,
                                  mizu_handle *h, void *ctx) {
  mizu_r_handle *rh = (mizu_r_handle *) ctx;
  if ((SEXP) obj == rh->spec) {
    /* the spec submit (mizu_pool_submit_spec / the map's runner submit):
       frame the task stream off the spec's components — one pointer
       compare (cleared as it matches) ahead of the value framing, the
       err_cond pattern. A raise inside the write (a non-portable
       argument) leaves nothing dangling: the field is already cleared,
       and the publish path never sets it. */
    rh->spec = NULL;
    SEXP spec = (SEXP) obj;
    uint32_t target = (uint32_t) (rh->worker_ident & 0xff);
    size_t n;
    if (XLENGTH(spec) == 3) {
      /* a map runner spec (name, gen_field, seed): the kind-2 stream,
         bounded by MIZU_NAME_MAX — always inline */
      if (TYPEOF(VECTOR_ELT(spec, 0)) != STRSXP ||
          XLENGTH(VECTOR_ELT(spec, 0)) != 1 ||
          TYPEOF(VECTOR_ELT(spec, 1)) != REALSXP ||
          XLENGTH(VECTOR_ELT(spec, 1)) != 1 ||
          (VECTOR_ELT(spec, 2) != R_NilValue &&
           (TYPEOF(VECTOR_ELT(spec, 2)) != REALSXP ||
            XLENGTH(VECTOR_ELT(spec, 2)) != 2)))
        Rf_error("mizu: a malformed map runner spec");
      n = mizu_interop_write_runner(payload, inline_max,
                                    VECTOR_ELT(spec, 0),
                                    REAL(VECTOR_ELT(spec, 1))[0],
                                    VECTOR_ELT(spec, 2), target,
                                    rh->spec_ident);
      if (n > inline_max)
        Rf_error("mizu: map runner stream exceeds the entry budget");
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = 0;
      return 0;
    }
    mizu_ix_decline rec;
    mizu_ix_plan plan;
    const uint32_t caps = (uint32_t) (rh->worker_ident >> 32);
    /* the W2 fold: one record walk — the optimistic write flips to
       count-only at the first SHM_VEC candidate, so a candidate-free
       inline spec completes in a single walk. Selection is arithmetic
       over the recorded subtree sizes (F1's D3, the first candidate
       whose remainder fits), never a size pass per candidate */
    const size_t total =
      mizu_interop_write_task(payload, inline_max, spec, target,
                              rh->spec_ident, &rec, h, caps, inline_max,
                              R_NilValue, 0, 0, &plan);
    if (total == 0)
      mizu_stop_not_portable(rec.path, rec.reason, rec.remedy);
    SEXP zc_node = R_NilValue;
    size_t zc_size = 0;
    for (int i = 0; i < plan.ncand; i++) {
      if (total - plan.size[i] + MIZU_IX_ZC_RESERVE <= (size_t) inline_max) {
        zc_node = plan.cand[i];
        zc_size = plan.zc_size[i];
        break;
      }
    }
    if ((plan.has_ref || zc_node != R_NilValue) && !(caps & MIZU_CAP_TASKREF))
      /* D2: fail locally rather than remotely — a remote failure loses
         the work to a task error stream */
      mizu_stop_not_portable(
        "args",
        "the pool's workers cannot read by-reference task arguments",
        "upgrade the workers' binding");
    if (zc_node != R_NilValue) {
      n = mizu_interop_write_task(payload, inline_max, spec, target,
                                  rh->spec_ident, &rec, h, caps,
                                  inline_max, zc_node, zc_size, 0, NULL);
      if (n != SIZE_MAX) {
        if (n == 0)
          mizu_stop_not_portable(rec.path, rec.reason, rec.remedy);
        hdr->kind = MIZU_KIND_INLINE;
        hdr->len = (uint32_t) n;
        hdr->aux = 0;      /* no keeperless claim: inert on task entries */
        if (mizu_zc_ref_fired())
          mizu_r_pin(h, ctx, spec);   /* D4: the submit-side handoff */
        return 0;
      }
      /* a mid-write checkout failure (a churn race): re-run by value,
         never a partial stream */
    }
    if (plan.ncand == 0 && total <= (size_t) inline_max) {
      /* one walk: the record walk's write is the stream */
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) total;
      hdr->aux = 0;        /* no keeperless claim: inert on task entries */
      if (mizu_zc_ref_fired())
        mizu_r_pin(h, ctx, spec);
      return 0;
    }
    /* candidates recorded, none fitting (or the zc write abandoned): the
       record walk counted the by-value stream — rewrite it inline, or
       spill it at the recorded size */
    if (total <= (size_t) inline_max) {
      n = mizu_interop_write_task(payload, inline_max, spec, target,
                                  rh->spec_ident, &rec, h, caps,
                                  inline_max, R_NilValue, 0, 1, NULL);
      if (n == 0)
        mizu_stop_not_portable(rec.path, rec.reason, rec.remedy);
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = 0;        /* no keeperless claim: inert on task entries */
      if (mizu_zc_ref_fired())
        mizu_r_pin(h, ctx, spec);
      return 0;
    }
    mizu_stage_reap(h);
    mizu_payload_spill_task(hdr, payload, spec, target, rh->spec_ident,
                            total, h, caps, inline_max);
    if (mizu_zc_ref_fired())
      mizu_r_pin(h, ctx, spec);
    return 0;
  }
  /* an Arrow frame stages off the Arrow buffers via the core writer — one
     MIZL region past the size gate, one 'I' stream at or under it;
     same-language only (the foreign test mirrors payload.c's), so the
     §4.2 foreign result policy and its pre-flight gate stay
     byte-identical. The capability word is this build's: a task frame is
     same-language by construction. In practice this serves results — a
     submitted table is always nested in the list(expr, args) payload and
     rides the codec's frame leaf instead */
  const int foreign = rh->peer_lang != 0 && rh->peer_lang != MIZU_LANG_R;
  if (!foreign && mizu_arrow_check((SEXP) obj)) {
    mizu_arrow_stage_r((SEXP) obj, hdr, payload, inline_max, h, ctx,
                       MIZU_R_CAPS, mizu_r_stage_pool);
    return 0;
  }
  mizu_payload_stage(hdr, payload, inline_max, (SEXP) obj, h, ctx);
  return 0;
}

int mizu_r_stage_pool(void *obj, mizu_slot_hdr *hdr,
                      unsigned char *payload, uint32_t inline_max,
                      mizu_handle *h, void *ctx) {
  return mizu_stage_guard(mizu_r_stage_pool_impl, obj, hdr, payload,
                          inline_max, h, ctx);
}

// Pool read (collect) ------------------------------------------------------------

/* The pool's read_fn: materialize a collected result. ctx->outcome carries
   the terminal state: OK/ERR read the payload frame (ERR's is the flattened
   transport condition), CANCEL/DIED carry no payload and build the binding's
   error object from the claimant record. Non-OK outcomes come back boxed in
   a mizu_caught list — the marker the collect veneer branches on (a task
   value that is itself a condition stays bare). The product is unprotected:
   the core's claim tail allocates nothing before the verb returns it. */
static void *mizu_r_read_pool_impl(const mizu_slot_hdr *hdr,
                                   const unsigned char *payload,
                                   size_t limit, mizu_read_ctx *ctx) {
  switch (ctx->outcome) {
  case MIZU_RS_OK:
    /* no consume_foreign: a pool is R-only, so a foreign stream is
       corruption — raise in place rather than consume */
    return mizu_payload_read(hdr, payload, (uint32_t) limit, ctx, 0);
  case MIZU_RS_ERR: {
    SEXP cond = mizu_payload_read(hdr, payload, (uint32_t) limit, ctx, 0);
    if (cond == NULL) return NULL;              /* ctx->gone set */
    return mizu_caught(cond);
  }
  case MIZU_RS_DIED:
    return mizu_caught_died((int) ctx->died_slot, (double) ctx->died_pid,
                            "mizu: worker died while executing this task");
  case MIZU_RS_LOST:
    /* the claimant published LOST itself (no death verdict), so ctx
       carries no claimant record */
    return mizu_caught_cond("mizu_error_task_lost",
                            "mizu: task lost: payload region vanished");
  case MIZU_RS_CANCEL:
    return mizu_caught_cond("mizu_error_cancelled",
                            "mizu: task cancelled or pool stopped");
  }
  return NULL;
}

/* The registered read_fn: the contained form of the above (no CONSUME on a
   caught raise — the result slot stays published for the task finalizer,
   pymizu's deliberate pool choice; the stash surfaces at the collect
   veneers). The trivial tiers stay unguarded, as the channel's: a NIL
   header also stands in for the CANCEL / DIED / LOST outcomes, whose
   caught-box builds cannot raise short of allocation failure. */
void *mizu_r_read_pool(const mizu_slot_hdr *hdr, const unsigned char *payload,
                       size_t limit, mizu_read_ctx *ctx) {
  switch (hdr->kind) {
  case MIZU_KIND_NIL:
  case MIZU_KIND_STR1:
  case MIZU_KIND_RAWVEC:
  case MIZU_KIND_RAWSPILL:
    return mizu_r_read_pool_impl(hdr, payload, limit, ctx);
  default:
    return mizu_read_guard(mizu_r_read_pool_impl, hdr, payload, limit,
                           ctx, 0);
  }
}

// Pool exec ---------------------------------------------------------------------

/* The task evaluator: one wire payload — list(expr, named args) — with the
   arguments bound into a fresh unhashed frame under the base environment
   (prot[0], set by mizu_pool_set_eval). Two error disciplines, chosen by
   the caller, and both cover the frame decode as well as the eval: a
   task-frame decode failure is the task's ERR, never worker death. The
   worker loop's hot path (catching = 0) arms no handler at all: the eval
   marker (mizu_pool_eval_mark) rides the whole exec — set at entry,
   cleared at every normal return — so a user error anywhere in the body
   longjmps out of mizu_pool_step with the task's identity recorded (the
   core's pool_execute writes the cur_* sink fields ahead of the exec
   call), and worker_main publishes the caught condition as this task's
   ERR result through mizu_pool_run_outcome; the marker is what separates
   those errors from infrastructure failure, which stays fatal. The set
   and the clear are catching = 0 only: the core defines in_eval and the
   cur_* fields as the outermost unwind-path eval's identity, so a nested
   exec must neither set nor clear — a naive clear-on-every-return would
   strip the mark from an outer task that collected a sibling handle and
   then raised, degrading its own ERR to worker death. Help mode and
   nested submit's inline execute (catching = 1) run inside a task's own
   evaluation, where an escaping error would land in the wrong task's
   frames: they contain the decode and the eval alike in R_tryCatchError
   and pay its R-closure trampoline. */
struct mizu_task_ctx {
  SEXP prot;                      /* prot[0] is the eval env base */
  const mizu_slot_hdr *hdr;
  const unsigned char *payload;
  size_t limit;
  mizu_read_ctx *ctx;
  mizu_result_sink *sink;
  uint64_t *identp;               /* this exec's submitter-identity word */
  int fail;                       /* test-only: a decode failure on demand */
  int died;                       /* decode: the enqueuer's region is gone */
  int ok;                         /* 0 once the catch handler has fired */
};

static SEXP pool_task_handler(SEXP cond, void *data) {
  ((struct mizu_task_ctx *) data)->ok = 0;
  return cond;
}

/* The outermost task's submitter identity word (mizu.h): 0 for a private
   frame (reads as same-language), the task stream's header word from
   decode on. Only the catching = 0 exec's identp points here — a nested
   exec's points at a word on the exec hook's stack — so help mode can
   never clobber the outer task's word, and a mid-task longjmp leaves the
   unwind path (mizu_pool_run_outcome) the failing task's. Borrowed
   discipline — a plain word, no GC concerns. */
uint64_t mizu_curpool_ident;

/* The entry's byte span for the task-magic dispatch (§4.2): INLINE,
   RAWVEC and STR1 in place; a SHM_RAW entry opened first through the
   read ctx's open cache (a spilled task stream's offsets are stream
   offsets, not entry-payload offsets). Legit task frames are codec or
   serialize streams on INLINE / SHM_RAW, so the inline byte tiers carry
   only crafted entries; RAWSPILL is skipped (its bytes are vector data,
   not a stream). NULL for every other kind, and for a vanished region
   (ctx->gone set). */
static const unsigned char *pool_entry_bytes(const mizu_slot_hdr *hdr,
                                             const unsigned char *payload,
                                             size_t limit,
                                             mizu_read_ctx *ctx,
                                             size_t *out_len) {
  switch (hdr->kind) {
  case MIZU_KIND_INLINE:
  case MIZU_KIND_RAWVEC:
  case MIZU_KIND_STR1: {
    /* one read, bounded to the slot: a crafted entry must not over-state
       the span handed to the interop cursor */
    const uint32_t len = hdr->len;
    if (len > limit) return NULL;
    *out_len = len;
    return payload;
  }
  case MIZU_KIND_SHM_RAW: {
    /* one read of each — the slot is peer-writable */
    const uint32_t len = hdr->len;
    if (len == 0 || len >= MIZU_NAME_MAX) return NULL;
    mizu_shm *shm = mizu_read_region(ctx, payload, len);
    if (shm == NULL) return NULL;         /* ctx->gone set */
    const uint64_t aux = hdr->aux;
    *out_len = aux != 0 && aux <= (uint64_t) shm->size ?
      (size_t) aux : (size_t) shm->size;
    return (const unsigned char *) shm->addr;
  }
  default:
    return NULL;
  }
}

/* The §4.2 result gate, inside the containment: a foreign result the
   publish could only decline fails the task here (an error stream), never
   the publish. The interop size pass runs first (cheap) — a writable
   value needs no layout walk. Shared by the call and runner task paths. */
static void pool_result_gate(SEXP value, mizu_result_sink *sink,
                             uint64_t ident) {
  if (ident != 0 && (ident & 0xff) != MIZU_LANG_R && value != R_NilValue) {
    const uint32_t caps = (uint32_t) (ident >> 32);
    size_t rawlen, total;
    mizu_ix_decline rec = { 0, "", "", "" };
    PROTECT(value);
    if (mizu_raw_type(value, &rawlen) == 0 &&
        mizu_interop_write(NULL, 0, value, &rec) == 0 &&
        !mizu_zc_eligible_foreign(value, sink->inline_max, &total,
                                  caps))
      mizu_stop_not_portable(rec.path, rec.reason, rec.remedy);
    UNPROTECT(1);
  }
}

/* The task-stream path: decode (the submitter identity lands in the ctx
   word ahead of every field read) and eval. Name kind evals the consed call;
   source kind evals each parsed form in order in the fresh frame, the
   result the last form's value — R's eval semantics give the §4.0
   trailing-expression convention for free. Ends with the §4.2 result
   gate. */
static SEXP pool_task_spec(struct mizu_task_ctx *c,
                           const unsigned char *bytes, size_t blen) {
  *c->identp = MIZU_IDENT(MIZU_LANG_BYTES, 0);
  SEXP base = VECTOR_ELT(c->prot, 0);
  if (TYPEOF(base) != ENVSXP)
    Rf_error("mizu: no evaluator registered on this worker handle");
  int kind;
  SEXP task =
    PROTECT(mizu_interop_exec_task(bytes, blen, base, &kind, c->identp));
  SEXP value = R_NilValue;
  if (kind == 0) {
    value = Rf_eval(task, R_GlobalEnv);
  } else {
    /* the source closure over its positional arguments — value types all,
       self-evaluating in the call */
    SEXP fn = VECTOR_ELT(task, 0);
    SEXP pos_args = VECTOR_ELT(task, 1);
    SEXP call = PROTECT(Rf_lcons(fn, R_NilValue));
    SEXP tail = call;
    for (R_xlen_t i = 0; i < Rf_xlength(pos_args); i++) {
      SEXP cell = PROTECT(Rf_cons(VECTOR_ELT(pos_args, i), R_NilValue));
      SETCDR(tail, cell);
      tail = cell;
      UNPROTECT(1);
    }
    value = Rf_eval(call, R_GlobalEnv);
    UNPROTECT(1);
  }
  UNPROTECT(1);
  pool_result_gate(value, c->sink, *c->identp);
  return value;
}

/* The mizu namespace and the runner entry, cached on first use on the
   precious root (the arrow_ns pattern — a worker's namespace cannot
   unload, and the lookups otherwise repeat per runner). */
static void runner_ns_cache(void) {
  if (mizu_priv(MIZU_PRIV_RUNNER_NS) != R_NilValue) return;
  SEXP runner_ns =
    PROTECT(R_FindNamespace(Rf_ScalarString(Rf_mkChar("mizu"))));
  SEXP fn_map_runner_ix =
    PROTECT(Rf_findFun(Rf_install("map_runner_ix"), runner_ns));
  mizu_priv_set(MIZU_PRIV_RUNNER_NS, runner_ns);
  mizu_priv_set(MIZU_PRIV_FN_MAP_RUNNER, fn_map_runner_ix);
  UNPROTECT(2);
}

/* The kind-2 (runner) path: decode the region reference and hand it to
   the binding's own runner loop (map_runner_ix unpacks the ordinal and
   generation, derives the seed state, and runs mizu:::map_runner against
   the named region). The runner is always same-language as the worker —
   a foreign submitter sends only the region reference. Ends with the
   §4.2 result gate: the runner's (histories, values) publish is a
   foreign-submitter result like any task's. */
static SEXP pool_task_runner(struct mizu_task_ctx *c,
                             const unsigned char *bytes, size_t blen) {
  *c->identp = MIZU_IDENT(MIZU_LANG_BYTES, 0);
  SEXP dec = PROTECT(mizu_interop_exec_runner(bytes, blen, c->identp));
  runner_ns_cache();
  SEXP call = PROTECT(Rf_lang4(mizu_priv(MIZU_PRIV_FN_MAP_RUNNER),
                               VECTOR_ELT(dec, 0), VECTOR_ELT(dec, 1),
                               VECTOR_ELT(dec, 2)));
  SEXP value = Rf_eval(call, mizu_priv(MIZU_PRIV_RUNNER_NS));
  UNPROTECT(2);
  pool_result_gate(value, c->sink, *c->identp);
  return value;
}

/* The eval proper: bind the named arguments into a fresh frame and
   Rf_eval. No marker, no handler — both live at exec level now. */
static SEXP pool_eval_expr(SEXP prot, SEXP expr, SEXP args) {
  SEXP names = PROTECT(Rf_getAttrib(args, R_NamesSymbol));
  R_xlen_t n = Rf_xlength(args);
  if (n > 0 && TYPEOF(names) != STRSXP)
    Rf_error("mizu: corrupt task payload");
  /* eval is the identity on value types: a constant task (the canonical
     trivial task, and every constant result of a nested computation) binds
     no arguments and needs no fresh environment — the per-task R_NewEnv is
     the whole cost here */
  switch (TYPEOF(expr)) {
  case NILSXP: case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP:
  case STRSXP: case RAWSXP: case VECSXP:
    UNPROTECT(1);                    /* names */
    return expr;
  }
  SEXP base = VECTOR_ELT(prot, 0);   /* the eval env */
  if (TYPEOF(base) != ENVSXP)
    Rf_error("mizu: no evaluator registered on this worker handle");
  SEXP env = PROTECT(R_NewEnv(base, 0, 0));
  for (R_xlen_t i = 0; i < n; i++)
    Rf_defineVar(Rf_installTrChar(STRING_ELT(names, i)),
                 VECTOR_ELT(args, i), env);
  SEXP value = Rf_eval(expr, env);
  UNPROTECT(2);                    /* names, env */
  return value;
}

/* Decode + eval as one body: catching mode runs it under R_tryCatchError,
   so a decode error there is this task's ERR, never the outer task's. An
   INLINE codec task frame stream-decodes in place — no list(expr, args)
   materialization, so a constant task allocates nothing on the worker.
   Anything else takes the generic read and its shape check over the
   exec's read ctx: the handle's open cache and the R-side view cache ride
   it. The decode products stay protected inside the body; the eval
   product crosses unprotected to the exec, which anchors it before the
   publish. */
static SEXP pool_task_body(void *data) {
  struct mizu_task_ctx *c = (struct mizu_task_ctx *) data;
  mizu_r_handle *rh = (mizu_r_handle *) c->ctx->binding_ctx;
  /* the per-task reset: non-task frames stash a 0 identity (reads as
     same-language), and a foreign-result publish that raised leaves the
     handle's policy fields set — a later task must not inherit either */
  *c->identp = 0;
  rh->peer_lang = 0;
  rh->peer_caps = 0;
  if (c->fail == 1)              /* test-only seam: see mizu_pool_exec_fail */
    Rf_error("mizu: corrupt task payload");
  if (c->fail == 2) {            /* test-only: a vanished task payload */
    c->died = 1;
    return R_NilValue;
  }
  size_t blen = 0;
  const unsigned char *bytes =
    pool_entry_bytes(c->hdr, c->payload, c->limit, c->ctx, &blen);
  if (c->ctx->gone) {
    c->died = 1;
    return R_NilValue;
  }
  if (bytes != NULL) {
    if (blen >= 3 && bytes[0] == MIZU_INTEROP_MAGIC &&
        bytes[2] == MIZU_IX_TAG_TASK) {
      /* the misroute guard (§4.2): the target byte at stream offset 3 is
         checked without a cursor — one byte compare between a torn or
         mis-stamped task stream and executing code written for another
         language. Fails with the neutral error stream, never executes */
      if (blen >= 4 && bytes[3] != MIZU_LANG_R) {
        *c->identp = MIZU_IDENT(MIZU_LANG_BYTES, 0);
        Rf_error("mizu: task language mismatch (the task targets language "
                 "%u, this worker is R)", (unsigned) bytes[3]);
      }
      /* the kind byte at stream offset 4: kind 2 is the map runner */
      if (blen >= 5 && bytes[4] == 2) {
        return pool_task_runner(c, bytes, blen);
      }
      return pool_task_spec(c, bytes, blen);
    }
    if (mizu_is_python_payload(bytes, blen)) {
      /* a foreign private frame ('P' or pickle at an R worker): the
         neutral error stream, never a private ERR the submitter could
         not read — the registry test the channel read hook runs */
      *c->identp = MIZU_IDENT(MIZU_LANG_BYTES, 0);
      Rf_error("mizu: task in a foreign private codec");
    }
  }
  SEXP expr = R_NilValue, args = R_NilValue;
  /* one read of the entry length — the slot header is peer-writable */
  const uint32_t entry_len = c->hdr->len;
  if (c->hdr->kind == MIZU_KIND_INLINE && entry_len <= c->limit &&
      mizu_codec_read_task(c->payload, (size_t) entry_len,
                           &expr, &args)) {
    PROTECT(expr);
    PROTECT(args);
  } else {
    /* a spilled private frame reads from the bytes the dispatch already
       resolved — a second open would be a cache probe at best */
    SEXP pl = c->hdr->kind == MIZU_KIND_SHM_RAW && bytes != NULL ?
      mizu_stream_read(bytes, blen, c->ctx, 0) :
      mizu_payload_read(c->hdr, c->payload, (uint32_t) c->limit, c->ctx, 0);
    if (c->ctx->gone) {
      c->died = 1;
      return R_NilValue;
    }
    if (pl == NULL || TYPEOF(pl) != VECSXP || Rf_xlength(pl) != 2 ||
        TYPEOF(VECTOR_ELT(pl, 1)) != VECSXP)
      Rf_error("mizu: corrupt task payload");
    expr = PROTECT(VECTOR_ELT(pl, 0));
    args = PROTECT(VECTOR_ELT(pl, 1));
  }
  SEXP value = pool_eval_expr(c->prot, expr, args);
  UNPROTECT(2);                    /* expr, args */
  return value;
}

/* The ERR envelope: the caught condition never crosses as itself — it is
   flattened to a transport condition (condition.c) and framed INLINE where
   it fits, so the publish cannot raise: fail the task, never the worker.
   Framed INLINE directly rather than through the tiered stage: flatten's
   verification pass already guarantees the fit. Below it (a tiny slot holds
   no classed condition inline) the tiered stage carries the terminal
   fallback out of line. Shared by exec's catching paths and the unwind path
   (mizu_pool_run_outcome). */
void mizu_r_publish_err(mizu_result_sink *sink, SEXP cond, uint64_t ident) {
  /* the ERR format keys on the task stream's submitter identity (§4.2):
     a foreign submitter gets the neutral error stream every binding
     reads — the private flatten would be unreadable there. Bounded by
     construction, INLINE, cannot fail. A same-language submitter (a 0
     stash included) keeps the rich private flatten. The word is the
     caller's: the exec's own on the catching paths, the global on the
     unwind path. */
  if (ident != 0 && (ident & 0xff) != MIZU_LANG_R) {
    size_t en = mizu_interop_write_err((unsigned char *) sink->payload,
                                       sink->inline_max, cond);
    mizu_result_publish_err(sink, (void *) cond, (uint32_t) en);
    return;
  }
  SEXP flat =
    PROTECT(mizu_condition_flatten(cond, (size_t) sink->inline_max));
  size_t n =
    mizu_codec_write(sink->payload, (size_t) sink->inline_max, flat);
  mizu_result_publish_err(sink, (void *) flat,
                         n != 0 && n <= (size_t) sink->inline_max ?
                         (uint32_t) n : 0);
  UNPROTECT(1);
}

/* The evaluating worker's own pool extptr: set per task by the exec hook
   below (multi-handle processes stay correct), restored on return, cleared
   by the pool finalizer. Borrowed — no allocation, no precious-list entry
   (R_NilValue at load, set in mizu_pool_init: no constant initializer
   exists for a file-scope SEXP). */
SEXP mizu_curpool_xp;

/* The worker's task: the eval mark rides the whole exec — set here ahead
   of the body, cleared only after the outcome write, both under
   catching = 0 only (the design comment above) — so a longjmp out of the
   body OR out of the OK publish's stage (a deep result's R_CheckStack
   error, a region-create failure), unwinds to worker_main with this
   task's sink mintable through mizu_pool_run_outcome and comes back as
   its ERR result: fail the task, never the worker. */
int mizu_r_exec_pool(const mizu_slot_hdr *hdr, const unsigned char *payload,
                     size_t limit, mizu_result_sink *sink, int catching,
                     mizu_read_ctx *ctx) {
  mizu_pool *p = sink->p;
  mizu_r_handle *rh = (mizu_r_handle *) ctx->binding_ctx;
  /* the boundary collection runs here: this step's reap (ahead of the
     exec) dropped the previous task's REF-result retain, so its argument
     view and task env are unreachable and still in New space — collected,
     not promoted. A nested exec skips it: a collection mid-outer-task
     would only promote the outer task's live views. */
  if (!catching) mizu_zc_gc_boundary();
  /* the submitter-identity word: a nested exec's lives on this stack frame
     (pymizu's sub_ident discipline — helping must not clobber the outer
     task's word); the outermost exec's is the global itself, left readable
     for the unwind path should the body longjmp */
  uint64_t ident_word = 0;
  struct mizu_task_ctx c =
    { rh->prot, hdr, payload, limit, ctx, sink,
      catching ? &ident_word : &mizu_curpool_ident, rh->exec_fail, 0, 1 };
  if (!catching) {
    mizu_pool_eval_mark(p, 1);
    rh->eval_marked = 1;
  }
  /* the current-pool global rides the task body only: the catching = 0
     unwind may longjmp past the restore (the worker is unwinding;
     accepted) */
  SEXP old_pool = mizu_curpool_xp;
  mizu_curpool_xp = rh->xp;
  SEXP value = catching ? R_tryCatchError(pool_task_body, &c,
                                          pool_task_handler, &c) :
                          pool_task_body(&c);
  mizu_curpool_xp = old_pool;
  if (c.died) {
    /* the enqueuer died and its region went along: the task can never run
       anywhere — it fails as LOST (no death verdict on the worker), and
       the drain continues */
    mizu_result_publish_lost(sink);
    if (!catching) {
      mizu_pool_eval_mark(p, 0);
      rh->eval_marked = 0;
    }
    return 0;
  }
  SEXP v = PROTECT(value);
  /* the D5 outcome sequence: one mark-list reset for the whole write
     (the §4.2 result write, or the ERR flatten inside the containment),
     and one after it — the emission records hold views */
  mizu_zc_ref_reset();
  if (c.ok) {
    /* a foreign submitter's result stages under the §4.2 foreign writer
       policy (the capability mask rides the handle through the publish);
       the body's gate already preflighted the fit inside the containment */
    const uint64_t ident = *c.identp;
    if (ident != 0 && (ident & 0xff) != MIZU_LANG_R) {
      rh->peer_lang = (uint32_t) (ident & 0xff);
      rh->peer_caps = (uint32_t) (ident >> 32);
      mizu_result_publish(sink, (void *) v);
      rh->peer_lang = 0;
      rh->peer_caps = 0;
    } else {
      mizu_result_publish(sink, (void *) v);
    }
  } else {
    mizu_r_publish_err(sink, v, *c.identp);
  }
  /* the second D5 reset: the write's emission record has served the
     publish. The argument loans themselves ride the views' ordinary
     release records — GC-gated like every other view flow (no public
     exact reference count exists to prove a task kept none) */
  mizu_zc_ref_reset();
  /* the mark stays armed across the publish: a raise inside it is this
     task's error, and the unwind path mints its ERR */
  if (!catching) {
    mizu_pool_eval_mark(p, 0);
    rh->eval_marked = 0;
  }
  UNPROTECT(1);                    /* v */
  return 0;
}

// Pool trace --------------------------------------------------------------------

/* The trace thunk: the core's emit sites call through the handle's
   registration; the R closure rides prot[1]. An error raised here longjmps
   like any infrastructure error at the emit site — never the in-flight
   task's own error, so the eval mark lifts around the R call (restored on
   return; the emit sites are not the task's computation, its own "done"
   emit at the publish included). */
void mizu_r_trace(mizu_trace_event event, uint64_t task_id, void *ctx) {
  static const char *const events[] = {
    "submit", "start", "done", "error", "drop", "rehome"
  };
  mizu_r_handle *rh = (mizu_r_handle *) ctx;
  SEXP fn = VECTOR_ELT(rh->prot, 1);
  if (TYPEOF(fn) != CLOSXP) return;
  char buf[32];
  snprintf(buf, sizeof(buf), "%u:%llu", (unsigned) (task_id >> 48),
           (unsigned long long) (task_id & ((1ull << 48) - 1)));
  SEXP ev = PROTECT(Rf_mkString(events[event]));
  SEXP tid = PROTECT(Rf_mkString(buf));
  SEXP call = PROTECT(Rf_lang3(fn, ev, tid));
  mizu_pool *p = (mizu_pool *) rh->core;
  const int marked = rh->eval_marked;
  if (marked) mizu_pool_eval_mark(p, 0);
  Rf_eval(call, R_GlobalEnv);
  if (marked) mizu_pool_eval_mark(p, 1);
  UNPROTECT(3);
}

// Pin chain ---------------------------------------------------------------------

/* Package-local precious list: one cons cell per pin, chained through the
   handle's prot vector. O(1) push at stage, O(1) tombstone at drop; dead
   cells are spliced lazily at stage time. The cell is the core's opaque
   pin token. Invariant: pinned objects are never R_NilValue (the NIL tier
   pins nothing), so CAR == R_NilValue marks a dead cell. */
#define MIZU_SPLICE_MIN 64   /* dead cells before a splice is considered */

/* Unlink the tombstoned cells and recount. Live cells keep their addresses,
   so outstanding tokens never dangle. Runs only at stage time — never in
   the drop hook, which mutates no chain linkage. */
static void pins_splice(mizu_r_handle *rh) MIZU_COLD;
static void pins_splice(mizu_r_handle *rh) {
  SEXP prev = R_NilValue;   /* R_NilValue while no live head cell is seen */
  SEXP cell = VECTOR_ELT(rh->prot, rh->pin_slot);
  uint32_t live = 0;
  while (cell != R_NilValue) {
    SEXP next = CDR(cell);
    if (CAR(cell) == R_NilValue) {
      if (prev == R_NilValue)
        SET_VECTOR_ELT(rh->prot, rh->pin_slot, next);
      else
        SETCDR(prev, next);
    } else {
      prev = cell;
      live++;
    }
    cell = next;
  }
  rh->pins_total = live;
  rh->pins_dead = 0;
}

/* Pin the staged object: push a fresh cons cell onto the handle's chain and
   register the cell as the core's opaque pin token. The cons precedes
   mizu_stage_pin, preserving the longjmp ordering — a failed cons abandons
   the stage with nothing pinned, and rollback pairs each committed pin with
   exactly one drop. The fresh cell is stored into the anchored prot slot
   with no allocation between creation and store. The splice gate (at least
   MIZU_SPLICE_MIN dead, and dead at least half the chain) bounds the chain
   to ~2x live pins between splices. ctx is the stage hook's binding ctx
   (the mizu_r_handle — no per-stage handle query needed). */
void mizu_r_pin(mizu_handle *h, void *ctx, SEXP x) {
  mizu_r_handle *rh = (mizu_r_handle *) ctx;
  if (rh->pins_dead >= MIZU_SPLICE_MIN &&
      rh->pins_dead >= rh->pins_total / 2)
    pins_splice(rh);
  SEXP cell = CONS(x, VECTOR_ELT(rh->prot, rh->pin_slot));
  SET_VECTOR_ELT(rh->prot, rh->pin_slot, cell);
  rh->pins_total++;
  mizu_stage_pin(h, (void *) cell);
}

// Interrupt / GC / cache hooks ----------------------------------------------------

/* The check hook: R_CheckUserInterrupt longjmps (never returns nonzero), so
   the abandon-safe-point contract is what makes the call legal through core
   frames. */
int mizu_r_check(void *ctx) {
  (void) ctx;
  R_CheckUserInterrupt();
  return 0;
}

/* The drop hook: tombstone the pin's chain cell, releasing the staged
   object to the GC. Balanced against the stager's mizu_r_pin at every
   release point (collect, slot reuse, the worker keeper sweep, a cancelled
   publish, a rollback, teardown). No allocation and no chain mutation, so
   safe from extptr finalizers; fires only on the handle-owning thread. Dead
   cells the splice never reached die with the extptr's prot at GC. */
void mizu_r_drop(void *ctx, void *pin) {
  SETCAR((SEXP) pin, R_NilValue);
  ((mizu_r_handle *) ctx)->pins_dead++;
}

/* The sweep hook: a pool worker going idle or departing drops its map
   cache (prot[2]) — the per-worker context env, re-created lazily by
   mizu_pool_map_cache on the next map. It deliberately does not run the
   boundary collection: at idle the previous REF result is still retained,
   so a collection would promote it and the task env to Old[0], slipping
   the release to the level-1 cadence (the generation trap — the exec-entry
   collection is the well-timed one). */
void mizu_r_sweep(void *ctx) {
  SET_VECTOR_ELT(((mizu_r_handle *) ctx)->prot, 2, R_NilValue);
}
