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
#include <R_ext/Utils.h>

/* ANY_ATTRIB() joined the C API in R 4.5.0; equivalent fallback for
   earlier R, where ATTRIB() was still the sanctioned spelling. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

// Channel stage -------------------------------------------------------------------

/* The channel's tier dispatch: frame x as (hdr, payload) — payload capacity
   inline_max — retaining through the handle's services (the core commits the
   staging entry on success). Arena chunks come from mizu_stage_arena_alloc,
   the arena base staying core-private; region checkouts ride the spill
   helpers on the handle's free list. Raises on failure, never returns
   nonzero. */
int mizu_r_stage_channel(void *obj, mizu_slot_hdr *hdr,
                         unsigned char *payload, uint32_t inline_max,
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

/* The batch-verb sink (recv_batch_fn / collect_all_fn): anchor each read
   product in a pre-PROTECTed VECSXP as it is delivered, so no SEXP sits
   unprotected across the next read's allocations. SET_VECTOR_ELT does
   not allocate. */
void mizu_vec_sink(void *ctx, size_t i, void *obj) {
  SET_VECTOR_ELT((SEXP) ctx, (R_xlen_t) i, (SEXP) obj);
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
void *mizu_r_read_channel(const mizu_slot_hdr *hdr,
                          const unsigned char *payload, size_t limit,
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
    if (elt == 0 || hdr->len % elt != 0)
      Rf_error("mizu: corrupt payload slot");
    SEXP y = mizu_wire_alloc(type, (R_xlen_t) (hdr->len / elt));
    memcpy(mizu_vec_ptr(y), payload, hdr->len);
    return (void *) y;
  }
  return mizu_payload_read(hdr, payload, (uint32_t) limit, ctx, 1);
}

// Pool stage --------------------------------------------------------------------

/* The pool's stage_fn: the payload.c tier dispatch on the handle's services,
   registered at create/join/attach. Serves task payloads at submit and
   result payloads at publish — a pool has no arena, so out-of-line frames
   are always named regions. Raises on failure, never returns nonzero. */
int mizu_r_stage_pool(void *obj, mizu_slot_hdr *hdr,
                      unsigned char *payload, uint32_t inline_max,
                      mizu_handle *h, void *ctx) {
  mizu_payload_stage(hdr, payload, inline_max, (SEXP) obj, h, ctx);
  return 0;
}

// Pool read (collect) ------------------------------------------------------------

/* The pool's read_fn: materialize a collected result. ctx->outcome carries
   the terminal state: OK/ERR read the payload frame (ERR's is the flattened
   transport condition), CANCEL/DIED carry no payload and build the binding's
   error object from the claimant record. Non-OK outcomes come back boxed in
   a mizu_caught list — the marker the collect veneer branches on (a task
   value that is itself a condition stays bare). The product is unprotected:
   the core's claim tail allocates nothing before the verb returns it. */
void *mizu_r_read_pool(const mizu_slot_hdr *hdr, const unsigned char *payload,
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
  case MIZU_RS_CANCEL:
    return mizu_caught_cond("mizu_error_cancelled",
                            "mizu: task cancelled or pool stopped");
  }
  return NULL;
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
  int fail;                       /* test-only: a decode failure on demand */
  int died;                       /* decode: the enqueuer's region is gone */
  int ok;                         /* 0 once the catch handler has fired */
};

static SEXP pool_task_handler(SEXP cond, void *data) {
  ((struct mizu_task_ctx *) data)->ok = 0;
  return cond;
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
  if (c->fail)                   /* test-only seam: see mizu_pool_exec_fail */
    Rf_error("mizu: corrupt task payload");
  SEXP expr = R_NilValue, args = R_NilValue;
  if (c->hdr->kind == MIZU_KIND_INLINE && c->hdr->len <= c->limit &&
      mizu_codec_read_task(c->payload, (size_t) c->hdr->len,
                           &expr, &args)) {
    PROTECT(expr);
    PROTECT(args);
  } else {
    SEXP pl = mizu_payload_read(c->hdr, c->payload, (uint32_t) c->limit,
                                c->ctx, 0);
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
void mizu_r_publish_err(mizu_result_sink *sink, SEXP cond) {
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
   of the body, cleared at every normal return, both under catching = 0
   only (the design comment above) — so a longjmp out of the body, a frame
   that fails to decode included, unwinds to worker_main with this task's
   sink mintable through mizu_pool_run_outcome and comes back as its ERR
   result: fail the task, never the worker. */
int mizu_r_exec_pool(const mizu_slot_hdr *hdr, const unsigned char *payload,
                     size_t limit, mizu_result_sink *sink, int catching,
                     mizu_read_ctx *ctx) {
  mizu_pool *p = sink->p;
  mizu_r_handle *rh = (mizu_r_handle *) ctx->binding_ctx;
  struct mizu_task_ctx c =
    { rh->prot, hdr, payload, limit, ctx, rh->exec_fail, 0, 1 };
  if (!catching)
    mizu_pool_eval_mark(p, 1);
  /* the current-pool global rides the task body only: the catching = 0
     unwind may longjmp past the restore (the worker is unwinding;
     accepted) */
  SEXP old_pool = mizu_curpool_xp;
  mizu_curpool_xp = rh->xp;
  SEXP value = catching ? R_tryCatchError(pool_task_body, &c,
                                          pool_task_handler, &c) :
                          pool_task_body(&c);
  mizu_curpool_xp = old_pool;
  if (!catching)
    mizu_pool_eval_mark(p, 0);
  if (c.died) {
    /* the enqueuer died and its region went along: the task can never run
       anywhere — it fails as DIED, and the drain continues */
    mizu_result_publish_died(sink);
    return 0;
  }
  SEXP v = PROTECT(value);
  if (c.ok) {
    mizu_result_publish(sink, (void *) v);
  } else {
    mizu_r_publish_err(sink, v);
  }
  UNPROTECT(1);                    /* v */
  return 0;
}

// Pool trace --------------------------------------------------------------------

/* The trace thunk: the core's emit sites call through the handle's
   registration; the R closure rides prot[1]. An error raised here longjmps
   like any infrastructure error at the emit site. */
void mizu_r_trace(mizu_trace_event event, uint64_t task_id, void *ctx) {
  static const char *const events[] = {
    "submit", "start", "done", "error", "drop", "rehome"
  };
  SEXP fn = VECTOR_ELT(((mizu_r_handle *) ctx)->prot, 1);
  if (TYPEOF(fn) != CLOSXP) return;
  char buf[32];
  snprintf(buf, sizeof(buf), "%u:%llu", (unsigned) (task_id >> 48),
           (unsigned long long) (task_id & ((1ull << 48) - 1)));
  SEXP ev = PROTECT(Rf_mkString(events[event]));
  SEXP tid = PROTECT(Rf_mkString(buf));
  SEXP call = PROTECT(Rf_lang3(fn, ev, tid));
  Rf_eval(call, R_GlobalEnv);
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
   mizu_pool_map_cache on the next map. */
void mizu_r_sweep(void *ctx) {
  SET_VECTOR_ELT(((mizu_r_handle *) ctx)->prot, 2, R_NilValue);
}
