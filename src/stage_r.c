/* The R binding's half of the language seam (rei.h's rei_binding). For the
   channel: the SEXP <-> framed-bytes tier dispatch for sends
   (rei_r_stage_channel) and the materialize for receives
   (rei_r_read_channel). For the pool: the stage_fn every handle registers
   (rei_r_stage_pool), the collect read_fn (rei_r_read_pool), the exec_fn a
   worker handle registers (rei_r_exec_pool — task frame decode, the eval,
   and the result publish through the sink), the ERR envelope framing shared
   by exec and the unwind path (rei_r_publish_err), and the trace thunk
   (rei_r_trace). The core owns the ring/deque mechanics, the wakes, and the
   retain-table commits; this file owns the payload framing policy and the
   task evaluation. The check/drop/sweep hooks bridge R's interrupt, GC, and
   per-worker cache lifecycle. */

#include <stdio.h>
#include <string.h>
#include "rei.h"
#include <R_ext/Utils.h>

// Channel stage -------------------------------------------------------------------

/* The channel's tier dispatch: frame x as (hdr, payload) — payload capacity
   inline_max — retaining through the handle's services (the core commits the
   staging entry on success). Arena chunks come from rei_stage_arena_alloc,
   the arena base staying core-private; region checkouts ride the spill
   helpers on the handle's free list. Raises on failure, never returns
   nonzero. */
int rei_r_stage_channel(void *obj, rei_slot_hdr *hdr,
                         unsigned char *payload, uint32_t inline_max,
                         rei_handle *h) {
  SEXP x = (SEXP) obj;
  size_t rawlen, total;
  uint64_t off;
  unsigned char *chunk;

  /* NULL is the immediate kind — no serialize pass, no receive alloc */
  if (x == R_NilValue) {
    hdr->kind = REI_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
  } else if (rei_zc_ref_stage(hdr, payload, inline_max, x)) {
    /* a rei-native view crosses by reference (REF) at any size — required
       once SHM_VEC views exist: the serialize-hook fallback resolves
       uncounted, and the producer could recycle under the far side's view;
       the pin is the view itself */
    R_PreserveObject(x);
    rei_stage_pin(h, (void *) x);
  } else if (rei_raw_eligible(x, inline_max, &rawlen)) {
    memcpy(payload, rei_vec_ptr(x), rawlen);
    hdr->kind = REI_KIND_RAWVEC;
    hdr->len = (uint32_t) rawlen;
    hdr->aux = (uint64_t) TYPEOF(x);
  } else if (rei_str1_stage(hdr, payload, inline_max, x)) {
    /* a length-1 string's bytes are self-contained: pin nothing */
  } else if (rei_raw_type(x, &rawlen) && rawlen > inline_max &&
             rawlen <= UINT32_MAX &&
             (rawlen <= REI_ZC_FLOOR_RAW || rei_handle_churn(h)) &&
             (chunk = rei_stage_arena_alloc(h, REI_ALIGN64(rawlen),
                                            &off)) != NULL) {
    /* Raw-bytes arena spill: the vectors RAWVEC takes inline, past the
       inline budget. Bare bytes skip the serialize pass here and the
       parse at the far end; the chunk's lifetime tracks ring advance like
       any arena payload, and nothing is pinned (no identifier can ride
       along). Sits ahead of the zc tier up to REI_ZC_FLOOR_RAW (the
       arena copy beats the view there) and serves as the churn-immune
       fallback past it; an arena miss falls through to zc/serialize. */
    memcpy(chunk, rei_vec_ptr(x), rawlen);
    hdr->kind = REI_KIND_RAWSPILL;
    hdr->len = (uint32_t) rawlen;
    hdr->aux = (uint64_t) TYPEOF(x);
    memcpy(payload, &off, sizeof(off));
  } else if (rei_zc_eligible(x, inline_max, &total) &&
             !rei_handle_churn(h)) {
    /* eligible objects past the budget go straight to SHM_VEC, skipping
       the arena: arena receive pays a full unserialize and a chunk can
       never hold a view (chunk lifetime tracks ring advance). The churn
       gate (Linux-only): while lent regions prove consumer views outlive
       their traffic, the copy tiers below are cheaper — the arena and
       SHM_RAW surrender deterministically, where a fresh SHM_VEC region
       per message would pile up in the ledger */
    rei_stage_reap(h);
    rei_zc_stage(hdr, payload, x, total, h);
  } else {
    /* the compact codec ahead of R_Serialize (payload.c): a codec stream
       is self-contained — the writer rejects ALTREP, so no hook-emitted
       mori identifier can ride along — and pins nothing */
    size_t n = rei_codec_write(payload, inline_max, x);
    int self_contained = n != 0;
    if (!self_contained)
      n = rei_serialize_bounded(payload, inline_max, x);
    if (n <= inline_max) {
      hdr->kind = REI_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = 0;
      if (!self_contained) {
        R_PreserveObject(x);
        rei_stage_pin(h, (void *) x);
      }
    } else {
      if ((chunk = rei_stage_arena_alloc(h, REI_ALIGN64(n), &off)) != NULL) {
        if (self_contained) {
          if (rei_codec_write(chunk, n, x) != n)
            Rf_error("rei: codec write mismatch");
        } else {
          mori_serialize_into(chunk, x);
        }
        hdr->kind = REI_KIND_ARENA;
        hdr->len = 0;
        hdr->aux = off;
        uint64_t n64 = (uint64_t) n;
        memcpy(payload, &n64, sizeof(n64));
        if (!self_contained) {
          R_PreserveObject(x);
          rei_stage_pin(h, (void *) x);
        }
      } else {
        /* reap before staging: the consumer's latest head publish may
           have released a fitting region for this very spill to pop */
        rei_stage_reap(h);
        if (self_contained)
          rei_payload_spill_codec(hdr, payload, x, n, h);
        else
          rei_payload_spill_shm(hdr, payload, x, n, h);
      }
    }
  }
  return 0;
}

/* The batch-verb sink (recv_batch_fn / collect_all_fn): anchor each read
   product in a pre-PROTECTed VECSXP as it is delivered, so no SEXP sits
   unprotected across the next read's allocations. SET_VECTOR_ELT does
   not allocate. */
void rei_vec_sink(void *ctx, size_t i, void *obj) {
  SET_VECTOR_ELT((SEXP) ctx, (R_xlen_t) i, (SEXP) obj);
}

// Channel read --------------------------------------------------------------------

/* The transport has already resolved an arena-referencing frame to its byte
   range (payload, limit), so the ARENA / channel RAWSPILL kinds below read
   resolved bytes — the arena base never leaves the transport. Everything
   else defers to the shared payload reader (ctx carries the handle's open
   cache and the R-side view cache). Returns the object, or NULL with
   ctx->gone set on a vanished out-of-line region. A foreign (Python) stream
   comes back as rei_mark_foreign with the handle's saw_foreign set, so the
   core consumes the slot before the recv veneer raises on the flag — a
   raise here would leave the slot in place and wedge the ring behind it. */
void *rei_r_read_channel(const rei_slot_hdr *hdr,
                          const unsigned char *payload, size_t limit,
                          rei_read_ctx *ctx) {
  if (hdr->kind == REI_KIND_ARENA) {
    /* resolved stream bytes; limit is the arena-validated length */
    if (payload[0] == REI_CODEC_MAGIC)
      return (void *) rei_codec_read(payload, limit);
    if (rei_is_python_payload(payload, limit)) {
      ((rei_r_handle *) ctx->binding_ctx)->saw_foreign = 1;
      return (void *) rei_mark_foreign;
    }
    return (void *) mori_unserialize_from((unsigned char *) payload, limit);
  }
  if (hdr->kind == REI_KIND_RAWSPILL) {
    /* resolved RAWVEC bytes (the pool's region framing of this kind is
       read in rei_payload_read) */
    int type = (int) hdr->aux;
    size_t elt = mori_sizeof_elt(type);
    if (elt == 0 || hdr->len % elt != 0)
      Rf_error("rei: corrupt payload slot");
    SEXP y = Rf_allocVector((SEXPTYPE) type, (R_xlen_t) (hdr->len / elt));
    memcpy(rei_vec_ptr(y), payload, hdr->len);
    return (void *) y;
  }
  return rei_payload_read(hdr, payload, (uint32_t) limit, ctx,
                          rei_mark_foreign);
}

// Pool stage --------------------------------------------------------------------

/* The pool's stage_fn: the payload.c tier dispatch on the handle's services,
   registered at create/join/attach. Serves task payloads at submit and
   result payloads at publish — a pool has no arena, so out-of-line frames
   are always named regions. Raises on failure, never returns nonzero. */
int rei_r_stage_pool(void *obj, rei_slot_hdr *hdr,
                      unsigned char *payload, uint32_t inline_max,
                      rei_handle *h) {
  rei_payload_stage(hdr, payload, inline_max, (SEXP) obj, h);
  return 0;
}

// Pool read (collect) ------------------------------------------------------------

/* The pool's read_fn: materialize a collected result. ctx->outcome carries
   the terminal state: OK/ERR read the payload frame (ERR's is the flattened
   transport condition), CANCEL/DIED carry no payload and build the binding's
   error object from the claimant record. Non-OK outcomes come back boxed in
   a rei_caught list — the marker the collect veneer branches on (a task
   value that is itself a condition stays bare). The product is unprotected:
   the core's claim tail allocates nothing before the verb returns it. */
void *rei_r_read_pool(const rei_slot_hdr *hdr, const unsigned char *payload,
                       size_t limit, rei_read_ctx *ctx) {
  switch (ctx->outcome) {
  case REI_RS_OK:
    /* NULL foreign: a pool is R-only, so a foreign stream is corruption —
       raise in place rather than consume */
    return rei_payload_read(hdr, payload, (uint32_t) limit, ctx, NULL);
  case REI_RS_ERR: {
    SEXP cond = rei_payload_read(hdr, payload, (uint32_t) limit, ctx, NULL);
    if (cond == NULL) return NULL;              /* ctx->gone set */
    return rei_caught(cond);
  }
  case REI_RS_DIED:
    return rei_caught_died((int) ctx->died_slot, (double) ctx->died_pid,
                            "rei: worker died while executing this task");
  case REI_RS_CANCEL:
    return rei_caught_cond("rei_error_cancelled",
                            "rei: task cancelled or pool stopped");
  }
  return NULL;
}

// Pool exec ---------------------------------------------------------------------

/* The task evaluator: one wire payload — list(expr, named args) — with the
   arguments bound into a fresh unhashed frame under the base environment
   (prot[0], set by rei_pool_set_eval). Two error disciplines, chosen by the
   caller. The worker loop's hot path (catching = 0) arms no handler at all:
   a user error longjmps out of rei_pool_step and worker_main publishes the
   caught condition as this task's ERR result through rei_pool_run_outcome —
   the eval marker (rei_pool_eval_mark) is what separates those errors from
   infrastructure failure, which stays fatal. Help mode and nested submit's
   inline execute (catching = 1) run inside a task's own evaluation, where an
   escaping error would land in the wrong task's frames: they contain it with
   R_tryCatchError and pay its R-closure trampoline. */
struct rei_eval_ctx { SEXP expr; SEXP env; int ok; };

static SEXP pool_eval_body(void *data) {
  struct rei_eval_ctx *c = (struct rei_eval_ctx *) data;
  return Rf_eval(c->expr, c->env);
}

static SEXP pool_eval_handler(SEXP cond, void *data) {
  ((struct rei_eval_ctx *) data)->ok = 0;
  return cond;
}

static SEXP pool_eval_expr(rei_pool *p, SEXP prot, SEXP expr, SEXP args,
                           int catching, int *ok) {
  SEXP names = PROTECT(Rf_getAttrib(args, R_NamesSymbol));
  R_xlen_t n = Rf_xlength(args);
  if (n > 0 && TYPEOF(names) != STRSXP)
    Rf_error("rei: corrupt task payload");
  /* eval is the identity on value types: a constant task (the canonical
     trivial task, and every constant result of a nested computation) binds
     no arguments and needs no fresh environment — the per-task R_NewEnv is
     the whole cost here */
  switch (TYPEOF(expr)) {
  case NILSXP: case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP:
  case STRSXP: case RAWSXP: case VECSXP:
    *ok = 1;
    UNPROTECT(1);                    /* names */
    return expr;
  }
  SEXP base = VECTOR_ELT(prot, 0);   /* the eval env */
  if (TYPEOF(base) != ENVSXP)
    Rf_error("rei: no evaluator registered on this worker handle");
  SEXP env = PROTECT(R_NewEnv(base, 0, 0));
  for (R_xlen_t i = 0; i < n; i++)
    Rf_defineVar(Rf_installTrChar(STRING_ELT(names, i)),
                 VECTOR_ELT(args, i), env);
  struct rei_eval_ctx c = { expr, env, 1 };
  SEXP value;
  if (catching) {
    value = R_tryCatchError(pool_eval_body, &c, pool_eval_handler, &c);
  } else {
    rei_pool_eval_mark(p, 1);
    value = Rf_eval(c.expr, c.env);
    rei_pool_eval_mark(p, 0);
  }
  *ok = c.ok;
  UNPROTECT(2);                    /* names, env */
  return value;
}

/* The ERR envelope: the caught condition never crosses as itself — it is
   flattened to a transport condition (condition.c) and framed INLINE where
   it fits, so the publish cannot raise: fail the task, never the worker.
   Framed INLINE directly rather than through the tiered stage: flatten's
   verification pass already guarantees the fit. Below it (a tiny slot holds
   no classed condition inline) the tiered stage carries the terminal
   fallback out of line. Shared by exec's catching paths and the unwind path
   (rei_pool_run_outcome). */
void rei_r_publish_err(rei_result_sink *sink, SEXP cond) {
  SEXP flat =
    PROTECT(rei_condition_flatten(cond, (size_t) sink->inline_max));
  size_t n =
    rei_codec_write(sink->payload, (size_t) sink->inline_max, flat);
  rei_result_publish_err(sink, (void *) flat,
                         n != 0 && n <= (size_t) sink->inline_max ?
                         (uint32_t) n : 0);
  UNPROTECT(1);
}

/* The worker's task: decode the frame, evaluate, publish through the sink.
   An INLINE codec task frame stream-decodes in place — no list(expr, args)
   materialization, so a constant task allocates nothing on the worker.
   Anything else takes the generic read and its shape check, over a read_ctx
   fabricated here (exec_fn receives none): the handle's open cache and the
   R-side view cache ride it. Both paths end with expr and args PROTECTed. */
int rei_r_exec_pool(const rei_slot_hdr *hdr, const unsigned char *payload,
                     size_t limit, rei_result_sink *sink, int catching,
                     void *ctx) {
  rei_pool *p = sink->p;
  SEXP expr = R_NilValue, args = R_NilValue;
  if (hdr->kind == REI_KIND_INLINE && hdr->len <= limit &&
      rei_codec_read_task(payload, (size_t) hdr->len, &expr, &args)) {
    PROTECT(expr);
    PROTECT(args);
  } else {
    rei_read_ctx rctx;
    memset(&rctx, 0, sizeof(rctx));
    rctx.size = (uint32_t) sizeof(rctx);
    rctx.outcome = REI_RS_OK;
    rctx.died_slot = -1;
    rctx.handle = (rei_handle *) p;
    rctx.binding_ctx = ctx;
    SEXP pl = rei_payload_read(hdr, payload, (uint32_t) limit, &rctx, NULL);
    if (rctx.gone) {
      /* the enqueuer died and its region went along: the task can never
         run anywhere — it fails as DIED, and the drain continues */
      rei_result_publish_died(sink);
      return 0;
    }
    if (pl == NULL || TYPEOF(pl) != VECSXP || Rf_xlength(pl) != 2 ||
        TYPEOF(VECTOR_ELT(pl, 1)) != VECSXP)
      Rf_error("rei: corrupt task payload");
    expr = PROTECT(VECTOR_ELT(pl, 0));
    args = PROTECT(VECTOR_ELT(pl, 1));
  }
  int ok = 1;
  SEXP value =
    PROTECT(pool_eval_expr(p, ((rei_r_handle *) ctx)->prot, expr, args,
                           catching, &ok));
  if (ok) {
    rei_result_publish(sink, (void *) value);
  } else {
    rei_r_publish_err(sink, value);
  }
  UNPROTECT(3);                    /* expr, args, value */
  return 0;
}

// Pool trace --------------------------------------------------------------------

/* The trace thunk: the core's emit sites call through the handle's
   registration; the R closure rides prot[1]. An error raised here longjmps
   like any infrastructure error at the emit site. */
void rei_r_trace(rei_trace_event event, uint64_t task_id, void *ctx) {
  static const char *const events[] = {
    "submit", "start", "done", "error", "drop", "rehome"
  };
  SEXP fn = VECTOR_ELT(((rei_r_handle *) ctx)->prot, 1);
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

// Interrupt / GC / cache hooks ----------------------------------------------------

/* The check hook: R_CheckUserInterrupt longjmps (never returns nonzero), so
   the abandon-safe-point contract is what makes the call legal through core
   frames. */
int rei_r_check(void *ctx) {
  (void) ctx;
  R_CheckUserInterrupt();
  return 0;
}

/* The drop hook: release the staged object's R_PreserveObject, balanced
   against the stager's preserve at every release point (collect, slot
   reuse, the worker keeper sweep, a cancelled publish, a rollback,
   teardown). Fires only on the handle-owning thread. */
void rei_r_drop(void *ctx, void *pin) {
  (void) ctx;
  R_ReleaseObject((SEXP) pin);
}

/* The sweep hook: a pool worker going idle or departing drops its map
   cache (prot[2]) — the per-worker context env, re-created lazily by
   rei_pool_map_cache on the next map. */
void rei_r_sweep(void *ctx) {
  SET_VECTOR_ELT(((rei_r_handle *) ctx)->prot, 2, R_NilValue);
}
