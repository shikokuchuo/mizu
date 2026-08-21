/* The R binding's half of the language seam (sora.h's sora_binding). For
   the channel: the SEXP <-> framed-bytes tier dispatch for sends
   (sora_r_stage_channel) and the materialize for receives
   (sora_r_read_channel), registered on every channel handle at
   create/attach and invoked by the transport (channel.c) through
   binding.stage / binding.read. For the pool: the stage_fn every handle
   registers (sora_r_stage_pool), the exec_fn a worker handle registers
   at sora_pool_set_eval (sora_r_exec_pool — task frame decode, the eval,
   and the result publish through the sink), the ERR envelope framing
   shared by exec and the unwind path (sora_r_publish_err), and the
   trace thunk (sora_r_trace). The transports own the ring/deque
   mechanics, the wakes, and the retain-table commits; this file owns
   the payload framing policy and the task evaluation. */

#include <stdio.h>
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

// Pool stage --------------------------------------------------------------------

/* The pool's stage_fn: the payload.c tier dispatch on the handle's free
   list, registered at create/join/attach. Serves task payloads at submit
   and result payloads at publish — a pool has no arena, so out-of-line
   frames are always named regions. Raises on failure, never returns
   nonzero. */
int sora_r_stage_pool(void *obj, sora_slot_hdr *hdr,
                      unsigned char *payload, uint32_t inline_max,
                      void *handle, sora_keeper *out) {
  sora_pool *p = (sora_pool *) handle;
  sora_payload_stage(hdr, payload, (size_t) inline_max, (SEXP) obj,
                     &p->fl, out);
  return 0;
}

// Pool exec ---------------------------------------------------------------------

/* The task evaluator: one wire payload — list(expr, named args) — with
   the arguments bound into a fresh unhashed frame under the base
   environment (prot[1], set by sora_pool_set_eval). Two error
   disciplines, chosen by the caller. The worker loop's hot path
   (catching = 0) arms no handler at all: a user error longjmps out of
   sora_pool_step and worker_main publishes the caught condition as this
   task's ERR result through sora_pool_run_outcome — the in_eval flag is
   what separates those errors from infrastructure failure, which stays
   fatal. Help mode and nested submit's inline execute (catching = 1)
   run inside a task's own evaluation, where an escaping error would
   land in the wrong task's frames: they contain it with R_tryCatchError
   and pay its R-closure trampoline — several µs, still cheaper than the
   park that helping replaced. */
struct sora_eval_ctx { SEXP expr; SEXP env; int ok; };

static SEXP pool_eval_body(void *data) {
  struct sora_eval_ctx *c = (struct sora_eval_ctx *) data;
  return Rf_eval(c->expr, c->env);
}

static SEXP pool_eval_handler(SEXP cond, void *data) {
  ((struct sora_eval_ctx *) data)->ok = 0;
  return cond;
}

static SEXP pool_eval_expr(sora_pool *p, SEXP prot, SEXP expr, SEXP args,
                           int catching, int *ok) {
  SEXP names = PROTECT(Rf_getAttrib(args, R_NamesSymbol));
  R_xlen_t n = Rf_xlength(args);
  if (n > 0 && TYPEOF(names) != STRSXP)
    Rf_error("sora: corrupt task payload");
  /* eval is the identity on value types: a constant task (the canonical
     trivial task, and every constant result of a nested computation)
     binds no arguments and needs no fresh environment — the per-task
     R_NewEnv is the whole cost here */
  switch (TYPEOF(expr)) {
  case NILSXP: case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP:
  case STRSXP: case RAWSXP: case VECSXP:
    *ok = 1;
    UNPROTECT(1);                    /* names */
    return expr;
  }
  SEXP base = VECTOR_ELT(prot, 1);
  if (TYPEOF(base) != ENVSXP)
    Rf_error("sora: no evaluator registered on this worker handle");
  SEXP env = PROTECT(R_NewEnv(base, 0, 0));
  for (R_xlen_t i = 0; i < n; i++)
    Rf_defineVar(Rf_installTrChar(STRING_ELT(names, i)),
                 VECTOR_ELT(args, i), env);
  struct sora_eval_ctx c = { expr, env, 1 };
  SEXP value;
  if (catching) {
    value = R_tryCatchError(pool_eval_body, &c, pool_eval_handler, &c);
  } else {
    p->in_eval = 1;
    value = Rf_eval(c.expr, c.env);
    p->in_eval = 0;
  }
  *ok = c.ok;
  UNPROTECT(2);                    /* names, env */
  return value;
}

/* The ERR envelope: the caught condition never crosses as itself — it is
   flattened to a transport condition (condition.c) and framed INLINE
   where it fits, so the publish cannot raise: fail the task, never the
   worker. Framed INLINE directly rather than through the tiered stage:
   flatten's verification pass already guarantees the fit, so the tier
   probes would only repeat the codec write to arrive at the same frame.
   Below flatten's guarantee (a 128-byte slot holds no classed condition
   inline) the tiered stage carries the terminal fallback out of line —
   the pre-flattening behavior for that configuration. Shared by exec's
   catching paths and the unwind path (sora_pool_run_outcome). */
void sora_r_publish_err(sora_result_sink *sink, SEXP cond) {
  SEXP flat =
    PROTECT(sora_condition_flatten(cond, (size_t) sink->inline_max));
  size_t n =
    sora_codec_write(sink->payload, (size_t) sink->inline_max, flat);
  sora_result_publish_err(sink, (void *) flat,
                          n != 0 && n <= (size_t) sink->inline_max ?
                          (uint32_t) n : 0);
  UNPROTECT(1);
}

/* The worker's task: decode the frame, evaluate, publish through the
   sink. An INLINE codec task frame stream-decodes in place — no
   list(expr, args) materialization, so a constant task allocates nothing
   on the worker. Anything else takes the generic read and its shape
   check. Both paths end with expr and args PROTECTed (2 total): the
   reads hand them over unprotected and nothing allocates before the
   PROTECTs. The frame rides the pool's claim scratch — decode completes
   before the task runs; a nested claim reuses the scratch. */
int sora_r_exec_pool(const sora_slot_hdr *hdr,
                     const unsigned char *payload, size_t limit,
                     sora_result_sink *sink, int catching, void *ctx) {
  sora_pool *p = sink->p;
  SEXP expr = R_NilValue, args = R_NilValue;
  int gone = 0;
  if (hdr->kind == SORA_KIND_INLINE && hdr->len <= limit &&
      sora_codec_read_task(payload, (size_t) hdr->len, &expr, &args)) {
    PROTECT(expr);
    PROTECT(args);
  } else {
    SEXP pl = sora_payload_read(hdr, payload, (uint32_t) limit, &gone,
                                &p->oc, &p->zoc);
    if (gone) {
      /* the enqueuer died and its region went along: the task can never
         run anywhere — it fails as DIED, and the drain continues */
      sora_result_publish_died(sink);
      return 0;
    }
    if (TYPEOF(pl) != VECSXP || Rf_xlength(pl) != 2 ||
        TYPEOF(VECTOR_ELT(pl, 1)) != VECSXP)
      Rf_error("sora: corrupt task payload");
    expr = PROTECT(VECTOR_ELT(pl, 0));
    args = PROTECT(VECTOR_ELT(pl, 1));
  }
  int ok = 1;
  SEXP value =
    PROTECT(pool_eval_expr(p, (SEXP) ctx, expr, args, catching, &ok));
  if (ok) {
    sora_result_publish(sink, (void *) value);
  } else {
    sora_r_publish_err(sink, value);
  }
  UNPROTECT(3);                    /* expr, args, value */
  return 0;
}

// Pool trace --------------------------------------------------------------------

/* The trace thunk: the core's emit sites call through the handle's
   registration; the R closure rides prot[2]. An error raised here
   longjmps like any infrastructure error at the emit site. */
void sora_r_trace(sora_trace_event event, uint64_t task_id, void *ctx) {
  static const char *const events[] = {
    "submit", "start", "done", "error", "drop", "rehome"
  };
  SEXP fn = VECTOR_ELT((SEXP) ctx, 2);
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
