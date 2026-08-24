/* The pool's .Call veneer (Part II): thin R entry points over the vendored
   core's rei_pool_* verbs (vendor/librei). Arg validation, the extptr handles
   (a rei_r_handle wrapping the opaque core handle; a task handle packs the
   core's 8-byte rei_task into its extptr address), the submit failure
   taxonomy, the collect outcome boxing, and the rei_status -> sentinel /
   classed-error mapping live here; the transport
   (registries, rings, deques, result slots, claim/steal, the reaper, the help
   doorbell, the worker loop) is all core-side. */

#include <stdlib.h>
#include <string.h>
#include "rei.h"

static SEXP rei_pool_tag;
static SEXP rei_task_tag;
static SEXP rei_sig_tag;
static SEXP rei_class_pool;
static SEXP rei_class_task;

static void rei_pool_finalizer(SEXP xp);
static void rei_task_finalizer(SEXP xp);

void rei_pool_init(void) {
  rei_pool_tag = Rf_install("rei_pool");
  rei_task_tag = Rf_install("rei_task");
  rei_sig_tag = Rf_install("rei_sig");
  rei_class_pool = Rf_mkString("rei_pool");
  R_PreserveObject(rei_class_pool);
  rei_class_task = Rf_mkString("rei_task");
  R_PreserveObject(rei_class_task);
}

void rei_pool_fini(void) {
  R_ReleaseObject(rei_class_task);
  R_ReleaseObject(rei_class_pool);
}

// Handle access -------------------------------------------------------------------

static rei_r_handle *pool_peek(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != rei_pool_tag)
    Rf_error("rei: not a pool handle");
  rei_r_handle *h = (rei_r_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL || h->core == NULL) return NULL;
  if (h->self_pid != rei_self_pid())
    Rf_error("rei: pool handles do not survive fork()");
  return h;
}

static rei_r_handle *pool_get(SEXP xp) {
  rei_r_handle *h = pool_peek(xp);
  if (h == NULL) Rf_error("rei: pool handle is closed");
  return h;
}

static rei_pool *pool_core(SEXP xp) {
  return (rei_pool *) pool_get(xp)->core;
}

static rei_r_handle *pool_get_worker(SEXP xp) {
  rei_r_handle *h = pool_get(xp);
  if (h->role != REI_ROLE_WORKER)
    Rf_error("rei: not a worker handle");
  return h;
}

/* The R binding registered on every pool handle. exec joins at worker_join
   (a worker always carries its evaluator hook; set_eval arms the env it
   closes over). sweep drops the map cache at idle/depart. */
static void pool_binding(rei_r_handle *h, rei_binding *b, int worker) {
  rei_binding_init(b);
  b->stage = rei_r_stage_pool;
  b->read = rei_r_read_pool;
  b->exec = worker ? rei_r_exec_pool : NULL;
  b->check = rei_r_check;
  b->drop = rei_r_drop;
  b->sweep = rei_r_sweep;
  b->ctx = h;
}

/* Build the extptr around a created/joined/attached core handle: the prot
   chain ([0] eval env, [1] trace fn, [2] map cache, [3] the zc view cache's
   wrap table) and the finalizer. */
static SEXP pool_wrap(rei_r_handle *h) {
  SEXP prot = PROTECT(Rf_allocVector(VECSXP, 4));
  SET_VECTOR_ELT(prot, 3, Rf_allocVector(VECSXP, REI_OPEN_CACHE_MAX));
  h->zoc.wraps = VECTOR_ELT(prot, 3);
  h->prot = prot;
  SEXP xp = PROTECT(R_MakeExternalPtr(h, rei_pool_tag, prot));
  R_RegisterCFinalizerEx(xp, rei_pool_finalizer, TRUE);
  Rf_setAttrib(xp, R_ClassSymbol, rei_class_pool);
  UNPROTECT(2);
  return xp;
}

static void rei_pool_finalizer(SEXP xp) {
  rei_r_handle *h = (rei_r_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) return;
  if (h->core != NULL) {
    /* a controller destroy broadcasts shutdown (no wait); a participant
       releases its slot */
    rei_pool_destroy((rei_pool *) h->core);
    h->core = NULL;
  }
  free(h);
  R_ClearExternalPtr(xp);
}

// Task handles --------------------------------------------------------------------

/* A task handle: the core's 8-byte rei_task packed into the extptr
   address itself (zero heap traffic per task), the pool extptr as its
   prot. The finalizer doubles as the release for an uncollected task. */
static SEXP rei_task_wrap(SEXP pool_xp, const rei_task *t) {
  SEXP txp = PROTECT(R_MakeExternalPtr((void *) (uintptr_t) t->word,
                                       rei_task_tag, pool_xp));
  R_RegisterCFinalizerEx(txp, rei_task_finalizer, TRUE);
  Rf_setAttrib(txp, R_ClassSymbol, rei_class_task);
  UNPROTECT(1);
  return txp;
}

static void rei_task_finalizer(SEXP xp) {
  uintptr_t word = (uintptr_t) R_ExternalPtrAddr(xp);
  if (word == 0) return;
  SEXP pool_xp = R_ExternalPtrProtected(xp);
  rei_r_handle *h = TYPEOF(pool_xp) == EXTPTRSXP ?
    (rei_r_handle *) R_ExternalPtrAddr(pool_xp) : NULL;
  /* the finalizer release: cancels a pending task, frees a terminal one —
     advisory and total (every edge folds to 0), so safe for a stale
     handle, a released pool, or a forked child (guarded) alike */
  if (h != NULL && h->core != NULL && h->self_pid == rei_self_pid()) {
    rei_task t = { (uint64_t) word };
    rei_pool_task_release((rei_pool *) h->core, &t);
  }
  R_ClearExternalPtr(xp);
}

/* Unpack a task handle and its pool. Errors on a foreign or finalized
   handle; the core detects a stale (collected/invalidated) sequence. */
static rei_task task_get(SEXP xp, rei_r_handle **h_out) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != rei_task_tag)
    Rf_error("rei: not a task handle");
  uintptr_t word = (uintptr_t) R_ExternalPtrAddr(xp);
  if (word == 0) Rf_error("rei: task handle is closed");
  rei_r_handle *h = pool_get(R_ExternalPtrProtected(xp));
  *h_out = h;
  rei_task t = { (uint64_t) word };
  return t;
}

// Small helpers -------------------------------------------------------------------

static double timeout_ms_of(SEXP timeout) {
  double t = Rf_asReal(timeout);
  if (!R_FINITE(t)) return -1;
  return t <= 0 ? 0 : t * 1000;
}

/* Raise a pool verb's REI_ERR as a classed error with the handle's recorded
   message (the core's messages are the R contract's). */
static NORET void pool_raise(rei_pool *p) {
  rei_errcat cat = rei_pool_errcat(p);
  const char *msg = rei_pool_error(p);
  switch (cat) {
  case REI_ERRCAT_STOPPED:
    rei_stop("rei_error_stopped", "rei: %s", msg);
  case REI_ERRCAT_EXHAUSTED:
    rei_stop("rei_error_slots_exhausted", "rei: %s", msg);
  default:
    rei_stop("rei_error", "rei: %s", msg);
  }
}

/* Raise a create/attach/join failure off the thread-local slot, where the
   core composes the full message (size + hint included). Space/existence
   failures carry the shm class; everything else is a plain error. */
static NORET void pool_raise_tls(void) {
  rei_errcat cat = rei_last_error_category();
  const char *msg = rei_last_error_message();
  switch (cat) {
  case REI_ERRCAT_NOSPACE:
  case REI_ERRCAT_NOMEMORY:
  case REI_ERRCAT_EXISTS:
    rei_stop_shm(NA_REAL, "rei: %s", msg);
  default:
    Rf_error("rei: %s", msg);
  }
}

// Create (controller) ---------------------------------------------------------------

static int rei_pow2_u64(uint64_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

SEXP rei_pool_create_call(SEXP maxw_sexp, SEXP maxs_sexp, SEXP inj_sexp,
                      SEXP deque_sexp, SEXP rslots_sexp, SEXP slot_sexp) {
  uint64_t maxw = (uint64_t) Rf_asInteger(maxw_sexp);
  uint64_t maxs = (uint64_t) Rf_asInteger(maxs_sexp);
  uint64_t inj_cap = (uint64_t) Rf_asInteger(inj_sexp);
  uint64_t deque_cap = (uint64_t) Rf_asInteger(deque_sexp);
  uint64_t rslots = (uint64_t) Rf_asInteger(rslots_sexp);
  uint64_t slot = (uint64_t) Rf_asInteger(slot_sexp);
  if (maxw < 1 || maxw > REI_MAX_WORKERS)
    Rf_error("rei: max_workers must be between 1 and %d", REI_MAX_WORKERS);
  if (maxs < 1 || maxs > 64)
    Rf_error("rei: max_submitters must be between 1 and 64");
  if (!rei_pow2_u64(inj_cap) || inj_cap < 2 || inj_cap > (1u << 24))
    Rf_error("rei: injection_cap must be a power of two between 2 and 2^24");
  if (!rei_pow2_u64(deque_cap) || deque_cap < 2 || deque_cap > (1u << 24))
    Rf_error("rei: per_worker_cap must be a power of two between 2 and 2^24");
  /* floor 128: a result slot's inline budget (slot - 40) must hold a
     region name (up to 27 bytes on Windows) for an SHM_RAW spill */
  if (!rei_pow2_u64(slot) || slot < 128 || slot > (1u << 20))
    Rf_error("rei: slot_size must be a power of two between 128 and 2^20");
  if (rslots < maxs || rslots > (1u << 24))
    Rf_error("rei: result_slots must be between max_submitters and 2^24");
  rslots = (rslots + maxs - 1) / maxs * maxs;   /* per-submitter partition */

  rei_r_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) Rf_error("rei: allocation failure");
  rei_pool_opts opts;
  rei_pool_opts_init(&opts);
  opts.max_workers = (uint32_t) maxw;
  opts.max_submitters = (uint32_t) maxs;
  opts.injection_cap = (uint32_t) inj_cap;
  opts.per_worker_cap = (uint32_t) deque_cap;
  opts.result_slots = (uint32_t) rslots;
  opts.slot_size = (uint32_t) slot;
  rei_binding b;
  pool_binding(h, &b, 0);

  rei_pool *p;
  if (rei_pool_create(&p, &opts, &b) != REI_OK) {
    free(h);
    pool_raise_tls();
  }
  h->core = (rei_handle *) p;
  h->self_pid = rei_self_pid();
  h->role = REI_ROLE_CONTROLLER;
  return pool_wrap(h);
}

SEXP rei_pool_suffix(SEXP xp) {
  rei_pool *p = pool_core(xp);
  char buf[64];
  if (rei_pool_token(p, buf, sizeof(buf)) != REI_OK)
    pool_raise(p);
  return Rf_mkString(buf);
}

/* Startup / elastic-spawn rendezvous: cover the given worker slots. Returns
   FALSE on deadline expiry. */
SEXP rei_pool_ready_wait_call(SEXP xp, SEXP slots_sexp, SEXP timeout) {
  rei_r_handle *h = pool_get(xp);
  if (h->role != REI_ROLE_CONTROLLER)
    Rf_error("rei: only the controller can wait for workers");
  rei_pool *p = (rei_pool *) h->core;
  if (TYPEOF(slots_sexp) != INTSXP)
    Rf_error("rei: expected worker slot indices");
  R_xlen_t n = XLENGTH(slots_sexp);
  uint32_t *slots = (uint32_t *) R_alloc(n, sizeof(uint32_t));
  for (R_xlen_t i = 0; i < n; i++)
    slots[i] = (uint32_t) INTEGER(slots_sexp)[i];
  rei_status st = rei_pool_ready_wait(p, slots, (size_t) n,
                                      timeout_ms_of(timeout));
  if (st == REI_ERR) pool_raise(p);
  return Rf_ScalarLogical(st == REI_OK);
}

/* Controller only: ask one worker to exit cleanly. */
SEXP rei_pool_retire_call(SEXP xp, SEXP slot_sexp) {
  rei_r_handle *h = pool_get(xp);
  if (h->role != REI_ROLE_CONTROLLER)
    Rf_error("rei: only the controller can retire a worker");
  rei_pool *p = (rei_pool *) h->core;
  uint32_t slot = (uint32_t) Rf_asInteger(slot_sexp);
  if (rei_pool_retire(p, slot) != REI_OK) pool_raise(p);
  return R_NilValue;
}

SEXP rei_pool_destroy_call(SEXP xp) {
  rei_r_handle *h = pool_get(xp);
  if (h->role != REI_ROLE_CONTROLLER)
    Rf_error("rei: only the controller can destroy a pool");
  rei_pool_destroy((rei_pool *) h->core);
  h->core = NULL;
  return R_NilValue;
}

// Worker / submitter join -----------------------------------------------------------

SEXP rei_pool_worker_join_call(SEXP suffix_sexp, SEXP slot_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("rei: expected a region-name suffix");
  const char *suffix = CHAR(STRING_ELT(suffix_sexp, 0));
  uint32_t slot = (uint32_t) Rf_asInteger(slot_sexp);

  rei_r_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) Rf_error("rei: allocation failure");
  rei_binding b;
  pool_binding(h, &b, 1);
  rei_pool *p;
  if (rei_pool_worker_join(&p, suffix, slot, &b) != REI_OK) {
    free(h);
    pool_raise_tls();
  }
  h->core = (rei_handle *) p;
  h->self_pid = rei_self_pid();
  h->role = REI_ROLE_WORKER;
  return pool_wrap(h);
}

SEXP rei_pool_attach_call(SEXP suffix_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("rei: expected a region-name suffix");
  const char *suffix = CHAR(STRING_ELT(suffix_sexp, 0));

  rei_r_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) Rf_error("rei: allocation failure");
  rei_binding b;
  pool_binding(h, &b, 0);
  rei_pool *p;
  if (rei_pool_attach(&p, suffix, &b) != REI_OK) {
    free(h);
    pool_raise_tls();
  }
  h->core = (rei_handle *) p;
  h->self_pid = rei_self_pid();
  h->role = REI_ROLE_SUBMITTER;
  return pool_wrap(h);
}

/* Clean worker exit; the return is unused (worker_main calls it for its
   effect). */
SEXP rei_pool_leave_call(SEXP xp) {
  rei_r_handle *h = pool_peek(xp);
  if (h == NULL) return R_NilValue;
  if (h->role != REI_ROLE_WORKER)
    Rf_error("rei: not a worker handle");
  if (rei_pool_leave((rei_pool *) h->core) != REI_OK)
    pool_raise((rei_pool *) h->core);
  return R_NilValue;
}

/* One lame-duck beat for a retired worker anchoring uncollected results:
   TRUE when the anchor may drop (shutdown or owner death ends the linger). */
SEXP rei_pool_lame_duck_call(SEXP xp) {
  rei_r_handle *h = pool_peek(xp);
  if (h == NULL) return Rf_ScalarLogical(TRUE);
  return Rf_ScalarLogical(rei_pool_lame_duck((rei_pool *) h->core));
}

// Eval / trace registration ---------------------------------------------------------

/* Arms a worker handle for evaluation: a base environment under globalenv()
   binding the handle itself as `pool` — what worker-side nested submit
   closes over. Stashed at prot[0]; the exec hook itself is registered at
   worker_join. */
SEXP rei_pool_set_eval(SEXP xp) {
  rei_r_handle *h = pool_get_worker(xp);
  SEXP base = PROTECT(R_NewEnv(R_GlobalEnv, 0, 0));
  Rf_defineVar(Rf_install("pool"), xp, base);
  SET_VECTOR_ELT(h->prot, 0, base);
  UNPROTECT(1);
  return R_NilValue;
}

/* Per-handle, per-process trace hook: fn(event, id). The R closure rides
   prot[1], reached through stage_r.c's thunk; NULL removes. */
SEXP rei_pool_set_trace_call(SEXP xp, SEXP fn) {
  rei_r_handle *h = pool_get(xp);
  if (fn != R_NilValue && TYPEOF(fn) != CLOSXP)
    Rf_error("rei: expected a function or NULL");
  SET_VECTOR_ELT(h->prot, 1, fn);
  rei_pool *p = (rei_pool *) h->core;
  if (rei_pool_set_trace(p, fn == R_NilValue ? NULL : rei_r_trace,
                         fn == R_NilValue ? NULL : (void *) h) != REI_OK)
    pool_raise(p);
  return R_NilValue;
}

// Submit -----------------------------------------------------------------------------

static void pool_check_task_args(SEXP args) {
  if (TYPEOF(args) != VECSXP)
    Rf_error("rei: expected a list of task arguments");
  R_xlen_t n = XLENGTH(args);
  if (n > 0) {
    SEXP names = Rf_getAttrib(args, R_NamesSymbol);
    int bad = TYPEOF(names) != STRSXP || XLENGTH(names) != n;
    for (R_xlen_t i = 0; !bad && i < n; i++) {
      SEXP nm = STRING_ELT(names, i);
      if (nm == NA_STRING || LENGTH(nm) == 0) bad = 1;
    }
    if (bad)
      Rf_error("rei: all task arguments must be named");
  }
}

/* The shared submit entry. tryflag: ring-full-past-timeout returns the
   rei_timeout sentinel (unambiguous — success returns an external pointer)
   instead of raising rei_error_submit_timeout, so the map submit loop
   needs no handler. Fatal outcomes (stopped, slots exhausted) raise in both
   modes. */
static SEXP pool_submit(SEXP xp, SEXP payload, SEXP timeout, int flags,
                        int tryflag) {
  rei_pool *p = pool_core(xp);
  rei_task t;
  rei_status st = rei_pool_submit_flags(p, (void *) payload,
                                       (uint16_t) flags, &t,
                                       timeout_ms_of(timeout));
  if (st == REI_OK) return rei_task_wrap(xp, &t);
  if (st == REI_FULL) {
    if (tryflag) return rei_sent_timeout;
    rei_stop("rei_error_submit_timeout",
             "rei: submission timed out (injection ring full)");
  }
  pool_raise(p);
}

SEXP rei_pool_submit_call(SEXP xp, SEXP payload, SEXP timeout, SEXP flags_sexp) {
  return pool_submit(xp, payload, timeout, Rf_asInteger(flags_sexp), 0);
}

/* rei_submit's entry: takes the quoted expression and the evaluated args
   list separately and assembles the list(expr, args) wire payload here. */
SEXP rei_pool_submit_expr(SEXP xp, SEXP expr, SEXP args, SEXP timeout,
                           SEXP flags_sexp) {
  pool_check_task_args(args);
  SEXP payload = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(payload, 0, expr);
  SET_VECTOR_ELT(payload, 1, args);
  SEXP out = pool_submit(xp, payload, timeout, Rf_asInteger(flags_sexp), 0);
  UNPROTECT(1);
  return out;
}

/* The map path's entries take the map's one deadline absolute (rei_now()
   timescale, Inf waits indefinitely) and convert once, at entry. */
SEXP rei_pool_submit_try(SEXP xp, SEXP payload, SEXP deadline,
                          SEXP flags_sexp) {
  double d = Rf_asReal(deadline);
  double timeout_s = R_FINITE(d) ? d - rei_now() : R_PosInf;
  return pool_submit(xp, payload, Rf_ScalarReal(timeout_s),
                     Rf_asInteger(flags_sexp), 1);
}

/* rei_submit_batch's entry: one crossing per burst. Each task's wire
   payload is rei_submit's list(expr, args), staged through ONE reusable
   pair — the supply callback swaps the expr in as the core's batch loop
   asks for element i, so a burst allocates nothing per task. Ring-full
   past timeout returns the handles accepted so far; fatal outcomes
   raise, the tasks already submitted staying valid and collectible. */
typedef struct rei_batch_supply_s {
  SEXP exprs;
  SEXP pair;
} rei_batch_supply;

static void *rei_batch_supply_next(void *ctx, size_t i) {
  rei_batch_supply *s = (rei_batch_supply *) ctx;
  SET_VECTOR_ELT(s->pair, 0, VECTOR_ELT(s->exprs, (R_xlen_t) i));
  return s->pair;
}

SEXP rei_pool_submit_batch_call(SEXP xp, SEXP exprs, SEXP args, SEXP timeout,
                            SEXP flags_sexp) {
  if (TYPEOF(exprs) != VECSXP)
    Rf_error("rei: exprs must be a list of expressions");
  pool_check_task_args(args);
  rei_pool *p = pool_core(xp);
  R_xlen_t n = XLENGTH(exprs);
  rei_batch_supply supply;
  supply.exprs = exprs;
  supply.pair = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(supply.pair, 1, args);
  rei_task *ts = (rei_task *) R_alloc(n, sizeof(rei_task));
  size_t done = 0;
  rei_status st = rei_pool_submit_batch_fn(p, rei_batch_supply_next, &supply,
                                           (size_t) n, ts, &done,
                                           timeout_ms_of(timeout));
  UNPROTECT(1);
  if (st == REI_ERR) pool_raise(p);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) done));
  for (size_t i = 0; i < done; i++)
    SET_VECTOR_ELT(out, (R_xlen_t) i, rei_task_wrap(xp, &ts[i]));
  UNPROTECT(1);
  return out;
}

// Worker step / run ------------------------------------------------------------------

/* Single-step (the test harness's discipline) and the worker loop. The
   core's loop owns the wait cadence and the interrupt-check cadence (the
   check hook), so run's timeout is vestigial. A task error longjmps out to
   worker_main's tryCatch; an exit code maps to the integer worker_main
   dispatches on; an infrastructure failure raises (run_outcome reads it as
   outside any task eval). */
SEXP rei_pool_step_call(SEXP xp, SEXP timeout) {
  rei_r_handle *h = pool_get_worker(xp);
  /* exec joins at worker_join unconditionally, so the refusal keys on the
     eval env set_eval arms — up front, so a claim never outruns a missing
     evaluator */
  if (TYPEOF(VECTOR_ELT(h->prot, 0)) != ENVSXP)
    Rf_error("rei: no evaluator registered on this worker handle");
  rei_pool *p = (rei_pool *) h->core;
  int rc = rei_pool_step(p, timeout_ms_of(timeout));
  if (rc == REI_STEP_SHUTDOWN && rei_pool_errcat(p) != REI_ERRCAT_NONE)
    pool_raise(p);   /* an exec infrastructure failure, not a real shutdown */
  return Rf_ScalarInteger(rc);
}

SEXP rei_pool_run(SEXP xp, SEXP timeout) {
  rei_r_handle *h = pool_get_worker(xp);
  if (TYPEOF(VECTOR_ELT(h->prot, 0)) != ENVSXP)
    Rf_error("rei: no evaluator registered on this worker handle");
  (void) timeout;
  rei_pool *p = (rei_pool *) h->core;
  rei_worker_exit ex = rei_pool_worker_run(p);
  switch (ex) {
  case REI_EXIT_SHUTDOWN:
  case REI_EXIT_OWNER_GONE: return Rf_ScalarInteger(-1);
  case REI_EXIT_RETIRED:    return Rf_ScalarInteger(-2);
  case REI_EXIT_ERROR:
  default:
    Rf_error("rei: %s", rei_pool_error(p));
  }
}

/* worker_main's dispatch on what the run produced: an exit code (INTSXP)
   passes through; a caught condition is this task's ERR result when the
   eval marker says a task eval was in flight (rei_pool_unwind_sink mints
   its sink), else infrastructure failure (1) taking the worker down. */
SEXP rei_pool_run_outcome(SEXP xp, SEXP cond) {
  rei_r_handle *h = pool_get_worker(xp);
  if (TYPEOF(cond) == INTSXP)
    return cond;
  rei_result_sink sink;
  if (!rei_pool_unwind_sink((rei_pool *) h->core, &sink))
    return Rf_ScalarInteger(1);
  rei_r_publish_err(&sink, cond);
  return Rf_ScalarInteger(0);
}

/* Test-only: claim up to n injection entries onto this worker's own deque. */
SEXP rei_pool_deque_pull_call(SEXP xp, SEXP n_sexp) {
  rei_r_handle *h = pool_get_worker(xp);
  rei_pool *p = (rei_pool *) h->core;
  int moved = rei_pool_deque_pull(p, (uint32_t) Rf_asInteger(n_sexp));
  if (moved < 0) pool_raise(p);
  return Rf_ScalarInteger(moved);
}

// Collect ----------------------------------------------------------------------------

/* The shared collect entry. tryflag: a terminal non-OK outcome returns the
   rei_caught box instead of signalling, so the map collect loop branches
   on class. */
static SEXP pool_collect_impl(SEXP xp, SEXP timeout, int tryflag) {
  rei_r_handle *h;
  rei_task t = task_get(xp, &h);
  void *v = NULL;
  rei_status st =
    rei_pool_collect((rei_pool *) h->core, &t, &v, timeout_ms_of(timeout));
  if (st == REI_TIMEOUT) return rei_sent_timeout;
  if (st == REI_ERR) pool_raise((rei_pool *) h->core);
  SEXP val = (SEXP) v;
  if (Rf_inherits(val, "rei_caught")) {
    if (tryflag) return val;
    rei_cond_signal(VECTOR_ELT(val, 0));      /* no return */
  }
  return val;
}

SEXP rei_pool_collect_call(SEXP xp, SEXP timeout) {
  return pool_collect_impl(xp, timeout, 0);
}

/* absolute deadline, as rei_pool_submit_try */
SEXP rei_pool_collect_try(SEXP xp, SEXP deadline) {
  double d = Rf_asReal(deadline);
  double timeout_s = R_FINITE(d) ? d - rei_now() : R_PosInf;
  return pool_collect_impl(xp, Rf_ScalarReal(timeout_s), 1);
}

/* Extract a task list's handles and shared pool. */
static rei_pool *tasks_get(SEXP tasks, rei_task **ts_out, R_xlen_t *n_out) {
  if (TYPEOF(tasks) != VECSXP || XLENGTH(tasks) == 0)
    Rf_error("rei: tasks must be a non-empty list of task handles");
  R_xlen_t n = XLENGTH(tasks);
  rei_task *ts = (rei_task *) R_alloc(n, sizeof(rei_task));
  rei_r_handle *h = NULL;
  for (R_xlen_t i = 0; i < n; i++) {
    rei_r_handle *hi;
    ts[i] = task_get(VECTOR_ELT(tasks, i), &hi);
    if (h == NULL) h = hi;
    else if (hi != h)
      Rf_error("rei: task handles must belong to the same pool handle");
  }
  *ts_out = ts;
  *n_out = n;
  return (rei_pool *) h->core;
}

/* Wait on any of a submitter's outstanding tasks. A terminal non-OK
   outcome is re-signalled here with the 1-based list position as the
   condition's "index" field. */
SEXP rei_pool_collect_any_call(SEXP tasks, SEXP timeout) {
  rei_task *ts;
  R_xlen_t n;
  rei_pool *p = tasks_get(tasks, &ts, &n);
  void *v = NULL;
  size_t idx = 0;
  rei_status st = rei_pool_collect_any(p, ts, (size_t) n, &idx, &v,
                                       timeout_ms_of(timeout));
  if (st == REI_TIMEOUT) return rei_sent_timeout;
  if (st == REI_ERR) pool_raise(p);
  SEXP val = PROTECT((SEXP) v);
  int index = (int) idx + 1;   /* R 1-based */
  if (Rf_inherits(val, "rei_caught")) {
    SEXP cond = VECTOR_ELT(val, 0);
    if (TYPEOF(cond) == VECSXP)
      cond = rei_cond_set_index(cond, index);
    PROTECT(cond);
    rei_cond_signal(cond);          /* no return */
    UNPROTECT(2);        /* unreachable: rei_cond_signal is NORET */
  }
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
  SEXP names = PROTECT(Rf_allocVector(STRSXP, 2));
  SET_STRING_ELT(names, 0, Rf_mkChar("index"));
  SET_STRING_ELT(names, 1, Rf_mkChar("value"));
  Rf_setAttrib(out, R_NamesSymbol, names);
  SET_VECTOR_ELT(out, 0, Rf_ScalarInteger(index));
  SET_VECTOR_ELT(out, 1, val);
  UNPROTECT(3);
  return out;
}

/* Wait on all of a submitter's outstanding tasks. On success fills in list
   order; on the first non-OK outcome by position re-signals its condition
   with the 1-based index, the rest staying collectible. A timeout
   consumes nothing. */
SEXP rei_pool_collect_all_call(SEXP tasks, SEXP timeout) {
  rei_task *ts;
  R_xlen_t n;
  rei_pool *p = tasks_get(tasks, &ts, &n);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, n));
  size_t err_idx = 0;
  rei_status st = rei_pool_collect_all_fn(p, ts, (size_t) n, rei_vec_sink,
                                          out, &err_idx,
                                          timeout_ms_of(timeout));
  if (st == REI_TIMEOUT) {
    UNPROTECT(1);
    return rei_sent_timeout;
  }
  if (st == REI_ERR) pool_raise(p);
  if (err_idx < (size_t) n) {
    /* the first non-OK by position: signal with the 1-based index */
    SEXP val = VECTOR_ELT(out, (R_xlen_t) err_idx);
    if (Rf_inherits(val, "rei_caught")) {
      SEXP cond = VECTOR_ELT(val, 0);
      if (TYPEOF(cond) == VECSXP)
        cond = rei_cond_set_index(cond, (int) err_idx + 1);
      PROTECT(cond);
      rei_cond_signal(cond);        /* no return */
      UNPROTECT(2);      /* unreachable: rei_cond_signal is NORET */
    }
    UNPROTECT(1);
    return val;
  }
  SEXP nms = Rf_getAttrib(tasks, R_NamesSymbol);
  if (nms != R_NilValue)
    Rf_setAttrib(out, R_NamesSymbol, nms);
  UNPROTECT(1);
  return out;
}

/* Advisory and discard-only, never preemptive; every edge folds to FALSE —
   no error path. Also the finalizer release for an uncollected handle. */
SEXP rei_pool_cancel_call(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != rei_task_tag)
    Rf_error("rei: not a task handle");
  uintptr_t word = (uintptr_t) R_ExternalPtrAddr(xp);
  if (word == 0) return Rf_ScalarLogical(FALSE);
  SEXP pool_xp = R_ExternalPtrProtected(xp);
  rei_r_handle *h = (rei_r_handle *) R_ExternalPtrAddr(pool_xp);
  if (h == NULL || h->core == NULL || h->self_pid != rei_self_pid())
    return Rf_ScalarLogical(FALSE);
  rei_task t = { (uint64_t) word };
  return Rf_ScalarLogical(rei_pool_cancel((rei_pool *) h->core, &t));
}

/* Non-consuming state probe for the print method. Total for every real
   handle — print must not error. */
SEXP rei_pool_task_state_call(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != rei_task_tag)
    Rf_error("rei: not a task handle");
  uintptr_t word = (uintptr_t) R_ExternalPtrAddr(xp);
  rei_r_handle *h = (rei_r_handle *) R_ExternalPtrAddr(R_ExternalPtrProtected(xp));
  if (word == 0 || h == NULL || h->core == NULL ||
      h->self_pid != rei_self_pid())
    return Rf_mkString("dropped");
  rei_task t = { (uint64_t) word };
  switch (rei_pool_task_state((rei_pool *) h->core, &t)) {
  case REI_RS_PENDING: return Rf_mkString("pending");
  case REI_RS_OK:      return Rf_mkString("ok");
  case REI_RS_ERR:     return Rf_mkString("err");
  case REI_RS_CANCEL:  return Rf_mkString("cancel");
  case REI_RS_DIED:    return Rf_mkString("died");
  default:             return Rf_mkString("collected");   /* FREE / stale */
  }
}

// Stop and introspection ---------------------------------------------------------------

/* Orderly shutdown, controller only: broadcast, cancel pending, wait for
   clean worker exits up to the timeout, unlink. Idempotent. */
SEXP rei_pool_stop_call(SEXP xp, SEXP timeout) {
  rei_r_handle *h = pool_peek(xp);
  if (h == NULL) return Rf_ScalarLogical(TRUE);   /* stop is idempotent */
  if (h->role != REI_ROLE_CONTROLLER)
    Rf_error("rei: only the controller can stop a pool");
  rei_pool *p = (rei_pool *) h->core;
  rei_status st = rei_pool_stop(p, timeout_ms_of(timeout));
  if (st == REI_ERR) pool_raise(p);
  /* REI_TIMEOUT: the workers still exit on their own */
  return Rf_ScalarLogical(st == REI_OK);
}

SEXP rei_pool_status_call(SEXP xp) {
  rei_pool *p = pool_core(xp);
  rei_pool_status st;
  if (rei_pool_status_get(p, &st) != REI_OK) pool_raise(p);
  const char *names[] = {"name", "role", "max_workers", "max_submitters",
                         "injection_cap", "result_slots", "slot_size",
                         "workers", "parked", "submitters", "injection",
                         "tasks", "deque", "shutdown", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(st.name));
  SET_VECTOR_ELT(out, 1, Rf_mkString(
    st.role == REI_ROLE_CONTROLLER ? "controller" :
    st.role == REI_ROLE_WORKER ? "worker" : "submitter"));
  SET_VECTOR_ELT(out, 2, Rf_ScalarInteger((int) st.max_workers));
  SET_VECTOR_ELT(out, 3, Rf_ScalarInteger((int) st.max_submitters));
  SET_VECTOR_ELT(out, 4, Rf_ScalarInteger((int) st.injection_cap));
  SET_VECTOR_ELT(out, 5, Rf_ScalarInteger((int) st.result_slots));
  SET_VECTOR_ELT(out, 6, Rf_ScalarInteger((int) st.slot_size));

  SEXP wk = Rf_allocVector(INTSXP, (R_xlen_t) st.max_workers);
  SET_VECTOR_ELT(out, 7, wk);
  for (uint32_t i = 0; i < st.max_workers; i++)
    INTEGER(wk)[i] = st.worker_state[i];
  int nparked = 0;
  for (uint32_t i = 0; i < 64; i++) nparked += (int) (st.parked_mask >> i) & 1;
  SET_VECTOR_ELT(out, 8, Rf_ScalarInteger(nparked));

  SEXP sub = Rf_allocVector(INTSXP, (R_xlen_t) st.max_submitters);
  SET_VECTOR_ELT(out, 9, sub);
  double queued = 0;
  for (uint32_t j = 0; j < st.max_submitters; j++) {
    INTEGER(sub)[j] = st.sub_state[j];
    queued += (double) st.inj_queued[j];
  }
  SET_VECTOR_ELT(out, 10, Rf_ScalarReal(queued));

  /* pending ok err cancel died (REI_RS_PENDING .. REI_RS_DIED) */
  SEXP tasks = Rf_allocVector(INTSXP, 5);
  SET_VECTOR_ELT(out, 11, tasks);
  for (int s = 0; s < 5; s++)
    INTEGER(tasks)[s] = (int) st.tasks_by_state[s + REI_RS_PENDING];

  SEXP dq = Rf_allocVector(REALSXP, (R_xlen_t) st.max_workers);
  SET_VECTOR_ELT(out, 12, dq);
  for (uint32_t i = 0; i < st.max_workers; i++)
    REAL(dq)[i] = (double) st.deque_depth[i];

  SET_VECTOR_ELT(out, 13, Rf_ScalarLogical(st.shutdown));
  UNPROTECT(1);
  return out;
}

/* Cumulative counters, read-only. Per-worker rows mirror the slots' stat
   fields; per-submitter injection totals are the ring positions themselves. */
SEXP rei_pool_stats_call(SEXP xp) {
  rei_pool *p = pool_core(xp);
  rei_pool_dump d;
  if (rei_pool_dump_get(p, &d) != REI_OK) pool_raise(p);
  uint32_t mw = d.status.max_workers, ms = d.status.max_submitters;
  const char *names[] = {"workers", "submitters", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));

  const char *wnames[] = {"status", "pid", "tasks", "steals", "injections",
                          "parks", "helps", "deque", ""};
  SEXP wk = Rf_mkNamed(VECSXP, wnames);
  SET_VECTOR_ELT(out, 0, wk);
  SET_VECTOR_ELT(wk, 0, Rf_allocVector(INTSXP, (R_xlen_t) mw));
  for (int f = 1; f < 8; f++)
    SET_VECTOR_ELT(wk, f, Rf_allocVector(REALSXP, (R_xlen_t) mw));
  for (uint32_t i = 0; i < mw; i++) {
    const rei_worker_stat *w = &d.workers[i];
    INTEGER(VECTOR_ELT(wk, 0))[i] = w->status;
    REAL(VECTOR_ELT(wk, 1))[i] = (double) w->pid;
    REAL(VECTOR_ELT(wk, 2))[i] = (double) w->tasks;
    REAL(VECTOR_ELT(wk, 3))[i] = (double) w->steals;
    REAL(VECTOR_ELT(wk, 4))[i] = (double) w->injections;
    REAL(VECTOR_ELT(wk, 5))[i] = (double) w->parks;
    REAL(VECTOR_ELT(wk, 6))[i] = (double) w->helps;
    int64_t dep = w->deque_bottom - w->deque_top;
    REAL(VECTOR_ELT(wk, 7))[i] = dep > 0 ? (double) dep : 0;
  }

  const char *snames[] = {"status", "pid", "injected", "claimed", "spills",
                          "spill_reuse", ""};
  SEXP sb = Rf_mkNamed(VECSXP, snames);
  SET_VECTOR_ELT(out, 1, sb);
  SET_VECTOR_ELT(sb, 0, Rf_allocVector(INTSXP, (R_xlen_t) ms));
  for (int f = 1; f < 6; f++)
    SET_VECTOR_ELT(sb, f, Rf_allocVector(REALSXP, (R_xlen_t) ms));
  for (uint32_t j = 0; j < ms; j++) {
    const rei_sub_stat *s = &d.submitters[j];
    INTEGER(VECTOR_ELT(sb, 0))[j] = s->status;
    REAL(VECTOR_ELT(sb, 1))[j] = (double) s->pid;
    REAL(VECTOR_ELT(sb, 2))[j] = (double) s->injected;
    REAL(VECTOR_ELT(sb, 3))[j] = (double) s->claimed;
    REAL(VECTOR_ELT(sb, 4))[j] = (double) s->spills;
    REAL(VECTOR_ELT(sb, 5))[j] = (double) s->spill_reuse;
  }
  UNPROTECT(1);
  return out;
}

/* Read-only region snapshot for debugging distributed state. States can
   move mid-fill — a cold-path snapshot. */
SEXP rei_pool_dump_call(SEXP xp) {
  rei_pool *p = pool_core(xp);
  rei_pool_dump d;
  if (rei_pool_dump_get(p, &d) != REI_OK) pool_raise(p);
  uint32_t mw = d.status.max_workers, ms = d.status.max_submitters;
  const char *names[] = {"name", "shutdown", "workers", "submitters",
                         "tasks", "local", "help", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(d.status.name));
  SET_VECTOR_ELT(out, 1, Rf_ScalarLogical(d.status.shutdown));
  SET_VECTOR_ELT(out, 6, Rf_ScalarLogical(d.help_wanted));

  /* the spill free list, mapping cache, and collect park count are
     handle-local, surfacing here rather than in the cross-process stats */
  const char *lnames[] = {"fl_entries", "fl_bytes", "fl_hits", "open_hits",
                          "open_misses", "collect_parks", ""};
  SEXP lo = Rf_mkNamed(VECSXP, lnames);
  SET_VECTOR_ELT(out, 5, lo);
  SET_VECTOR_ELT(lo, 0, Rf_ScalarInteger((int) d.fl_entries));
  SET_VECTOR_ELT(lo, 1, Rf_ScalarReal((double) d.fl_bytes));
  SET_VECTOR_ELT(lo, 2, Rf_ScalarReal((double) d.fl_hits));
  SET_VECTOR_ELT(lo, 3, Rf_ScalarReal((double) d.open_hits));
  SET_VECTOR_ELT(lo, 4, Rf_ScalarReal((double) d.open_misses));
  SET_VECTOR_ELT(lo, 5, Rf_ScalarReal((double) d.collect_parks));

  const char *wnames[] = {"status", "pid", "park_state", "parked", "top",
                          "bottom", "in_flight", ""};
  SEXP wk = Rf_mkNamed(VECSXP, wnames);
  SET_VECTOR_ELT(out, 2, wk);
  for (int f = 0; f < 7; f++)
    SET_VECTOR_ELT(wk, f, Rf_allocVector(
      f == 1 || f == 4 || f == 5 ? REALSXP :
      f == 3 ? LGLSXP : INTSXP, (R_xlen_t) mw));
  for (uint32_t i = 0; i < mw; i++) {
    const rei_worker_stat *w = &d.workers[i];
    INTEGER(VECTOR_ELT(wk, 0))[i] = w->status;
    REAL(VECTOR_ELT(wk, 1))[i] = (double) w->pid;
    INTEGER(VECTOR_ELT(wk, 2))[i] = w->park_state;
    LOGICAL(VECTOR_ELT(wk, 3))[i] = (int) (d.status.parked_mask >> i) & 1;
    REAL(VECTOR_ELT(wk, 4))[i] = (double) w->deque_top;
    REAL(VECTOR_ELT(wk, 5))[i] = (double) w->deque_bottom;
    INTEGER(VECTOR_ELT(wk, 6))[i] = w->in_flight_rs;
  }

  const char *snames[] = {"status", "pid", "rs_start", "rs_count", "queued",
                          "ready", "full_waiter", ""};
  SEXP sb = Rf_mkNamed(VECSXP, snames);
  SET_VECTOR_ELT(out, 3, sb);
  for (int f = 0; f < 7; f++)
    SET_VECTOR_ELT(sb, f, Rf_allocVector(
      f == 1 || f == 4 ? REALSXP :
      f == 5 || f == 6 ? LGLSXP : INTSXP, (R_xlen_t) ms));
  for (uint32_t j = 0; j < ms; j++) {
    const rei_sub_stat *s = &d.submitters[j];
    INTEGER(VECTOR_ELT(sb, 0))[j] = s->status;
    REAL(VECTOR_ELT(sb, 1))[j] = (double) s->pid;
    INTEGER(VECTOR_ELT(sb, 2))[j] = (int) s->rs_start;
    INTEGER(VECTOR_ELT(sb, 3))[j] = (int) s->rs_count;
    REAL(VECTOR_ELT(sb, 4))[j] = (double) (s->injected - s->claimed);
    LOGICAL(VECTOR_ELT(sb, 5))[j] = s->ready;
    LOGICAL(VECTOR_ELT(sb, 6))[j] = s->full_waiter;
  }

  /* every non-FREE result slot, paged in one call (result_slots rows is
     always enough) */
  uint32_t rs_total = d.status.result_slots;
  rei_rs_row *rows = (rei_rs_row *) R_alloc(rs_total, sizeof(rei_rs_row));
  uint32_t n = 0;
  if (rei_pool_tasks_get(p, rows, rs_total, &n) != REI_OK) pool_raise(p);
  const char *tnames[] = {"slot", "status", "sequence", "worker", "waiter",
                          ""};
  SEXP tk = Rf_mkNamed(VECSXP, tnames);
  SET_VECTOR_ELT(out, 4, tk);
  for (int f = 0; f < 5; f++)
    SET_VECTOR_ELT(tk, f, Rf_allocVector(f == 2 ? REALSXP : INTSXP,
                                         (R_xlen_t) n));
  for (uint32_t m = 0; m < n; m++) {
    INTEGER(VECTOR_ELT(tk, 0))[m] = (int) rows[m].slot;
    INTEGER(VECTOR_ELT(tk, 1))[m] = rows[m].status;
    REAL(VECTOR_ELT(tk, 2))[m] = (double) rows[m].sequence;
    INTEGER(VECTOR_ELT(tk, 3))[m] = rows[m].worker_slot;
    INTEGER(VECTOR_ELT(tk, 4))[m] = rows[m].waiter_slot;
  }
  UNPROTECT(1);
  return out;
}

// rei_map support -------------------------------------------------------------------

/* A map's batch-sizing inputs: live workers, the caller's FREE result slots,
   the injection cap, and the entry inline budget. */
SEXP rei_pool_map_caps_call(SEXP xp) {
  rei_pool *p = pool_core(xp);
  uint32_t free_rs, inj_cap, inline_entry;
  if (rei_pool_map_caps(p, &free_rs, &inj_cap, &inline_entry) != 0)
    pool_raise(p);
  rei_pool_status st;
  if (rei_pool_status_get(p, &st) != REI_OK) pool_raise(p);
  int live = 0;
  for (uint32_t i = 0; i < st.max_workers; i++)
    live += st.worker_state[i] == REI_WK_LIVE;
  SEXP out = Rf_allocVector(INTSXP, 4);
  INTEGER(out)[0] = live;
  INTEGER(out)[1] = (int) free_rs;
  INTEGER(out)[2] = (int) inj_cap;
  INTEGER(out)[3] = (int) inline_entry;
  return out;
}

/* The pool-signal handle a map runner threads through rei_map_next: three
   opaque word addresses (a malloc'd copy the caller frees). */
static void pool_sig_finalizer(SEXP xp) {
  free(R_ExternalPtrAddr(xp));
  R_ClearExternalPtr(xp);
}

SEXP rei_pool_signals_call(SEXP xp) {
  rei_pool *p = pool_core(xp);
  rei_pool_sig *s = rei_pool_signals(p);
  if (s == NULL) Rf_error("rei: allocation failure");
  SEXP sig = PROTECT(R_MakeExternalPtr(s, rei_sig_tag, xp));
  R_RegisterCFinalizerEx(sig, pool_sig_finalizer, TRUE);
  UNPROTECT(1);
  return sig;
}

rei_pool_sig *rei_pool_sig_get(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != rei_sig_tag)
    Rf_error("rei: not a pool-signal handle");
  rei_pool_sig *s = (rei_pool_sig *) R_ExternalPtrAddr(xp);
  if (s == NULL) Rf_error("rei: pool-signal handle is closed");
  return s;
}

/* One doorbell-gated help beat at a runner's batch boundary. */
SEXP rei_pool_help_once_call(SEXP xp) {
  rei_r_handle *h = pool_get_worker(xp);
  rei_pool *p = (rei_pool *) h->core;
  int got = rei_pool_help_once(p);
  if (got < 0) pool_raise(p);   /* an exec infrastructure failure */
  return Rf_ScalarLogical(got);
}

/* The worker's map-context cache env (prot[2]), created lazily; the idle
   sweep clears it through the binding's sweep hook. */
SEXP rei_pool_map_cache(SEXP xp) {
  rei_r_handle *h = pool_get_worker(xp);
  SEXP cache = VECTOR_ELT(h->prot, 2);
  if (TYPEOF(cache) != ENVSXP) {
    cache = R_NewEnv(R_EmptyEnv, 0, 0);
    SET_VECTOR_ELT(h->prot, 2, cache);
  }
  return cache;
}

/* Test / debug surface: c(free-list entries, lent-ledger entries). */
SEXP rei_pool_zc_info(SEXP xp) {
  rei_pool *p = pool_core(xp);
  return rei_zc_fl_info(&((rei_handle *) p)->fl);
}
