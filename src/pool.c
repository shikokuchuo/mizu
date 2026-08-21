/* Part II pool, Phases 1 + 2: per-submitter injection rings, multiple
   workers with Chase-Lev deques and stealing, and result slots, over one
   pool SHM region mapped read-write by every participant. External
   submissions are SPSC-published into the submitting slot's own ring;
   workers consume them — and steal from each other's deques — with the
   same copy-then-CAS claim: both sides crash-atomic, with no claim state a
   mid-operation death can wedge. A worker looks for work in tier order
   (fairness tick, own deque bottom, random-victim steal, injection scan)
   and parks through the announce-then-rescan handshake, which is the sole
   guarantee against lost wakeups — there is no watchdog behind it. Results
   publish through a CAS'd status word per slot; payload lifetime is
   bridged by keepers on both sides of every queue (submitter task keepers
   released at collect or slot reuse; worker result keepers released when
   the slot leaves OK/ERR). The region layout is the SHM-layout table in
   ipc-plan.md; the registry and slot structs in sora.h are its wire
   format. */

#include <stdlib.h>
#include <stdio.h>
#include "sora.h"
#include <R_ext/Utils.h>

enum { SORA_ROLE_CONTROLLER = 0, SORA_ROLE_WORKER, SORA_ROLE_SUBMITTER };

struct sora_reap_ctx_s { void *pool; uint32_t slot; };

typedef struct sora_pool_s {
  mori_shm shm;                  /* our mapping; unmapped only in release */
  sora_pool_hdr hdr;
  unsigned char *base;
  int role;
  int released;
  long self_pid;                 /* fork guard */
  int wk_slot;                   /* our worker slot (-1 unless worker) */
  int sub_slot;                  /* our submitter slot (-1 unless we submit) */
  uint32_t inline_entry;         /* slot - sizeof(sora_entry_hdr) */
  uint32_t inline_rs;            /* slot - sizeof(sora_rs_hdr) */
  sora_binding binding;          /* the language binding's check/park hooks */

  sora_wk_slot *wk;
  sora_sub_slot *sub;
  _Atomic uint64_t *inj_ready;
  _Atomic uint64_t *full_waiters;
  _Atomic uint32_t *shutdown;
  _Atomic uint64_t *parked_workers;
  _Atomic uint32_t *help_wanted;
  unsigned char *rings;
  unsigned char *results;

  sora_parker *pks;               /* every entity: workers, then submitters */
  int pk_ok;

  intptr_t live_self;            /* our held lock (worker / submitter slot) */
  intptr_t live_sub;             /* a worker's nested-submitter slot lock */
  intptr_t live_owner;           /* kept fd on the owner file; 0 = not open */
  intptr_t *live_all;            /* controller: kept probe fds, wk then sub */
  char livedir[1024];

  /* controller-only: per-worker death watches whose C callbacks run the
     reap off the R main thread */
  sora_death_watch **wk_watch;
  _Atomic int *wk_dead;
  struct sora_reap_ctx_s *reap_ctx;

  /* submitter-local */
  uint32_t rs_cursor;
  uint64_t task_counter;
  int64_t inj_ltail;             /* producer-local tail */
  int64_t inj_cached_head;       /* head is monotonic: stale only
                                    under-reports space; refreshed on
                                    apparent-full */

  /* Payload lifetime: the retain tables with their GC-visible pin stores
     (pinned by the extptr's prot). keepers/pins is the submitter's task
     table (rs_count entries) or the worker's result table (result_slots);
     sub_keepers/sub_pins the worker's nested-submit task table (rs_count,
     allocated on first nested submit). The producer spill free list +
     lent-region ledger and the consumer mapping cache are owned outright
     (spill.c); the prot also pins the eval env, the trace hook, the
     map-context cache, and the zc view cache's wraps */
  SEXP pins;
  sora_keeper *keepers;
  uint32_t keepers_n;
  SEXP sub_pins;
  sora_keeper *sub_keepers;
  uint32_t sub_keepers_n;
  sora_spill_fl fl;
  sora_open_cache oc;
  sora_zc_cache zoc;

  /* worker-local */
  unsigned char *scratch;        /* slot-sized claim copy buffer */
  uint32_t scan_start;           /* rotating ring-scan start */
  uint64_t claims;               /* fairness-tick counter (% 61) */
  uint64_t rng;                  /* xorshift state for victim selection */
  int announced;                 /* park announce (mask bit + park_state)
                                    not yet restored: gates the entry heal */
  int help_depth;                /* nested-collect help recursion depth */
  /* identity of the outermost (unwind-path) task eval, for
     sora_pool_run_outcome: written only by catching = 0 executes — inner
     help / inline recursion clears the shm announce, so it cannot serve
     the unwind path */
  int in_eval;
  uint32_t cur_rs_index;
  uint64_t cur_seq, cur_task_id;
  uint16_t cur_sub_slot;
  uint32_t probe_streak;         /* thief-probe backstop state */
  uint32_t probe_victim;
  struct sora_rk_s { uint32_t idx; uint64_t seq; } *rk;
  uint32_t *rk_pos;              /* per result slot: rk position + 1, 0 = none */
  uint32_t rk_n, rk_cap, rk_cursor;
  /* cumulative stat counters, mirrored into the slot's stat_* fields by
     pool_stats_publish at park/fairness-tick cadence */
  uint64_t st_tasks, st_steals, st_inj, st_parks, st_helps;
  /* process-local adaptive spin budgets (ns; see sora_spin_learn) and
     collect park count: st_collect_parks is the collect-side mirror of
     st_parks. None are mirrored to shm; the dump surfaces them under
     "local" */
  uint64_t scan_budget_ns;
  uint64_t collect_budget_ns;
  uint64_t st_collect_parks;

  _Atomic int owner_dead;        /* death-listener flag: wake trigger only */
  sora_death_watch *watch;
} sora_pool;

static SEXP sora_pool_tag;
static SEXP sora_task_tag;
static SEXP sora_sig_tag;
static SEXP sora_class_pool;
static SEXP sora_class_task;
static SEXP sora_index_sym;

void sora_pool_init(void) {
  sora_pool_tag = Rf_install("sora_pool");
  sora_task_tag = Rf_install("sora_task");
  sora_sig_tag = Rf_install("sora_sig");
  sora_class_pool = Rf_mkString("sora_pool");
  R_PreserveObject(sora_class_pool);
  sora_class_task = Rf_mkString("sora_task");
  R_PreserveObject(sora_class_task);
  sora_index_sym = Rf_install("index");
}

void sora_pool_fini(void) {
  R_ReleaseObject(sora_class_task);
  R_ReleaseObject(sora_class_pool);
}

// Layout ----------------------------------------------------------------------------

static uint64_t pool_ring_bytes(const sora_pool_hdr *h) {
  return SORA_INJ_META_SIZE + (uint64_t) h->inj_cap * h->slot;
}

/* Region size implied by a header; the create sizes with it and the attach
   validator checks against it, so both sides share one piece of offset math. */
static uint64_t pool_fixed_size(const sora_pool_hdr *h) {
  return 64 +
    (uint64_t) h->max_workers * sizeof(sora_wk_slot) +
    (uint64_t) h->max_submitters * sizeof(sora_sub_slot) +
    128 +
    (uint64_t) h->max_submitters * pool_ring_bytes(h) +
    (uint64_t) h->max_workers * ((uint64_t) h->deque_cap * h->slot) +
    (uint64_t) h->result_slots * h->slot +
    192;
}

static void pool_wire(sora_pool *p) {
  unsigned char *b = (unsigned char *) p->shm.addr;
  const sora_pool_hdr *h = &p->hdr;
  p->base = b;
  p->inline_entry = h->slot - (uint32_t) sizeof(sora_entry_hdr);
  p->inline_rs = h->slot - (uint32_t) sizeof(sora_rs_hdr);

  size_t off = 64;
  p->wk = (sora_wk_slot *) (b + off);
  off += (size_t) h->max_workers * sizeof(sora_wk_slot);
  p->sub = (sora_sub_slot *) (b + off);
  off += (size_t) h->max_submitters * sizeof(sora_sub_slot);
  p->inj_ready = (_Atomic uint64_t *) (b + off + SORA_TIER_READY_OFF);
  p->full_waiters = (_Atomic uint64_t *) (b + off + SORA_TIER_FULL_OFF);
  off += 128;
  p->rings = b + off;
  off += (size_t) h->max_submitters * pool_ring_bytes(h);
  off += (size_t) h->max_workers * ((size_t) h->deque_cap * h->slot);
  p->results = b + off;
  off += (size_t) h->result_slots * h->slot;
  p->shutdown = (_Atomic uint32_t *) (b + off + SORA_CTRL_SHUTDOWN_OFF);
  p->parked_workers = (_Atomic uint64_t *) (b + off + SORA_CTRL_PARKED_OFF);
  p->help_wanted = (_Atomic uint32_t *) (b + off + SORA_CTRL_HELP_OFF);
}

static unsigned char *pool_ring(sora_pool *p, uint32_t s) {
  return p->rings + (size_t) s * pool_ring_bytes(&p->hdr);
}

static _Atomic int64_t *ring_tail(unsigned char *r) {
  return (_Atomic int64_t *) (r + SORA_INJ_TAIL_OFF);
}

static _Atomic int64_t *ring_head(unsigned char *r) {
  return (_Atomic int64_t *) (r + SORA_INJ_HEAD_OFF);
}

static unsigned char *ring_entry(sora_pool *p, unsigned char *r, uint64_t i) {
  return r + SORA_INJ_META_SIZE +
    (i & ((uint64_t) p->hdr.inj_cap - 1)) * p->hdr.slot;
}

static sora_rs_hdr *pool_rs(sora_pool *p, uint32_t idx) {
  return (sora_rs_hdr *) (p->results + (size_t) idx * p->hdr.slot);
}

static unsigned char *deque_entry_at(sora_pool *p, sora_wk_slot *w, int64_t i) {
  return p->base + w->deque_buf_off +
    ((uint64_t) i & ((uint64_t) w->deque_cap - 1)) * p->hdr.slot;
}

static int deque_nonempty(sora_wk_slot *w) {
  return atomic_load_explicit(&w->deque_top, memory_order_acquire) <
    atomic_load_explicit(&w->deque_bottom, memory_order_acquire);
}

static const char *pool_hdr_validate(const void *region, size_t region_size,
                                     sora_pool_hdr *out) {
  if (region_size < 64)
    return "region is smaller than a pool header";
  sora_pool_hdr h;
  memcpy(&h, region, sizeof(h));
  if (h.magic != SORA_POOL_MAGIC)
    return "bad magic: not a sora pool region";
  if (h.version != SORA_ABI_VERSION)
    return "ABI version mismatch: participant and controller were built "
           "against different sora wire formats";
  if (h.max_workers == 0 || h.max_workers > SORA_MAX_WORKERS ||
      h.max_submitters == 0 || h.max_submitters > 64)
    return "registry capacities out of range";
  if ((h.inj_cap & (h.inj_cap - 1)) != 0 || h.inj_cap < 2 ||
      (h.deque_cap & (h.deque_cap - 1)) != 0 || h.deque_cap < 2 ||
      (h.slot & (h.slot - 1)) != 0 || h.slot < 128 || h.slot > (1u << 20))
    return "queue capacities or slot size are not valid powers of two";
  if (h.result_slots == 0 || h.result_slots % h.max_submitters != 0)
    return "result slots are not a multiple of the submitter capacity";
  if (pool_fixed_size(&h) > region_size)
    return "pool sections exceed the mapped region";
  if (h.livedir_offset > region_size ||
      h.livedir_size > region_size - h.livedir_offset ||
      h.livedir_size == 0 || h.livedir_size > 900)
    return "liveness-dir string is missing or lies outside the region";
  if (out != NULL) *out = h;
  return NULL;
}

// Parkers and liveness paths -----------------------------------------------------------

static sora_parker *pool_wk_pk(sora_pool *p, uint32_t i) {
  return &p->pks[i];
}

static sora_parker *pool_sub_pk(sora_pool *p, uint32_t j) {
  return &p->pks[p->hdr.max_workers + j];
}

/* Entity numbering (the Windows event-name key): workers 0..MW-1, submitters
   MW..MW+MS-1. Every participant attaches every entity's parker up front —
   submitters unpark workers, workers unpark submitters — with create = 1 only
   on the controller, before any spawn. */
static int pool_parkers_attach(sora_pool *p, int create) {
  uint32_t mw = p->hdr.max_workers, ms = p->hdr.max_submitters;
  p->pks = calloc(mw + ms, sizeof(sora_parker));
  if (p->pks == NULL) return -1;
  for (uint32_t i = 0; i < mw + ms; i++) {
    _Atomic uint32_t *epoch = i < mw ? &p->wk[i].park_epoch
                                     : &p->sub[i - mw].park_epoch;
    if (sora_parker_attach(&p->pks[i], epoch, p->shm.name, (int) i,
                          create) != 0) {
      for (uint32_t k = 0; k < i; k++) sora_parker_detach(&p->pks[k]);
      free(p->pks);
      p->pks = NULL;
      return -1;
    }
  }
  p->pk_ok = 1;
  return 0;
}

/* kind is "wk" / "sub" (with idx) or "owner" (idx ignored). All paths live in
   the controller-chosen directory recorded in the header — no participant
   ever resolves the directory independently. */
static int pool_live_path(sora_pool *p, char *buf, size_t size,
                          const char *kind, uint32_t idx) {
  const char *suffix = p->shm.name + strlen(MORI_PREFIX_LITERAL);
  int n = strcmp(kind, "owner") == 0 ?
    snprintf(buf, size, "%s/sora_%s.owner", p->livedir, suffix) :
    snprintf(buf, size, "%s/sora_%s.%s.%u", p->livedir, suffix, kind, idx);
  return (n > 0 && (size_t) n < size) ? 0 : -1;
}

// Release ----------------------------------------------------------------------------

/* Full teardown of a handle's process-local state, idempotent. Never
   unlinks: the region name and liveness files are removed only by the
   controller's stop / destroy protocol. Order is load-bearing, as in the
   channel: the death watch and parkers reference the mapping. */
static void pool_release(sora_pool *p) {
  if (p->released) return;
  p->released = 1;
  if (p->watch != NULL) {
    sora_death_watch_stop(p->watch);
    p->watch = NULL;
  }
  if (p->wk_watch != NULL) {
    /* stop synchronizes with in-flight reap callbacks: after this loop
       nothing touches the mapping from another thread */
    for (uint32_t i = 0; i < p->hdr.max_workers; i++)
      if (p->wk_watch[i] != NULL) {
        sora_death_watch_stop(p->wk_watch[i]);
        p->wk_watch[i] = NULL;
      }
  }
  if (p->pk_ok) {
    uint32_t n = p->hdr.max_workers + p->hdr.max_submitters;
    for (uint32_t i = 0; i < n; i++) sora_parker_detach(&p->pks[i]);
    p->pk_ok = 0;
  }
  free(p->pks);
  p->pks = NULL;
  /* release the retain tables, the free list + ledger, and the mapping
     cache (their regions are independent of the pool region) */
  sora_keepers_teardown(p->keepers, p->pins, p->keepers_n);
  sora_keepers_teardown(p->sub_keepers, p->sub_pins, p->sub_keepers_n);
  sora_spill_fl_teardown(&p->fl);
  sora_oc_teardown(&p->oc);
  if (p->shm.addr != NULL) mori_shm_close(&p->shm, 0);
  p->base = NULL;
  if (p->live_self != 0) {
    sora_live_close(p->live_self);
    p->live_self = 0;
  }
  if (p->live_sub != 0) {
    sora_live_close(p->live_sub);
    p->live_sub = 0;
  }
  if (p->live_owner != 0) {
    sora_live_close(p->live_owner);
    p->live_owner = 0;
  }
  if (p->live_all != NULL) {
    uint32_t n = p->hdr.max_workers + p->hdr.max_submitters;
    for (uint32_t i = 0; i < n; i++)
      if (p->live_all[i] != 0) sora_live_close(p->live_all[i]);
    free(p->live_all);
    p->live_all = NULL;
  }
}

/* The controller half of teardown, shared by sora_pool_stop, the startup
   walk-back, and the handle finalizer: broadcast shutdown, wake everyone,
   cancel every pending result, unlink the names. Waiting for workers is the
   caller's business (the finalizer cannot wait). */
static void pool_shutdown_broadcast(sora_pool *p) {
  atomic_store_explicit(p->shutdown, 1u, memory_order_seq_cst);
  /* no parkers means a create walked back before attaching them: the token
     never left the process, so there is nobody to wake (and p->pks is NULL) */
  if (!p->pk_ok) return;
  for (uint32_t i = 0; i < p->hdr.max_workers; i++)
    sora_unpark(pool_wk_pk(p, i));
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++)
    if (atomic_load_explicit(p->full_waiters, memory_order_acquire) &
        (1ull << j))
      sora_unpark(pool_sub_pk(p, j));
  for (uint32_t r = 0; r < p->hdr.result_slots; r++) {
    sora_rs_hdr *rs = pool_rs(p, r);
    int32_t expected = SORA_RS_PENDING;
    if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                SORA_RS_CANCEL,
                                                memory_order_seq_cst,
                                                memory_order_relaxed)) {
      int32_t ws = atomic_load_explicit(&rs->waiter_slot,
                                        memory_order_acquire);
      if (ws >= 0 && (uint32_t) ws < p->hdr.max_submitters)
        sora_unpark(pool_sub_pk(p, (uint32_t) ws));
    }
  }
}

static void pool_remove_live_files(sora_pool *p);

static void pool_unlink_names(sora_pool *p) {
  sora_region_unlink(&p->shm);
  pool_remove_live_files(p);
}

// Handle access ---------------------------------------------------------------------

static sora_pool *pool_peek(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != sora_pool_tag)
    Rf_error("sora: not a pool handle");
  sora_pool *p = (sora_pool *) R_ExternalPtrAddr(xp);
  if (p == NULL || p->released) return NULL;
  if (p->self_pid != sora_self_pid())
    Rf_error("sora: pool handles do not survive fork()");
  return p;
}

static sora_pool *pool_get(SEXP xp) {
  sora_pool *p = pool_peek(xp);
  if (p == NULL) Rf_error("sora: pool handle is closed");
  return p;
}

static void sora_pool_finalizer(SEXP xp) {
  sora_pool *p = (sora_pool *) R_ExternalPtrAddr(xp);
  if (p == NULL) return;
  if (!p->released && p->role == SORA_ROLE_CONTROLLER && p->base != NULL) {
    pool_shutdown_broadcast(p);
    pool_unlink_names(p);
  }
  pool_release(p);
  free(p->scratch);
  free(p->rk);
  free(p->rk_pos);
  free(p->keepers);
  free(p->sub_keepers);
  free(p->wk_watch);
  free((void *) p->wk_dead);
  free(p->reap_ctx);
  free(p);
  R_ClearExternalPtr(xp);
}

/* prot layout: [0] the pin store parallel to the retain table — the
   submitter's task pins (length rs_count) or the worker's result pins
   (length result_slots); [1] the worker's evaluation base env
   (sora_pool_set_eval); [2] the handle's trace hook (sora_pool_set_trace);
   [3] the worker's map-context cache env (sora_map; created lazily by
   sora_pool_map_cache, cleared whole by the idle sweep — clear-all is its
   entire eviction policy); [4] the zc view cache's wrap table (never
   reassigned — p->zoc holds the raw pointer for the handle's lifetime);
   [5] a worker's nested-submit pin store (length rs_count, allocated when
   the worker claims a submitter slot). The retain tables themselves are
   malloc'd (p->keepers / p->sub_keepers); the free list, ledger, and
   mapping cache are owned outright (spill.c). */
static SEXP pool_make_handle(sora_pool *p, SEXP pins) {
  SEXP prot = PROTECT(Rf_allocVector(VECSXP, 6));
  SET_VECTOR_ELT(prot, 0, pins);
  SET_VECTOR_ELT(prot, 4, Rf_allocVector(VECSXP, SORA_OPEN_CACHE_MAX));
  p->zoc.wraps = VECTOR_ELT(prot, 4);
  p->pins = pins;
  SEXP xp = PROTECT(R_MakeExternalPtr(p, sora_pool_tag, prot));
  R_RegisterCFinalizerEx(xp, sora_pool_finalizer, TRUE);
  Rf_setAttrib(xp, R_ClassSymbol, sora_class_pool);
  UNPROTECT(2);
  return xp;
}

/* The submitter's task-keeper table and its pin store: p->keepers /
   p->pins on submitter handles, p->sub_keepers / p->sub_pins on worker
   handles (nested submits). */
static sora_keeper *pool_task_keepers(sora_pool *p) {
  return p->role == SORA_ROLE_WORKER ? p->sub_keepers : p->keepers;
}
static SEXP pool_task_pins(sora_pool *p) {
  return p->role == SORA_ROLE_WORKER ? p->sub_pins : p->pins;
}

/* Arms a worker handle for evaluation: a base environment under
   globalenv() binding the handle itself as `pool` — what worker-side
   nested submit closes over; task argument frames chain beneath it, so a
   task argument named `pool` shadows it. Stashed on the handle so nested
   submit's inline-execute fallback and collect's help mode can run tasks
   outside sora_pool_step. */
SEXP sora_pool_set_eval(SEXP xp) {
  sora_pool *p = pool_get(xp);
  if (p->role != SORA_ROLE_WORKER)
    Rf_error("sora: not a worker handle");
  SEXP base = PROTECT(R_NewEnv(R_GlobalEnv, 0, 0));
  Rf_defineVar(Rf_install("pool"), xp, base);
  SET_VECTOR_ELT(R_ExternalPtrProtected(xp), 1, base);
  UNPROTECT(1);
  return R_NilValue;
}

static SEXP pool_eval_env(SEXP xp) {
  SEXP env = VECTOR_ELT(R_ExternalPtrProtected(xp), 1);
  if (TYPEOF(env) != ENVSXP)
    Rf_error("sora: no evaluator registered on this worker handle");
  return env;
}

/* The task evaluator: one wire payload — list(expr, named args) — with the
   arguments bound into a fresh unhashed frame under the base environment.
   Two error disciplines, chosen by the caller. The worker loop's hot path
   (catching = 0) arms no handler at all: a user error longjmps out of
   sora_pool_step and worker_main publishes the caught condition as this
   task's ERR result through sora_pool_run_outcome — the in_eval flag is
   what separates those errors from infrastructure failure, which stays
   fatal. Help mode and nested submit's inline execute (catching = 1) run
   inside a task's own evaluation, where an escaping error would land in
   the wrong task's frames: they contain it with R_tryCatchError and pay
   its R-closure trampoline — several µs, still cheaper than the park that
   helping replaced. */
struct sora_eval_ctx { SEXP expr; SEXP env; int ok; };

static SEXP pool_eval_body(void *data) {
  struct sora_eval_ctx *c = (struct sora_eval_ctx *) data;
  return Rf_eval(c->expr, c->env);
}

static SEXP pool_eval_handler(SEXP cond, void *data) {
  ((struct sora_eval_ctx *) data)->ok = 0;
  return cond;
}

static SEXP pool_eval_expr(sora_pool *p, SEXP xp, SEXP expr, SEXP args,
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
  SEXP env = PROTECT(R_NewEnv(pool_eval_env(xp), 0, 0));
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

/* Per-handle, per-process trace hook: fn(event, id), with id the entry
   header's task_id (reserved for exactly this) as "<submitter>:<counter>".
   Emitting sites exist only on the pool's per-task paths — submit,
   pool_execute, and the help beat's runner re-home — never in the channel,
   whose per-message budget has no room even for a clock read. Disabled
   cost is one pointer check per site. */
SEXP sora_pool_set_trace(SEXP xp, SEXP fn) {
  (void) pool_get(xp);
  if (fn != R_NilValue && TYPEOF(fn) != CLOSXP)
    Rf_error("sora: expected a function or NULL");
  SET_VECTOR_ELT(R_ExternalPtrProtected(xp), 2, fn);
  return R_NilValue;
}

/* An error raised by the hook longjmps like any infrastructure error at
   its site: at submit the task stays committed (the dropped handle's
   finalizer then cancels it); in pool_execute it takes the worker down —
   its stranded in-flight task fails through the ordinary death path. */
static void pool_trace_emit(SEXP xp, const char *event, uint64_t id) {
  SEXP fn = VECTOR_ELT(R_ExternalPtrProtected(xp), 2);
  if (TYPEOF(fn) != CLOSXP) return;
  char buf[32];
  snprintf(buf, sizeof(buf), "%u:%llu", (unsigned) (id >> 48),
           (unsigned long long) (id & ((1ull << 48) - 1)));
  SEXP ev = PROTECT(Rf_mkString(event));
  SEXP tid = PROTECT(Rf_mkString(buf));
  SEXP call = PROTECT(Rf_lang3(fn, ev, tid));
  Rf_eval(call, R_GlobalEnv);
  UNPROTECT(3);
}

// Create (controller) -----------------------------------------------------------------

static int sora_pow2_u64(uint64_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

SEXP sora_pool_create(SEXP maxw_sexp, SEXP maxs_sexp, SEXP inj_sexp,
                     SEXP deque_sexp, SEXP rslots_sexp, SEXP slot_sexp) {
  uint64_t maxw = (uint64_t) Rf_asInteger(maxw_sexp);
  uint64_t maxs = (uint64_t) Rf_asInteger(maxs_sexp);
  uint64_t inj_cap = (uint64_t) Rf_asInteger(inj_sexp);
  uint64_t deque_cap = (uint64_t) Rf_asInteger(deque_sexp);
  uint64_t rslots = (uint64_t) Rf_asInteger(rslots_sexp);
  uint64_t slot = (uint64_t) Rf_asInteger(slot_sexp);
  if (maxw < 1 || maxw > SORA_MAX_WORKERS)
    Rf_error("sora: max_workers must be between 1 and %d", SORA_MAX_WORKERS);
  if (maxs < 1 || maxs > 64)
    Rf_error("sora: max_submitters must be between 1 and 64");
  if (!sora_pow2_u64(inj_cap) || inj_cap < 2 || inj_cap > (1u << 24))
    Rf_error("sora: injection_cap must be a power of two between 2 and 2^24");
  if (!sora_pow2_u64(deque_cap) || deque_cap < 2 || deque_cap > (1u << 24))
    Rf_error("sora: per_worker_cap must be a power of two between 2 and 2^24");
  /* floor 128: a result slot's inline budget (slot - 40) must hold a
     region name (up to 27 bytes on Windows) for an SHM_RAW spill */
  if (!sora_pow2_u64(slot) || slot < 128 || slot > (1u << 20))
    Rf_error("sora: slot_size must be a power of two between 128 and 2^20");
  if (rslots < maxs || rslots > (1u << 24))
    Rf_error("sora: result_slots must be between max_submitters and 2^24");
  rslots = (rslots + maxs - 1) / maxs * maxs;   /* per-submitter partition */
  const char *livedir = sora_live_dir();
  if (livedir == NULL)
    Rf_error("sora: cannot resolve liveness lock directory");
  size_t livedir_len = strlen(livedir);
  if (livedir_len > 900)
    Rf_error("sora: liveness directory path too long");

  sora_pool_hdr h = {
    .magic = SORA_POOL_MAGIC,
    .version = SORA_ABI_VERSION,
    .max_workers = (uint32_t) maxw,
    .max_submitters = (uint32_t) maxs,
    .inj_cap = (uint32_t) inj_cap,
    .deque_cap = (uint32_t) deque_cap,
    .result_slots = (uint32_t) rslots,
    .slot = (uint32_t) slot,
    .owner_pid = (uint64_t) sora_self_pid(),
  };
  uint64_t fixed = pool_fixed_size(&h);
  h.livedir_offset = MORI_ALIGN64(fixed);
  h.livedir_size = livedir_len;
  uint64_t total = h.livedir_offset + livedir_len;
  if (total > ((uint64_t) 1 << 46))
    Rf_error("sora: pool region too large");

  sora_pool *p = calloc(1, sizeof(*p));
  if (p == NULL) Rf_error("sora: allocation failure");
  int rc = sora_shm_create_populate(&p->shm, (size_t) total);
  if (rc != MORI_OK) {
    free(p);
    const char *summary, *hint;
    mori_err_describe(rc, &summary, &hint);
    sora_stop_shm((double) total,
                 "sora: cannot create pool region (%llu bytes): %s%s%s",
                 (unsigned long long) total, summary,
                 hint[0] != '\0' ? ". " : "", hint);
  }
  p->role = SORA_ROLE_CONTROLLER;
  p->self_pid = sora_self_pid();
  p->wk_slot = -1;
  p->sub_slot = 0;
  p->scan_budget_ns = SORA_SPIN_BUDGET_NS;
  p->collect_budget_ns = SORA_COLLECT_SPIN_BUDGET_NS;
  p->binding.check = sora_r_check;
  p->hdr = h;
  memcpy(p->livedir, livedir, livedir_len + 1);

  /* From here cleanup is the finalizer's: build the handle before anything
     that can longjmp. */
  SEXP pins = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) (rslots / maxs)));
  SEXP xp = PROTECT(pool_make_handle(p, pins));
  p->keepers = calloc((size_t) (rslots / maxs), sizeof(sora_keeper));
  if (p->keepers == NULL) Rf_error("sora: allocation failure");
  p->keepers_n = (uint32_t) (rslots / maxs);

  unsigned char *b = (unsigned char *) p->shm.addr;
  memcpy(b, &h, sizeof(h));
  memcpy(b + h.livedir_offset, livedir, livedir_len);
  pool_wire(p);

  /* Host-assigned worker-slot geometry, written once before any spawn. The
     deque space feeds the stealing tier; a fresh region is
     zero-filled, so every status word starts FREE and every index at 0. */
  size_t deques_off = (size_t) (p->rings - b) +
    (size_t) h.max_submitters * pool_ring_bytes(&h);
  for (uint32_t i = 0; i < h.max_workers; i++) {
    p->wk[i].id = (int32_t) i;
    p->wk[i].deque_buf_off =
      (int64_t) (deques_off + (size_t) i * ((size_t) h.deque_cap * h.slot));
    p->wk[i].deque_cap = (int32_t) h.deque_cap;
    atomic_store_explicit(&p->wk[i].in_flight_rs, -1, memory_order_relaxed);
  }

  /* Liveness: create every file up front and keep the fds — the controller
     always probes on kept fds — then take the owner lock before any spawn
     (pool lifetime = creator lifetime) and the submitter-slot-0 lock. */
  char path[1024];
  p->live_all = calloc(h.max_workers + h.max_submitters, sizeof(intptr_t));
  if (p->live_all == NULL) Rf_error("sora: allocation failure");
  for (uint32_t i = 0; i < h.max_workers + h.max_submitters; i++) {
    int is_wk = i < h.max_workers;
    if (pool_live_path(p, path, sizeof(path), is_wk ? "wk" : "sub",
                       is_wk ? i : i - h.max_workers) != 0)
      Rf_error("sora: liveness file path too long");
    if (sora_live_open(path, &p->live_all[i]) != 0)
      Rf_error("sora: cannot create liveness file '%s'", path);
  }
  if (pool_live_path(p, path, sizeof(path), "owner", 0) != 0)
    Rf_error("sora: liveness file path too long");
  if (sora_live_open(path, &p->live_owner) != 0 ||
      sora_live_try(p->live_owner) != SORA_LIVE_ACQUIRED)
    Rf_error("sora: cannot lock owner liveness file '%s'", path);

  /* Windows: every entity's named parker event must exist before any spawn */
  if (pool_parkers_attach(p, 1) != 0)
    Rf_error("sora: cannot attach pool parkers");

  /* Claim submitter slot 0 for the calling process: lock-before-CAS, as in
     every join. */
  p->live_self = p->live_all[h.max_workers + 0];
  if (sora_live_try(p->live_self) != SORA_LIVE_ACQUIRED)
    Rf_error("sora: cannot lock submitter liveness file");
  sora_sub_slot *s0 = &p->sub[0];
  int32_t expected = SORA_SUB_FREE;
  if (!atomic_compare_exchange_strong_explicit(&s0->status, &expected,
                                               SORA_SUB_LIVE,
                                               memory_order_seq_cst,
                                               memory_order_relaxed))
    Rf_error("sora: submitter slot 0 is not free in a fresh region");
  s0->pid = (int64_t) p->self_pid;
  s0->rs_start = 0;
  s0->rs_count = (uint32_t) (rslots / maxs);
  sora_live_ident(p->live_self, &s0->live_dev, &s0->live_ino);
  p->rs_cursor = 0;

  UNPROTECT(2);
  return xp;
}

SEXP sora_pool_suffix(SEXP xp) {
  sora_pool *p = pool_get(xp);
  return Rf_mkString(p->shm.name + strlen(MORI_PREFIX_LITERAL));
}

static void pool_wk_death_cb(void *arg);

/* The controller points its death listener at every LIVE target slot's
   pid, stopping any stale watch first so respawned slots get fresh
   watches. The callback reap then runs off the R main thread at OS
   notification latency. In-process joins (the test harness) are skipped —
   a process cannot meaningfully watch itself. */
static void pool_watch_workers(sora_pool *p, const int *slots, R_xlen_t n) {
  if (p->wk_watch == NULL) {
    p->wk_watch = calloc(p->hdr.max_workers, sizeof(*p->wk_watch));
    p->wk_dead = calloc(p->hdr.max_workers, sizeof(*p->wk_dead));
    p->reap_ctx = calloc(p->hdr.max_workers, sizeof(*p->reap_ctx));
    if (p->wk_watch == NULL || p->wk_dead == NULL || p->reap_ctx == NULL)
      Rf_error("sora: allocation failure");
  }
  for (R_xlen_t i = 0; i < n; i++) {
    uint32_t s = (uint32_t) slots[i];
    sora_wk_slot *w = &p->wk[s];
    if (atomic_load_explicit(&w->status, memory_order_acquire) !=
        SORA_WK_LIVE)
      continue;
    if ((long) w->pid == p->self_pid) continue;
    if (p->wk_watch[s] != NULL) {
      sora_death_watch_stop(p->wk_watch[s]);
      p->wk_watch[s] = NULL;
    }
    atomic_store_explicit(&p->wk_dead[s], 0, memory_order_relaxed);
    p->reap_ctx[s].pool = p;
    p->reap_ctx[s].slot = s;
    /* NULL leaves the probes and the teardown sweep as the backstops */
    p->wk_watch[s] = sora_death_watch_start2((long) w->pid, &p->wk_dead[s],
                                            NULL, pool_wk_death_cb,
                                            &p->reap_ctx[s]);
  }
}

/* Startup / elastic-spawn rendezvous: park on the creator's submitter-0
   parker, re-checking each target slot for LIVE on each wake; workers
   unpark the creator on reaching LIVE. On the way out — success or
   deadline expiry — the death listener is pointed at whichever targets did
   join. FALSE on expiry; the initial-creation caller walks the pool back
   via sora_pool_destroy, an elastic caller just errors. */
SEXP sora_pool_ready_wait(SEXP xp, SEXP slots_sexp, SEXP timeout) {
  sora_pool *p = pool_get(xp);
  if (p->role != SORA_ROLE_CONTROLLER)
    Rf_error("sora: only the controller can wait for workers");
  if (TYPEOF(slots_sexp) != INTSXP)
    Rf_error("sora: expected worker slot indices");
  R_xlen_t n = XLENGTH(slots_sexp);
  const int *slots = INTEGER(slots_sexp);
  for (R_xlen_t i = 0; i < n; i++)
    if (slots[i] < 0 || (uint32_t) slots[i] >= p->hdr.max_workers)
      Rf_error("sora: worker slot index out of range");
  double deadline = sora_now() + Rf_asReal(timeout);
  int ok;
  for (;;) {
    uint32_t e = sora_parker_snapshot(pool_sub_pk(p, 0));
    R_xlen_t live = 0;
    for (R_xlen_t i = 0; i < n; i++)
      live += atomic_load_explicit(&p->wk[slots[i]].status,
                                   memory_order_acquire) == SORA_WK_LIVE;
    ok = live == n;
    if (ok) break;
    double rem = deadline - sora_now();
    if (rem <= 0) break;
    long ms = (long) (rem * 1000) + 1;
    if (ms > SORA_INTERRUPT_BOUND_MS) ms = SORA_INTERRUPT_BOUND_MS;
    sora_park_bracket(&p->binding, 1);
    sora_park(pool_sub_pk(p, 0), e, ms);
    sora_park_bracket(&p->binding, 0);
    sora_check_interrupt(&p->binding);
  }
  pool_watch_workers(p, slots, n);
  return Rf_ScalarLogical(ok);
}

/* Clean-exit request: the worker observes the word between tasks and takes
   its LEAVING path — non-blocking here, and never preemptive. Its deque is
   consumed in place (REAPING) and the process may linger as a lifetime
   anchor for uncollected results. */
SEXP sora_pool_retire(SEXP xp, SEXP slot_sexp) {
  sora_pool *p = pool_get(xp);
  if (p->role != SORA_ROLE_CONTROLLER)
    Rf_error("sora: only the controller can retire workers");
  uint32_t slot = (uint32_t) Rf_asInteger(slot_sexp);
  if (slot >= p->hdr.max_workers)
    Rf_error("sora: worker slot index out of range");
  if (atomic_load_explicit(&p->wk[slot].status, memory_order_acquire) !=
      SORA_WK_LIVE)
    Rf_error("sora: worker slot %u is not live", slot);
  atomic_store_explicit(&p->wk[slot].retire, 1, memory_order_seq_cst);
  sora_unpark(pool_wk_pk(p, slot));
  return R_NilValue;
}

/* Startup walk-back and finalizer-free explicit destroy: broadcast so a
   late-joining worker exits instead of parking against a pool that gave up,
   then unlink everything. */
SEXP sora_pool_destroy(SEXP xp) {
  sora_pool *p = pool_get(xp);
  if (p->role != SORA_ROLE_CONTROLLER)
    Rf_error("sora: only the controller can destroy a pool");
  pool_shutdown_broadcast(p);
  pool_unlink_names(p);
  pool_release(p);
  return R_NilValue;
}

// Attach helpers ----------------------------------------------------------------------

static sora_pool *pool_open_common(const char *suffix, SEXP *xp_out,
                                  int keeper_len_is_rs) {
  for (const char *q = suffix; *q != '\0'; q++)
    if (!((*q >= '0' && *q <= '9') || (*q >= 'a' && *q <= 'f') || *q == '_'))
      Rf_error("sora: malformed region-name suffix");
  char name[MORI_NAME_MAX];
  int nn = snprintf(name, sizeof(name), "%s%s", MORI_PREFIX_LITERAL, suffix);
  if (nn <= 0 || (size_t) nn >= sizeof(name))
    Rf_error("sora: malformed region-name suffix");

  sora_pool *p = calloc(1, sizeof(*p));
  if (p == NULL) Rf_error("sora: allocation failure");
  if (sora_shm_open_rw(&p->shm, name, 1) != 0) {
    free(p);
    sora_stop_shm(NA_REAL, "sora: cannot open pool region '%s'", name);
  }
  p->self_pid = sora_self_pid();
  p->wk_slot = -1;
  p->sub_slot = -1;
  p->scan_budget_ns = SORA_SPIN_BUDGET_NS;
  p->collect_budget_ns = SORA_COLLECT_SPIN_BUDGET_NS;
  p->binding.check = sora_r_check;

  /* validate before touching any other field */
  const char *err = pool_hdr_validate(p->shm.addr, p->shm.size, &p->hdr);
  if (err != NULL) {
    mori_shm_close(&p->shm, 0);
    free(p);
    Rf_error("sora: invalid pool region: %s", err);
  }

  R_xlen_t klen = keeper_len_is_rs ?
    (R_xlen_t) p->hdr.result_slots :
    (R_xlen_t) (p->hdr.result_slots / p->hdr.max_submitters);
  SEXP pins = PROTECT(Rf_allocVector(VECSXP, klen));
  SEXP xp = PROTECT(pool_make_handle(p, pins));
  p->keepers = calloc((size_t) klen, sizeof(sora_keeper));
  if (p->keepers == NULL) Rf_error("sora: allocation failure");
  p->keepers_n = (uint32_t) klen;
  UNPROTECT(2);
  *xp_out = xp;

  pool_wire(p);
  memcpy(p->livedir, p->base + p->hdr.livedir_offset,
         (size_t) p->hdr.livedir_size);
  p->livedir[p->hdr.livedir_size] = '\0';
  return p;
}

/* Open the owner liveness file and keep the fd. A successful non-blocking
   acquire means the previous holder is dead — Phase 2 has no orphan-teardown
   acquirer, so the probe simply refuses the join (closing the handle releases
   the momentarily-held lock). */
static void pool_owner_check(sora_pool *p) {
  char path[1024];
  if (pool_live_path(p, path, sizeof(path), "owner", 0) != 0)
    Rf_error("sora: liveness file path too long");
  if (sora_live_open(path, &p->live_owner) != 0)
    Rf_error("sora: cannot open owner liveness file '%s'", path);
  if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0 ||
      sora_live_try(p->live_owner) == SORA_LIVE_ACQUIRED)
    sora_stop("sora_error_stopped", "sora: pool stopped or owner dead");
}

static void pool_watch_owner(sora_pool *p, sora_parker *own_pk) {
  if ((long) p->hdr.owner_pid == p->self_pid) return;   /* in-process join */
  p->watch = sora_death_watch_start((long) p->hdr.owner_pid, &p->owner_dead,
                                   own_pk);
  if (p->watch == NULL)
    Rf_error("sora: cannot watch owner process %llu",
             (unsigned long long) p->hdr.owner_pid);
}

/* Lock-first claim of a FREE submitter slot, mirroring the worker join: a
   dead submitter is recognisable by its free liveness lock regardless of
   which side of the CAS it died on. Fills the slot's identity fields, sets
   p->sub_slot, and returns the held lock through *lock_out. Shared by
   sora_pool_attach and a worker's first nested submit. */
static void pool_claim_sub_slot(sora_pool *p, intptr_t *lock_out) {
  char path[1024];
  uint32_t per = p->hdr.result_slots / p->hdr.max_submitters;
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++) {
    if (atomic_load_explicit(&p->sub[j].status, memory_order_acquire) !=
        SORA_SUB_FREE)
      continue;
    if (pool_live_path(p, path, sizeof(path), "sub", j) != 0)
      Rf_error("sora: liveness file path too long");
    intptr_t h;
    if (sora_live_open(path, &h) != 0) continue;
    if (sora_live_try(h) != SORA_LIVE_ACQUIRED) {
      sora_live_close(h);
      continue;                    /* another claimant beat us */
    }
    int32_t expected = SORA_SUB_FREE;
    if (!atomic_compare_exchange_strong_explicit(&p->sub[j].status, &expected,
                                                 SORA_SUB_LIVE,
                                                 memory_order_seq_cst,
                                                 memory_order_relaxed)) {
      sora_live_close(h);           /* stale FREE reading */
      continue;
    }
    *lock_out = h;
    p->sub_slot = (int) j;
    break;
  }
  if (p->sub_slot < 0)
    Rf_error("sora: submitter registry full");

  sora_sub_slot *me = &p->sub[p->sub_slot];
  me->pid = (int64_t) p->self_pid;
  me->rs_start = (uint32_t) p->sub_slot * per;
  me->rs_count = per;
  atomic_store_explicit(&me->stat_spills, 0, memory_order_relaxed);
  atomic_store_explicit(&me->stat_spill_reuse, 0, memory_order_relaxed);
  sora_live_ident(*lock_out, &me->live_dev, &me->live_ino);
}

// Worker join --------------------------------------------------------------------------

static void pool_stats_publish(sora_pool *p);

SEXP sora_pool_worker_join(SEXP suffix_sexp, SEXP slot_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("sora: expected a region-name suffix");
  SEXP xp;
  sora_pool *p = pool_open_common(CHAR(STRING_ELT(suffix_sexp, 0)), &xp, 1);
  PROTECT(xp);
  p->role = SORA_ROLE_WORKER;

  uint32_t slot = (uint32_t) Rf_asInteger(slot_sexp);
  if (slot >= p->hdr.max_workers)
    Rf_error("sora: worker slot index out of range");
  pool_owner_check(p);

  /* Lock-before-CAS: what makes "CLAIMING + free lock" a reliable dead-worker
     signal for the Phase 4 reaper, and what fail-fasts against a leftover
     ghost from a previous spawn. */
  char path[1024];
  if (pool_live_path(p, path, sizeof(path), "wk", slot) != 0)
    Rf_error("sora: liveness file path too long");
  if (sora_live_open(path, &p->live_self) != 0 ||
      sora_live_try(p->live_self) != SORA_LIVE_ACQUIRED)
    Rf_error("sora: worker slot %u already held — stale spawn?", slot);
  sora_wk_slot *me = &p->wk[slot];
  int32_t expected = SORA_WK_FREE;
  if (!atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                               SORA_WK_CLAIMING,
                                               memory_order_seq_cst,
                                               memory_order_relaxed))
    Rf_error("sora: worker slot %u not free — stale spawn?", slot);
  p->wk_slot = (int) slot;

  me->pid = (int64_t) p->self_pid;
  sora_live_ident(p->live_self, &me->live_dev, &me->live_ino);
  atomic_store_explicit(&me->in_flight_rs, -1, memory_order_relaxed);
  atomic_store_explicit(&me->retire, 0, memory_order_relaxed);
  atomic_store_explicit(&me->park_state, SORA_WPK_RUNNING,
                        memory_order_relaxed);
  pool_stats_publish(p);   /* zero any previous incarnation's counters */

  if (pool_parkers_attach(p, 0) != 0)
    Rf_error("sora: cannot attach pool parkers");
  p->scratch = malloc(p->hdr.slot);
  if (p->scratch == NULL) Rf_error("sora: allocation failure");
  p->rng = ((uint64_t) p->self_pid * 0x9E3779B97F4A7C15ull) ^
    ((uint64_t) (sora_now() * 1e9)) ^ ((uint64_t) slot << 32);
  if (p->rng == 0) p->rng = 1;
  pool_watch_owner(p, pool_wk_pk(p, slot));

  expected = SORA_WK_CLAIMING;
  atomic_compare_exchange_strong_explicit(&me->status, &expected, SORA_WK_LIVE,
                                          memory_order_seq_cst,
                                          memory_order_relaxed);
  sora_unpark(pool_sub_pk(p, 0));   /* the creator's startup wait */

  UNPROTECT(1);
  return xp;
}

static void pool_unpark_result_waiter(sora_pool *p, sora_rs_hdr *rs);
static void pool_unpark_one_worker(sora_pool *p);
static int pool_reaping_free(sora_wk_slot *w);
static int pool_probe_worker(sora_pool *p, uint32_t slot);
static void pool_probe_submitter(sora_pool *p, uint32_t j);
static void pool_orphan_teardown_try(sora_pool *p);
static void pool_idle_sweep(sora_pool *p, SEXP xp);

/* Clean worker exit. A nonempty deque is never drained anywhere: it
   becomes an ordinary steal target while the slot reads REAPING, and the
   observer of the drained deque returns the slot to FREE. Kept results are
   abandoned: the Phase 2 exits are shutdown and owner death, both of which
   cancel or orphan every outstanding collect anyway. */
SEXP sora_pool_leave(SEXP xp) {
  sora_pool *p = pool_peek(xp);
  if (p == NULL) return R_NilValue;
  if (p->role != SORA_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("sora: not a worker handle");
  sora_wk_slot *me = &p->wk[p->wk_slot];
  pool_stats_publish(p);   /* final, exact mirror for the departed slot */
  atomic_fetch_and_explicit(p->parked_workers, ~(1ull << p->wk_slot),
                            memory_order_seq_cst);
  int32_t expected = SORA_WK_LIVE;
  if (atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                              SORA_WK_LEAVING,
                                              memory_order_seq_cst,
                                              memory_order_relaxed)) {
    /* walk the deque read-only, unparking each entry's result waiter;
       thieves may be advancing top concurrently — a wake for an
       already-stolen entry is a spurious wake, absorbed by the re-check */
    int64_t t = atomic_load_explicit(&me->deque_top, memory_order_acquire);
    int64_t b = atomic_load_explicit(&me->deque_bottom,
                                     memory_order_acquire);
    for (int64_t i = t; i < b; i++) {
      sora_entry_hdr *eh = (sora_entry_hdr *) deque_entry_at(p, me, i);
      if (eh->rs_index < p->hdr.result_slots)
        pool_unpark_result_waiter(p, pool_rs(p, eh->rs_index));
    }
    if (atomic_load_explicit(&me->deque_top, memory_order_acquire) >= b) {
      expected = SORA_WK_LEAVING;
      atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                              SORA_WK_FREE,
                                              memory_order_seq_cst,
                                              memory_order_relaxed);
    } else {
      expected = SORA_WK_LEAVING;
      atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                              SORA_WK_REAPING,
                                              memory_order_seq_cst,
                                              memory_order_relaxed);
      /* a thief that emptied the deque while we still read LEAVING saw
         nothing to free: re-check now that REAPING is published */
      if (!pool_reaping_free(me))
        pool_unpark_one_worker(p);
    }
  }
  /* the slot's own lock releases now, so the slot reads properly departed
     to any prober; the full release is deferred while kept results anchor
     region lifetimes — the retiree's lame-duck loop drives it from R. The
     sweep makes the anchor check exact: the step loop's busy-path reap is
     quota-bounded and may leave consumed records behind */
  if (p->live_self != 0) {
    sora_live_close(p->live_self);
    p->live_self = 0;
  }
  pool_idle_sweep(p, xp);
  if (p->rk_n == 0) pool_release(p);
  return Rf_ScalarLogical(p->released);
}

/* One lame-duck beat for a retired worker anchoring uncollected results:
   reap the keeper table and report whether the anchor may drop. Plain
   bounded sleeps drive this from R — the slot's parker may already be
   reclaimed by a respawn, so no unpark can reach this process — and
   shutdown or owner death ends the linger. */
SEXP sora_pool_lame_duck(SEXP xp) {
  sora_pool *p = pool_peek(xp);
  if (p == NULL) return Rf_ScalarLogical(TRUE);
  pool_idle_sweep(p, xp);
  if (p->rk_n == 0 ||
      atomic_load_explicit(p->shutdown, memory_order_acquire) != 0 ||
      atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0) {
    pool_release(p);
    return Rf_ScalarLogical(TRUE);
  }
  return Rf_ScalarLogical(FALSE);
}

// Submitter join ------------------------------------------------------------------------

SEXP sora_pool_attach_call(SEXP suffix_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("sora: expected a region-name suffix");
  SEXP xp;
  sora_pool *p = pool_open_common(CHAR(STRING_ELT(suffix_sexp, 0)), &xp, 0);
  PROTECT(xp);
  p->role = SORA_ROLE_SUBMITTER;
  pool_owner_check(p);

  pool_claim_sub_slot(p, &p->live_self);

  if (pool_parkers_attach(p, 0) != 0)
    Rf_error("sora: cannot attach pool parkers");
  pool_watch_owner(p, pool_sub_pk(p, (uint32_t) p->sub_slot));
  unsigned char *ring = pool_ring(p, (uint32_t) p->sub_slot);
  p->inj_ltail = atomic_load_explicit(ring_tail(ring), memory_order_acquire);
  p->inj_cached_head = atomic_load_explicit(ring_head(ring),
                                            memory_order_acquire);

  UNPROTECT(1);
  return xp;
}

// Submit --------------------------------------------------------------------------------

/* Directed-unpark half of the pusher wake: find a parked (or announcing)
   worker and unpark it. Returns 0 only when the mask read empty — every
   worker busy. Deque pushes (nested submit, help re-home, orphan drains)
   use this half alone: help beats scan injection rings only, so a
   doorbell rung for deque work buys nothing but one wasted no-op beat at
   some runner's next transition — idle workers reach deque work through
   their steal tiers, and the pre-park rescan (pool_any_work) guarantees
   nobody parks past it. The explicit-start variants exist for the reap
   paths, which run on the death listener's callback thread and must not
   touch the handle's process-local scan rotation. */
static int pool_unpark_worker_from(sora_pool *p, uint32_t start) {
  /* pusher protocol: push, fence, then the mask load — either the parking
     worker's rescan sees the push or we see its bit */
  atomic_thread_fence(memory_order_seq_cst);
  uint64_t w = atomic_load_explicit(p->parked_workers, memory_order_relaxed);
  if (w == 0) return 0;
  uint32_t mw = p->hdr.max_workers;
  for (uint32_t k = 0; k < mw; k++) {
    uint32_t i = (start + k) % mw;
    if (!(w & (1ull << i))) continue;
    int32_t expected = SORA_WPK_PARKED;
    if (atomic_compare_exchange_strong_explicit(&p->wk[i].park_state,
                                                &expected, SORA_WPK_WAKING,
                                                memory_order_seq_cst,
                                                memory_order_relaxed)) {
      sora_unpark(pool_wk_pk(p, i));
      return 1;
    }
    if (expected == SORA_WPK_IDLE) {
      /* announced but not yet parked — its rescan may already have missed
         this publish, so an uncontested wake is not safe to skip. Its epoch
         snapshot predates the announce, so this unpark turns the upcoming
         sleep into an immediate return; at worst one spurious wake. */
      sora_unpark(pool_wk_pk(p, i));
      return 1;
    }
    /* RUNNING or WAKING: the worker transitioned away or another pusher
       claimed the wake; try the next set bit */
  }
  return 1;   /* someone was mid-transition: no doorbell, as ever */
}

/* The injection-publish wake: the unpark half, falling through to the
   doorbell when every worker is busy — map runners poll it once per batch
   transition, so queued injection work is picked up within ~a batch
   instead of at map end (sora_pool_help_once consumes it). */
static void pool_wake_one_worker_from(sora_pool *p, uint32_t start) {
  if (!pool_unpark_worker_from(p, start))
    atomic_store_explicit(p->help_wanted, 1u, memory_order_seq_cst);
}

static void pool_wake_one_worker(sora_pool *p) {
  pool_wake_one_worker_from(p, p->scan_start++);
}

static void pool_unpark_one_worker(sora_pool *p) {
  (void) pool_unpark_worker_from(p, p->scan_start++);
}

/* Block until the submitter's own ring has space (announce-then-rescan on
   full_waiters, parked on the submitter's own parker, woken directly by the
   worker whose pop freed a slot) or the deadline passes. Space is checked
   against the producer-local cached head first — workers CAS the shared
   head once per claim, so a fresh load would miss once per submit; a stale
   cache only under-reports space and apparent-full refreshes it, as with
   the channel's cached_head. */
static int pool_ring_space_wait(sora_pool *p, _Atomic int64_t *head,
                                double timeout_s) {
  if (p->inj_ltail - p->inj_cached_head < (int64_t) p->hdr.inj_cap)
    return 1;
  p->inj_cached_head = atomic_load_explicit(head, memory_order_acquire);
  if (p->inj_ltail - p->inj_cached_head < (int64_t) p->hdr.inj_cap)
    return 1;
  uint64_t bit = 1ull << p->sub_slot;
  double deadline = R_FINITE(timeout_s) ? sora_now() + timeout_s : -1;
  for (;;) {
    uint32_t e = sora_parker_snapshot(pool_sub_pk(p, (uint32_t) p->sub_slot));
    atomic_fetch_or_explicit(p->full_waiters, bit, memory_order_seq_cst);
    atomic_thread_fence(memory_order_seq_cst);
    p->inj_cached_head = atomic_load_explicit(head, memory_order_acquire);
    if (p->inj_ltail - p->inj_cached_head < (int64_t) p->hdr.inj_cap) {
      atomic_fetch_and_explicit(p->full_waiters, ~bit, memory_order_seq_cst);
      return 1;
    }
    if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0) {
      atomic_fetch_and_explicit(p->full_waiters, ~bit, memory_order_seq_cst);
      sora_stop("sora_error_stopped", "sora: pool stopped");
    }
    if (atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0) {
      atomic_fetch_and_explicit(p->full_waiters, ~bit, memory_order_seq_cst);
      pool_orphan_teardown_try(p);
      sora_stop("sora_error_stopped", "sora: pool stopped or owner dead");
    }
    long ms = SORA_INTERRUPT_BOUND_MS;
    if (deadline >= 0) {
      double rem = deadline - sora_now();
      if (rem <= 0) {
        atomic_fetch_and_explicit(p->full_waiters, ~bit,
                                  memory_order_seq_cst);
        return 0;
      }
      long rem_ms = (long) (rem * 1000) + 1;
      if (rem_ms < ms) ms = rem_ms;
    }
    sora_park_bracket(&p->binding, 1);
    sora_park(pool_sub_pk(p, (uint32_t) p->sub_slot), e, ms);
    sora_park_bracket(&p->binding, 0);
    atomic_fetch_and_explicit(p->full_waiters, ~bit, memory_order_seq_cst);
    sora_check_interrupt(&p->binding);
  }
}

/* A task handle carries no heap memory of its own: the result-slot index
   (< 2^24, the result_slots cap) and the slot sequence it was minted
   against pack into the extptr address itself, the sequence truncated to
   40 bits (2^40 reuses of one slot is ~34 years at 1M tasks/s). The
   address is opaque to R — the GC and finalizer machinery never
   dereference it — so a handle costs no malloc at submit and no free at
   GC. seq >= 1 keeps the packed address non-NULL (the mod-2^40 wrap to 0
   needs 2^40 reuses of slot 0), so NULL still reads as a finalized
   handle. */
#define SORA_TASK_SEQ_BITS 40
#define SORA_TASK_SEQ_MASK ((((uint64_t) 1) << SORA_TASK_SEQ_BITS) - 1)

typedef struct sora_task_s {
  uint32_t idx;                  /* global result-slot index */
  uint64_t seq;                  /* slot sequence, SORA_TASK_SEQ_BITS wide */
} sora_task;

static void *sora_task_addr(uint32_t idx, uint64_t seq) {
  return (void *) (uintptr_t)
    (((seq & SORA_TASK_SEQ_MASK) << 24) | (uint64_t) idx);
}

static sora_task sora_task_unpack(const void *addr) {
  uintptr_t v = (uintptr_t) addr;
  sora_task t = { (uint32_t) (v & 0xffffffu), v >> 24 };
  return t;
}

/* Handle-vs-slot sequence check, mask-truncated on the slot side. */
static int sora_task_seq_match(_Atomic uint64_t *slot_seq, uint64_t seq) {
  return (atomic_load_explicit(slot_seq, memory_order_relaxed) &
          SORA_TASK_SEQ_MASK) == seq;
}

static void pool_unpark_result_waiter(sora_pool *p, sora_rs_hdr *rs) {
  int32_t ws = atomic_load_explicit(&rs->waiter_slot, memory_order_acquire);
  if (ws >= 0 && (uint32_t) ws < p->hdr.max_submitters)
    sora_unpark(pool_sub_pk(p, (uint32_t) ws));
}

/* Keeper-drop wake for the worker that produced a freed result slot, gated
   on the parked mask: a running worker's own reap visits consume the FREE,
   so only an announced (idle or parked) worker needs the syscall. Pairs
   with the step loop's announce-before-sweep order through the same fence
   protocol as pool_wake_one_worker_from: either this fence-then-load sees
   the announce bit, or the worker's post-announce keeper sweep sees the
   FREE. Pure C — the submitter reaper calls it off the R main thread. */
static void pool_unpark_keeper_drop(sora_pool *p, int32_t w) {
  if (w < 0 || (uint32_t) w >= p->hdr.max_workers) return;
  atomic_thread_fence(memory_order_seq_cst);
  if (atomic_load_explicit(p->parked_workers, memory_order_relaxed) &
      (1ull << (uint32_t) w))
    sora_unpark(pool_wk_pk(p, (uint32_t) w));
}

/* Spill staging (SHM_RAW, SHM_VEC, or RAWSPILL) is the off-ramp from the
   inline fast path — a region per payload, recycled from the handle's free
   list when steady-state traffic permits. Counted against the task's
   submitter for task and result payloads alike, so sora_pool_stats
   surfaces an undersized slot_size from either direction of the traffic;
   the reuse count alongside says how much of that spill traffic is
   churn-free. */
static void pool_count_spill(sora_pool *p, uint32_t sub_slot,
                             const sora_slot_hdr *ph) {
  if ((ph->kind == SORA_KIND_SHM_RAW || ph->kind == SORA_KIND_SHM_VEC ||
       ph->kind == SORA_KIND_RAWSPILL) &&
      sub_slot < p->hdr.max_submitters) {
    atomic_fetch_add_explicit(&p->sub[sub_slot].stat_spills, 1,
                              memory_order_relaxed);
    if (p->fl.last_reused)
      atomic_fetch_add_explicit(&p->sub[sub_slot].stat_spill_reuse, 1,
                                memory_order_relaxed);
  }
}

/* The handle finalizer's state machine (also invoked deliberately by
   sora_cancel's PENDING arm). A task keeper is never dropped here: CANCEL is
   not a release point — FREE strictly implies the worker is done with the
   entry, materialize included, so release-at-reuse stays safe. */
static void sora_task_finalizer(SEXP xp) {
  void *addr = R_ExternalPtrAddr(xp);
  if (addr == NULL) return;
  sora_task t = sora_task_unpack(addr);
  SEXP pool_xp = R_ExternalPtrProtected(xp);
  sora_pool *p = (sora_pool *) R_ExternalPtrAddr(pool_xp);
  if (p != NULL && !p->released && p->base != NULL &&
      p->self_pid == sora_self_pid()) {
    sora_rs_hdr *rs = pool_rs(p, t.idx);
    if (sora_task_seq_match(&rs->sequence, t.seq)) {
      for (;;) {
        int32_t st = atomic_load_explicit(&rs->status, memory_order_acquire);
        if (st == SORA_RS_PENDING) {
          int32_t expected = SORA_RS_PENDING;
          if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                      SORA_RS_CANCEL,
                                                      memory_order_seq_cst,
                                                      memory_order_acquire)) {
            pool_unpark_result_waiter(p, rs);
            break;
          }
        } else if (st == SORA_RS_OK || st == SORA_RS_ERR || st == SORA_RS_DIED) {
          /* the deliberate "never collected" drop: FREE releases the
             producing worker's result keeper and any region unlinks */
          int32_t w = atomic_load_explicit(&rs->worker_slot,
                                           memory_order_acquire);
          int32_t expected = st;
          if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                      SORA_RS_FREE,
                                                      memory_order_seq_cst,
                                                      memory_order_acquire)) {
            pool_unpark_keeper_drop(p, w);
            break;
          }
        } else {
          break;                 /* another party owns the free */
        }
      }
    }
  }
  R_ClearExternalPtr(xp);
}

/* Result slot from the submitter's own subrange, its previous task keeper
   dropped en route: reusing a FREE slot is the lazy release backstop for
   keepers no collect dropped (cancelled or never-collected tasks) — safe
   because FREE strictly implies the worker is done with the entry — and it
   runs before staging, so a surrendered spill region can be popped by the
   very payload that recycles the slot. A longjmp out of staging leaves the
   keeper dropped early, which any FREE slot already permits. */
static uint32_t pool_alloc_rs(sora_pool *p, sora_keeper *keepers,
                              SEXP pins) {
  sora_sub_slot *me = &p->sub[p->sub_slot];
  for (uint32_t k = 0; k < me->rs_count; k++) {
    uint32_t cand = (p->rs_cursor + k) % me->rs_count;
    if (atomic_load_explicit(&pool_rs(p, me->rs_start + cand)->status,
                             memory_order_acquire) == SORA_RS_FREE) {
      /* the slot's previous consumer is the zc entry's ledger key (the
         worker-death backstop); -1 when it was never claimed */
      sora_keeper *old = &keepers[cand];
      if (old->kind == SORA_KEEP_ZC)
        old->key =
          atomic_load_explicit(&pool_rs(p, me->rs_start + cand)->worker_slot,
                               memory_order_acquire);
      sora_keeper_release(&p->fl, keepers, pins, (R_xlen_t) cand);
      return cand;
    }
  }
  sora_stop("sora_error_slots_exhausted",
           "sora: result slots exhausted — collect or cancel outstanding "
           "tasks first");
}

/* The handle carries the sequence about to be installed at commit, so
   until the bump it is simply stale. */
static SEXP pool_make_task(sora_pool *p, SEXP xp, uint32_t rs_index) {
  uint64_t seq = atomic_load_explicit(&pool_rs(p, rs_index)->sequence,
                                      memory_order_relaxed) + 1;
  SEXP txp = PROTECT(R_MakeExternalPtr(sora_task_addr(rs_index, seq),
                                       sora_task_tag, xp));
  R_RegisterCFinalizerEx(txp, sora_task_finalizer, TRUE);
  Rf_setAttrib(txp, R_ClassSymbol, sora_class_task);
  UNPROTECT(1);
  return txp;
}

/* Commit point: pin the task keeper — unconditionally, since whether a
   stream carries hook-emitted mori identifiers is not knowable without
   inspecting it — install the sequence, and open the slot as PENDING.
   Everything that can longjmp ran before this. */
static void pool_commit_rs(sora_pool *p, sora_keeper *keepers, SEXP pins,
                           uint32_t local, const sora_keeper *keep,
                           sora_rs_hdr *rs) {
  sora_keeper_commit(&p->fl, keepers, pins, (R_xlen_t) local, keep);
  p->rs_cursor = local + 1;
  atomic_fetch_add_explicit(&rs->sequence, 1, memory_order_relaxed);
  atomic_store_explicit(&rs->waiter_slot, -1, memory_order_relaxed);
  atomic_store_explicit(&rs->worker_slot, -1, memory_order_relaxed);
  atomic_store_explicit(&rs->status, SORA_RS_PENDING, memory_order_release);
}

static void pool_fill_entry(sora_pool *p, sora_entry_hdr *eh,
                            uint32_t rs_index, uint16_t flags) {
  eh->task_id = ((uint64_t) p->sub_slot << 48) | ++p->task_counter;
  eh->rs_index = rs_index;
  eh->submitter_slot = (uint16_t) p->sub_slot;
  eh->flags = flags;   /* assign, never OR: ring and deque slots are reused */
}

static void pool_execute(sora_pool *p, SEXP xp, int catching);
static void pool_announce(sora_pool *p);

/* Worker-side nested submit: the local-deque push. The worker becomes a
   submitter on first use — same keeper-table discipline, keyed by its own
   subrange, claimed lazily so pools that never nest spend no submitter
   slots on workers. The entry is staged directly into the worker's own
   deque slot and published by the bottom store; a full deque executes the
   task inline instead (work-first), so nested submit never blocks. */
static SEXP pool_submit_nested(sora_pool *p, SEXP xp, SEXP payload,
                               uint16_t flags) {
  if (p->wk_slot < 0)
    Rf_error("sora: not a worker handle");
  if (p->sub_slot < 0) {
    pool_claim_sub_slot(p, &p->live_sub);
    SEXP prot = R_ExternalPtrProtected(xp);
    SET_VECTOR_ELT(prot, 5, Rf_allocVector(VECSXP,
      (R_xlen_t) p->sub[p->sub_slot].rs_count));
    p->sub_pins = VECTOR_ELT(prot, 5);
    p->sub_keepers = calloc((size_t) p->sub[p->sub_slot].rs_count,
                            sizeof(sora_keeper));
    if (p->sub_keepers == NULL) Rf_error("sora: allocation failure");
    p->sub_keepers_n = p->sub[p->sub_slot].rs_count;
  }
  sora_keeper *keepers = p->sub_keepers;
  SEXP pins = p->sub_pins;
  sora_stage_rollback(&p->fl);
  uint32_t local = pool_alloc_rs(p, keepers, pins);
  uint32_t rs_index = p->sub[p->sub_slot].rs_start + local;
  sora_rs_hdr *rs = pool_rs(p, rs_index);

  sora_wk_slot *w = &p->wk[p->wk_slot];
  int64_t b = atomic_load_explicit(&w->deque_bottom, memory_order_relaxed);
  int64_t t = atomic_load_explicit(&w->deque_top, memory_order_acquire);
  int inline_exec = b - t >= (int64_t) w->deque_cap;
  unsigned char *e = inline_exec ? p->scratch : deque_entry_at(p, w, b);
  sora_entry_hdr *eh = (sora_entry_hdr *) e;

  /* everything that can longjmp — staging, handle allocation, the
     evaluator lookup the inline path needs — runs before any observable
     mutation: an error leaves the entry unpublished and the slot FREE */
  if (inline_exec) (void) pool_eval_env(xp);
  sora_keeper keep = { NULL, R_NilValue, -1, SORA_KEEP_FREE };
  sora_payload_stage(&eh->ph, e + sizeof(sora_entry_hdr), p->inline_entry,
                     payload, &p->fl, &keep);
  pool_count_spill(p, (uint32_t) p->sub_slot, &eh->ph);
  SEXP txp = PROTECT(pool_make_task(p, xp, rs_index));
  pool_commit_rs(p, keepers, pins, local, &keep, rs);
  pool_fill_entry(p, eh, rs_index, flags);
  uint64_t tid = eh->task_id;
  if (!inline_exec) {
    atomic_store_explicit(&w->deque_bottom, b + 1, memory_order_release);
    if (b <= t) pool_unpark_one_worker(p);   /* empty -> non-empty */
    pool_trace_emit(xp, "submit", tid);
  } else {
    pool_trace_emit(xp, "submit", tid);
    pool_announce(p);
    pool_execute(p, xp, 1);
  }
  UNPROTECT(1);
  return txp;
}

/* Per-task core of the submitter path: result slot, staging, handle,
   commit, entry fill — everything except the space wait, the tail
   publish, and the wake, which the batch entry amortizes across a
   burst. The submit trace emits at stage time. */
static SEXP pool_submit1(sora_pool *p, SEXP xp, sora_keeper *keepers,
                         SEXP pins, unsigned char *ring, SEXP payload,
                         uint16_t flags) {
  sora_stage_rollback(&p->fl);
  uint32_t local = pool_alloc_rs(p, keepers, pins);
  uint32_t rs_index = p->sub[p->sub_slot].rs_start + local;
  sora_rs_hdr *rs = pool_rs(p, rs_index);

  /* staging and handle allocation can longjmp: nothing observable yet */
  unsigned char *e = ring_entry(p, ring, (uint64_t) p->inj_ltail);
  sora_entry_hdr *eh = (sora_entry_hdr *) e;
  sora_keeper keep = { NULL, R_NilValue, -1, SORA_KEEP_FREE };
  sora_payload_stage(&eh->ph, e + sizeof(sora_entry_hdr), p->inline_entry,
                     payload, &p->fl, &keep);
  pool_count_spill(p, (uint32_t) p->sub_slot, &eh->ph);
  SEXP txp = PROTECT(pool_make_task(p, xp, rs_index));
  pool_commit_rs(p, keepers, pins, local, &keep, rs);
  pool_fill_entry(p, eh, rs_index, flags);
  p->inj_ltail++;
  pool_trace_emit(xp, "submit", eh->task_id);
  UNPROTECT(1);
  return txp;
}

/* Injection publish: the tail store, then the ready bit, in that order. */
static void pool_inj_publish(sora_pool *p, unsigned char *ring) {
  atomic_store_explicit(ring_tail(ring), p->inj_ltail, memory_order_release);
  uint64_t bit = 1ull << p->sub_slot;
  if (!(atomic_load_explicit(p->inj_ready, memory_order_relaxed) & bit))
    atomic_fetch_or_explicit(p->inj_ready, bit, memory_order_seq_cst);
}

/* tryflag: ring-full-past-timeout returns the sora_timeout sentinel
   (unambiguous — success returns an external pointer) instead of raising
   sora_error_submit_timeout, so the map submit loop needs no handler.
   Fatal outcomes (stopped, slots exhausted) raise in both modes. */
static SEXP pool_submit(SEXP xp, SEXP payload, double timeout_s,
                        int flags_i, int tryflag) {
  sora_pool *p = pool_get(xp);
  uint16_t flags = (uint16_t) flags_i;
  if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0)
    sora_stop("sora_error_stopped", "sora: pool stopped");
  if (atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0) {
    pool_orphan_teardown_try(p);
    sora_stop("sora_error_stopped", "sora: pool stopped or owner dead");
  }
  if (p->role == SORA_ROLE_WORKER)
    return pool_submit_nested(p, xp, payload, flags);
  if (p->sub_slot < 0)
    Rf_error("sora: not a submitter handle");
  sora_keeper *keepers = p->keepers;
  SEXP pins = p->pins;

  unsigned char *ring = pool_ring(p, (uint32_t) p->sub_slot);
  if (!pool_ring_space_wait(p, ring_head(ring), timeout_s)) {
    if (tryflag) return sora_sent_timeout;
    sora_stop("sora_error_submit_timeout",
             "sora: submission timed out (injection ring full)");
  }

  SEXP txp = pool_submit1(p, xp, keepers, pins, ring, payload, flags);
  pool_inj_publish(p, ring);
  pool_wake_one_worker(p);
  return txp;
}

SEXP sora_pool_submit(SEXP xp, SEXP payload, SEXP timeout, SEXP flags_sexp) {
  return pool_submit(xp, payload, Rf_asReal(timeout), Rf_asInteger(flags_sexp),
                     0);
}

static void pool_check_task_args(SEXP args) {
  if (TYPEOF(args) != VECSXP)
    Rf_error("sora: expected a list of task arguments");
  R_xlen_t n = XLENGTH(args);
  if (n > 0) {
    SEXP names = Rf_getAttrib(args, R_NamesSymbol);
    int bad = TYPEOF(names) != STRSXP || XLENGTH(names) != n;
    for (R_xlen_t i = 0; !bad && i < n; i++) {
      SEXP nm = STRING_ELT(names, i);
      if (nm == NA_STRING || LENGTH(nm) == 0) bad = 1;
    }
    if (bad)
      Rf_error("sora: all task arguments must be named");
  }
}

/* sora_submit's wire payload is list(expr, args), assembled here with the
   names validation a C loop instead of R closures. */
static SEXP pool_submit_expr(SEXP xp, SEXP expr, SEXP args, double timeout_s,
                             int flags) {
  pool_check_task_args(args);
  SEXP payload = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(payload, 0, expr);
  SET_VECTOR_ELT(payload, 1, args);
  SEXP out = pool_submit(xp, payload, timeout_s, flags, 0);
  UNPROTECT(1);
  return out;
}

/* sora_submit's entry: takes the quoted expression and the evaluated args
   list separately and assembles the list(expr, args) wire payload here —
   the R wrapper's per-call cost is then one substitute + one list(...) +
   one .Call, with the names validation a C loop instead of R closures. */
SEXP sora_pool_submit_expr(SEXP xp, SEXP expr, SEXP args, SEXP timeout,
                           SEXP flags_sexp) {
  return pool_submit_expr(xp, expr, args, Rf_asReal(timeout),
                          Rf_asInteger(flags_sexp));
}

/* The map path's entries take the map's one deadline absolute (sora_now()
   timescale, Inf waits indefinitely) and convert once, at entry: R
   threads a single deadline through submit and collect instead of
   re-deriving a relative timeout per call against a second clock. */
SEXP sora_pool_submit_try(SEXP xp, SEXP payload, SEXP deadline,
                         SEXP flags_sexp) {
  double d = Rf_asReal(deadline);
  return pool_submit(xp, payload, R_FINITE(d) ? d - sora_now() : R_PosInf,
                     Rf_asInteger(flags_sexp), 1);
}

/* sora_submit_batch's entry: one crossing per burst. Each task's wire
   payload is sora_submit's list(expr, args), assembled by swapping the
   expression through one reusable pair; the tail store and ready bit
   publish per element, so workers drain as the burst stages and a burst
   larger than the ring cannot deadlock. Wakes keep the pusher half of
   the parker handshake: a cadence of every 64 publishes (never wider
   than the ring, so a parked worker is roused long before the ring can
   fill and the space wait always has a popper to unpark it), then one
   pass per worker after the last publish — a worker can still lose the
   park race mid-burst (its pre-park re-check read a stale tail), and
   only a wake paired with the final publish contains that race.
   Ring-full past .timeout mid-burst returns the handles accepted so
   far; fatal outcomes (stopped, slots exhausted) raise, with the tasks
   already submitted staying valid and collectible. */
SEXP sora_pool_submit_batch(SEXP xp, SEXP exprs, SEXP args, SEXP timeout,
                            SEXP flags_sexp) {
  if (TYPEOF(exprs) != VECSXP)
    Rf_error("sora: exprs must be a list of expressions");
  pool_check_task_args(args);
  sora_pool *p = pool_get(xp);
  uint16_t flags = (uint16_t) Rf_asInteger(flags_sexp);
  if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0)
    sora_stop("sora_error_stopped", "sora: pool stopped");
  if (atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0) {
    pool_orphan_teardown_try(p);
    sora_stop("sora_error_stopped", "sora: pool stopped or owner dead");
  }

  R_xlen_t n = XLENGTH(exprs);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, n));
  SEXP payload = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(payload, 1, args);

  if (p->role == SORA_ROLE_WORKER) {
    for (R_xlen_t i = 0; i < n; i++) {
      SET_VECTOR_ELT(payload, 0, VECTOR_ELT(exprs, i));
      SET_VECTOR_ELT(out, i, pool_submit_nested(p, xp, payload, flags));
    }
    UNPROTECT(2);
    return out;
  }
  if (p->sub_slot < 0)
    Rf_error("sora: not a submitter handle");
  sora_keeper *keepers = p->keepers;
  SEXP pins = p->pins;
  unsigned char *ring = pool_ring(p, (uint32_t) p->sub_slot);

  double timeout_s = Rf_asReal(timeout);
  double deadline = R_FINITE(timeout_s) ? sora_now() + timeout_s : -1;
  /* wake cadence: a power of two no wider than the ring (both are), so
     one wake lands within any ring-filling window */
  int64_t cad = (int64_t) p->hdr.inj_cap < 64 ? (int64_t) p->hdr.inj_cap : 64;
  R_xlen_t done = 0;
  for (R_xlen_t i = 0; i < n; i++) {
    double rem = deadline < 0 ? R_PosInf : deadline - sora_now();
    if (!pool_ring_space_wait(p, ring_head(ring), rem)) break;
    SET_VECTOR_ELT(payload, 0, VECTOR_ELT(exprs, i));
    SET_VECTOR_ELT(out, i,
                   pool_submit1(p, xp, keepers, pins, ring, payload, flags));
    done++;
    pool_inj_publish(p, ring);
    if ((i & (cad - 1)) == 0)
      for (uint32_t k = 0; k < p->hdr.max_workers; k++)
        pool_wake_one_worker(p);
  }
  /* the protocol wake: pair the burst's last publish with a parked-mask
     check per worker, as the single submit pairs every publish */
  for (R_xlen_t k = 0; k < done && k < (R_xlen_t) p->hdr.max_workers; k++)
    pool_wake_one_worker(p);

  if (done < n) {
    SEXP part = PROTECT(Rf_allocVector(VECSXP, done));
    for (R_xlen_t i = 0; i < done; i++)
      SET_VECTOR_ELT(part, i, VECTOR_ELT(out, i));
    UNPROTECT(3);
    return part;
  }
  UNPROTECT(2);
  return out;
}

// Worker step ----------------------------------------------------------------------------

/* Drop a kept result once its slot has left OK/ERR (or been resequenced):
   the collector's or finalizer's FREE transition is the consumed-signal,
   its directed unpark what re-runs the reap on a parked worker. rk_pos
   keys the table by slot — at most one record per slot, updated in place
   when a reused slot republishes — so a stale record can never alias (and
   nil) a successor's keeper. Every exit from OK/ERR is a consumer-done
   signal (collect materialized, or the finalizer / submitter reaper freed
   an uncollectable slot), so a spilled result region surrenders to the
   free list here. Returns 1 when the record at i was retained, 0 when it
   was removed (the swapped-in tail record then sits at i). */
static int pool_rk_visit(sora_pool *p, uint32_t i) {
  sora_rs_hdr *rs = pool_rs(p, p->rk[i].idx);
  int32_t st = atomic_load_explicit(&rs->status, memory_order_acquire);
  uint64_t seq = atomic_load_explicit(&rs->sequence, memory_order_relaxed);
  if ((st == SORA_RS_OK || st == SORA_RS_ERR) && seq == p->rk[i].seq)
    return 1;
  sora_keeper_release(&p->fl, p->keepers, p->pins, (R_xlen_t) p->rk[i].idx);
  p->rk_pos[p->rk[i].idx] = 0;
  p->rk_n--;
  if (i < p->rk_n) {
    p->rk[i] = p->rk[p->rk_n];
    p->rk_pos[p->rk[i].idx] = i + 1;
  }
  return 0;
}

/* The full sweep, for the idle and departure paths (pre-park, empty step
   returns, the lame-duck beat) where visiting every record costs nothing
   the pool feels. The idle path also runs the full lent-ledger sweep
   (zc.c) — the busy paths' quota'd sweeps clear only residue. */
static void pool_reap_result_keepers(sora_pool *p) {
  uint32_t i = 0;
  while (i < p->rk_n) i += (uint32_t) pool_rk_visit(p, i);
  p->rk_cursor = 0;
  sora_ledger_sweep(&p->fl, SORA_LEDGER_MAX);
}

/* The idle-path sweep proper: the full keeper reap plus the map-context
   cache drop (prot[5], see R/map.R) — its only eviction beyond the
   R side's clear-at-8 bound, since workers cannot observe map completion.
   An idle or departing worker holds nothing; a map whose chunks span a
   park pays one re-open + re-unserialize. In-flight chunks are unaffected:
   their own R references keep a dropped context's mapping alive until
   their frames drop. */
static void pool_idle_sweep(sora_pool *p, SEXP xp) {
  pool_reap_result_keepers(p);
  SET_VECTOR_ELT(R_ExternalPtrProtected(xp), 3, R_NilValue);  /* map cache */
}

/* The busy-path reap: at most SORA_REAP_QUOTA visits under a rotating
   cursor, so a loaded worker's per-task reap cost is O(1) against any
   number of results outstanding — under a fire-then-collect backlog the
   old whole-table scan went quadratic. Consumption keeps pace as long as
   the quota exceeds the frees a task period can see; the idle-path sweep
   clears any residue. */
static void pool_reap_quota(sora_pool *p) {
  uint32_t lim = p->rk_n < SORA_REAP_QUOTA ? p->rk_n : SORA_REAP_QUOTA;
  for (uint32_t k = 0; k < lim && p->rk_n > 0; k++) {
    if (p->rk_cursor >= p->rk_n) p->rk_cursor = 0;
    p->rk_cursor += (uint32_t) pool_rk_visit(p, p->rk_cursor);
  }
  /* the busy-path ledger sweep, quota'd like the reap itself */
  sora_ledger_sweep(&p->fl, SORA_REAP_QUOTA);
}

/* Growth is split from recording so it can run before the publish CAS: an
   allocation failure after publish would leave a pinned keeper the reap
   never visits. First use also sizes the slot -> record map. */
static void pool_rk_reserve(sora_pool *p) {
  if (p->rk_pos == NULL) {
    p->rk_pos = calloc(p->hdr.result_slots, sizeof(*p->rk_pos));
    if (p->rk_pos == NULL) Rf_error("sora: allocation failure");
  }
  if (p->rk_n < p->rk_cap) return;
  uint32_t cap = p->rk_cap == 0 ? 64 : p->rk_cap * 2;
  struct sora_rk_s *rk = realloc(p->rk, cap * sizeof(*rk));
  if (rk == NULL) Rf_error("sora: allocation failure");
  p->rk = rk;
  p->rk_cap = cap;
}

static void pool_rk_add(sora_pool *p, uint32_t idx, uint64_t seq) {
  uint32_t pos = p->rk_pos[idx];
  if (pos != 0) {          /* reused slot: the record follows the new
                              incarnation — publish replaced the keeper */
    p->rk[pos - 1].seq = seq;
    return;
  }
  p->rk[p->rk_n].idx = idx;
  p->rk[p->rk_n].seq = seq;
  p->rk_pos[idx] = ++p->rk_n;
}

/* Announce-before-claim: a worker dying after a claim CAS but before
   recording the task would otherwise vanish it. Recorded from the entry
   copied into scratch, before any claim (ring-head CAS, deque-bottom
   commit, or steal CAS) is attempted; plain stores suffice — the only
   reader is a post-mortem reaper serialized by the liveness lock. */
static void pool_announce(sora_pool *p) {
  sora_entry_hdr *eh = (sora_entry_hdr *) p->scratch;
  if (eh->rs_index >= p->hdr.result_slots)
    Rf_error("sora: corrupt pool entry");
  sora_wk_slot *me = &p->wk[p->wk_slot];
  atomic_store_explicit(&me->in_flight_rs, (int32_t) eh->rs_index,
                        memory_order_relaxed);
  atomic_store_explicit(&me->in_flight_seq,
                        atomic_load_explicit(&pool_rs(p, eh->rs_index)->
                                             sequence,
                                             memory_order_relaxed),
                        memory_order_relaxed);
}

static void pool_announce_clear(sora_pool *p) {
  atomic_store_explicit(&p->wk[p->wk_slot].in_flight_rs, -1,
                        memory_order_relaxed);
}

/* Claims must copy before their CAS so a dying claimer leaves the entry
   untouched. Only the fixed header and its framed bytes are meaningful: a
   large inline slot would otherwise turn every claim into a slot-sized
   copy. A slot is rewritten only once head/top has moved past it, so a
   winning claim read a coherent header and hdr + len covers every
   meaningful byte; a loser may read a stale, torn one, but any
   slot-bounded copy is safe — it is discarded with the failed CAS. */
static size_t pool_entry_copy_bytes(sora_pool *p, const sora_entry_hdr *eh) {
  switch (eh->ph.kind) {
  case SORA_KIND_NIL:
    return sizeof(*eh);
  case SORA_KIND_INLINE:
  case SORA_KIND_RAWVEC:
  case SORA_KIND_STR1:
  case SORA_KIND_SHM_RAW:
  case SORA_KIND_SHM_VEC:
  case SORA_KIND_REF:
    if (eh->ph.len <= p->inline_entry) return sizeof(*eh) + eh->ph.len;
    break;
  case SORA_KIND_RAWSPILL:
    /* the framed bytes are the region name, its length in aux >> 8 */
    if ((eh->ph.aux >> 8) < MORI_NAME_MAX)
      return sizeof(*eh) + (uint32_t) (eh->ph.aux >> 8);
    break;
  }
  return p->hdr.slot;               /* torn or foreign header: full slot */
}

static void pool_copy_entry(sora_pool *p, unsigned char *dst,
                            const unsigned char *src) {
  sora_entry_hdr hdr;
  memcpy(&hdr, src, sizeof(hdr));
  size_t n = pool_entry_copy_bytes(p, &hdr);
  memcpy(dst, src, n);
}

/* Mirror the local counters into the slot — only at the park announce,
   post-park, the fairness tick, step returns, and leave. Never per task:
   the stat_* fields share line 1 with deque_top, and a per-task write
   would reintroduce exactly the thief-CAS pingpong that line's layout
   avoids. Under load the mirrors lag by up to one tick (61 claims); a
   parked or departed worker's are exact. */
static void pool_stats_publish(sora_pool *p) {
  sora_wk_slot *me = &p->wk[p->wk_slot];
  atomic_store_explicit(&me->stat_tasks, p->st_tasks, memory_order_relaxed);
  atomic_store_explicit(&me->stat_steals, p->st_steals,
                        memory_order_relaxed);
  atomic_store_explicit(&me->stat_inj, p->st_inj, memory_order_relaxed);
  atomic_store_explicit(&me->stat_parks, p->st_parks, memory_order_relaxed);
  atomic_store_explicit(&me->stat_helps, p->st_helps, memory_order_relaxed);
}

/* Copy-then-CAS claim over the ring scan, ready-mask-gated on the fast
   path (use_mask) and unfiltered on the fairness tick. The mask is a hint
   only: a stale clear bit is repaired by the pre-park full rescan, so it
   costs one trip to the park path, never a lost task. */
static int pool_claim_rings(sora_pool *p, int use_mask) {
  uint64_t ready = ~0ull;
  if (use_mask) {
    ready = atomic_load_explicit(p->inj_ready, memory_order_acquire);
    if (ready == 0) return 0;
  }
  uint32_t ms = p->hdr.max_submitters;
  uint32_t start = p->scan_start;
  for (uint32_t k = 0; k < ms; k++) {
    uint32_t s = (start + k) % ms;
    if (!(ready & (1ull << s))) continue;
    unsigned char *ring = pool_ring(p, s);
    _Atomic int64_t *hd = ring_head(ring), *tl = ring_tail(ring);
    for (;;) {
      int64_t head = atomic_load_explicit(hd, memory_order_acquire);
      int64_t tail = atomic_load_explicit(tl, memory_order_acquire);
      if (head >= tail) {
        /* clear, then re-check: the producer's OR follows its tail publish,
           so a publish racing the clear is caught and the bit restored */
        if (use_mask) {
          atomic_fetch_and_explicit(p->inj_ready, ~(1ull << s),
                                    memory_order_seq_cst);
          if (atomic_load_explicit(tl, memory_order_acquire) >
              atomic_load_explicit(hd, memory_order_acquire))
            atomic_fetch_or_explicit(p->inj_ready, 1ull << s,
                                     memory_order_seq_cst);
        }
        break;
      }
      pool_copy_entry(p, p->scratch, ring_entry(p, ring, (uint64_t) head));
      pool_announce(p);
      if (atomic_compare_exchange_strong_explicit(hd, &head, head + 1,
                                                  memory_order_seq_cst,
                                                  memory_order_relaxed)) {
        p->scan_start = s + 1;
        p->st_inj++;
        /* backpressure wake: ring and waiter correspond one-to-one */
        uint64_t bit = 1ull << s;
        if (atomic_load_explicit(p->full_waiters, memory_order_relaxed) &
            bit) {
          uint64_t w = atomic_fetch_and_explicit(p->full_waiters, ~bit,
                                                 memory_order_seq_cst);
          if (w & bit) sora_unpark(pool_sub_pk(p, s));
        }
        return 1;
      }
      pool_announce_clear(p);
      /* lost the claim race to another worker: retry this ring */
    }
  }
  return 0;
}

// Deques and stealing ---------------------------------------------------------------------

/* Owner push at the bottom. Entry bytes are written before the release
   store of bottom, which is what publishes them to thieves. The full check
   loads top fresh: the steal path's stale-copy argument (a slot is
   overwritten only after top advanced past it, failing the thief's CAS)
   relies on the owner never lapping an unadvanced top. */
static int pool_deque_push(sora_pool *p, const unsigned char *entry) {
  sora_wk_slot *me = &p->wk[p->wk_slot];
  int64_t b = atomic_load_explicit(&me->deque_bottom, memory_order_relaxed);
  int64_t t = atomic_load_explicit(&me->deque_top, memory_order_acquire);
  if (b - t >= (int64_t) me->deque_cap) return 0;
  pool_copy_entry(p, deque_entry_at(p, me, b), entry);
  atomic_store_explicit(&me->deque_bottom, b + 1, memory_order_release);
  return 1;
}

/* Chase-Lev take (Le et al. orderings): decrement bottom, seq_cst fence,
   load top; the last element resolves the owner-vs-thief race by CAS on
   top. The entry is copied and announced before the claim can commit —
   only the owner writes the buffer, so the pre-decrement copy is stable —
   and the announce is cleared on the lost race. */
static int pool_deque_pop(sora_pool *p) {
  sora_wk_slot *me = &p->wk[p->wk_slot];
  int64_t b = atomic_load_explicit(&me->deque_bottom, memory_order_relaxed);
  int64_t t = atomic_load_explicit(&me->deque_top, memory_order_acquire);
  if (t >= b) return 0;
  b--;
  pool_copy_entry(p, p->scratch, deque_entry_at(p, me, b));
  pool_announce(p);
  atomic_store_explicit(&me->deque_bottom, b, memory_order_relaxed);
  atomic_thread_fence(memory_order_seq_cst);
  t = atomic_load_explicit(&me->deque_top, memory_order_relaxed);
  if (t < b) return 1;                       /* not the last: ours outright */
  int got = 0;
  if (t == b)                                /* last element: race thieves */
    got = atomic_compare_exchange_strong_explicit(&me->deque_top, &t, t + 1,
                                                  memory_order_seq_cst,
                                                  memory_order_relaxed);
  atomic_store_explicit(&me->deque_bottom, b + 1, memory_order_relaxed);
  if (!got) pool_announce_clear(p);
  return got;
}

enum { SORA_STEAL_EMPTY = 0, SORA_STEAL_GOT, SORA_STEAL_ABORT };

/* Attempt REAPING -> FREE. Conclusive only with top/bottom re-loaded after
   the status acquire (an earlier bottom read may predate the leaver's final
   writes): for an orphaned deque bottom is static and top monotonic, so
   top >= bottom then means truly drained, and any observer may free the
   slot — which closes the race where a thief empties the deque while the
   owner still reads LEAVING. Returns 1 when the deque is drained. */
static int pool_reaping_free(sora_wk_slot *w) {
  if (deque_nonempty(w)) return 0;
  int32_t expected = SORA_WK_REAPING;
  atomic_compare_exchange_strong_explicit(&w->status, &expected, SORA_WK_FREE,
                                          memory_order_seq_cst,
                                          memory_order_relaxed);
  return 1;
}

/* Chase-Lev steal: copy the entry at top, then CAS top to claim it; only
   the CAS publishes the theft, so a lost race or a torn copy from the
   owner lapping the buffer is discarded unobserved. An orphaned (REAPING)
   deque is consumed through this same path; whoever observes it drained
   returns the slot to FREE. */
static int pool_steal_from(sora_pool *p, uint32_t v) {
  sora_wk_slot *w = &p->wk[v];
  int64_t t = atomic_load_explicit(&w->deque_top, memory_order_acquire);
  atomic_thread_fence(memory_order_seq_cst);
  int64_t b = atomic_load_explicit(&w->deque_bottom, memory_order_acquire);
  if (t >= b) {
    if (atomic_load_explicit(&w->status, memory_order_acquire) ==
        SORA_WK_REAPING && !pool_reaping_free(w))
      return SORA_STEAL_ABORT;   /* orphaned and nonempty after all: retry */
    return SORA_STEAL_EMPTY;
  }
  pool_copy_entry(p, p->scratch, deque_entry_at(p, w, t));
  pool_announce(p);
  if (!atomic_compare_exchange_strong_explicit(&w->deque_top, &t, t + 1,
                                               memory_order_seq_cst,
                                               memory_order_relaxed)) {
    pool_announce_clear(p);
    return SORA_STEAL_ABORT;
  }
  if (atomic_load_explicit(&w->status, memory_order_acquire) ==
      SORA_WK_REAPING)
    pool_reaping_free(w);
  p->st_steals++;
  return SORA_STEAL_GOT;
}

static uint64_t pool_rng(sora_pool *p) {   /* xorshift64 */
  uint64_t x = p->rng;
  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;
  return p->rng = x;
}

#define SORA_STEAL_ROUNDS 4
#define SORA_HELP_DEPTH_LIMIT 32

/* Random victim, one attempt per victim, bounded rounds; EMPTY and ABORT
   alike move to the next victim. Victims are LIVE and REAPING slots — or
   REAPING only for a help-mode collector at its depth limit, since live
   peers' deques have a guaranteed executor (their owner) while ownerless
   work does not. Exhausting the rounds falls through to the caller's next
   tier (injection scan, then the pre-park spin + announce-then-rescan),
   which is what makes a missed steal safe. */
static int pool_steal_any(sora_pool *p, int reaping_only) {
  uint32_t mw = p->hdr.max_workers;
  if (mw <= 1) return 0;
  for (int r = 0; r < SORA_STEAL_ROUNDS; r++) {
    uint32_t start = (uint32_t) (pool_rng(p) % mw);
    for (uint32_t k = 0; k < mw; k++) {
      uint32_t i = (start + k) % mw;
      if ((int) i == p->wk_slot) continue;
      int32_t st = atomic_load_explicit(&p->wk[i].status,
                                        memory_order_acquire);
      if (st != SORA_WK_REAPING && (reaping_only || st != SORA_WK_LIVE))
        continue;
      if (pool_steal_from(p, i) == SORA_STEAL_GOT) return 1;
      if (st == SORA_WK_LIVE && deque_nonempty(&p->wk[i]))
        p->probe_victim = i;
    }
  }
  return 0;
}

#define SORA_PROBE_STREAK 16

static int pool_steal(sora_pool *p) {
  p->probe_victim = UINT32_MAX;
  if (pool_steal_any(p, 0)) {
    p->probe_streak = 0;
    return 1;
  }
  /* thief backstop: repeated failures against an apparently-live,
     apparently-nonempty victim warrant one death probe — the cross-check
     for a reap the listener never ran */
  if (p->probe_victim != UINT32_MAX &&
      ++p->probe_streak >= SORA_PROBE_STREAK) {
    p->probe_streak = 0;
    pool_probe_worker(p, p->probe_victim);
  }
  return 0;
}

/* The fairness tick's full scan: every injection ring unfiltered by the
   ready mask, then every REAPING slot's orphaned deque — the bound on
   external-submission (and ownerless-work) latency when a saturated pool
   never otherwise falls through its local tiers. */
static int pool_fairness_scan(sora_pool *p) {
  if (pool_claim_rings(p, 0)) return 1;
  for (uint32_t i = 0; i < p->hdr.max_workers; i++) {
    if ((int) i == p->wk_slot) continue;
    if (atomic_load_explicit(&p->wk[i].status, memory_order_acquire) ==
        SORA_WK_REAPING && pool_steal_from(p, i) == SORA_STEAL_GOT)
      return 1;
  }
  return 0;
}

/* One claim attempt in tier order. On success the entry is in scratch and
   announced; the caller executes it. */
static int pool_next_task(sora_pool *p) {
  if (++p->claims % 61 == 0) {
    pool_stats_publish(p);
    if (pool_fairness_scan(p)) return 1;
  }
  if (pool_deque_pop(p)) return 1;
  if (pool_steal(p)) return 1;
  return pool_claim_rings(p, 1);
}

/* Cheap work probe for the pre-announce spin: one load of the ready mask
   plus a sweep of the deque index lines. */
static int pool_work_hint(sora_pool *p) {
  if (atomic_load_explicit(p->inj_ready, memory_order_acquire) != 0)
    return 1;
  for (uint32_t i = 0; i < p->hdr.max_workers; i++) {
    int32_t st = atomic_load_explicit(&p->wk[i].status,
                                      memory_order_acquire);
    if ((st == SORA_WK_LIVE || st == SORA_WK_REAPING) &&
        deque_nonempty(&p->wk[i]))
      return 1;
  }
  return 0;
}

/* Unfiltered work check for the pre-park rescan, covering every claim
   source — injection rings (repairing a stale-clear ready bit as it goes)
   and every LIVE or REAPING deque, own included. */
static int pool_any_work(sora_pool *p) {
  int any = 0;
  for (uint32_t s = 0; s < p->hdr.max_submitters; s++) {
    unsigned char *ring = pool_ring(p, s);
    if (atomic_load_explicit(ring_head(ring), memory_order_acquire) <
        atomic_load_explicit(ring_tail(ring), memory_order_acquire)) {
      atomic_fetch_or_explicit(p->inj_ready, 1ull << s,
                               memory_order_seq_cst);
      any = 1;
    }
  }
  for (uint32_t i = 0; i < p->hdr.max_workers; i++) {
    int32_t st = atomic_load_explicit(&p->wk[i].status,
                                      memory_order_acquire);
    if ((st == SORA_WK_LIVE || st == SORA_WK_REAPING) &&
        deque_nonempty(&p->wk[i]))
      any = 1;
  }
  return any;
}

// Death reaping ---------------------------------------------------------------------------

/* Everything in this section is pure SHM/CAS/flock/unpark code with no R
   API: the controller's death-listener callback runs it off the R main
   thread, concurrently with whatever the handle's own thread is doing, so
   nothing here touches scratch, keeper vectors, or the scan rotation.
   Holding the slot's liveness lock is the reap grant; every store is
   CAS-guarded or idempotent, so concurrent or repeated reaps are harmless
   (a kept-fd flock re-acquire succeeds and re-runs the reap; a LockFileEx
   re-acquire reads HELD and skips — the holding prober's reap suffices). */

/* Fail the dead worker's tasks — the announced claim, then a worker_slot
   sweep for whatever it was executing — unpark every waiter its orphaned
   deque names (their help scans then steal from it), and either wake a
   drainer or free the emptied slot. Read-only walk; resumable. */
static void pool_orphan_and_finalize(sora_pool *p, sora_wk_slot *w,
                                     uint32_t slot) {
  int32_t inf = atomic_load_explicit(&w->in_flight_rs, memory_order_acquire);
  if (inf >= 0 && (uint32_t) inf < p->hdr.result_slots) {
    sora_rs_hdr *rs = pool_rs(p, (uint32_t) inf);
    if (atomic_load_explicit(&rs->sequence, memory_order_relaxed) ==
        atomic_load_explicit(&w->in_flight_seq, memory_order_relaxed)) {
      int32_t expected = SORA_RS_PENDING;
      if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                  SORA_RS_DIED,
                                                  memory_order_seq_cst,
                                                  memory_order_relaxed)) {
        pool_unpark_result_waiter(p, rs);
      } else if (expected == SORA_RS_CANCEL) {
        /* handle already dropped: no collector waits; return the slot */
        atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                SORA_RS_FREE,
                                                memory_order_seq_cst,
                                                memory_order_relaxed);
      }
    }
    atomic_store_explicit(&w->in_flight_rs, -1, memory_order_relaxed);
  }
  /* The announce covers claimed-but-not-yet-executing (worker_slot still
     -1); a nested claim overwrites it and its publish clears it, so tasks
     the worker was executing — at any nesting depth — are found by their
     worker_slot stamp instead. No sequence check needed: pool_commit_rs
     resets the stamp before its PENDING release store, so a slot freed
     and recommitted can never read as the dead worker's. */
  for (uint32_t i = 0; i < p->hdr.result_slots; i++) {
    sora_rs_hdr *rs = pool_rs(p, i);
    int32_t st = atomic_load_explicit(&rs->status, memory_order_acquire);
    if ((st != SORA_RS_PENDING && st != SORA_RS_CANCEL) ||
        atomic_load_explicit(&rs->worker_slot, memory_order_relaxed) !=
        (int32_t) slot)
      continue;
    int32_t expected = SORA_RS_PENDING;
    if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                SORA_RS_DIED,
                                                memory_order_seq_cst,
                                                memory_order_relaxed)) {
      pool_unpark_result_waiter(p, rs);
    } else if (expected == SORA_RS_CANCEL) {
      /* the dead executor owed the CANCEL consume: return the slot */
      atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                              SORA_RS_FREE,
                                              memory_order_seq_cst,
                                              memory_order_relaxed);
    }
  }
  int64_t b = atomic_load_explicit(&w->deque_bottom, memory_order_acquire);
  for (int64_t i = atomic_load_explicit(&w->deque_top, memory_order_acquire);
       i < b; i++) {
    sora_entry_hdr *eh = (sora_entry_hdr *) deque_entry_at(p, w, i);
    if (eh->rs_index < p->hdr.result_slots)
      pool_unpark_result_waiter(p, pool_rs(p, eh->rs_index));
  }
  if (atomic_load_explicit(&w->deque_top, memory_order_acquire) < b) {
    /* orphaned work must drain even when no waiter is parked; deque work
       is help-unreachable, so the unpark half suffices */
    (void) pool_unpark_worker_from(p, slot);
  } else {
    int32_t expected = SORA_WK_REAPING;
    atomic_compare_exchange_strong_explicit(&w->status, &expected,
                                            SORA_WK_FREE,
                                            memory_order_seq_cst,
                                            memory_order_relaxed);
  }
}

/* Precondition: the caller holds the slot's liveness lock. */
static void pool_reap_worker(sora_pool *p, uint32_t slot) {
  sora_wk_slot *w = &p->wk[slot];
  for (;;) {
    int32_t expected = atomic_load_explicit(&w->status,
                                            memory_order_acquire);
    if (expected == SORA_WK_LIVE || expected == SORA_WK_LEAVING) {
      if (!atomic_compare_exchange_strong_explicit(&w->status, &expected,
                                                   SORA_WK_REAPING,
                                                   memory_order_seq_cst,
                                                   memory_order_relaxed))
        continue;
      pool_orphan_and_finalize(p, w, slot);
    } else if (expected == SORA_WK_REAPING) {
      /* predecessor reaper died mid-walk: re-running it is harmless */
      pool_orphan_and_finalize(p, w, slot);
    } else if (expected == SORA_WK_CLAIMING) {
      /* died between its lock acquire and CLAIMING -> LIVE: deque
         uninitialized, nothing in flight — lock-before-CAS in the join is
         what makes this state conclusive for a lock holder */
      if (!atomic_compare_exchange_strong_explicit(&w->status, &expected,
                                                   SORA_WK_FREE,
                                                   memory_order_seq_cst,
                                                   memory_order_relaxed))
        continue;
    }
    return;   /* FREE: predecessor reap complete */
  }
}

/* Non-blocking death probe + reap. The controller probes on its kept fds;
   everyone else opens the path without O_CREAT and must prove the file's
   identity against the slot's recorded (dev, inode) before trusting an
   acquire — ENOENT or a mismatch reads as indeterminate, never a verdict,
   because a false DEAD is the one verdict the protocol cannot absorb.
   Returns 1 when a reap ran. */
static int pool_probe_worker(sora_pool *p, uint32_t slot) {
  sora_wk_slot *w = &p->wk[slot];
  if (atomic_load_explicit(&w->status, memory_order_acquire) == SORA_WK_FREE)
    return 0;
  int dead = 0;
  if (p->live_all != NULL) {
    if (sora_live_try(p->live_all[slot]) != SORA_LIVE_ACQUIRED) return 0;
    pool_reap_worker(p, slot);
    sora_live_unlock(p->live_all[slot]);
    dead = 1;
  } else {
    char path[1024];
    intptr_t h;
    if (pool_live_path(p, path, sizeof(path), "wk", slot) != 0) return 0;
    if (sora_live_open_existing(path, &h) != 0) return 0;
    uint64_t dev, ino;
    dead = sora_live_ident(h, &dev, &ino) == 0 &&
      dev == w->live_dev && ino == w->live_ino &&
      sora_live_try(h) == SORA_LIVE_ACQUIRED;
    if (dead) pool_reap_worker(p, slot);
    sora_live_close(h);
  }
  /* the zc death backstop: this handle's lent regions consumed by the
     dead worker (task-arg payloads, keyed at the release point)
     force-reclaim. Runs on the R main thread only — every probe caller
     is one — never in the controller's off-thread death callback */
  if (dead) sora_ledger_force(&p->fl, (int32_t) slot);
  return dead;
}

/* Precondition: the caller holds the dead submitter's liveness lock.
   Cancels its PENDING slots (mid-execution workers observe the publish-CAS
   failure and discard), frees its published-but-uncollected results
   (releasing the producing workers' keepers), and leaves CANCEL slots
   alone — they free at pop, which is what keeps release-at-reuse safe for
   entries still queued in the dead submitter's ring. */
static void pool_reap_submitter(sora_pool *p, uint32_t j) {
  sora_sub_slot *s = &p->sub[j];
  int32_t expected = SORA_SUB_LIVE;
  if (!atomic_compare_exchange_strong_explicit(&s->status, &expected,
                                               SORA_SUB_REAPING,
                                               memory_order_seq_cst,
                                               memory_order_acquire) &&
      expected != SORA_SUB_REAPING)
    return;                                  /* FREE: nothing to do */
  for (uint32_t k = 0; k < s->rs_count; k++) {
    sora_rs_hdr *rs = pool_rs(p, s->rs_start + k);
    for (;;) {
      int32_t st = atomic_load_explicit(&rs->status, memory_order_acquire);
      if (st == SORA_RS_PENDING) {
        int32_t e2 = SORA_RS_PENDING;
        if (!atomic_compare_exchange_strong_explicit(&rs->status, &e2,
                                                     SORA_RS_CANCEL,
                                                     memory_order_seq_cst,
                                                     memory_order_relaxed))
          continue;
        pool_unpark_result_waiter(p, rs);
      } else if (st == SORA_RS_OK || st == SORA_RS_ERR || st == SORA_RS_DIED) {
        int32_t wk = atomic_load_explicit(&rs->worker_slot,
                                          memory_order_acquire);
        int32_t e2 = st;
        if (!atomic_compare_exchange_strong_explicit(&rs->status, &e2,
                                                     SORA_RS_FREE,
                                                     memory_order_seq_cst,
                                                     memory_order_relaxed))
          continue;
        pool_unpark_keeper_drop(p, wk);
      }
      break;
    }
  }
  atomic_store_explicit(&s->status, SORA_SUB_FREE, memory_order_seq_cst);
}

static void pool_probe_submitter(sora_pool *p, uint32_t j) {
  if ((int) j == p->sub_slot) return;        /* our own held lock */
  sora_sub_slot *s = &p->sub[j];
  if (atomic_load_explicit(&s->status, memory_order_acquire) ==
      SORA_SUB_FREE)
    return;
  int reaped = 0;
  if (p->live_all != NULL) {
    intptr_t h = p->live_all[p->hdr.max_workers + j];
    if (sora_live_try(h) != SORA_LIVE_ACQUIRED) return;
    pool_reap_submitter(p, j);
    sora_live_unlock(h);
    reaped = 1;
  } else {
    char path[1024];
    intptr_t h;
    if (pool_live_path(p, path, sizeof(path), "sub", j) != 0) return;
    if (sora_live_open_existing(path, &h) != 0) return;
    uint64_t dev, ino;
    if (sora_live_ident(h, &dev, &ino) == 0 &&
        dev == s->live_dev && ino == s->live_ino &&
        sora_live_try(h) == SORA_LIVE_ACQUIRED) {
      pool_reap_submitter(p, j);
      reaped = 1;
    }
    sora_live_close(h);
  }
  /* the zc death backstop rides the submitter reap: this handle's lent
     regions consumed by the dead submitter force-reclaim (REFHELD ones
     leak + unlink). Only ever runs on the R main thread (the probe
     callers), never in the controller's off-thread death callback */
  if (reaped) sora_ledger_force(&p->fl, (int32_t) j);
}

/* The controller's per-worker death callback: OS notification -> lock
   verdict -> reap, entirely off the R main thread. A pid-reuse race is
   absorbed by the lock (the impostor holds nothing here). */
static void pool_wk_death_cb(void *arg) {
  struct sora_reap_ctx_s *c = arg;
  sora_pool *p = (sora_pool *) c->pool;
  if (sora_live_try(p->live_all[c->slot]) == SORA_LIVE_ACQUIRED) {
    pool_reap_worker(p, c->slot);
    sora_live_unlock(p->live_all[c->slot]);
  }
}

/* Owner-death cleanup: acquiring the owner lock (the kept fd from join)
   grants exclusive teardown; a caller finding it held knows teardown is in
   progress elsewhere and simply fails locally. The broadcast is exactly
   the stop broadcast; the tail is janitorial best-effort — liveness files
   by path, the region via the vendored dead-PID reaper (its name embeds
   the dead creator's pid). */
static void pool_remove_live_files(sora_pool *p) {
  char path[1024];
  for (uint32_t i = 0; i < p->hdr.max_workers; i++)
    if (pool_live_path(p, path, sizeof(path), "wk", i) == 0) remove(path);
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++)
    if (pool_live_path(p, path, sizeof(path), "sub", j) == 0) remove(path);
  if (pool_live_path(p, path, sizeof(path), "owner", 0) == 0) remove(path);
}

static void pool_orphan_teardown_try(sora_pool *p) {
  if (p->live_owner == 0 ||
      sora_live_try(p->live_owner) != SORA_LIVE_ACQUIRED)
    return;
  pool_shutdown_broadcast(p);
  pool_remove_live_files(p);
  int n = 0;
  char **list = mori_shm_reap(&n);
  for (int i = 0; i < n; i++) free(list[i]);
  free(list);
}

/* The publish tail shared by pool_execute and the unwind path
   (sora_pool_run_outcome): stage the outcome into the result slot, CAS it
   OK/ERR, pin the keeper, wake the waiter — or consume a concurrent CANCEL
   and probe the (possibly dead) submitter. Retires the in-flight announce.
   Payload writes are plain stores into a slot no allocator can touch
   (status stays PENDING/CANCEL until the FREE transition); the publish CAS
   is the release barrier a collector's acquire load pairs with. An ERR
   outcome never stages the caught condition itself: it is flattened to a
   transport condition (condition.c) framed INLINE, so the publish cannot
   raise — fail the task, never the worker. */
static int pool_publish_result(sora_pool *p, SEXP xp, uint32_t rs_index,
                               uint16_t sub_slot, uint64_t seq, int ok,
                               SEXP value) {
  sora_rs_hdr *rs = pool_rs(p, rs_index);
  unsigned char *payload = (unsigned char *) rs + sizeof(sora_rs_hdr);
  sora_stage_rollback(&p->fl);
  pool_rk_reserve(p);
  sora_keeper keep = { NULL, R_NilValue, -1, SORA_KEEP_FREE };
  SEXP staged = value;
  int nprot = 0, framed = 0;
  if (!ok) {
    staged = PROTECT(sora_condition_flatten(value, (size_t) p->inline_rs));
    nprot = 1;
    size_t n = sora_codec_write(payload, (size_t) p->inline_rs, staged);
    if (n != 0 && n <= (size_t) p->inline_rs) {
      rs->ph.kind = SORA_KIND_INLINE;
      rs->ph.len = (uint32_t) n;
      rs->ph.aux = 0;
      framed = 1;
      /* a self-contained codec stream pins nothing — the keeperless
         kinds' discipline, which pool_rs_claim's keeperless gate already
         reads off the magic byte. Framed INLINE directly rather than
         through the tiered stage: flatten's verification pass already
         guarantees the fit, so the tier probes would only repeat the
         codec write to arrive at the same frame. Below flatten's
         guarantee (a 128-byte slot holds no classed condition inline)
         the tiered stage carries the terminal fallback out of line —
         the pre-flattening behavior for that configuration. */
    }
  }
  if (!framed)
    sora_payload_stage(&rs->ph, payload, p->inline_rs, staged, &p->fl,
                       &keep);
  /* a spilled result region's consumer is the task's submitter — key the
     zc entry so the submitter-death backstop can force-reclaim it */
  if (keep.kind == SORA_KEEP_ZC) keep.key = (int32_t) sub_slot;
  pool_count_spill(p, sub_slot, &rs->ph);
  int32_t expected = SORA_RS_PENDING;
  int published =
    atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                            ok ? SORA_RS_OK : SORA_RS_ERR,
                                            memory_order_seq_cst,
                                            memory_order_acquire);
  if (published) {
    /* a same-slot republish can overwrite the previous incarnation's
       keeper before any reap visit ran; the slot was freed and recommitted
       in between, so that region surrenders rather than falling to GC */
    sora_keeper_release(&p->fl, p->keepers, p->pins, (R_xlen_t) rs_index);
    sora_keeper_commit(&p->fl, p->keepers, p->pins, (R_xlen_t) rs_index,
                       &keep);
    /* a keeperless result (the self-contained kinds) pins nothing: no
       record */
    if (keep.kind != SORA_KEEP_FREE) pool_rk_add(p, rs_index, seq);
    pool_unpark_result_waiter(p, rs);
  } else {
    /* cancelled while we ran: drop the result, return the slot — a spilled
       result region was never published, so it recycles immediately */
    sora_keeper_discard(&p->fl, &keep);
    expected = SORA_RS_CANCEL;
    atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                            SORA_RS_FREE,
                                            memory_order_seq_cst,
                                            memory_order_relaxed);
    /* consuming a CANCEL — here or at the pre-eval skip — is submitter
       death's one hot-path trigger: a live submitter means a genuine
       cancellation, a dead one is reaped in-line */
    if (sub_slot < p->hdr.max_submitters)
      pool_probe_submitter(p, sub_slot);
  }
  atomic_store_explicit(&p->wk[p->wk_slot].in_flight_rs, -1,
                        memory_order_relaxed);
  UNPROTECT(nprot);
  return published;
}

/* Executes the claimed, announced entry in scratch and publishes into its
   result slot. Reentrant: help-mode and nested-submit execution recurse
   through here from inside Rf_eval, and every claim path reuses scratch —
   so everything needed from the entry and the announce is copied out
   before the eval. */
static void pool_execute(sora_pool *p, SEXP xp, int catching) {
  (void) pool_eval_env(xp);
  sora_wk_slot *me = &p->wk[p->wk_slot];
  sora_entry_hdr *eh = (sora_entry_hdr *) p->scratch;
  uint32_t rs_index = eh->rs_index;
  uint16_t sub_slot = eh->submitter_slot;
  uint64_t task_id = eh->task_id;
  sora_rs_hdr *rs = pool_rs(p, rs_index);
  uint64_t seq = atomic_load_explicit(&me->in_flight_seq,
                                      memory_order_relaxed);
  atomic_store_explicit(&rs->worker_slot, p->wk_slot, memory_order_relaxed);
  if (!catching) {
    p->cur_rs_index = rs_index;
    p->cur_seq = seq;
    p->cur_task_id = task_id;
    p->cur_sub_slot = sub_slot;
  }

  /* skip dead work: the check races the finalizer's CANCEL, and correctness
     rests on the publish CAS below either way. The probe rides here as at
     the failed publish — a submitter that died before its queued work was
     claimed would otherwise pin its slot until the stop sweep */
  if (atomic_load_explicit(&rs->status, memory_order_acquire) ==
      SORA_RS_CANCEL) {
    int32_t expected = SORA_RS_CANCEL;
    atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                            SORA_RS_FREE,
                                            memory_order_seq_cst,
                                            memory_order_relaxed);
    if (sub_slot < p->hdr.max_submitters)
      pool_probe_submitter(p, sub_slot);
    atomic_store_explicit(&me->in_flight_rs, -1, memory_order_relaxed);
    pool_trace_emit(xp, "drop", task_id);
    return;
  }

  /* An INLINE codec task frame stream-decodes in place — no list(expr,
     args) materialization, so a constant task allocates nothing on the
     worker. Anything else takes the generic read and its shape check. Both
     paths end with expr and args PROTECTed (2 total): the reads hand them
     over unprotected and nothing allocates before the PROTECTs. */
  SEXP expr = R_NilValue, args = R_NilValue;
  int gone = 0;
  if (eh->ph.kind == SORA_KIND_INLINE && eh->ph.len <= p->inline_entry &&
      sora_codec_read_task(p->scratch + sizeof(sora_entry_hdr),
                           (size_t) eh->ph.len, &expr, &args)) {
    PROTECT(expr);
    PROTECT(args);
  } else {
    /* A vanished out-of-line entry payload means the enqueuer died and its
       region went along (Win32 mappings cannot outlive their creator): the
       task can never run anywhere — it fails as DIED exactly like a claimed
       task whose worker died, and the drain continues in this thief. */
    SEXP pl = sora_payload_read(&eh->ph,
                                p->scratch + sizeof(sora_entry_hdr),
                                p->inline_entry, &gone, &p->oc, &p->zoc);
    if (gone) {
      int32_t expected = SORA_RS_PENDING;
      if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                  SORA_RS_DIED,
                                                  memory_order_seq_cst,
                                                  memory_order_relaxed)) {
        pool_unpark_result_waiter(p, rs);
      } else if (expected == SORA_RS_CANCEL) {
        atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                SORA_RS_FREE,
                                                memory_order_seq_cst,
                                                memory_order_relaxed);
        if (sub_slot < p->hdr.max_submitters)
          pool_probe_submitter(p, sub_slot);
      }
      atomic_store_explicit(&me->in_flight_rs, -1, memory_order_relaxed);
      pool_trace_emit(xp, "drop", task_id);
      return;
    }
    if (TYPEOF(pl) != VECSXP || Rf_xlength(pl) != 2 ||
        TYPEOF(VECTOR_ELT(pl, 1)) != VECSXP)
      Rf_error("sora: corrupt task payload");
    expr = PROTECT(VECTOR_ELT(pl, 0));
    args = PROTECT(VECTOR_ELT(pl, 1));
  }
  pool_trace_emit(xp, "start", task_id);
  /* scratch (and eh with it) is dead from here: the eval below may claim
     into it */
  int ok = 1;
  SEXP value = PROTECT(pool_eval_expr(p, xp, expr, args, catching, &ok));
  p->st_tasks++;
  int published = pool_publish_result(p, xp, rs_index, sub_slot, seq, ok,
                                      value);
  UNPROTECT(3);                    /* expr, args, value */
  pool_trace_emit(xp, published ? (ok ? "done" : "error") : "drop", task_id);
}

/* worker_main's dispatcher for whatever sora_pool_run produced, and the
   test harness's unwind-path publisher. An integer is the run's exit
   code — no error at all — and passes through for R to end its loop on
   (a caught condition is never INTSXP, so the dispatch is exact). A
   condition's task eval longjmped out of the loop: publish it as that
   task's ERR result and return 0 to continue. 1 means the error did not
   come from inside a task eval and the caller must treat it as fatal
   infrastructure failure. The in_eval gate is what keeps errors from
   staging, payload reads, or trace hooks on the fatal path. */
SEXP sora_pool_run_outcome(SEXP xp, SEXP cond) {
  sora_pool *p = pool_get(xp);
  if (p->role != SORA_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("sora: not a worker handle");
  if (TYPEOF(cond) == INTSXP)
    return cond;
  if (!p->in_eval)
    return Rf_ScalarInteger(1);
  p->in_eval = 0;
  p->st_tasks++;
  int published = pool_publish_result(p, xp, p->cur_rs_index,
                                      p->cur_sub_slot, p->cur_seq, 0, cond);
  pool_trace_emit(xp, published ? "error" : "drop", p->cur_task_id);
  return Rf_ScalarInteger(0);
}

/* The worker loop, shared by its two entry points. sora_pool_step
   (single = 1) runs one iteration — claim + execute one task or park —
   returning 1 after a task, 0 on timeout, -1 on shutdown or owner death,
   -2 on retire; the in-process test harness single-steps it. sora_pool_run
   (single = 0), worker_main's loop, stays in C across tasks and returns
   only the negative exits; a task error still longjmps to worker_main's
   tryCatch, which publishes through sora_pool_run_outcome and re-enters.
   Two per-task disciplines replace what the R round-trip provided
   implicitly: an interrupt check after each execute (the R repeat's
   back-edge check) and a deadline recompute at park-timeout expiry (the
   re-entry's fresh timeout). The evaluator comes from the handle
   (sora_pool_set_eval), checked up front so a claim can never outrun a
   missing evaluator. */
static SEXP pool_step_impl(SEXP xp, SEXP timeout, int single) {
  sora_pool *p = pool_get(xp);
  if (p->role != SORA_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("sora: not a worker handle");
  (void) pool_eval_env(xp);
  sora_wk_slot *me = &p->wk[p->wk_slot];
  uint64_t my_bit = 1ull << p->wk_slot;
  double timeout_s = Rf_asReal(timeout);
  double deadline = R_FINITE(timeout_s) ? sora_now() + timeout_s : -1;

  /* heal any announce (or unwind-path eval flag) left dangling by an
     interrupt longjmp out of a previous step: a stale bit costs the pusher
     one failed CAS. Gated on the process-local flag — only this worker
     ever sets its own bit, so the flag is exact and a clean previous exit
     skips a per-task seq_cst RMW on the mask line every worker shares. */
  p->in_eval = 0;
  if (p->announced) {
    atomic_store_explicit(&me->park_state, SORA_WPK_RUNNING,
                          memory_order_relaxed);
    atomic_fetch_and_explicit(p->parked_workers, ~my_bit,
                              memory_order_seq_cst);
    p->announced = 0;
  }

  for (;;) {
    pool_reap_quota(p);
    if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0) {
      pool_stats_publish(p);
      return Rf_ScalarInteger(-1);
    }
    if (atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0) {
      /* first confirmed detector tears the orphan pool down; losers just
         exit — the winner's broadcast collapses everyone's discovery */
      pool_orphan_teardown_try(p);
      pool_stats_publish(p);
      return Rf_ScalarInteger(-1);
    }
    if (atomic_load_explicit(&me->retire, memory_order_acquire) != 0) {
      pool_stats_publish(p);
      return Rf_ScalarInteger(-2);
    }

    if (pool_next_task(p)) {
      /* work found since the last reset recovers the scan budget —
         without this, work caught after a park wake strands it at the
         floor under trickle traffic */
      p->scan_budget_ns = SORA_SPIN_BUDGET_NS;
      pool_execute(p, xp, 0);
      if (single) return Rf_ScalarInteger(1);
      sora_check_interrupt(&p->binding);
      continue;
    }

    if (timeout_s <= 0) {
      pool_idle_sweep(p, xp);
      pool_stats_publish(p);
      return Rf_ScalarInteger(0);
    }

    /* time-boxed spin before announcing: sub-µs submit gaps are
       absorbed without touching the parked_workers line. The budget
       decays only when the spin comes up empty. The hint scan is
       O(max_workers): the light stride applies only to small pools,
       where the scan is a few loads and the clock would dominate */
    double until = sora_now() + (double) p->scan_budget_ns / 1e9;
    if (deadline >= 0 && deadline < until) until = deadline;
    int caught;
    SORA_SPIN_WAIT(pool_work_hint(p), until,
                   p->hdr.max_workers <= 4 ? SORA_SPIN_CLOCK_EVERY_LIGHT / 2
                                           : SORA_SPIN_CLOCK_EVERY,
                   caught);
    if (caught) {
      p->scan_budget_ns = SORA_SPIN_BUDGET_NS;
      continue;
    }
    p->scan_budget_ns = p->scan_budget_ns / 2 < SORA_SPIN_FLOOR_NS ?
      SORA_SPIN_FLOOR_NS : p->scan_budget_ns / 2;

    /* announce-then-rescan (the sleep race): either our rescan sees the
       push or the pusher's mask load sees our bit */
    uint32_t e = sora_parker_snapshot(pool_wk_pk(p, (uint32_t) p->wk_slot));
    p->announced = 1;
    atomic_store_explicit(&me->park_state, SORA_WPK_IDLE,
                          memory_order_relaxed);
    pool_stats_publish(p);
    atomic_fetch_or_explicit(p->parked_workers, my_bit,
                             memory_order_seq_cst);
    atomic_thread_fence(memory_order_seq_cst);
    /* going idle: sweep the whole keeper table (and drop the map-context
       cache), so a parked worker holds only what is genuinely
       uncollected. After the announce, so a keeper-drop FREE racing this
       park is either consumed here or its dropper saw our bit and unparks
       us (pool_unpark_keeper_drop) — the epoch snapshot above predates
       the bit, so that unpark turns the park below into an immediate
       return */
    pool_idle_sweep(p, xp);
    /* retire joins the wake conditions here: its store + unpark landing
       between the loop-top check and the epoch snapshot above would
       otherwise be a lost wake, and the park below sleeps for the full
       run bound (worker_main passes 3600s) */
    if (pool_any_work(p) ||
        atomic_load_explicit(&me->retire, memory_order_acquire) != 0 ||
        atomic_load_explicit(p->shutdown, memory_order_acquire) != 0 ||
        atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0) {
      atomic_fetch_and_explicit(p->parked_workers, ~my_bit,
                                memory_order_seq_cst);
      atomic_store_explicit(&me->park_state, SORA_WPK_RUNNING,
                            memory_order_relaxed);
      p->announced = 0;
      continue;
    }
    int32_t expected = SORA_WPK_IDLE;
    if (atomic_compare_exchange_strong_explicit(&me->park_state, &expected,
                                                SORA_WPK_PARKED,
                                                memory_order_seq_cst,
                                                memory_order_relaxed)) {
      long ms = -1;
      if (deadline >= 0) {
        double rem = deadline - sora_now();
        ms = rem <= 0 ? 0 : (long) (rem * 1000) + 1;
      }
      sora_park_bracket(&p->binding, 1);
      sora_park(pool_wk_pk(p, (uint32_t) p->wk_slot), e, ms);
      sora_park_bracket(&p->binding, 0);
      p->st_parks++;
    }
    atomic_fetch_and_explicit(p->parked_workers, ~my_bit,
                              memory_order_seq_cst);
    atomic_store_explicit(&me->park_state, SORA_WPK_RUNNING,
                          memory_order_relaxed);
    p->announced = 0;
    pool_stats_publish(p);
    sora_check_interrupt(&p->binding);
    if (deadline >= 0 && sora_now() >= deadline) {
      pool_idle_sweep(p, xp);
      if (single) return Rf_ScalarInteger(0);
      deadline = sora_now() + timeout_s;
    }
  }
}

SEXP sora_pool_step(SEXP xp, SEXP timeout) {
  return pool_step_impl(xp, timeout, 1);
}

SEXP sora_pool_run(SEXP xp, SEXP timeout) {
  return pool_step_impl(xp, timeout, 0);
}

/* Test-only: claim up to n injection entries and queue them on this
   worker's own deque instead of executing them — the deterministic way to
   populate a deque before Phase 3's nested submit exists. The full check
   precedes the claim (space only grows once we own the bottom), so a
   claimed entry can always be queued; the announce clears *before* the
   push, the re-home discipline this call templates — the reverse order
   leaves the entry both announced and deque-published, a double-recovery
   state no production path produces. */
SEXP sora_pool_deque_pull(SEXP xp, SEXP n_sexp) {
  sora_pool *p = pool_get(xp);
  if (p->role != SORA_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("sora: not a worker handle");
  sora_wk_slot *me = &p->wk[p->wk_slot];
  int n = Rf_asInteger(n_sexp);
  int was_empty = !deque_nonempty(me);
  int moved = 0;
  while (moved < n) {
    if (atomic_load_explicit(&me->deque_bottom, memory_order_relaxed) -
        atomic_load_explicit(&me->deque_top, memory_order_acquire) >=
        (int64_t) me->deque_cap)
      break;
    if (!pool_claim_rings(p, 1)) break;
    pool_announce_clear(p);
    pool_deque_push(p, p->scratch);
    moved++;
  }
  /* the nested-submit wake rule: a push taking the deque from empty to
     non-empty wakes one parked peer */
  if (moved > 0 && was_empty) pool_unpark_one_worker(p);
  return Rf_ScalarInteger(moved);
}

// Collect and cancel ----------------------------------------------------------------------

static sora_task task_get(SEXP xp, sora_pool **pool_out, SEXP *pool_xp_out) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != sora_task_tag)
    Rf_error("sora: not a task handle");
  void *addr = R_ExternalPtrAddr(xp);
  if (addr == NULL) Rf_error("sora: task handle is stale");
  SEXP pool_xp = R_ExternalPtrProtected(xp);
  sora_pool *p = pool_get(pool_xp);
  *pool_out = p;
  if (pool_xp_out != NULL) *pool_xp_out = pool_xp;
  return sora_task_unpack(addr);
}

/* Drop the task keeper of a terminal slot in this submitter's subrange.
   OK/ERR means the worker materialized the task args, DIED that the entry's
   claimant is dead: either way no reader remains, so a spilled task region
   surrenders to the free list with the keeper. key names the consuming
   worker for the zc ledger (the worker-death backstop); -1 when unknown. */
static void pool_task_keeper_drop(sora_pool *p, uint32_t idx, int32_t key) {
  sora_sub_slot *me = &p->sub[p->sub_slot];
  if (idx < me->rs_start || idx >= me->rs_start + me->rs_count) return;
  sora_keeper *keepers = pool_task_keepers(p);
  SEXP pins = pool_task_pins(p);
  R_xlen_t local = (R_xlen_t) (idx - me->rs_start);
  /* key names the consuming worker for the zc ledger (the worker-death
     backstop); -1 when unknown */
  if (keepers[local].kind == SORA_KEEP_ZC) keepers[local].key = key;
  sora_keeper_release(&p->fl, keepers, pins, local);
}

/* The shared terminal claim for an OK/ERR slot: materialize BEFORE the
   FREE transition — publication of FREE is what lets the worker's keeper
   reap unlink everything this payload references — then drop the task
   keeper, FREE the slot, and wake the producer's keeper sweep. Returns
   the materialized value (the task's condition for ERR). */
static SEXP pool_rs_claim(sora_pool *p, SEXP pool_xp, sora_rs_hdr *rs,
                          uint32_t idx, int32_t st) {
  SEXP v = PROTECT(sora_payload_read(&rs->ph,
                                    (unsigned char *) rs +
                                    sizeof(sora_rs_hdr), p->inline_rs,
                                    NULL, &p->oc, &p->zoc));
  uint32_t kind = rs->ph.kind;
  int32_t w = atomic_load_explicit(&rs->worker_slot, memory_order_acquire);
  pool_task_keeper_drop(p, idx, w);
  int32_t expected = st;
  if (!atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                               SORA_RS_FREE,
                                               memory_order_seq_cst,
                                               memory_order_acquire)) {
    UNPROTECT(1);
    Rf_error("sora: task handle already collected");
  }
  /* The keeper-drop wake exists for the worker's keeper reap: a FREE slot
     gives it a record to consume. The keeperless kinds (the immediates and
     self-contained codec streams inline — the ones sora_payload_stage pins
     nothing for) create no record (pool_publish_result skips it), so the
     FREE is invisible to the reap and the fence + parked-mask load +
     syscall are pure cost — under a fire-then-collect burst with a parked
     worker, one wake per collect. The lame-duck retiree is unaffected: it
     lingers only while rk_n > 0, i.e. while a keepered (waking) result is
     still outstanding. */
  if (!sora_keeperless(kind, (const unsigned char *) rs +
                       sizeof(sora_rs_hdr)))
    pool_unpark_keeper_drop(p, w);
  UNPROTECT(1);
  return v;
}

/* The DIED counterpart of pool_rs_claim: status-word only, no payload
   (see the DIED note in sora.h). The claimant is confirmed dead (DIED
   implies the reap ran under the liveness lock), so its lent task-arg
   regions are force-reclaimed here. */
static void pool_rs_claim_died(sora_pool *p, SEXP pool_xp, sora_rs_hdr *rs,
                               uint32_t idx, int32_t *w_out,
                               double *wpid_out) {
  int32_t w = atomic_load_explicit(&rs->worker_slot, memory_order_acquire);
  pool_task_keeper_drop(p, idx, w);
  if (w >= 0 && (uint32_t) w < p->hdr.max_workers)
    sora_ledger_force(&p->fl, w);
  *w_out = w;
  *wpid_out =
    (w >= 0 && (uint32_t) w < p->hdr.max_workers) ? (double) p->wk[w].pid : 0;
  int32_t expected = SORA_RS_DIED;
  if (!atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                               SORA_RS_FREE,
                                               memory_order_seq_cst,
                                               memory_order_relaxed))
    Rf_error("sora: task handle already collected");
}

/* Learn the collect budget from a completed wait (t_wait < 0 on the
   never-waited fast path); the tried budget seeds the halve branch. */
static void pool_collect_learn(sora_pool *p, double t_wait,
                               uint64_t budget) {
  if (t_wait < 0) return;
  p->collect_budget_ns =
    sora_spin_learn((sora_now() - t_wait) * 1e9, budget,
                    (uint64_t) SORA_COLLECT_SPIN_BUDGET_NS);
}

/* tryflag: the expected terminal outcomes (ERR payload, DIED, CANCEL)
   return sora_caught-boxed instead of signalling, so the map collect loops
   need no handler; contract violations (stale or already-collected
   handles) raise in both modes. */
static SEXP pool_collect(SEXP xp, double timeout_s, int tryflag) {
  sora_pool *p;
  SEXP pool_xp;
  sora_task t = task_get(xp, &p, &pool_xp);
  if (p->sub_slot < 0)
    Rf_error("sora: not a submitter's task handle");
  sora_rs_hdr *rs = pool_rs(p, t.idx);
  double deadline = -1;
  double t_wait = -1;

  int32_t st;
  for (;;) {
    if (!sora_task_seq_match(&rs->sequence, t.seq))
      Rf_error("sora: task handle already collected or invalidated");
    if (atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0)
      pool_orphan_teardown_try(p);   /* its cancel sweep ends this wait */
    st = atomic_load_explicit(&rs->status, memory_order_acquire);
    if (st != SORA_RS_PENDING) break;

    /* Help mode: a worker blocked on its own subtree must make progress
       on runnable work, not park — N workers simultaneously parked in
       nested collects would deadlock the pool. The awaited task is on this
       worker's own deque bottom, claimed by a peer (progress either way),
       or on a dead worker's orphaned deque; help covers own pops and
       steals, and deliberately excludes the submitter injection rings. At
       the depth limit only ownerless work remains eligible (own bottom +
       REAPING deques): live peers' deques have a guaranteed executor, so
       excluding them there bounds C-stack growth without a hang — every
       chain of waiting collectors bottoms out in a worker executing. */
    if (p->role == SORA_ROLE_WORKER && p->wk_slot >= 0) {
      int got = pool_deque_pop(p);
      if (!got)
        got = pool_steal_any(p, p->help_depth >= SORA_HELP_DEPTH_LIMIT);
      if (got) {
        p->st_helps++;
        p->help_depth++;
        pool_execute(p, pool_xp, 1);
        p->help_depth--;
        continue;
      }
    }

    if (timeout_s <= 0) return sora_sent_timeout;

    /* hoisted above the spin so the first episode's `until` is clamped
       too — a finite timeout's clock covers the whole wait. Still after
       the fast-path break and the timeout_s <= 0 return and still
       R_FINITE-guarded, so the Inf and already-done paths pay no extra
       clock read. One read serves the deadline compute and the bound. */
    double now = sora_now();
    if (t_wait < 0) t_wait = now;
    if (deadline < 0 && R_FINITE(timeout_s))
      deadline = now + timeout_s;

    /* time-boxed spin before the park announce: a short task's publish
       is absorbed without the park/wake syscall pair on either side,
       since waiter_slot stays unannounced through the spin and the
       publisher skips its wake. Every wait exit learns its measured
       turnaround via sora_spin_learn. */
    uint64_t budget = p->collect_budget_ns;
    double until = now + (double) budget / 1e9;
    if (deadline >= 0 && deadline < until) until = deadline;
    int caught;
    SORA_SPIN_WAIT((st = atomic_load_explicit(&rs->status,
                                             memory_order_acquire)) !=
                  SORA_RS_PENDING, until, SORA_SPIN_CLOCK_EVERY_LIGHT,
                  caught);
    if (caught) {
      pool_collect_learn(p, t_wait, budget);
      break;
    }

    /* announce -> fence -> re-check -> park bounded; the publishing worker
       reads waiter_slot after its publish CAS and unparks us */
    uint32_t e = sora_parker_snapshot(pool_sub_pk(p, (uint32_t) p->sub_slot));
    atomic_store_explicit(&rs->waiter_slot, p->sub_slot,
                          memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    if (atomic_load_explicit(&rs->status, memory_order_acquire) !=
        SORA_RS_PENDING) {
      pool_collect_learn(p, t_wait, budget);
      continue;
    }
    long ms = SORA_INTERRUPT_BOUND_MS;
    if (deadline >= 0) {
      double rem = deadline - sora_now();
      if (rem <= 0) return sora_sent_timeout;
      long rem_ms = (long) (rem * 1000) + 1;
      if (rem_ms < ms) ms = rem_ms;
    }
    sora_park_bracket(&p->binding, 1);
    sora_park(pool_sub_pk(p, (uint32_t) p->sub_slot), e, ms);
    sora_park_bracket(&p->binding, 0);
    p->st_collect_parks++;
    sora_check_interrupt(&p->binding);
    st = atomic_load_explicit(&rs->status, memory_order_acquire);
    if (st != SORA_RS_PENDING) {
      pool_collect_learn(p, t_wait, budget);
      break;
    }
    /* backstop for a missed death notification, piggybacked on a wake
       that happened regardless — never a wakeup of its own */
    {
      int32_t claimant = atomic_load_explicit(&rs->worker_slot,
                                              memory_order_acquire);
      if (claimant >= 0 && (uint32_t) claimant < p->hdr.max_workers)
        pool_probe_worker(p, (uint32_t) claimant);
      if (deadline >= 0 && sora_now() >= deadline &&
          atomic_load_explicit(&rs->status, memory_order_acquire) ==
          SORA_RS_PENDING)
        return sora_sent_timeout;
    }
  }

  switch (st) {
  case SORA_RS_OK:
  case SORA_RS_ERR: {
    SEXP v = PROTECT(pool_rs_claim(p, pool_xp, rs, t.idx, st));
    if (st == SORA_RS_ERR) {
      if (tryflag) {
        SEXP out = sora_caught(v);
        UNPROTECT(1);
        return out;
      }
      sora_cond_signal(v);                       /* no return */
    }
    UNPROTECT(1);
    return v;
  }
  case SORA_RS_DIED: {
    int32_t w;
    double wpid;
    pool_rs_claim_died(p, pool_xp, rs, t.idx, &w, &wpid);
    if (tryflag)
      return sora_caught_died((int) w, wpid,
                             "sora: worker died while executing this task");
    sora_stop_died((int) w, wpid,
                  "sora: worker died while executing this task");
  }
  case SORA_RS_CANCEL:
    /* the task keeper releases at slot reuse, not here — the worker may not
       have materialized yet */
    if (tryflag)
      return sora_caught_cond("sora_error_cancelled",
                             "sora: task cancelled or pool stopped");
    sora_stop("sora_error_cancelled", "sora: task cancelled or pool stopped");
  case SORA_RS_FREE:
  default:
    Rf_error("sora: task handle already collected");
  }
}

/* The collect-any predicate: the first handle (list order) whose slot
   reached a terminal state. OK, ERR, DIED, and CANCEL all report — a
   cancelled task is "done", as in asyncio's FIRST_COMPLETED. */
static int pool_any_terminal(sora_rs_hdr **rss, R_xlen_t n, R_xlen_t *found,
                             int32_t *st) {
  for (R_xlen_t i = 0; i < n; i++) {
    int32_t si = atomic_load_explicit(&rss[i]->status, memory_order_acquire);
    if (si != SORA_RS_PENDING) {
      *found = i;
      *st = si;
      return 1;
    }
  }
  return 0;
}

/* Withdraw the waiter announcement from every still-PENDING slot: a
   publisher reads waiter_slot after its publish CAS, and a stale
   announcement would cost it an unpark syscall nobody needs — the caller
   is awake by construction, the only parker on these slots being this
   submitter. */
static void pool_any_unannounce(sora_rs_hdr **rss, R_xlen_t n) {
  for (R_xlen_t i = 0; i < n; i++) {
    if (atomic_load_explicit(&rss[i]->status, memory_order_acquire) ==
        SORA_RS_PENDING)
      atomic_store_explicit(&rss[i]->waiter_slot, -1, memory_order_relaxed);
  }
}

/* Wait on any of a submitter's outstanding tasks, on the existing slot
   mechanics: the wait predicate is the whole slot set — announce on every
   slot, park once on the submitter's one parker (every publish's directed
   unpark lands on it), scan on wake. Announcements are withdrawn at each
   exit so a later publish pays no stray unpark (a Ctrl-C longjmp leaks
   them until collect or slot reuse — a bounded stray-unpark cost,
   self-healing). O(N) per scan; no protocol change. Terminal outcomes
   return sora_caught-boxed with the 1-based list position on an "index"
   attribute for the R wrapper to re-signal. */
SEXP sora_pool_collect_any(SEXP tasks, SEXP timeout) {
  if (TYPEOF(tasks) != VECSXP || XLENGTH(tasks) == 0)
    Rf_error("sora: tasks must be a non-empty list of task handles");
  double timeout_s = Rf_asReal(timeout);
  R_xlen_t n = XLENGTH(tasks);
  sora_pool *p = NULL;
  SEXP pool_xp = R_NilValue;
  sora_task *ts = (sora_task *) R_alloc(n, sizeof(*ts));
  sora_rs_hdr **rss = (sora_rs_hdr **) R_alloc(n, sizeof(sora_rs_hdr *));
  for (R_xlen_t i = 0; i < n; i++) {
    sora_pool *pi;
    SEXP pool_xpi;
    ts[i] = task_get(VECTOR_ELT(tasks, i), &pi, &pool_xpi);
    if (pi->sub_slot < 0)
      Rf_error("sora: not a submitter's task handle");
    if (p == NULL) {
      p = pi;
      pool_xp = pool_xpi;
    } else if (pi != p) {
      Rf_error("sora: task handles must belong to the same pool handle");
    }
    sora_rs_hdr *rs = pool_rs(pi, ts[i].idx);
    if (!sora_task_seq_match(&rs->sequence, ts[i].seq))
      Rf_error("sora: task handle already collected or invalidated");
    rss[i] = rs;
  }

  double deadline = -1;
  double t_wait = -1;
  R_xlen_t found = -1;
  int32_t st = SORA_RS_PENDING;

  for (;;) {
    if (atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0)
      pool_orphan_teardown_try(p);   /* its cancel sweep ends this wait */
    if (pool_any_terminal(rss, n, &found, &st)) break;

    /* Help mode, as in collect: a worker blocked on its own subtree must
       make progress on runnable work, not park. */
    if (p->role == SORA_ROLE_WORKER && p->wk_slot >= 0) {
      int got = pool_deque_pop(p);
      if (!got)
        got = pool_steal_any(p, p->help_depth >= SORA_HELP_DEPTH_LIMIT);
      if (got) {
        p->st_helps++;
        p->help_depth++;
        pool_execute(p, pool_xp, 1);
        p->help_depth--;
        continue;
      }
    }

    if (timeout_s <= 0) return sora_sent_timeout;

    /* hoisted above the spin so the first episode's `until` is clamped
       too — a finite timeout's clock covers the whole wait. One read
       serves the deadline compute and the bound. */
    double now = sora_now();
    if (t_wait < 0) t_wait = now;
    if (deadline < 0 && R_FINITE(timeout_s))
      deadline = now + timeout_s;

    /* time-boxed spin before the park announce, as in collect: a fast
       result caught here costs neither side a syscall, waiter_slot
       staying unannounced through the spin. The predicate is O(n), so
       the light stride applies only to the common small-n case */
    uint64_t budget = p->collect_budget_ns;
    double until = now + (double) budget / 1e9;
    if (deadline >= 0 && deadline < until) until = deadline;
    int caught;
    SORA_SPIN_WAIT(pool_any_terminal(rss, n, &found, &st), until,
                   n <= 4 ? SORA_SPIN_CLOCK_EVERY_LIGHT :
                   SORA_SPIN_CLOCK_EVERY, caught);
    if (caught) {
      pool_collect_learn(p, t_wait, budget);
      break;
    }

    /* announce on every slot -> fence -> re-check -> park bounded; each
       publishing worker reads its slot's waiter_slot after the publish
       CAS and unparks this submitter's one parker */
    uint32_t e = sora_parker_snapshot(pool_sub_pk(p, (uint32_t) p->sub_slot));
    for (R_xlen_t i = 0; i < n; i++)
      atomic_store_explicit(&rss[i]->waiter_slot, p->sub_slot,
                            memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    if (pool_any_terminal(rss, n, &found, &st)) {
      pool_collect_learn(p, t_wait, budget);
      break;
    }
    long ms = SORA_INTERRUPT_BOUND_MS;
    if (deadline >= 0) {
      double rem = deadline - sora_now();
      if (rem <= 0) {
        pool_any_unannounce(rss, n);
        return sora_sent_timeout;
      }
      long rem_ms = (long) (rem * 1000) + 1;
      if (rem_ms < ms) ms = rem_ms;
    }
    sora_park_bracket(&p->binding, 1);
    sora_park(pool_sub_pk(p, (uint32_t) p->sub_slot), e, ms);
    sora_park_bracket(&p->binding, 0);
    p->st_collect_parks++;
    sora_check_interrupt(&p->binding);
    if (pool_any_terminal(rss, n, &found, &st)) {
      pool_collect_learn(p, t_wait, budget);
      break;
    }
    /* backstop for a missed death notification, piggybacked on a wake
       that happened regardless — never a wakeup of its own */
    for (R_xlen_t i = 0; i < n; i++) {
      int32_t claimant = atomic_load_explicit(&rss[i]->worker_slot,
                                              memory_order_acquire);
      if (claimant >= 0 && (uint32_t) claimant < p->hdr.max_workers)
        pool_probe_worker(p, (uint32_t) claimant);
    }
    if (deadline >= 0 && sora_now() >= deadline &&
        !pool_any_terminal(rss, n, &found, &st)) {
      pool_any_unannounce(rss, n);
      return sora_sent_timeout;
    }
  }

  pool_any_unannounce(rss, n);
  int index = (int) found + 1;   /* R 1-based */
  sora_rs_hdr *rs = rss[found];

  switch (st) {
  case SORA_RS_OK:
  case SORA_RS_ERR: {
    SEXP v = PROTECT(pool_rs_claim(p, pool_xp, rs, ts[found].idx, st));
    if (st == SORA_RS_ERR) {
      SEXP out = PROTECT(sora_caught(v));
      Rf_setAttrib(out, sora_index_sym, Rf_ScalarInteger(index));
      UNPROTECT(2);
      return out;
    }
    SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, 2));
    SET_STRING_ELT(names, 0, Rf_mkChar("index"));
    SET_STRING_ELT(names, 1, Rf_mkChar("value"));
    Rf_setAttrib(out, R_NamesSymbol, names);
    SET_VECTOR_ELT(out, 0, Rf_ScalarInteger(index));
    SET_VECTOR_ELT(out, 1, v);
    UNPROTECT(3);
    return out;
  }
  case SORA_RS_DIED: {
    int32_t w;
    double wpid;
    pool_rs_claim_died(p, pool_xp, rs, ts[found].idx, &w, &wpid);
    SEXP out = PROTECT(sora_caught_died((int) w, wpid,
                                       "sora: worker died while executing "
                                       "this task"));
    Rf_setAttrib(out, sora_index_sym, Rf_ScalarInteger(index));
    UNPROTECT(1);
    return out;
  }
  case SORA_RS_CANCEL: {
    /* the task keeper releases at slot reuse, not here — the worker may
       not have materialized yet */
    SEXP out = PROTECT(sora_caught_cond("sora_error_cancelled",
                                       "sora: task cancelled or pool "
                                       "stopped"));
    Rf_setAttrib(out, sora_index_sym, Rf_ScalarInteger(index));
    UNPROTECT(1);
    return out;
  }
  case SORA_RS_FREE:
  default:
    Rf_error("sora: task handle already collected");
  }
}

/* The collect-all predicate: every slot terminal (OK, ERR, DIED, or
   CANCEL — a cancelled task is "done", as in collect_any). */
static int pool_all_terminal(sora_rs_hdr **rss, R_xlen_t n) {
  for (R_xlen_t i = 0; i < n; i++) {
    if (atomic_load_explicit(&rss[i]->status, memory_order_acquire) ==
        SORA_RS_PENDING)
      return 0;
  }
  return 1;
}

/* Wait on all of a submitter's outstanding tasks, on the collect_any
   mechanics with an all-terminal predicate: announce on every pending
   slot, park once on the submitter's one parker, scan on wake. While
   parked the waiter is announced on every pending slot, so each
   publishing worker unparks it — K slow completions cost K wakes x O(N)
   scans, still well under the per-collect loop the verb replaces. One
   overall deadline; a timeout claims nothing, so no completed result is
   silently discarded. On success the claim runs in list order and the
   first ERR/DIED/CANCEL by position reports sora_caught-boxed with its
   1-based index: slots before it are claimed, slots after it stay
   collectible. */
SEXP sora_pool_collect_all(SEXP tasks, SEXP timeout) {
  if (TYPEOF(tasks) != VECSXP || XLENGTH(tasks) == 0)
    Rf_error("sora: tasks must be a non-empty list of task handles");
  double timeout_s = Rf_asReal(timeout);
  R_xlen_t n = XLENGTH(tasks);
  sora_pool *p = NULL;
  SEXP pool_xp = R_NilValue;
  sora_task *ts = (sora_task *) R_alloc(n, sizeof(*ts));
  sora_rs_hdr **rss = (sora_rs_hdr **) R_alloc(n, sizeof(sora_rs_hdr *));
  for (R_xlen_t i = 0; i < n; i++) {
    sora_pool *pi;
    SEXP pool_xpi;
    ts[i] = task_get(VECTOR_ELT(tasks, i), &pi, &pool_xpi);
    if (pi->sub_slot < 0)
      Rf_error("sora: not a submitter's task handle");
    if (p == NULL) {
      p = pi;
      pool_xp = pool_xpi;
    } else if (pi != p) {
      Rf_error("sora: task handles must belong to the same pool handle");
    }
    sora_rs_hdr *rs = pool_rs(pi, ts[i].idx);
    if (!sora_task_seq_match(&rs->sequence, ts[i].seq))
      Rf_error("sora: task handle already collected or invalidated");
    rss[i] = rs;
  }

  double deadline = -1;
  double t_wait = -1;

  for (;;) {
    if (atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0)
      pool_orphan_teardown_try(p);   /* its cancel sweep ends this wait */
    if (pool_all_terminal(rss, n)) break;

    /* Help mode, as in collect: a worker blocked on its own subtree must
       make progress on runnable work, not park. */
    if (p->role == SORA_ROLE_WORKER && p->wk_slot >= 0) {
      int got = pool_deque_pop(p);
      if (!got)
        got = pool_steal_any(p, p->help_depth >= SORA_HELP_DEPTH_LIMIT);
      if (got) {
        p->st_helps++;
        p->help_depth++;
        pool_execute(p, pool_xp, 1);
        p->help_depth--;
        continue;
      }
    }

    if (timeout_s <= 0) return sora_sent_timeout;

    /* hoisted above the spin so the first episode's `until` is clamped
       too — a finite timeout's clock covers the whole wait. One read
       serves the deadline compute and the bound. */
    double now = sora_now();
    if (t_wait < 0) t_wait = now;
    if (deadline < 0 && R_FINITE(timeout_s))
      deadline = now + timeout_s;

    /* time-boxed spin before the park announce, as in collect_any: a
       fast completion caught here costs neither side a syscall,
       waiter_slot staying unannounced through the spin */
    uint64_t budget = p->collect_budget_ns;
    double until = now + (double) budget / 1e9;
    if (deadline >= 0 && deadline < until) until = deadline;
    int caught;
    SORA_SPIN_WAIT(pool_all_terminal(rss, n), until,
                   n <= 4 ? SORA_SPIN_CLOCK_EVERY_LIGHT :
                   SORA_SPIN_CLOCK_EVERY, caught);
    if (caught) {
      pool_collect_learn(p, t_wait, budget);
      break;
    }

    /* announce on every pending slot -> fence -> re-check -> park
       bounded; each publishing worker reads its slot's waiter_slot after
       the publish CAS and unparks this submitter's one parker */
    uint32_t e = sora_parker_snapshot(pool_sub_pk(p, (uint32_t) p->sub_slot));
    for (R_xlen_t i = 0; i < n; i++)
      if (atomic_load_explicit(&rss[i]->status, memory_order_acquire) ==
          SORA_RS_PENDING)
        atomic_store_explicit(&rss[i]->waiter_slot, p->sub_slot,
                              memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    if (pool_all_terminal(rss, n)) {
      pool_collect_learn(p, t_wait, budget);
      break;
    }
    long ms = SORA_INTERRUPT_BOUND_MS;
    if (deadline >= 0) {
      double rem = deadline - sora_now();
      if (rem <= 0) {
        pool_any_unannounce(rss, n);
        return sora_sent_timeout;
      }
      long rem_ms = (long) (rem * 1000) + 1;
      if (rem_ms < ms) ms = rem_ms;
    }
    sora_park_bracket(&p->binding, 1);
    sora_park(pool_sub_pk(p, (uint32_t) p->sub_slot), e, ms);
    sora_park_bracket(&p->binding, 0);
    p->st_collect_parks++;
    sora_check_interrupt(&p->binding);
    if (pool_all_terminal(rss, n)) {
      pool_collect_learn(p, t_wait, budget);
      break;
    }
    /* backstop for a missed death notification, piggybacked on a wake
       that happened regardless — never a wakeup of its own */
    for (R_xlen_t i = 0; i < n; i++) {
      int32_t claimant = atomic_load_explicit(&rss[i]->worker_slot,
                                              memory_order_acquire);
      if (claimant >= 0 && (uint32_t) claimant < p->hdr.max_workers)
        pool_probe_worker(p, (uint32_t) claimant);
    }
    if (deadline >= 0 && sora_now() >= deadline &&
        !pool_all_terminal(rss, n)) {
      pool_any_unannounce(rss, n);
      return sora_sent_timeout;
    }
  }

  pool_any_unannounce(rss, n);

  /* every slot terminal: claim in list order */
  SEXP out = PROTECT(Rf_allocVector(VECSXP, n));
  SEXP nms = Rf_getAttrib(tasks, R_NamesSymbol);
  if (nms != R_NilValue)
    Rf_setAttrib(out, R_NamesSymbol, nms);
  for (R_xlen_t i = 0; i < n; i++) {
    int32_t st = atomic_load_explicit(&rss[i]->status, memory_order_acquire);
    int index = (int) i + 1;   /* R 1-based */
    switch (st) {
    case SORA_RS_OK:
      SET_VECTOR_ELT(out, i,
                     pool_rs_claim(p, pool_xp, rss[i], ts[i].idx, st));
      break;
    case SORA_RS_ERR: {
      SEXP v = PROTECT(pool_rs_claim(p, pool_xp, rss[i], ts[i].idx, st));
      SEXP caught = PROTECT(sora_caught(v));
      Rf_setAttrib(caught, sora_index_sym, Rf_ScalarInteger(index));
      UNPROTECT(3);
      return caught;
    }
    case SORA_RS_DIED: {
      int32_t w;
      double wpid;
      pool_rs_claim_died(p, pool_xp, rss[i], ts[i].idx, &w, &wpid);
      SEXP caught = PROTECT(sora_caught_died((int) w, wpid,
                                            "sora: worker died while "
                                            "executing this task"));
      Rf_setAttrib(caught, sora_index_sym, Rf_ScalarInteger(index));
      UNPROTECT(2);
      return caught;
    }
    case SORA_RS_CANCEL: {
      /* the task keeper releases at slot reuse, not here — the worker may
         not have materialized yet */
      SEXP caught = PROTECT(sora_caught_cond("sora_error_cancelled",
                                            "sora: task cancelled or pool "
                                            "stopped"));
      Rf_setAttrib(caught, sora_index_sym, Rf_ScalarInteger(index));
      UNPROTECT(2);
      return caught;
    }
    case SORA_RS_FREE:
    default:
      Rf_error("sora: task handle already collected");
    }
  }
  UNPROTECT(1);
  return out;
}

SEXP sora_pool_collect(SEXP xp, SEXP timeout) {
  return pool_collect(xp, Rf_asReal(timeout), 0);
}

/* absolute deadline, as sora_pool_submit_try */
SEXP sora_pool_collect_try(SEXP xp, SEXP deadline) {
  double d = Rf_asReal(deadline);
  return pool_collect(xp, R_FINITE(d) ? d - sora_now() : R_PosInf, 1);
}

/* Advisory and discard-only, never preemptive: a task already executing runs
   to completion and its result is dropped by the worker's failed publish
   CAS. Total past the tag check — cancel runs from unwind paths
   (map_cancel under on.exit), so a stale handle, a closed pool, or a
   forked child answers FALSE instead of raising. */
SEXP sora_pool_cancel(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != sora_task_tag)
    Rf_error("sora: not a task handle");
  void *addr = R_ExternalPtrAddr(xp);
  if (addr == NULL) return Rf_ScalarLogical(FALSE);
  sora_task t = sora_task_unpack(addr);
  SEXP pool_xp = R_ExternalPtrProtected(xp);
  sora_pool *p = (sora_pool *) R_ExternalPtrAddr(pool_xp);
  if (p == NULL || p->released || p->self_pid != sora_self_pid())
    return Rf_ScalarLogical(FALSE);
  sora_rs_hdr *rs = pool_rs(p, t.idx);
  if (!sora_task_seq_match(&rs->sequence, t.seq))
    return Rf_ScalarLogical(FALSE);
  int32_t expected = SORA_RS_PENDING;
  if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                              SORA_RS_CANCEL,
                                              memory_order_seq_cst,
                                              memory_order_relaxed)) {
    pool_unpark_result_waiter(p, rs);
    return Rf_ScalarLogical(TRUE);
  }
  return Rf_ScalarLogical(FALSE);
}

/* Non-consuming state probe for the print method: two single reads, racy
   against slot reuse exactly as sora_pool_dump is. Collect leaves the handle
   intact and moves the slot on, so FREE (or an advanced sequence) reads as
   collected; a released pool means the task can never be collected. Total
   for every real handle — print must not error. */
SEXP sora_pool_task_state(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != sora_task_tag)
    Rf_error("sora: not a task handle");
  void *addr = R_ExternalPtrAddr(xp);
  sora_pool *p = (sora_pool *) R_ExternalPtrAddr(R_ExternalPtrProtected(xp));
  if (addr == NULL || p == NULL || p->released || p->base == NULL ||
      p->self_pid != sora_self_pid())
    return Rf_mkString("dropped");
  sora_task t = sora_task_unpack(addr);
  sora_rs_hdr *rs = pool_rs(p, t.idx);
  if (!sora_task_seq_match(&rs->sequence, t.seq))
    return Rf_mkString("collected");
  switch (atomic_load_explicit(&rs->status, memory_order_acquire)) {
  case SORA_RS_PENDING: return Rf_mkString("pending");
  case SORA_RS_OK:      return Rf_mkString("ok");
  case SORA_RS_ERR:     return Rf_mkString("err");
  case SORA_RS_CANCEL:  return Rf_mkString("cancel");
  case SORA_RS_DIED:    return Rf_mkString("died");
  default:             return Rf_mkString("collected");   /* FREE */
  }
}

// sora_map support -----------------------------------------------------------------------------

/* The inputs to sora_map's chunk-count formula, in one read pass: live
   workers, FREE result slots in the caller's own subrange (not rs_count —
   the subrange is shared with whatever tasks are already outstanding, and
   pool_alloc_rs would otherwise error mid-submit), the injection cap, and
   the entry inline budget (the region-less probe's bound). Only this
   process allocates from its own subrange, so the FREE count can only
   grow under it. A worker's first nested map claims its submitter slot
   here — before the count, which would otherwise read an unclaimed
   subrange — exactly as nested submit does. */
SEXP sora_pool_map_caps(SEXP xp) {
  sora_pool *p = pool_get(xp);
  if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0)
    sora_stop("sora_error_stopped", "sora: pool stopped");
  if (p->role == SORA_ROLE_WORKER && p->wk_slot >= 0 && p->sub_slot < 0) {
    pool_claim_sub_slot(p, &p->live_sub);
    SEXP prot = R_ExternalPtrProtected(xp);
    SET_VECTOR_ELT(prot, 5,
                   Rf_allocVector(VECSXP,
                                  (R_xlen_t) p->sub[p->sub_slot].rs_count));
    p->sub_pins = VECTOR_ELT(prot, 5);
    p->sub_keepers = calloc((size_t) p->sub[p->sub_slot].rs_count,
                            sizeof(sora_keeper));
    if (p->sub_keepers == NULL) Rf_error("sora: allocation failure");
    p->sub_keepers_n = p->sub[p->sub_slot].rs_count;
  }
  if (p->sub_slot < 0)
    Rf_error("sora: not a submitter handle");
  int live = 0;
  for (uint32_t i = 0; i < p->hdr.max_workers; i++)
    live += atomic_load_explicit(&p->wk[i].status, memory_order_acquire) ==
      SORA_WK_LIVE;
  sora_sub_slot *me = &p->sub[p->sub_slot];
  int free_rs = 0;
  for (uint32_t k = 0; k < me->rs_count; k++)
    free_rs += atomic_load_explicit(&pool_rs(p, me->rs_start + k)->status,
                                    memory_order_acquire) == SORA_RS_FREE;
  SEXP out = Rf_allocVector(INTSXP, 4);
  INTEGER(out)[0] = live;
  INTEGER(out)[1] = free_rs;
  INTEGER(out)[2] = (int) p->hdr.inj_cap;
  INTEGER(out)[3] = (int) p->inline_entry;
  return out;
}

/* The pool-signal handle a map runner threads through sora_map_next: three
   opaque word addresses, loaded relaxed once per batch transition. The
   extptr protects the worker's pool handle, so the mapping and the
   process-local struct both outlive it. */
static void pool_sig_finalizer(SEXP xp) {
  free(R_ExternalPtrAddr(xp));
  R_ClearExternalPtr(xp);
}

SEXP sora_pool_signals(SEXP xp) {
  sora_pool *p = pool_get(xp);
  sora_pool_sig *s = calloc(1, sizeof(*s));
  if (s == NULL) Rf_error("sora: allocation failure");
  s->help_wanted = p->help_wanted;
  s->shutdown = p->shutdown;
  s->owner_dead = &p->owner_dead;
  SEXP sig = PROTECT(R_MakeExternalPtr(s, sora_sig_tag, xp));
  R_RegisterCFinalizerEx(sig, pool_sig_finalizer, TRUE);
  UNPROTECT(1);
  return sig;
}

sora_pool_sig *sora_pool_sig_get(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != sora_sig_tag)
    Rf_error("sora: not a pool-signal handle");
  sora_pool_sig *s = (sora_pool_sig *) R_ExternalPtrAddr(xp);
  if (s == NULL) Rf_error("sora: pool-signal handle is closed");
  return s;
}

/* One doorbell-gated help beat at a runner's batch boundary: clear the
   doorbell, claim one injection entry (unfiltered scan — the bell rang, so
   a stale ready mask must not hide the task it rang for), then re-check
   the rings and restore the doorbell if entries remain — the clear ->
   re-check -> restore discipline pool_claim_rings applies to inj_ready,
   without which a second submitter's store racing the clear is eaten with
   it and that task waits until the map ends. Injection-only, the mirror
   image of collect's help mode (which excludes injection): a blocked
   collect helps to unblock its own subtree, a runner helps precisely to
   hand foreign submitters their chunk-boundary interleave back.

   An ordinary claim executes inline under the help-mode machinery
   (help_depth bounds the recursion). A runner-flagged claim must not: a
   map's runners are its join tickets, and a helper nested inside its own
   cursor drain adds zero parallelism while consuming one — left alone it
   swallows the whole runner set within microseconds (the bell restore
   re-arms it each beat) and silently serializes the map. The claim itself
   stays unfiltered — the ring is SPSC FIFO, so refusing a runner would
   block ordinary tasks queued behind it — but the runner is re-homed onto
   this worker's own deque instead, where an idle peer is woken (bell-less:
   deque work is help-unreachable) or steals it at its next scan, and the
   owner's own pop after its current runner bounds the worst case at
   today's serialization. The flag is read only after the winning head CAS
   (a loser may copy a torn header; a winner's is coherent by the rs_index
   argument), and the announce is cleared *before* the deque push: the
   reverse order would leave the entry both announced and deque-published,
   so a death in that window fires both recovery paths and the survivor's
   late publish can land in a recommitted slot. Clear-first shrinks the
   window to a lost entry, which is benign for runners only — the lane
   never claims the cursor, so sora_map_abandon trims it — which is why the
   flag stays runner-only. A full deque (reachable at deque_cap = 2) falls
   back to today's inline execute, announce intact. */
SEXP sora_pool_help_once(SEXP xp) {
  sora_pool *p = pool_get(xp);
  if (p->role != SORA_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("sora: not a worker handle");
  int got = 0;
  if (p->help_depth < SORA_HELP_DEPTH_LIMIT) {
    atomic_store_explicit(p->help_wanted, 0u, memory_order_seq_cst);
    got = pool_claim_rings(p, 0);
    if (got) {
      sora_entry_hdr *eh = (sora_entry_hdr *) p->scratch;
      sora_wk_slot *me = &p->wk[p->wk_slot];
      int64_t b = atomic_load_explicit(&me->deque_bottom,
                                       memory_order_relaxed);
      int64_t t = atomic_load_explicit(&me->deque_top,
                                       memory_order_acquire);
      if ((eh->flags & SORA_ENTRY_RUNNER) &&
          b - t < (int64_t) me->deque_cap) {
        /* the space pre-check cannot be invalidated — only the owner
           pushes to its own deque, thieves only free space — so the push
           below cannot fail */
        uint64_t tid = eh->task_id;
        pool_announce_clear(p);
        pool_deque_push(p, p->scratch);
        if (b <= t) pool_unpark_one_worker(p);   /* empty -> non-empty */
        pool_trace_emit(xp, "rehome", tid);
      } else {
        p->st_helps++;                 /* helps = executed foreign work */
        p->help_depth++;
        pool_execute(p, xp, 1);
        p->help_depth--;
      }
    }
    for (uint32_t s = 0; s < p->hdr.max_submitters; s++) {
      unsigned char *ring = pool_ring(p, s);
      if (atomic_load_explicit(ring_head(ring), memory_order_acquire) <
          atomic_load_explicit(ring_tail(ring), memory_order_acquire)) {
        atomic_store_explicit(p->help_wanted, 1u, memory_order_seq_cst);
        break;
      }
    }
  }
  return Rf_ScalarLogical(got);
}

/* The worker's map-context cache env (prot[5]), created lazily so pools
   that never map spend nothing on it. The idle sweep clears the slot back
   to NULL; the R side re-creates through here and bounds residency at ~8
   contexts with a clear-all. */
SEXP sora_pool_map_cache(SEXP xp) {
  sora_pool *p = pool_get(xp);
  if (p->role != SORA_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("sora: not a worker handle");
  SEXP prot = R_ExternalPtrProtected(xp);
  SEXP cache = VECTOR_ELT(prot, 3);
  if (TYPEOF(cache) != ENVSXP) {
    cache = R_NewEnv(R_EmptyEnv, 0, 0);
    SET_VECTOR_ELT(prot, 3, cache);
  }
  return cache;
}

/* Test / debug surface: c(free-list entries, lent-ledger entries). */
SEXP sora_pool_zc_info(SEXP xp) {
  sora_pool *p = pool_get(xp);
  return sora_zc_fl_info(&p->fl);
}

// Stop and introspection ---------------------------------------------------------------------

SEXP sora_pool_stop_call(SEXP xp, SEXP timeout) {
  sora_pool *p = pool_peek(xp);
  if (p == NULL) return Rf_ScalarLogical(TRUE);   /* stop is idempotent */
  if (p->role != SORA_ROLE_CONTROLLER)
    Rf_error("sora: only the controller can stop a pool");

  pool_shutdown_broadcast(p);

  /* wait for workers to take their clean-exit path; their liveness locks
     release with their fds either way */
  double deadline = sora_now() + Rf_asReal(timeout);
  int clean;
  for (;;) {
    clean = 1;
    for (uint32_t i = 0; i < p->hdr.max_workers; i++) {
      int32_t st = atomic_load_explicit(&p->wk[i].status,
                                        memory_order_acquire);
      if (st == SORA_WK_LIVE || st == SORA_WK_LEAVING || st == SORA_WK_CLAIMING)
        clean = 0;
    }
    if (clean || sora_now() >= deadline) break;
    for (uint32_t i = 0; i < p->hdr.max_workers; i++)
      sora_unpark(pool_wk_pk(p, i));
    uint32_t e = sora_parker_snapshot(pool_sub_pk(p, 0));
    sora_park_bracket(&p->binding, 1);
    sora_park(pool_sub_pk(p, 0), e, 50);
    sora_park_bracket(&p->binding, 0);
    sora_check_interrupt(&p->binding);
  }

  /* teardown sweep: probe + reap whatever did not exit cleanly — dead
     workers (their in-flight tasks fail, their deques orphan) and dead or
     detached submitters (their result slots release). A live hung worker
     stays unreaped, correctly: the lock adjudicates exit, not stall. */
  for (uint32_t i = 0; i < p->hdr.max_workers; i++)
    pool_probe_worker(p, i);
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++)
    pool_probe_submitter(p, j);

  pool_unlink_names(p);
  pool_release(p);
  return Rf_ScalarLogical(clean);
}

SEXP sora_pool_status_call(SEXP xp) {
  sora_pool *p = pool_get(xp);
  const char *names[] = {"name", "role", "max_workers", "max_submitters",
                         "injection_cap", "result_slots", "slot_size",
                         "workers", "parked", "submitters", "injection",
                         "tasks", "deque", "shutdown", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(p->shm.name));
  SET_VECTOR_ELT(out, 1, Rf_mkString(
    p->role == SORA_ROLE_CONTROLLER ? "controller" :
    p->role == SORA_ROLE_WORKER ? "worker" : "submitter"));
  SET_VECTOR_ELT(out, 2, Rf_ScalarInteger((int) p->hdr.max_workers));
  SET_VECTOR_ELT(out, 3, Rf_ScalarInteger((int) p->hdr.max_submitters));
  SET_VECTOR_ELT(out, 4, Rf_ScalarInteger((int) p->hdr.inj_cap));
  SET_VECTOR_ELT(out, 5, Rf_ScalarInteger((int) p->hdr.result_slots));
  SET_VECTOR_ELT(out, 6, Rf_ScalarInteger((int) p->hdr.slot));

  SEXP wk = Rf_allocVector(INTSXP, (R_xlen_t) p->hdr.max_workers);
  SET_VECTOR_ELT(out, 7, wk);
  for (uint32_t i = 0; i < p->hdr.max_workers; i++)
    INTEGER(wk)[i] = (int) atomic_load_explicit(&p->wk[i].status,
                                                memory_order_acquire);
  uint64_t parked = atomic_load_explicit(p->parked_workers,
                                         memory_order_acquire);
  int nparked = 0;
  for (uint32_t i = 0; i < 64; i++) nparked += (parked >> i) & 1;
  SET_VECTOR_ELT(out, 8, Rf_ScalarInteger(nparked));

  SEXP sub = Rf_allocVector(INTSXP, (R_xlen_t) p->hdr.max_submitters);
  SET_VECTOR_ELT(out, 9, sub);
  double queued = 0;
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++) {
    INTEGER(sub)[j] = (int) atomic_load_explicit(&p->sub[j].status,
                                                 memory_order_acquire);
    unsigned char *ring = pool_ring(p, j);
    queued += (double)
      (atomic_load_explicit(ring_tail(ring), memory_order_acquire) -
       atomic_load_explicit(ring_head(ring), memory_order_acquire));
  }
  SET_VECTOR_ELT(out, 10, Rf_ScalarReal(queued));

  SEXP tasks = Rf_allocVector(INTSXP, 5);   /* pending ok err cancel died */
  SET_VECTOR_ELT(out, 11, tasks);
  memset(INTEGER(tasks), 0, 5 * sizeof(int));
  for (uint32_t r = 0; r < p->hdr.result_slots; r++) {
    int32_t st = atomic_load_explicit(&pool_rs(p, r)->status,
                                      memory_order_acquire);
    if (st >= SORA_RS_PENDING && st <= SORA_RS_DIED)
      INTEGER(tasks)[st - SORA_RS_PENDING]++;
  }
  SEXP dq = Rf_allocVector(REALSXP, (R_xlen_t) p->hdr.max_workers);
  SET_VECTOR_ELT(out, 12, dq);
  for (uint32_t i = 0; i < p->hdr.max_workers; i++) {
    int64_t d =
      atomic_load_explicit(&p->wk[i].deque_bottom, memory_order_acquire) -
      atomic_load_explicit(&p->wk[i].deque_top, memory_order_acquire);
    REAL(dq)[i] = d > 0 ? (double) d : 0;   /* a pop transiently reads -1 */
  }

  SET_VECTOR_ELT(out, 13, Rf_ScalarLogical(
    (int) atomic_load_explicit(p->shutdown, memory_order_acquire)));
  UNPROTECT(1);
  return out;
}

/* Cumulative counters, read-only. Per-worker rows mirror the slots'
   stat_* fields (see the sora_wk_slot comment for their publish cadence);
   per-submitter injection totals are the ring positions themselves —
   tail = entries ever published, head = entries ever claimed, both
   monotonic from zero — so the wire state is the metric and the submit
   path writes nothing extra. */
SEXP sora_pool_stats_call(SEXP xp) {
  sora_pool *p = pool_get(xp);
  uint32_t mw = p->hdr.max_workers, ms = p->hdr.max_submitters;
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
    sora_wk_slot *w = &p->wk[i];
    INTEGER(VECTOR_ELT(wk, 0))[i] =
      (int) atomic_load_explicit(&w->status, memory_order_acquire);
    REAL(VECTOR_ELT(wk, 1))[i] = (double) w->pid;
    REAL(VECTOR_ELT(wk, 2))[i] = (double)
      atomic_load_explicit(&w->stat_tasks, memory_order_relaxed);
    REAL(VECTOR_ELT(wk, 3))[i] = (double)
      atomic_load_explicit(&w->stat_steals, memory_order_relaxed);
    REAL(VECTOR_ELT(wk, 4))[i] = (double)
      atomic_load_explicit(&w->stat_inj, memory_order_relaxed);
    REAL(VECTOR_ELT(wk, 5))[i] = (double)
      atomic_load_explicit(&w->stat_parks, memory_order_relaxed);
    REAL(VECTOR_ELT(wk, 6))[i] = (double)
      atomic_load_explicit(&w->stat_helps, memory_order_relaxed);
    int64_t d =
      atomic_load_explicit(&w->deque_bottom, memory_order_acquire) -
      atomic_load_explicit(&w->deque_top, memory_order_acquire);
    REAL(VECTOR_ELT(wk, 7))[i] = d > 0 ? (double) d : 0;
  }

  const char *snames[] = {"status", "pid", "injected", "claimed", "spills",
                          "spill_reuse", ""};
  SEXP sb = Rf_mkNamed(VECSXP, snames);
  SET_VECTOR_ELT(out, 1, sb);
  SET_VECTOR_ELT(sb, 0, Rf_allocVector(INTSXP, (R_xlen_t) ms));
  for (int f = 1; f < 6; f++)
    SET_VECTOR_ELT(sb, f, Rf_allocVector(REALSXP, (R_xlen_t) ms));
  for (uint32_t j = 0; j < ms; j++) {
    unsigned char *ring = pool_ring(p, j);
    INTEGER(VECTOR_ELT(sb, 0))[j] =
      (int) atomic_load_explicit(&p->sub[j].status, memory_order_acquire);
    REAL(VECTOR_ELT(sb, 1))[j] = (double) p->sub[j].pid;
    REAL(VECTOR_ELT(sb, 2))[j] = (double)
      atomic_load_explicit(ring_tail(ring), memory_order_acquire);
    REAL(VECTOR_ELT(sb, 3))[j] = (double)
      atomic_load_explicit(ring_head(ring), memory_order_acquire);
    REAL(VECTOR_ELT(sb, 4))[j] = (double)
      atomic_load_explicit(&p->sub[j].stat_spills, memory_order_relaxed);
    REAL(VECTOR_ELT(sb, 5))[j] = (double)
      atomic_load_explicit(&p->sub[j].stat_spill_reuse,
                           memory_order_relaxed);
  }
  UNPROTECT(1);
  return out;
}

/* Read-only region snapshot for debugging distributed state: per-slot
   registry detail, the three hot masks unpacked per slot, and every
   non-FREE result slot. States can move between the count pass and the
   fill pass; short rows are padded with NA and trimmed on the R side. */
SEXP sora_pool_dump_call(SEXP xp) {
  sora_pool *p = pool_get(xp);
  uint32_t mw = p->hdr.max_workers, ms = p->hdr.max_submitters;
  const char *names[] = {"name", "shutdown", "workers", "submitters",
                         "tasks", "local", "help", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(p->shm.name));
  SET_VECTOR_ELT(out, 1, Rf_ScalarLogical(
    (int) atomic_load_explicit(p->shutdown, memory_order_acquire)));
  SET_VECTOR_ELT(out, 6, Rf_ScalarLogical(
    (int) atomic_load_explicit(p->help_wanted, memory_order_acquire)));

  /* the spill free list, consumer mapping cache, and collect-side park
     count are handle-local, so their counters surface here rather than
     in the cross-process stats (st_parks covers only worker parks) */
  const char *lnames[] = {"fl_entries", "fl_bytes", "fl_hits", "open_hits",
                          "open_misses", "collect_parks", ""};
  SEXP lo = Rf_mkNamed(VECSXP, lnames);
  SET_VECTOR_ELT(out, 5, lo);
  SET_VECTOR_ELT(lo, 0, Rf_ScalarInteger((int) p->fl.n));
  SET_VECTOR_ELT(lo, 1, Rf_ScalarReal((double) p->fl.total));
  SET_VECTOR_ELT(lo, 2, Rf_ScalarReal((double) p->fl.hits));
  SET_VECTOR_ELT(lo, 3, Rf_ScalarReal((double) p->oc.hits));
  SET_VECTOR_ELT(lo, 4, Rf_ScalarReal((double) p->oc.misses));
  SET_VECTOR_ELT(lo, 5, Rf_ScalarReal((double) p->st_collect_parks));

  const char *wnames[] = {"status", "pid", "park_state", "parked", "top",
                          "bottom", "in_flight", ""};
  SEXP wk = Rf_mkNamed(VECSXP, wnames);
  SET_VECTOR_ELT(out, 2, wk);
  for (int f = 0; f < 7; f++)
    SET_VECTOR_ELT(wk, f, Rf_allocVector(
      f == 1 || f == 4 || f == 5 ? REALSXP :
      f == 3 ? LGLSXP : INTSXP, (R_xlen_t) mw));
  uint64_t parked = atomic_load_explicit(p->parked_workers,
                                         memory_order_acquire);
  for (uint32_t i = 0; i < mw; i++) {
    sora_wk_slot *w = &p->wk[i];
    INTEGER(VECTOR_ELT(wk, 0))[i] =
      (int) atomic_load_explicit(&w->status, memory_order_acquire);
    REAL(VECTOR_ELT(wk, 1))[i] = (double) w->pid;
    INTEGER(VECTOR_ELT(wk, 2))[i] =
      (int) atomic_load_explicit(&w->park_state, memory_order_acquire);
    LOGICAL(VECTOR_ELT(wk, 3))[i] = (parked >> i) & 1;
    REAL(VECTOR_ELT(wk, 4))[i] = (double)
      atomic_load_explicit(&w->deque_top, memory_order_acquire);
    REAL(VECTOR_ELT(wk, 5))[i] = (double)
      atomic_load_explicit(&w->deque_bottom, memory_order_acquire);
    INTEGER(VECTOR_ELT(wk, 6))[i] =
      (int) atomic_load_explicit(&w->in_flight_rs, memory_order_acquire);
  }

  const char *snames[] = {"status", "pid", "rs_start", "rs_count", "queued",
                          "ready", "full_waiter", ""};
  SEXP sb = Rf_mkNamed(VECSXP, snames);
  SET_VECTOR_ELT(out, 3, sb);
  for (int f = 0; f < 7; f++)
    SET_VECTOR_ELT(sb, f, Rf_allocVector(
      f == 1 || f == 4 ? REALSXP :
      f == 5 || f == 6 ? LGLSXP : INTSXP, (R_xlen_t) ms));
  uint64_t ready = atomic_load_explicit(p->inj_ready, memory_order_acquire);
  uint64_t full = atomic_load_explicit(p->full_waiters,
                                       memory_order_acquire);
  for (uint32_t j = 0; j < ms; j++) {
    sora_sub_slot *s = &p->sub[j];
    unsigned char *ring = pool_ring(p, j);
    INTEGER(VECTOR_ELT(sb, 0))[j] =
      (int) atomic_load_explicit(&s->status, memory_order_acquire);
    REAL(VECTOR_ELT(sb, 1))[j] = (double) s->pid;
    INTEGER(VECTOR_ELT(sb, 2))[j] = (int) s->rs_start;
    INTEGER(VECTOR_ELT(sb, 3))[j] = (int) s->rs_count;
    REAL(VECTOR_ELT(sb, 4))[j] = (double)
      (atomic_load_explicit(ring_tail(ring), memory_order_acquire) -
       atomic_load_explicit(ring_head(ring), memory_order_acquire));
    LOGICAL(VECTOR_ELT(sb, 5))[j] = (ready >> j) & 1;
    LOGICAL(VECTOR_ELT(sb, 6))[j] = (full >> j) & 1;
  }

  uint32_t n = 0;
  for (uint32_t r = 0; r < p->hdr.result_slots; r++)
    n += atomic_load_explicit(&pool_rs(p, r)->status,
                              memory_order_acquire) != SORA_RS_FREE;
  const char *tnames[] = {"slot", "status", "sequence", "worker", "waiter",
                          ""};
  SEXP tk = Rf_mkNamed(VECSXP, tnames);
  SET_VECTOR_ELT(out, 4, tk);
  for (int f = 0; f < 5; f++)
    SET_VECTOR_ELT(tk, f, Rf_allocVector(f == 2 ? REALSXP : INTSXP,
                                         (R_xlen_t) n));
  uint32_t m = 0;
  for (uint32_t r = 0; r < p->hdr.result_slots && m < n; r++) {
    sora_rs_hdr *rs = pool_rs(p, r);
    int32_t st = atomic_load_explicit(&rs->status, memory_order_acquire);
    if (st == SORA_RS_FREE) continue;
    INTEGER(VECTOR_ELT(tk, 0))[m] = (int) r;
    INTEGER(VECTOR_ELT(tk, 1))[m] = (int) st;
    REAL(VECTOR_ELT(tk, 2))[m] = (double)
      atomic_load_explicit(&rs->sequence, memory_order_relaxed);
    INTEGER(VECTOR_ELT(tk, 3))[m] =
      (int) atomic_load_explicit(&rs->worker_slot, memory_order_acquire);
    INTEGER(VECTOR_ELT(tk, 4))[m] =
      (int) atomic_load_explicit(&rs->waiter_slot, memory_order_acquire);
    m++;
  }
  for (; m < n; m++) {
    INTEGER(VECTOR_ELT(tk, 0))[m] = NA_INTEGER;
    INTEGER(VECTOR_ELT(tk, 1))[m] = NA_INTEGER;
    REAL(VECTOR_ELT(tk, 2))[m] = NA_REAL;
    INTEGER(VECTOR_ELT(tk, 3))[m] = NA_INTEGER;
    INTEGER(VECTOR_ELT(tk, 4))[m] = NA_INTEGER;
  }
  UNPROTECT(1);
  return out;
}
