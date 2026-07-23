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
   ipc-plan.md; the registry and slot structs in kioto.h are its wire
   format. */

#include <stdlib.h>
#include <stdio.h>
#include "kioto.h"
#include <R_ext/Utils.h>

enum { KIO_ROLE_CONTROLLER = 0, KIO_ROLE_WORKER, KIO_ROLE_SUBMITTER };

struct kio_reap_ctx_s { void *pool; uint32_t slot; };

typedef struct kio_pool_s {
  mori_shm shm;                  /* our mapping; unmapped only in release */
  kio_pool_hdr hdr;
  unsigned char *base;
  int role;
  int released;
  long self_pid;                 /* fork guard */
  int wk_slot;                   /* our worker slot (-1 unless worker) */
  int sub_slot;                  /* our submitter slot (-1 unless we submit) */
  uint32_t inline_entry;         /* slot - sizeof(kio_entry_hdr) */
  uint32_t inline_rs;            /* slot - sizeof(kio_rs_hdr) */

  kio_wk_slot *wk;
  kio_sub_slot *sub;
  _Atomic uint64_t *inj_ready;
  _Atomic uint64_t *full_waiters;
  _Atomic uint32_t *shutdown;
  _Atomic uint64_t *parked_workers;
  unsigned char *rings;
  unsigned char *results;

  kio_parker *pks;               /* every entity: workers, then submitters */
  int pk_ok;

  intptr_t live_self;            /* our held lock (worker / submitter slot) */
  intptr_t live_sub;             /* a worker's nested-submitter slot lock */
  intptr_t live_owner;           /* kept fd on the owner file; 0 = not open */
  intptr_t *live_all;            /* controller: kept probe fds, wk then sub */
  char livedir[1024];

  /* controller-only: per-worker death watches whose C callbacks run the
     reap off the R main thread */
  kio_death_watch **wk_watch;
  _Atomic int *wk_dead;
  struct kio_reap_ctx_s *reap_ctx;

  /* submitter-local */
  uint32_t rs_cursor;
  uint64_t task_counter;
  int64_t inj_ltail;             /* producer-local tail */

  /* worker-local */
  unsigned char *scratch;        /* slot-sized claim copy buffer */
  uint32_t scan_start;           /* rotating ring-scan start */
  uint64_t claims;               /* fairness-tick counter (% 61) */
  uint64_t rng;                  /* xorshift state for victim selection */
  int help_depth;                /* nested-collect help recursion depth */
  /* identity of the outermost (unwind-path) task eval, for
     kio_pool_fail_inflight: written only by catching = 0 executes — inner
     help / inline recursion clears the shm announce, so it cannot serve
     the unwind path */
  int in_eval;
  uint32_t cur_rs_index;
  uint64_t cur_seq, cur_task_id;
  uint16_t cur_sub_slot;
  uint32_t probe_streak;         /* thief-probe backstop state */
  uint32_t probe_victim;
  struct kio_rk_s { uint32_t idx; uint64_t seq; } *rk;
  uint32_t *rk_pos;              /* per result slot: rk position + 1, 0 = none */
  uint32_t rk_n, rk_cap, rk_cursor;
  /* cumulative stat counters, mirrored into the slot's stat_* fields by
     pool_stats_publish at park/fairness-tick cadence */
  uint64_t st_tasks, st_steals, st_inj, st_parks, st_helps;

  _Atomic int owner_dead;        /* death-listener flag: wake trigger only */
  kio_death_watch *watch;
} kio_pool;

static SEXP kio_pool_tag;
static SEXP kio_task_tag;
static SEXP kio_class_pool;
static SEXP kio_class_task;

void kio_pool_init(void) {
  kio_pool_tag = Rf_install("kio_pool");
  kio_task_tag = Rf_install("kio_task");
  kio_class_pool = Rf_mkString("kio_pool");
  R_PreserveObject(kio_class_pool);
  kio_class_task = Rf_mkString("kio_task");
  R_PreserveObject(kio_class_task);
}

// Layout ----------------------------------------------------------------------------

static uint64_t pool_ring_bytes(const kio_pool_hdr *h) {
  return KIO_INJ_META_SIZE + (uint64_t) h->inj_cap * h->slot;
}

/* Region size implied by a header; the create sizes with it and the attach
   validator checks against it, so both sides share one piece of offset math. */
static uint64_t pool_fixed_size(const kio_pool_hdr *h) {
  return 64 +
    (uint64_t) h->max_workers * sizeof(kio_wk_slot) +
    (uint64_t) h->max_submitters * sizeof(kio_sub_slot) +
    128 +
    (uint64_t) h->max_submitters * pool_ring_bytes(h) +
    (uint64_t) h->max_workers * ((uint64_t) h->deque_cap * h->slot) +
    (uint64_t) h->result_slots * h->slot +
    128;
}

static void pool_wire(kio_pool *p) {
  unsigned char *b = (unsigned char *) p->shm.addr;
  const kio_pool_hdr *h = &p->hdr;
  p->base = b;
  p->inline_entry = h->slot - (uint32_t) sizeof(kio_entry_hdr);
  p->inline_rs = h->slot - (uint32_t) sizeof(kio_rs_hdr);

  size_t off = 64;
  p->wk = (kio_wk_slot *) (b + off);
  off += (size_t) h->max_workers * sizeof(kio_wk_slot);
  p->sub = (kio_sub_slot *) (b + off);
  off += (size_t) h->max_submitters * sizeof(kio_sub_slot);
  p->inj_ready = (_Atomic uint64_t *) (b + off + KIO_TIER_READY_OFF);
  p->full_waiters = (_Atomic uint64_t *) (b + off + KIO_TIER_FULL_OFF);
  off += 128;
  p->rings = b + off;
  off += (size_t) h->max_submitters * pool_ring_bytes(h);
  off += (size_t) h->max_workers * ((size_t) h->deque_cap * h->slot);
  p->results = b + off;
  off += (size_t) h->result_slots * h->slot;
  p->shutdown = (_Atomic uint32_t *) (b + off + KIO_CTRL_SHUTDOWN_OFF);
  p->parked_workers = (_Atomic uint64_t *) (b + off + KIO_CTRL_PARKED_OFF);
}

static unsigned char *pool_ring(kio_pool *p, uint32_t s) {
  return p->rings + (size_t) s * pool_ring_bytes(&p->hdr);
}

static _Atomic int64_t *ring_tail(unsigned char *r) {
  return (_Atomic int64_t *) (r + KIO_INJ_TAIL_OFF);
}

static _Atomic int64_t *ring_head(unsigned char *r) {
  return (_Atomic int64_t *) (r + KIO_INJ_HEAD_OFF);
}

static unsigned char *ring_entry(kio_pool *p, unsigned char *r, uint64_t i) {
  return r + KIO_INJ_META_SIZE +
    (i & ((uint64_t) p->hdr.inj_cap - 1)) * p->hdr.slot;
}

static kio_rs_hdr *pool_rs(kio_pool *p, uint32_t idx) {
  return (kio_rs_hdr *) (p->results + (size_t) idx * p->hdr.slot);
}

static unsigned char *deque_entry_at(kio_pool *p, kio_wk_slot *w, int64_t i) {
  return p->base + w->deque_buf_off +
    ((uint64_t) i & ((uint64_t) w->deque_cap - 1)) * p->hdr.slot;
}

static int deque_nonempty(kio_wk_slot *w) {
  return atomic_load_explicit(&w->deque_top, memory_order_acquire) <
    atomic_load_explicit(&w->deque_bottom, memory_order_acquire);
}

static const char *pool_hdr_validate(const void *region, size_t region_size,
                                     kio_pool_hdr *out) {
  if (region_size < 64)
    return "region is smaller than a pool header";
  kio_pool_hdr h;
  memcpy(&h, region, sizeof(h));
  if (h.magic != KIO_POOL_MAGIC)
    return "bad magic: not a kioto pool region";
  if (h.version != KIO_ABI_VERSION)
    return "ABI version mismatch: participant and controller were built "
           "against different kioto wire formats";
  if (h.max_workers == 0 || h.max_workers > 64 ||
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

static kio_parker *pool_wk_pk(kio_pool *p, uint32_t i) {
  return &p->pks[i];
}

static kio_parker *pool_sub_pk(kio_pool *p, uint32_t j) {
  return &p->pks[p->hdr.max_workers + j];
}

/* Entity numbering (the Windows event-name key): workers 0..MW-1, submitters
   MW..MW+MS-1. Every participant attaches every entity's parker up front —
   submitters unpark workers, workers unpark submitters — with create = 1 only
   on the controller, before any spawn. */
static int pool_parkers_attach(kio_pool *p, int create) {
  uint32_t mw = p->hdr.max_workers, ms = p->hdr.max_submitters;
  p->pks = calloc(mw + ms, sizeof(kio_parker));
  if (p->pks == NULL) return -1;
  for (uint32_t i = 0; i < mw + ms; i++) {
    _Atomic uint32_t *epoch = i < mw ? &p->wk[i].park_epoch
                                     : &p->sub[i - mw].park_epoch;
    if (kio_parker_attach(&p->pks[i], epoch, p->shm.name, (int) i,
                          create) != 0) {
      for (uint32_t k = 0; k < i; k++) kio_parker_detach(&p->pks[k]);
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
static int pool_live_path(kio_pool *p, char *buf, size_t size,
                          const char *kind, uint32_t idx) {
  const char *suffix = p->shm.name + strlen(MORI_PREFIX_LITERAL);
  int n = strcmp(kind, "owner") == 0 ?
    snprintf(buf, size, "%s/kio_%s.owner", p->livedir, suffix) :
    snprintf(buf, size, "%s/kio_%s.%s.%u", p->livedir, suffix, kind, idx);
  return (n > 0 && (size_t) n < size) ? 0 : -1;
}

// Release ----------------------------------------------------------------------------

/* Full teardown of a handle's process-local state, idempotent. Never
   unlinks: the region name and liveness files are removed only by the
   controller's stop / destroy protocol. Order is load-bearing, as in the
   channel: the death watch and parkers reference the mapping. */
static void pool_release(kio_pool *p) {
  if (p->released) return;
  p->released = 1;
  if (p->watch != NULL) {
    kio_death_watch_stop(p->watch);
    p->watch = NULL;
  }
  if (p->wk_watch != NULL) {
    /* stop synchronizes with in-flight reap callbacks: after this loop
       nothing touches the mapping from another thread */
    for (uint32_t i = 0; i < p->hdr.max_workers; i++)
      if (p->wk_watch[i] != NULL) {
        kio_death_watch_stop(p->wk_watch[i]);
        p->wk_watch[i] = NULL;
      }
  }
  if (p->pk_ok) {
    uint32_t n = p->hdr.max_workers + p->hdr.max_submitters;
    for (uint32_t i = 0; i < n; i++) kio_parker_detach(&p->pks[i]);
    p->pk_ok = 0;
  }
  free(p->pks);
  p->pks = NULL;
  if (p->shm.addr != NULL) mori_shm_close(&p->shm, 0);
  p->base = NULL;
  if (p->live_self != 0) {
    kio_live_close(p->live_self);
    p->live_self = 0;
  }
  if (p->live_sub != 0) {
    kio_live_close(p->live_sub);
    p->live_sub = 0;
  }
  if (p->live_owner != 0) {
    kio_live_close(p->live_owner);
    p->live_owner = 0;
  }
  if (p->live_all != NULL) {
    uint32_t n = p->hdr.max_workers + p->hdr.max_submitters;
    for (uint32_t i = 0; i < n; i++)
      if (p->live_all[i] != 0) kio_live_close(p->live_all[i]);
    free(p->live_all);
    p->live_all = NULL;
  }
}

/* The controller half of teardown, shared by kio_pool_stop, the startup
   walk-back, and the handle finalizer: broadcast shutdown, wake everyone,
   cancel every pending result, unlink the names. Waiting for workers is the
   caller's business (the finalizer cannot wait). */
static void pool_shutdown_broadcast(kio_pool *p) {
  atomic_store_explicit(p->shutdown, 1u, memory_order_seq_cst);
  for (uint32_t i = 0; i < p->hdr.max_workers; i++)
    kio_unpark(pool_wk_pk(p, i));
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++)
    if (atomic_load_explicit(p->full_waiters, memory_order_acquire) &
        (1ull << j))
      kio_unpark(pool_sub_pk(p, j));
  for (uint32_t r = 0; r < p->hdr.result_slots; r++) {
    kio_rs_hdr *rs = pool_rs(p, r);
    int32_t expected = KIO_RS_PENDING;
    if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                KIO_RS_CANCEL,
                                                memory_order_seq_cst,
                                                memory_order_relaxed)) {
      int32_t ws = atomic_load_explicit(&rs->waiter_slot,
                                        memory_order_acquire);
      if (ws >= 0 && (uint32_t) ws < p->hdr.max_submitters)
        kio_unpark(pool_sub_pk(p, (uint32_t) ws));
    }
  }
}

static void pool_remove_live_files(kio_pool *p);

static void pool_unlink_names(kio_pool *p, SEXP prot) {
  SEXP host_ptr = VECTOR_ELT(prot, 1);
  if (host_ptr != R_NilValue) mori_host_finalizer(host_ptr);
  pool_remove_live_files(p);
}

// Handle access ---------------------------------------------------------------------

static kio_pool *pool_peek(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != kio_pool_tag)
    Rf_error("kioto: not a pool handle");
  kio_pool *p = (kio_pool *) R_ExternalPtrAddr(xp);
  if (p == NULL || p->released) return NULL;
  if (p->self_pid != kio_self_pid())
    Rf_error("kioto: pool handles do not survive fork()");
  return p;
}

static kio_pool *pool_get(SEXP xp) {
  kio_pool *p = pool_peek(xp);
  if (p == NULL) Rf_error("kioto: pool handle is closed");
  return p;
}

static void kio_pool_finalizer(SEXP xp) {
  kio_pool *p = (kio_pool *) R_ExternalPtrAddr(xp);
  if (p == NULL) return;
  if (!p->released && p->role == KIO_ROLE_CONTROLLER && p->base != NULL) {
    pool_shutdown_broadcast(p);
    pool_unlink_names(p, R_ExternalPtrProtected(xp));
  }
  pool_release(p);
  free(p->scratch);
  free(p->rk);
  free(p->rk_pos);
  free(p->wk_watch);
  free((void *) p->wk_dead);
  free(p->reap_ctx);
  free(p);
  R_ClearExternalPtr(xp);
}

/* prot layout: [0] keepers — the submitter's task keepers (length rs_count)
   or the worker's result keepers (length result_slots); [1] the host unlink
   extptr (controller only); [2] a worker's nested-submit task keepers
   (length rs_count, allocated when the worker claims a submitter slot);
   [3] the worker's evaluation base env (kio_pool_set_eval); [4] the
   handle's trace hook (kio_pool_set_trace). */
static SEXP pool_make_handle(kio_pool *p, SEXP keepers, SEXP host_ptr) {
  SEXP prot = PROTECT(Rf_allocVector(VECSXP, 5));
  SET_VECTOR_ELT(prot, 0, keepers);
  SET_VECTOR_ELT(prot, 1, host_ptr);
  SEXP xp = PROTECT(R_MakeExternalPtr(p, kio_pool_tag, prot));
  R_RegisterCFinalizerEx(xp, kio_pool_finalizer, TRUE);
  Rf_setAttrib(xp, R_ClassSymbol, kio_class_pool);
  UNPROTECT(2);
  return xp;
}

/* The submitter's task-keeper table: prot[0] on submitter handles, prot[2]
   on worker handles (nested submits). */
static SEXP pool_task_keepers(kio_pool *p, SEXP xp) {
  return VECTOR_ELT(R_ExternalPtrProtected(xp),
                    p->role == KIO_ROLE_WORKER ? 2 : 0);
}

/* Arms a worker handle for evaluation: a base environment under
   globalenv() binding the handle itself as `pool` — what worker-side
   nested submit closes over; task argument frames chain beneath it, so a
   task argument named `pool` shadows it. Stashed on the handle so nested
   submit's inline-execute fallback and collect's help mode can run tasks
   outside kio_pool_step. */
SEXP kio_pool_set_eval(SEXP xp) {
  kio_pool *p = pool_get(xp);
  if (p->role != KIO_ROLE_WORKER)
    Rf_error("kioto: not a worker handle");
  SEXP base = PROTECT(R_NewEnv(R_GlobalEnv, 0, 0));
  Rf_defineVar(Rf_install("pool"), xp, base);
  SET_VECTOR_ELT(R_ExternalPtrProtected(xp), 3, base);
  UNPROTECT(1);
  return R_NilValue;
}

static SEXP pool_eval_env(SEXP xp) {
  SEXP env = VECTOR_ELT(R_ExternalPtrProtected(xp), 3);
  if (TYPEOF(env) != ENVSXP)
    Rf_error("kioto: no evaluator registered on this worker handle");
  return env;
}

/* The task evaluator: one wire payload — list(expr, named args) — with the
   arguments bound into a fresh unhashed frame under the base environment.
   Two error disciplines, chosen by the caller. The worker loop's hot path
   (catching = 0) arms no handler at all: a user error longjmps out of
   kio_pool_step and worker_main publishes the caught condition as this
   task's ERR result through kio_pool_fail_inflight — the in_eval flag is
   what separates those errors from infrastructure failure, which stays
   fatal. Help mode and nested submit's inline execute (catching = 1) run
   inside a task's own evaluation, where an escaping error would land in
   the wrong task's frames: they contain it with R_tryCatchError and pay
   its R-closure trampoline — several µs, still cheaper than the park that
   helping replaced. */
struct kio_eval_ctx { SEXP expr; SEXP env; int ok; };

static SEXP pool_eval_body(void *data) {
  struct kio_eval_ctx *c = (struct kio_eval_ctx *) data;
  return Rf_eval(c->expr, c->env);
}

static SEXP pool_eval_handler(SEXP cond, void *data) {
  ((struct kio_eval_ctx *) data)->ok = 0;
  return cond;
}

static SEXP pool_eval_task(kio_pool *p, SEXP xp, SEXP payload, int catching,
                           int *ok) {
  if (TYPEOF(payload) != VECSXP || Rf_xlength(payload) != 2 ||
      TYPEOF(VECTOR_ELT(payload, 1)) != VECSXP)
    Rf_error("kioto: corrupt task payload");
  SEXP args = VECTOR_ELT(payload, 1);
  SEXP names = Rf_getAttrib(args, R_NamesSymbol);
  R_xlen_t n = Rf_xlength(args);
  if (n > 0 && TYPEOF(names) != STRSXP)
    Rf_error("kioto: corrupt task payload");
  SEXP env = PROTECT(R_NewEnv(pool_eval_env(xp), 0, 0));
  for (R_xlen_t i = 0; i < n; i++)
    Rf_defineVar(Rf_installTrChar(STRING_ELT(names, i)),
                 VECTOR_ELT(args, i), env);
  struct kio_eval_ctx c = { VECTOR_ELT(payload, 0), env, 1 };
  SEXP value;
  if (catching) {
    value = R_tryCatchError(pool_eval_body, &c, pool_eval_handler, &c);
  } else {
    p->in_eval = 1;
    value = Rf_eval(c.expr, c.env);
    p->in_eval = 0;
  }
  *ok = c.ok;
  UNPROTECT(1);
  return value;
}

/* Per-handle, per-process trace hook: fn(event, id), with id the entry
   header's task_id (reserved for exactly this) as "<submitter>:<counter>".
   Emitting sites exist only on the pool's per-task paths — submit and
   pool_execute — never in the channel, whose per-message budget has no
   room even for a clock read. Disabled cost is one pointer check per
   site. */
SEXP kio_pool_set_trace(SEXP xp, SEXP fn) {
  (void) pool_get(xp);
  if (fn != R_NilValue && TYPEOF(fn) != CLOSXP)
    Rf_error("kioto: expected a function or NULL");
  SET_VECTOR_ELT(R_ExternalPtrProtected(xp), 4, fn);
  return R_NilValue;
}

/* An error raised by the hook longjmps like any infrastructure error at
   its site: at submit the task stays committed (the dropped handle's
   finalizer then cancels it); in pool_execute it takes the worker down —
   its stranded in-flight task fails through the ordinary death path. */
static void pool_trace_emit(SEXP xp, const char *event, uint64_t id) {
  SEXP fn = VECTOR_ELT(R_ExternalPtrProtected(xp), 4);
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

static int kio_pow2_u64(uint64_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

SEXP kio_pool_create(SEXP maxw_sexp, SEXP maxs_sexp, SEXP inj_sexp,
                     SEXP deque_sexp, SEXP rslots_sexp, SEXP slot_sexp,
                     SEXP livedir_sexp) {
  uint64_t maxw = (uint64_t) Rf_asInteger(maxw_sexp);
  uint64_t maxs = (uint64_t) Rf_asInteger(maxs_sexp);
  uint64_t inj_cap = (uint64_t) Rf_asInteger(inj_sexp);
  uint64_t deque_cap = (uint64_t) Rf_asInteger(deque_sexp);
  uint64_t rslots = (uint64_t) Rf_asInteger(rslots_sexp);
  uint64_t slot = (uint64_t) Rf_asInteger(slot_sexp);
  if (maxw < 1 || maxw > 64)
    Rf_error("kioto: max_workers must be between 1 and 64");
  if (maxs < 1 || maxs > 64)
    Rf_error("kioto: max_submitters must be between 1 and 64");
  if (!kio_pow2_u64(inj_cap) || inj_cap < 2 || inj_cap > (1u << 24))
    Rf_error("kioto: injection_cap must be a power of two between 2 and 2^24");
  if (!kio_pow2_u64(deque_cap) || deque_cap < 2 || deque_cap > (1u << 24))
    Rf_error("kioto: per_worker_cap must be a power of two between 2 and 2^24");
  /* floor 128: a result slot's inline budget (slot - 40) must hold a
     region name (up to 27 bytes on Windows) for an SHM_RAW spill */
  if (!kio_pow2_u64(slot) || slot < 128 || slot > (1u << 20))
    Rf_error("kioto: slot_size must be a power of two between 128 and 2^20");
  if (rslots < maxs || rslots > (1u << 24))
    Rf_error("kioto: result_slots must be between max_submitters and 2^24");
  rslots = (rslots + maxs - 1) / maxs * maxs;   /* per-submitter partition */
  if (TYPEOF(livedir_sexp) != STRSXP || XLENGTH(livedir_sexp) != 1)
    Rf_error("kioto: expected a liveness directory path");
  const char *livedir = CHAR(STRING_ELT(livedir_sexp, 0));
  size_t livedir_len = strlen(livedir);
  if (livedir_len == 0 || livedir_len > 900)
    Rf_error("kioto: liveness directory path too long");

  kio_pool_hdr h = {
    .magic = KIO_POOL_MAGIC,
    .version = KIO_ABI_VERSION,
    .max_workers = (uint32_t) maxw,
    .max_submitters = (uint32_t) maxs,
    .inj_cap = (uint32_t) inj_cap,
    .deque_cap = (uint32_t) deque_cap,
    .result_slots = (uint32_t) rslots,
    .slot = (uint32_t) slot,
    .owner_pid = (uint64_t) kio_self_pid(),
  };
  uint64_t fixed = pool_fixed_size(&h);
  h.livedir_offset = MORI_ALIGN64(fixed);
  h.livedir_size = livedir_len;
  uint64_t total = h.livedir_offset + livedir_len;
  if (total > ((uint64_t) 1 << 46))
    Rf_error("kioto: pool region too large");

  kio_pool *p = calloc(1, sizeof(*p));
  if (p == NULL) Rf_error("kioto: allocation failure");
  int rc = mori_shm_create(&p->shm, (size_t) total);
  if (rc != MORI_OK) {
    free(p);
    const char *summary, *hint;
    mori_err_describe(rc, &summary, &hint);
    Rf_error("kioto: cannot create pool region (%llu bytes): %s%s%s",
             (unsigned long long) total, summary,
             hint[0] != '\0' ? ". " : "", hint);
  }
  p->role = KIO_ROLE_CONTROLLER;
  p->self_pid = kio_self_pid();
  p->wk_slot = -1;
  p->sub_slot = 0;
  p->hdr = h;
  memcpy(p->livedir, livedir, livedir_len + 1);

  /* From here cleanup is the finalizer's: build the handle before anything
     that can longjmp. */
  SEXP host_ptr = PROTECT(kio_shm_wrap_host(&p->shm));
  SEXP keepers = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) (rslots / maxs)));
  SEXP xp = PROTECT(pool_make_handle(p, keepers, host_ptr));

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
  if (p->live_all == NULL) Rf_error("kioto: allocation failure");
  for (uint32_t i = 0; i < h.max_workers + h.max_submitters; i++) {
    int is_wk = i < h.max_workers;
    if (pool_live_path(p, path, sizeof(path), is_wk ? "wk" : "sub",
                       is_wk ? i : i - h.max_workers) != 0)
      Rf_error("kioto: liveness file path too long");
    if (kio_live_open(path, &p->live_all[i]) != 0)
      Rf_error("kioto: cannot create liveness file '%s'", path);
  }
  if (pool_live_path(p, path, sizeof(path), "owner", 0) != 0)
    Rf_error("kioto: liveness file path too long");
  if (kio_live_open(path, &p->live_owner) != 0 ||
      kio_live_try(p->live_owner) != KIO_LIVE_ACQUIRED)
    Rf_error("kioto: cannot lock owner liveness file '%s'", path);

  /* Windows: every entity's named parker event must exist before any spawn */
  if (pool_parkers_attach(p, 1) != 0)
    Rf_error("kioto: cannot attach pool parkers");

  /* Claim submitter slot 0 for the calling process: lock-before-CAS, as in
     every join. */
  p->live_self = p->live_all[h.max_workers + 0];
  if (kio_live_try(p->live_self) != KIO_LIVE_ACQUIRED)
    Rf_error("kioto: cannot lock submitter liveness file");
  kio_sub_slot *s0 = &p->sub[0];
  int32_t expected = KIO_SUB_FREE;
  if (!atomic_compare_exchange_strong_explicit(&s0->status, &expected,
                                               KIO_SUB_LIVE,
                                               memory_order_seq_cst,
                                               memory_order_relaxed))
    Rf_error("kioto: submitter slot 0 is not free in a fresh region");
  s0->pid = (int64_t) p->self_pid;
  s0->rs_start = 0;
  s0->rs_count = (uint32_t) (rslots / maxs);
  kio_live_ident(p->live_self, &s0->live_dev, &s0->live_ino);
  p->rs_cursor = 0;

  UNPROTECT(3);
  return xp;
}

SEXP kio_pool_suffix(SEXP xp) {
  kio_pool *p = pool_get(xp);
  return Rf_mkString(p->shm.name + strlen(MORI_PREFIX_LITERAL));
}

static void pool_wk_death_cb(void *arg);

/* The controller points its death listener at every LIVE target slot's
   pid, stopping any stale watch first so respawned slots get fresh
   watches. The callback reap then runs off the R main thread at OS
   notification latency. In-process joins (the test harness) are skipped —
   a process cannot meaningfully watch itself. */
static void pool_watch_workers(kio_pool *p, const int *slots, R_xlen_t n) {
  if (p->wk_watch == NULL) {
    p->wk_watch = calloc(p->hdr.max_workers, sizeof(*p->wk_watch));
    p->wk_dead = calloc(p->hdr.max_workers, sizeof(*p->wk_dead));
    p->reap_ctx = calloc(p->hdr.max_workers, sizeof(*p->reap_ctx));
    if (p->wk_watch == NULL || p->wk_dead == NULL || p->reap_ctx == NULL)
      Rf_error("kioto: allocation failure");
  }
  for (R_xlen_t i = 0; i < n; i++) {
    uint32_t s = (uint32_t) slots[i];
    kio_wk_slot *w = &p->wk[s];
    if (atomic_load_explicit(&w->status, memory_order_acquire) !=
        KIO_WK_LIVE)
      continue;
    if ((long) w->pid == p->self_pid) continue;
    if (p->wk_watch[s] != NULL) {
      kio_death_watch_stop(p->wk_watch[s]);
      p->wk_watch[s] = NULL;
    }
    atomic_store_explicit(&p->wk_dead[s], 0, memory_order_relaxed);
    p->reap_ctx[s].pool = p;
    p->reap_ctx[s].slot = s;
    /* NULL leaves the probes and the teardown sweep as the backstops */
    p->wk_watch[s] = kio_death_watch_start2((long) w->pid, &p->wk_dead[s],
                                            NULL, pool_wk_death_cb,
                                            &p->reap_ctx[s]);
  }
}

/* Startup / elastic-spawn rendezvous: park on the creator's submitter-0
   parker, re-checking each target slot for LIVE on each wake; workers
   unpark the creator on reaching LIVE. On the way out — success or
   deadline expiry — the death listener is pointed at whichever targets did
   join. FALSE on expiry; the initial-creation caller walks the pool back
   via kio_pool_destroy, an elastic caller just errors. */
SEXP kio_pool_ready_wait(SEXP xp, SEXP slots_sexp, SEXP timeout) {
  kio_pool *p = pool_get(xp);
  if (p->role != KIO_ROLE_CONTROLLER)
    Rf_error("kioto: only the controller can wait for workers");
  if (TYPEOF(slots_sexp) != INTSXP)
    Rf_error("kioto: expected worker slot indices");
  R_xlen_t n = XLENGTH(slots_sexp);
  const int *slots = INTEGER(slots_sexp);
  for (R_xlen_t i = 0; i < n; i++)
    if (slots[i] < 0 || (uint32_t) slots[i] >= p->hdr.max_workers)
      Rf_error("kioto: worker slot index out of range");
  double deadline = kio_now() + Rf_asReal(timeout);
  int ok;
  for (;;) {
    uint32_t e = kio_parker_snapshot(pool_sub_pk(p, 0));
    R_xlen_t live = 0;
    for (R_xlen_t i = 0; i < n; i++)
      live += atomic_load_explicit(&p->wk[slots[i]].status,
                                   memory_order_acquire) == KIO_WK_LIVE;
    ok = live == n;
    if (ok) break;
    double rem = deadline - kio_now();
    if (rem <= 0) break;
    long ms = (long) (rem * 1000) + 1;
    if (ms > KIO_INTERRUPT_BOUND_MS) ms = KIO_INTERRUPT_BOUND_MS;
    kio_park(pool_sub_pk(p, 0), e, ms);
    R_CheckUserInterrupt();
  }
  pool_watch_workers(p, slots, n);
  return Rf_ScalarLogical(ok);
}

/* Clean-exit request: the worker observes the word between tasks and takes
   its LEAVING path — non-blocking here, and never preemptive. Its deque is
   consumed in place (REAPING) and the process may linger as a lifetime
   anchor for uncollected results. */
SEXP kio_pool_retire(SEXP xp, SEXP slot_sexp) {
  kio_pool *p = pool_get(xp);
  if (p->role != KIO_ROLE_CONTROLLER)
    Rf_error("kioto: only the controller can retire workers");
  uint32_t slot = (uint32_t) Rf_asInteger(slot_sexp);
  if (slot >= p->hdr.max_workers)
    Rf_error("kioto: worker slot index out of range");
  if (atomic_load_explicit(&p->wk[slot].status, memory_order_acquire) !=
      KIO_WK_LIVE)
    Rf_error("kioto: worker slot %u is not live", slot);
  atomic_store_explicit(&p->wk[slot].retire, 1, memory_order_seq_cst);
  kio_unpark(pool_wk_pk(p, slot));
  return R_NilValue;
}

/* Startup walk-back and finalizer-free explicit destroy: broadcast so a
   late-joining worker exits instead of parking against a pool that gave up,
   then unlink everything. */
SEXP kio_pool_destroy(SEXP xp) {
  kio_pool *p = pool_get(xp);
  if (p->role != KIO_ROLE_CONTROLLER)
    Rf_error("kioto: only the controller can destroy a pool");
  pool_shutdown_broadcast(p);
  pool_unlink_names(p, R_ExternalPtrProtected(xp));
  pool_release(p);
  return R_NilValue;
}

// Attach helpers ----------------------------------------------------------------------

static kio_pool *pool_open_common(const char *suffix, SEXP *xp_out,
                                  int keeper_len_is_rs) {
  for (const char *q = suffix; *q != '\0'; q++)
    if (!((*q >= '0' && *q <= '9') || (*q >= 'a' && *q <= 'f') || *q == '_'))
      Rf_error("kioto: malformed region-name suffix");
  char name[MORI_NAME_MAX];
  int nn = snprintf(name, sizeof(name), "%s%s", MORI_PREFIX_LITERAL, suffix);
  if (nn <= 0 || (size_t) nn >= sizeof(name))
    Rf_error("kioto: malformed region-name suffix");

  kio_pool *p = calloc(1, sizeof(*p));
  if (p == NULL) Rf_error("kioto: allocation failure");
  if (kio_shm_open_rw(&p->shm, name) != 0) {
    free(p);
    Rf_error("kioto: cannot open pool region '%s'", name);
  }
  p->self_pid = kio_self_pid();
  p->wk_slot = -1;
  p->sub_slot = -1;

  /* validate before touching any other field */
  const char *err = pool_hdr_validate(p->shm.addr, p->shm.size, &p->hdr);
  if (err != NULL) {
    mori_shm_close(&p->shm, 0);
    free(p);
    Rf_error("kioto: invalid pool region: %s", err);
  }

  R_xlen_t klen = keeper_len_is_rs ?
    (R_xlen_t) p->hdr.result_slots :
    (R_xlen_t) (p->hdr.result_slots / p->hdr.max_submitters);
  SEXP keepers = PROTECT(Rf_allocVector(VECSXP, klen));
  SEXP xp = PROTECT(pool_make_handle(p, keepers, R_NilValue));
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
static void pool_owner_check(kio_pool *p) {
  char path[1024];
  if (pool_live_path(p, path, sizeof(path), "owner", 0) != 0)
    Rf_error("kioto: liveness file path too long");
  if (kio_live_open(path, &p->live_owner) != 0)
    Rf_error("kioto: cannot open owner liveness file '%s'", path);
  if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0 ||
      kio_live_try(p->live_owner) == KIO_LIVE_ACQUIRED)
    Rf_error("kioto: pool stopped or owner dead");
}

static void pool_watch_owner(kio_pool *p, kio_parker *own_pk) {
  if ((long) p->hdr.owner_pid == p->self_pid) return;   /* in-process join */
  p->watch = kio_death_watch_start((long) p->hdr.owner_pid, &p->owner_dead,
                                   own_pk);
  if (p->watch == NULL)
    Rf_error("kioto: cannot watch owner process %llu",
             (unsigned long long) p->hdr.owner_pid);
}

/* Lock-first claim of a FREE submitter slot, mirroring the worker join: a
   dead submitter is recognisable by its free liveness lock regardless of
   which side of the CAS it died on. Fills the slot's identity fields, sets
   p->sub_slot, and returns the held lock through *lock_out. Shared by
   kio_pool_attach and a worker's first nested submit. */
static void pool_claim_sub_slot(kio_pool *p, intptr_t *lock_out) {
  char path[1024];
  uint32_t per = p->hdr.result_slots / p->hdr.max_submitters;
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++) {
    if (atomic_load_explicit(&p->sub[j].status, memory_order_acquire) !=
        KIO_SUB_FREE)
      continue;
    if (pool_live_path(p, path, sizeof(path), "sub", j) != 0)
      Rf_error("kioto: liveness file path too long");
    intptr_t h;
    if (kio_live_open(path, &h) != 0) continue;
    if (kio_live_try(h) != KIO_LIVE_ACQUIRED) {
      kio_live_close(h);
      continue;                    /* another claimant beat us */
    }
    int32_t expected = KIO_SUB_FREE;
    if (!atomic_compare_exchange_strong_explicit(&p->sub[j].status, &expected,
                                                 KIO_SUB_LIVE,
                                                 memory_order_seq_cst,
                                                 memory_order_relaxed)) {
      kio_live_close(h);           /* stale FREE reading */
      continue;
    }
    *lock_out = h;
    p->sub_slot = (int) j;
    break;
  }
  if (p->sub_slot < 0)
    Rf_error("kioto: submitter registry full");

  kio_sub_slot *me = &p->sub[p->sub_slot];
  me->pid = (int64_t) p->self_pid;
  me->rs_start = (uint32_t) p->sub_slot * per;
  me->rs_count = per;
  kio_live_ident(*lock_out, &me->live_dev, &me->live_ino);
}

// Worker join --------------------------------------------------------------------------

static void pool_stats_publish(kio_pool *p);

SEXP kio_pool_worker_join(SEXP suffix_sexp, SEXP slot_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("kioto: expected a region-name suffix");
  SEXP xp;
  kio_pool *p = pool_open_common(CHAR(STRING_ELT(suffix_sexp, 0)), &xp, 1);
  PROTECT(xp);
  p->role = KIO_ROLE_WORKER;

  uint32_t slot = (uint32_t) Rf_asInteger(slot_sexp);
  if (slot >= p->hdr.max_workers)
    Rf_error("kioto: worker slot index out of range");
  pool_owner_check(p);

  /* Lock-before-CAS: what makes "CLAIMING + free lock" a reliable dead-worker
     signal for the Phase 4 reaper, and what fail-fasts against a leftover
     ghost from a previous spawn. */
  char path[1024];
  if (pool_live_path(p, path, sizeof(path), "wk", slot) != 0)
    Rf_error("kioto: liveness file path too long");
  if (kio_live_open(path, &p->live_self) != 0 ||
      kio_live_try(p->live_self) != KIO_LIVE_ACQUIRED)
    Rf_error("kioto: worker slot %u already held — stale spawn?", slot);
  kio_wk_slot *me = &p->wk[slot];
  int32_t expected = KIO_WK_FREE;
  if (!atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                               KIO_WK_CLAIMING,
                                               memory_order_seq_cst,
                                               memory_order_relaxed))
    Rf_error("kioto: worker slot %u not free — stale spawn?", slot);
  p->wk_slot = (int) slot;

  me->pid = (int64_t) p->self_pid;
  kio_live_ident(p->live_self, &me->live_dev, &me->live_ino);
  atomic_store_explicit(&me->in_flight_rs, -1, memory_order_relaxed);
  atomic_store_explicit(&me->retire, 0, memory_order_relaxed);
  atomic_store_explicit(&me->park_state, KIO_WPK_RUNNING,
                        memory_order_relaxed);
  pool_stats_publish(p);   /* zero any previous incarnation's counters */

  if (pool_parkers_attach(p, 0) != 0)
    Rf_error("kioto: cannot attach pool parkers");
  p->scratch = malloc(p->hdr.slot);
  if (p->scratch == NULL) Rf_error("kioto: allocation failure");
  p->rng = ((uint64_t) p->self_pid * 0x9E3779B97F4A7C15ull) ^
    ((uint64_t) (kio_now() * 1e9)) ^ ((uint64_t) slot << 32);
  if (p->rng == 0) p->rng = 1;
  pool_watch_owner(p, pool_wk_pk(p, slot));

  expected = KIO_WK_CLAIMING;
  atomic_compare_exchange_strong_explicit(&me->status, &expected, KIO_WK_LIVE,
                                          memory_order_seq_cst,
                                          memory_order_relaxed);
  kio_unpark(pool_sub_pk(p, 0));   /* the creator's startup wait */

  UNPROTECT(1);
  return xp;
}

static void pool_unpark_result_waiter(kio_pool *p, kio_rs_hdr *rs);
static void pool_wake_one_worker(kio_pool *p);
static int pool_reaping_free(kio_pool *p, kio_wk_slot *w);
static int pool_probe_worker(kio_pool *p, uint32_t slot);
static void pool_probe_submitter(kio_pool *p, uint32_t j);
static void pool_orphan_teardown_try(kio_pool *p);
static void pool_reap_result_keepers(kio_pool *p, SEXP keepers);

/* Clean worker exit. A nonempty deque is never drained anywhere: it
   becomes an ordinary steal target while the slot reads REAPING, and the
   observer of the drained deque returns the slot to FREE. Kept results are
   abandoned: the Phase 2 exits are shutdown and owner death, both of which
   cancel or orphan every outstanding collect anyway. */
SEXP kio_pool_leave(SEXP xp) {
  kio_pool *p = pool_peek(xp);
  if (p == NULL) return R_NilValue;
  if (p->role != KIO_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("kioto: not a worker handle");
  kio_wk_slot *me = &p->wk[p->wk_slot];
  pool_stats_publish(p);   /* final, exact mirror for the departed slot */
  atomic_fetch_and_explicit(p->parked_workers, ~(1ull << p->wk_slot),
                            memory_order_seq_cst);
  int32_t expected = KIO_WK_LIVE;
  if (atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                              KIO_WK_LEAVING,
                                              memory_order_seq_cst,
                                              memory_order_relaxed)) {
    /* walk the deque read-only, unparking each entry's result waiter;
       thieves may be advancing top concurrently — a wake for an
       already-stolen entry is a spurious wake, absorbed by the re-check */
    int64_t t = atomic_load_explicit(&me->deque_top, memory_order_acquire);
    int64_t b = atomic_load_explicit(&me->deque_bottom,
                                     memory_order_acquire);
    for (int64_t i = t; i < b; i++) {
      kio_entry_hdr *eh = (kio_entry_hdr *) deque_entry_at(p, me, i);
      if (eh->rs_index < p->hdr.result_slots)
        pool_unpark_result_waiter(p, pool_rs(p, eh->rs_index));
    }
    if (atomic_load_explicit(&me->deque_top, memory_order_acquire) >= b) {
      expected = KIO_WK_LEAVING;
      atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                              KIO_WK_FREE,
                                              memory_order_seq_cst,
                                              memory_order_relaxed);
    } else {
      expected = KIO_WK_LEAVING;
      atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                              KIO_WK_REAPING,
                                              memory_order_seq_cst,
                                              memory_order_relaxed);
      /* a thief that emptied the deque while we still read LEAVING saw
         nothing to free: re-check now that REAPING is published */
      if (!pool_reaping_free(p, me))
        pool_wake_one_worker(p);
    }
  }
  /* the slot's own lock releases now, so the slot reads properly departed
     to any prober; the full release is deferred while kept results anchor
     region lifetimes — the retiree's lame-duck loop drives it from R. The
     sweep makes the anchor check exact: the step loop's busy-path reap is
     quota-bounded and may leave consumed records behind */
  if (p->live_self != 0) {
    kio_live_close(p->live_self);
    p->live_self = 0;
  }
  pool_reap_result_keepers(p, VECTOR_ELT(R_ExternalPtrProtected(xp), 0));
  if (p->rk_n == 0) pool_release(p);
  return Rf_ScalarLogical(p->released);
}

/* One lame-duck beat for a retired worker anchoring uncollected results:
   reap the keeper table and report whether the anchor may drop. Plain
   bounded sleeps drive this from R — the slot's parker may already be
   reclaimed by a respawn, so no unpark can reach this process — and
   shutdown or owner death ends the linger. */
SEXP kio_pool_lame_duck(SEXP xp) {
  kio_pool *p = pool_peek(xp);
  if (p == NULL) return Rf_ScalarLogical(TRUE);
  pool_reap_result_keepers(p, VECTOR_ELT(R_ExternalPtrProtected(xp), 0));
  if (p->rk_n == 0 ||
      atomic_load_explicit(p->shutdown, memory_order_acquire) != 0 ||
      atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0) {
    pool_release(p);
    return Rf_ScalarLogical(TRUE);
  }
  return Rf_ScalarLogical(FALSE);
}

// Submitter join ------------------------------------------------------------------------

SEXP kio_pool_attach_call(SEXP suffix_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("kioto: expected a region-name suffix");
  SEXP xp;
  kio_pool *p = pool_open_common(CHAR(STRING_ELT(suffix_sexp, 0)), &xp, 0);
  PROTECT(xp);
  p->role = KIO_ROLE_SUBMITTER;
  pool_owner_check(p);

  pool_claim_sub_slot(p, &p->live_self);

  if (pool_parkers_attach(p, 0) != 0)
    Rf_error("kioto: cannot attach pool parkers");
  pool_watch_owner(p, pool_sub_pk(p, (uint32_t) p->sub_slot));
  p->inj_ltail = atomic_load_explicit(ring_tail(pool_ring(p,
    (uint32_t) p->sub_slot)), memory_order_acquire);

  UNPROTECT(1);
  return xp;
}

// Submit --------------------------------------------------------------------------------

/* The explicit-start variant exists for the reap paths, which run on the
   death listener's callback thread and must not touch the handle's
   process-local scan rotation. */
static void pool_wake_one_worker_from(kio_pool *p, uint32_t start) {
  /* pusher protocol: push, fence, then the mask load — either the parking
     worker's rescan sees the push or we see its bit */
  atomic_thread_fence(memory_order_seq_cst);
  uint64_t w = atomic_load_explicit(p->parked_workers, memory_order_relaxed);
  if (w == 0) return;
  uint32_t mw = p->hdr.max_workers;
  for (uint32_t k = 0; k < mw; k++) {
    uint32_t i = (start + k) % mw;
    if (!(w & (1ull << i))) continue;
    int32_t expected = KIO_WPK_PARKED;
    if (atomic_compare_exchange_strong_explicit(&p->wk[i].park_state,
                                                &expected, KIO_WPK_WAKING,
                                                memory_order_seq_cst,
                                                memory_order_relaxed)) {
      kio_unpark(pool_wk_pk(p, i));
      return;
    }
    if (expected == KIO_WPK_IDLE) {
      /* announced but not yet parked — its rescan may already have missed
         this publish, so an uncontested wake is not safe to skip. Its epoch
         snapshot predates the announce, so this unpark turns the upcoming
         sleep into an immediate return; at worst one spurious wake. */
      kio_unpark(pool_wk_pk(p, i));
      return;
    }
    /* RUNNING or WAKING: the worker transitioned away or another pusher
       claimed the wake; try the next set bit */
  }
}

static void pool_wake_one_worker(kio_pool *p) {
  pool_wake_one_worker_from(p, p->scan_start++);
}

/* Block until the submitter's own ring has space (announce-then-rescan on
   full_waiters, parked on the submitter's own parker, woken directly by the
   worker whose pop freed a slot) or the deadline passes. */
static int pool_ring_space_wait(kio_pool *p, _Atomic int64_t *head,
                                double timeout_s) {
  if (p->inj_ltail - atomic_load_explicit(head, memory_order_acquire) <
      (int64_t) p->hdr.inj_cap)
    return 1;
  uint64_t bit = 1ull << p->sub_slot;
  double deadline = R_FINITE(timeout_s) ? kio_now() + timeout_s : -1;
  for (;;) {
    uint32_t e = kio_parker_snapshot(pool_sub_pk(p, (uint32_t) p->sub_slot));
    atomic_fetch_or_explicit(p->full_waiters, bit, memory_order_seq_cst);
    atomic_thread_fence(memory_order_seq_cst);
    if (p->inj_ltail - atomic_load_explicit(head, memory_order_acquire) <
        (int64_t) p->hdr.inj_cap) {
      atomic_fetch_and_explicit(p->full_waiters, ~bit, memory_order_seq_cst);
      return 1;
    }
    if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0) {
      atomic_fetch_and_explicit(p->full_waiters, ~bit, memory_order_seq_cst);
      Rf_error("kioto: pool stopped");
    }
    if (atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0) {
      atomic_fetch_and_explicit(p->full_waiters, ~bit, memory_order_seq_cst);
      pool_orphan_teardown_try(p);
      Rf_error("kioto: pool stopped or owner dead");
    }
    long ms = KIO_INTERRUPT_BOUND_MS;
    if (deadline >= 0) {
      double rem = deadline - kio_now();
      if (rem <= 0) {
        atomic_fetch_and_explicit(p->full_waiters, ~bit,
                                  memory_order_seq_cst);
        return 0;
      }
      long rem_ms = (long) (rem * 1000) + 1;
      if (rem_ms < ms) ms = rem_ms;
    }
    kio_park(pool_sub_pk(p, (uint32_t) p->sub_slot), e, ms);
    atomic_fetch_and_explicit(p->full_waiters, ~bit, memory_order_seq_cst);
    R_CheckUserInterrupt();
  }
}

typedef struct kio_task_s {
  uint32_t idx;                  /* global result-slot index */
  uint64_t seq;
} kio_task;

static void pool_unpark_result_waiter(kio_pool *p, kio_rs_hdr *rs) {
  int32_t ws = atomic_load_explicit(&rs->waiter_slot, memory_order_acquire);
  if (ws >= 0 && (uint32_t) ws < p->hdr.max_submitters)
    kio_unpark(pool_sub_pk(p, (uint32_t) ws));
}

/* The handle finalizer's state machine (also invoked deliberately by
   kio_cancel's PENDING arm). A task keeper is never dropped here: CANCEL is
   not a release point — FREE strictly implies the worker is done with the
   entry, materialize included, so release-at-reuse stays safe. */
static void kio_task_finalizer(SEXP xp) {
  kio_task *t = (kio_task *) R_ExternalPtrAddr(xp);
  if (t == NULL) return;
  SEXP pool_xp = R_ExternalPtrProtected(xp);
  kio_pool *p = (kio_pool *) R_ExternalPtrAddr(pool_xp);
  if (p != NULL && !p->released && p->base != NULL &&
      p->self_pid == kio_self_pid()) {
    kio_rs_hdr *rs = pool_rs(p, t->idx);
    if (atomic_load_explicit(&rs->sequence, memory_order_relaxed) == t->seq) {
      for (;;) {
        int32_t st = atomic_load_explicit(&rs->status, memory_order_acquire);
        if (st == KIO_RS_PENDING) {
          int32_t expected = KIO_RS_PENDING;
          if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                      KIO_RS_CANCEL,
                                                      memory_order_seq_cst,
                                                      memory_order_acquire)) {
            pool_unpark_result_waiter(p, rs);
            break;
          }
        } else if (st == KIO_RS_OK || st == KIO_RS_ERR || st == KIO_RS_DIED) {
          /* the deliberate "never collected" drop: FREE releases the
             producing worker's result keeper and any region unlinks */
          int32_t w = atomic_load_explicit(&rs->worker_slot,
                                           memory_order_acquire);
          int32_t expected = st;
          if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                      KIO_RS_FREE,
                                                      memory_order_seq_cst,
                                                      memory_order_acquire)) {
            if (w >= 0 && (uint32_t) w < p->hdr.max_workers)
              kio_unpark(pool_wk_pk(p, (uint32_t) w));
            break;
          }
        } else {
          break;                 /* another party owns the free */
        }
      }
    }
  }
  free(t);
  R_ClearExternalPtr(xp);
}

/* Result slot from the submitter's own subrange. Reusing a FREE slot drops
   its previous task keeper — the lazy backstop — by overwrite at commit. */
static uint32_t pool_alloc_rs(kio_pool *p) {
  kio_sub_slot *me = &p->sub[p->sub_slot];
  for (uint32_t k = 0; k < me->rs_count; k++) {
    uint32_t cand = (p->rs_cursor + k) % me->rs_count;
    if (atomic_load_explicit(&pool_rs(p, me->rs_start + cand)->status,
                             memory_order_acquire) == KIO_RS_FREE)
      return cand;
  }
  Rf_error("kioto: result slots exhausted — collect or cancel outstanding "
           "tasks first");
}

/* The handle carries the sequence about to be installed at commit, so
   until the bump it is simply stale. */
static SEXP pool_make_task(kio_pool *p, SEXP xp, uint32_t rs_index) {
  kio_task *t = malloc(sizeof(*t));
  if (t == NULL) Rf_error("kioto: allocation failure");
  t->idx = rs_index;
  t->seq = atomic_load_explicit(&pool_rs(p, rs_index)->sequence,
                                memory_order_relaxed) + 1;
  SEXP txp = PROTECT(R_MakeExternalPtr(t, kio_task_tag, xp));
  R_RegisterCFinalizerEx(txp, kio_task_finalizer, TRUE);
  Rf_setAttrib(txp, R_ClassSymbol, kio_class_task);
  UNPROTECT(1);
  return txp;
}

/* Commit point: pin the task keeper — unconditionally, since whether a
   stream carries hook-emitted mori identifiers is not knowable without
   inspecting it — install the sequence, and open the slot as PENDING.
   Everything that can longjmp ran before this. */
static void pool_commit_rs(kio_pool *p, SEXP keepers, uint32_t local,
                           SEXP keep, kio_rs_hdr *rs) {
  SET_VECTOR_ELT(keepers, (R_xlen_t) local, keep);
  p->rs_cursor = local + 1;
  atomic_fetch_add_explicit(&rs->sequence, 1, memory_order_relaxed);
  atomic_store_explicit(&rs->waiter_slot, -1, memory_order_relaxed);
  atomic_store_explicit(&rs->worker_slot, -1, memory_order_relaxed);
  atomic_store_explicit(&rs->status, KIO_RS_PENDING, memory_order_release);
}

static void pool_fill_entry(kio_pool *p, kio_entry_hdr *eh,
                            uint32_t rs_index) {
  eh->task_id = ((uint64_t) p->sub_slot << 48) | ++p->task_counter;
  eh->rs_index = rs_index;
  eh->submitter_slot = (uint16_t) p->sub_slot;
  eh->pad = 0;
}

static void pool_execute(kio_pool *p, SEXP xp, int catching);
static void pool_announce(kio_pool *p);

/* Worker-side nested submit: the local-deque push. The worker becomes a
   submitter on first use — same keeper-table discipline, keyed by its own
   subrange, claimed lazily so pools that never nest spend no submitter
   slots on workers. The entry is staged directly into the worker's own
   deque slot and published by the bottom store; a full deque executes the
   task inline instead (work-first), so nested submit never blocks. */
static SEXP pool_submit_nested(kio_pool *p, SEXP xp, SEXP payload) {
  if (p->wk_slot < 0)
    Rf_error("kioto: not a worker handle");
  SEXP prot = R_ExternalPtrProtected(xp);
  if (p->sub_slot < 0) {
    pool_claim_sub_slot(p, &p->live_sub);
    SET_VECTOR_ELT(prot, 2, Rf_allocVector(VECSXP,
      (R_xlen_t) p->sub[p->sub_slot].rs_count));
  }
  SEXP keepers = VECTOR_ELT(prot, 2);
  uint32_t local = pool_alloc_rs(p);
  uint32_t rs_index = p->sub[p->sub_slot].rs_start + local;
  kio_rs_hdr *rs = pool_rs(p, rs_index);

  kio_wk_slot *w = &p->wk[p->wk_slot];
  int64_t b = atomic_load_explicit(&w->deque_bottom, memory_order_relaxed);
  int64_t t = atomic_load_explicit(&w->deque_top, memory_order_acquire);
  int inline_exec = b - t >= (int64_t) w->deque_cap;
  unsigned char *e = inline_exec ? p->scratch : deque_entry_at(p, w, b);
  kio_entry_hdr *eh = (kio_entry_hdr *) e;

  /* everything that can longjmp — staging, handle allocation, the
     evaluator lookup the inline path needs — runs before any observable
     mutation: an error leaves the entry unpublished and the slot FREE */
  if (inline_exec) (void) pool_eval_env(xp);
  SEXP keep = PROTECT(kio_payload_stage(&eh->ph, e + sizeof(kio_entry_hdr),
                                        p->inline_entry, payload));
  SEXP txp = PROTECT(pool_make_task(p, xp, rs_index));
  pool_commit_rs(p, keepers, local, keep, rs);
  pool_fill_entry(p, eh, rs_index);
  uint64_t tid = eh->task_id;
  if (!inline_exec) {
    atomic_store_explicit(&w->deque_bottom, b + 1, memory_order_release);
    if (b <= t) pool_wake_one_worker(p);   /* empty -> non-empty */
    pool_trace_emit(xp, "submit", tid);
  } else {
    pool_trace_emit(xp, "submit", tid);
    pool_announce(p);
    pool_execute(p, xp, 1);
  }
  UNPROTECT(2);
  return txp;
}

SEXP kio_pool_submit(SEXP xp, SEXP payload, SEXP timeout) {
  kio_pool *p = pool_get(xp);
  if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0)
    Rf_error("kioto: pool stopped");
  if (atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0) {
    pool_orphan_teardown_try(p);
    Rf_error("kioto: pool stopped or owner dead");
  }
  if (p->role == KIO_ROLE_WORKER)
    return pool_submit_nested(p, xp, payload);
  if (p->sub_slot < 0)
    Rf_error("kioto: not a submitter handle");
  SEXP keepers = VECTOR_ELT(R_ExternalPtrProtected(xp), 0);

  unsigned char *ring = pool_ring(p, (uint32_t) p->sub_slot);
  if (!pool_ring_space_wait(p, ring_head(ring), Rf_asReal(timeout)))
    Rf_error("kioto: submission timed out (injection ring full)");

  uint32_t local = pool_alloc_rs(p);
  uint32_t rs_index = p->sub[p->sub_slot].rs_start + local;
  kio_rs_hdr *rs = pool_rs(p, rs_index);

  /* staging and handle allocation can longjmp: nothing observable yet */
  unsigned char *e = ring_entry(p, ring, (uint64_t) p->inj_ltail);
  kio_entry_hdr *eh = (kio_entry_hdr *) e;
  SEXP keep = PROTECT(kio_payload_stage(&eh->ph, e + sizeof(kio_entry_hdr),
                                        p->inline_entry, payload));
  SEXP txp = PROTECT(pool_make_task(p, xp, rs_index));
  pool_commit_rs(p, keepers, local, keep, rs);
  pool_fill_entry(p, eh, rs_index);
  uint64_t tid = eh->task_id;
  p->inj_ltail++;
  atomic_store_explicit(ring_tail(ring), p->inj_ltail, memory_order_release);
  uint64_t bit = 1ull << p->sub_slot;
  if (!(atomic_load_explicit(p->inj_ready, memory_order_relaxed) & bit))
    atomic_fetch_or_explicit(p->inj_ready, bit, memory_order_seq_cst);
  pool_wake_one_worker(p);
  pool_trace_emit(xp, "submit", tid);

  UNPROTECT(2);
  return txp;
}

// Worker step ----------------------------------------------------------------------------

/* Drop a kept result once its slot has left OK/ERR (or been resequenced):
   the collector's or finalizer's FREE transition is the consumed-signal,
   its directed unpark what re-runs the reap on a parked worker. rk_pos
   keys the table by slot — at most one record per slot, updated in place
   when a reused slot republishes — so a stale record can never alias (and
   nil) a successor's keeper. Returns 1 when the record at i was retained,
   0 when it was removed (the swapped-in tail record then sits at i). */
static int pool_rk_visit(kio_pool *p, SEXP keepers, uint32_t i) {
  kio_rs_hdr *rs = pool_rs(p, p->rk[i].idx);
  int32_t st = atomic_load_explicit(&rs->status, memory_order_acquire);
  uint64_t seq = atomic_load_explicit(&rs->sequence, memory_order_relaxed);
  if ((st == KIO_RS_OK || st == KIO_RS_ERR) && seq == p->rk[i].seq)
    return 1;
  SET_VECTOR_ELT(keepers, (R_xlen_t) p->rk[i].idx, R_NilValue);
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
   the pool feels. */
static void pool_reap_result_keepers(kio_pool *p, SEXP keepers) {
  uint32_t i = 0;
  while (i < p->rk_n) i += (uint32_t) pool_rk_visit(p, keepers, i);
  p->rk_cursor = 0;
}

/* The busy-path reap: at most KIO_REAP_QUOTA visits under a rotating
   cursor, so a loaded worker's per-task reap cost is O(1) against any
   number of results outstanding — under a fire-then-collect backlog the
   old whole-table scan went quadratic. Consumption keeps pace as long as
   the quota exceeds the frees a task period can see; the idle-path sweep
   clears any residue. */
static void pool_reap_quota(kio_pool *p, SEXP keepers) {
  uint32_t lim = p->rk_n < KIO_REAP_QUOTA ? p->rk_n : KIO_REAP_QUOTA;
  for (uint32_t k = 0; k < lim && p->rk_n > 0; k++) {
    if (p->rk_cursor >= p->rk_n) p->rk_cursor = 0;
    p->rk_cursor += (uint32_t) pool_rk_visit(p, keepers, p->rk_cursor);
  }
}

/* Growth is split from recording so it can run before the publish CAS: an
   allocation failure after publish would leave a pinned keeper the reap
   never visits. First use also sizes the slot -> record map. */
static void pool_rk_reserve(kio_pool *p) {
  if (p->rk_pos == NULL) {
    p->rk_pos = calloc(p->hdr.result_slots, sizeof(*p->rk_pos));
    if (p->rk_pos == NULL) Rf_error("kioto: allocation failure");
  }
  if (p->rk_n < p->rk_cap) return;
  uint32_t cap = p->rk_cap == 0 ? 64 : p->rk_cap * 2;
  struct kio_rk_s *rk = realloc(p->rk, cap * sizeof(*rk));
  if (rk == NULL) Rf_error("kioto: allocation failure");
  p->rk = rk;
  p->rk_cap = cap;
}

static void pool_rk_add(kio_pool *p, uint32_t idx, uint64_t seq) {
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
static void pool_announce(kio_pool *p) {
  kio_entry_hdr *eh = (kio_entry_hdr *) p->scratch;
  if (eh->rs_index >= p->hdr.result_slots)
    Rf_error("kioto: corrupt pool entry");
  kio_wk_slot *me = &p->wk[p->wk_slot];
  atomic_store_explicit(&me->in_flight_rs, (int32_t) eh->rs_index,
                        memory_order_relaxed);
  atomic_store_explicit(&me->in_flight_seq,
                        atomic_load_explicit(&pool_rs(p, eh->rs_index)->
                                             sequence,
                                             memory_order_relaxed),
                        memory_order_relaxed);
}

static void pool_announce_clear(kio_pool *p) {
  atomic_store_explicit(&p->wk[p->wk_slot].in_flight_rs, -1,
                        memory_order_relaxed);
}

/* Mirror the local counters into the slot — only at the park announce,
   post-park, the fairness tick, step returns, and leave. Never per task:
   the stat_* fields share line 1 with deque_top, and a per-task write
   would reintroduce exactly the thief-CAS pingpong that line's layout
   avoids. Under load the mirrors lag by up to one tick (61 claims); a
   parked or departed worker's are exact. */
static void pool_stats_publish(kio_pool *p) {
  kio_wk_slot *me = &p->wk[p->wk_slot];
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
static int pool_claim_rings(kio_pool *p, int use_mask) {
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
      memcpy(p->scratch, ring_entry(p, ring, (uint64_t) head), p->hdr.slot);
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
          if (w & bit) kio_unpark(pool_sub_pk(p, s));
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
static int pool_deque_push(kio_pool *p, const unsigned char *entry) {
  kio_wk_slot *me = &p->wk[p->wk_slot];
  int64_t b = atomic_load_explicit(&me->deque_bottom, memory_order_relaxed);
  int64_t t = atomic_load_explicit(&me->deque_top, memory_order_acquire);
  if (b - t >= (int64_t) me->deque_cap) return 0;
  memcpy(deque_entry_at(p, me, b), entry, p->hdr.slot);
  atomic_store_explicit(&me->deque_bottom, b + 1, memory_order_release);
  return 1;
}

/* Chase-Lev take (Le et al. orderings): decrement bottom, seq_cst fence,
   load top; the last element resolves the owner-vs-thief race by CAS on
   top. The entry is copied and announced before the claim can commit —
   only the owner writes the buffer, so the pre-decrement copy is stable —
   and the announce is cleared on the lost race. */
static int pool_deque_pop(kio_pool *p) {
  kio_wk_slot *me = &p->wk[p->wk_slot];
  int64_t b = atomic_load_explicit(&me->deque_bottom, memory_order_relaxed);
  int64_t t = atomic_load_explicit(&me->deque_top, memory_order_acquire);
  if (t >= b) return 0;
  b--;
  memcpy(p->scratch, deque_entry_at(p, me, b), p->hdr.slot);
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

enum { KIO_STEAL_EMPTY = 0, KIO_STEAL_GOT, KIO_STEAL_ABORT };

/* Attempt REAPING -> FREE. Conclusive only with top/bottom re-loaded after
   the status acquire (an earlier bottom read may predate the leaver's final
   writes): for an orphaned deque bottom is static and top monotonic, so
   top >= bottom then means truly drained, and any observer may free the
   slot — which closes the race where a thief empties the deque while the
   owner still reads LEAVING. Returns 1 when the deque is drained. */
static int pool_reaping_free(kio_pool *p, kio_wk_slot *w) {
  if (deque_nonempty(w)) return 0;
  int32_t expected = KIO_WK_REAPING;
  atomic_compare_exchange_strong_explicit(&w->status, &expected, KIO_WK_FREE,
                                          memory_order_seq_cst,
                                          memory_order_relaxed);
  return 1;
}

/* Chase-Lev steal: copy the entry at top, then CAS top to claim it; only
   the CAS publishes the theft, so a lost race or a torn copy from the
   owner lapping the buffer is discarded unobserved. An orphaned (REAPING)
   deque is consumed through this same path; whoever observes it drained
   returns the slot to FREE. */
static int pool_steal_from(kio_pool *p, uint32_t v) {
  kio_wk_slot *w = &p->wk[v];
  int64_t t = atomic_load_explicit(&w->deque_top, memory_order_acquire);
  atomic_thread_fence(memory_order_seq_cst);
  int64_t b = atomic_load_explicit(&w->deque_bottom, memory_order_acquire);
  if (t >= b) {
    if (atomic_load_explicit(&w->status, memory_order_acquire) ==
        KIO_WK_REAPING && !pool_reaping_free(p, w))
      return KIO_STEAL_ABORT;   /* orphaned and nonempty after all: retry */
    return KIO_STEAL_EMPTY;
  }
  memcpy(p->scratch, deque_entry_at(p, w, t), p->hdr.slot);
  pool_announce(p);
  if (!atomic_compare_exchange_strong_explicit(&w->deque_top, &t, t + 1,
                                               memory_order_seq_cst,
                                               memory_order_relaxed)) {
    pool_announce_clear(p);
    return KIO_STEAL_ABORT;
  }
  if (atomic_load_explicit(&w->status, memory_order_acquire) ==
      KIO_WK_REAPING)
    pool_reaping_free(p, w);
  p->st_steals++;
  return KIO_STEAL_GOT;
}

static uint64_t pool_rng(kio_pool *p) {   /* xorshift64 */
  uint64_t x = p->rng;
  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;
  return p->rng = x;
}

#define KIO_STEAL_ROUNDS 4
#define KIO_HELP_DEPTH_LIMIT 32

/* Random victim, one attempt per victim, bounded rounds; EMPTY and ABORT
   alike move to the next victim. Victims are LIVE and REAPING slots — or
   REAPING only for a help-mode collector at its depth limit, since live
   peers' deques have a guaranteed executor (their owner) while ownerless
   work does not. Exhausting the rounds falls through to the caller's next
   tier (injection scan, then the pre-park spin + announce-then-rescan),
   which is what makes a missed steal safe. */
static int pool_steal_any(kio_pool *p, int reaping_only) {
  uint32_t mw = p->hdr.max_workers;
  if (mw <= 1) return 0;
  for (int r = 0; r < KIO_STEAL_ROUNDS; r++) {
    uint32_t start = (uint32_t) (pool_rng(p) % mw);
    for (uint32_t k = 0; k < mw; k++) {
      uint32_t i = (start + k) % mw;
      if ((int) i == p->wk_slot) continue;
      int32_t st = atomic_load_explicit(&p->wk[i].status,
                                        memory_order_acquire);
      if (st != KIO_WK_REAPING && (reaping_only || st != KIO_WK_LIVE))
        continue;
      if (pool_steal_from(p, i) == KIO_STEAL_GOT) return 1;
      if (st == KIO_WK_LIVE && deque_nonempty(&p->wk[i]))
        p->probe_victim = i;
    }
  }
  return 0;
}

#define KIO_PROBE_STREAK 16

static int pool_steal(kio_pool *p) {
  p->probe_victim = UINT32_MAX;
  if (pool_steal_any(p, 0)) {
    p->probe_streak = 0;
    return 1;
  }
  /* thief backstop: repeated failures against an apparently-live,
     apparently-nonempty victim warrant one death probe — the cross-check
     for a reap the listener never ran */
  if (p->probe_victim != UINT32_MAX &&
      ++p->probe_streak >= KIO_PROBE_STREAK) {
    p->probe_streak = 0;
    pool_probe_worker(p, p->probe_victim);
  }
  return 0;
}

/* The fairness tick's full scan: every injection ring unfiltered by the
   ready mask, then every REAPING slot's orphaned deque — the bound on
   external-submission (and ownerless-work) latency when a saturated pool
   never otherwise falls through its local tiers. */
static int pool_fairness_scan(kio_pool *p) {
  if (pool_claim_rings(p, 0)) return 1;
  for (uint32_t i = 0; i < p->hdr.max_workers; i++) {
    if ((int) i == p->wk_slot) continue;
    if (atomic_load_explicit(&p->wk[i].status, memory_order_acquire) ==
        KIO_WK_REAPING && pool_steal_from(p, i) == KIO_STEAL_GOT)
      return 1;
  }
  return 0;
}

/* One claim attempt in tier order. On success the entry is in scratch and
   announced; the caller executes it. */
static int pool_next_task(kio_pool *p) {
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
static int pool_work_hint(kio_pool *p) {
  if (atomic_load_explicit(p->inj_ready, memory_order_acquire) != 0)
    return 1;
  for (uint32_t i = 0; i < p->hdr.max_workers; i++) {
    int32_t st = atomic_load_explicit(&p->wk[i].status,
                                      memory_order_acquire);
    if ((st == KIO_WK_LIVE || st == KIO_WK_REAPING) &&
        deque_nonempty(&p->wk[i]))
      return 1;
  }
  return 0;
}

/* Unfiltered work check for the pre-park rescan, covering every claim
   source — injection rings (repairing a stale-clear ready bit as it goes)
   and every LIVE or REAPING deque, own included. */
static int pool_any_work(kio_pool *p) {
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
    if ((st == KIO_WK_LIVE || st == KIO_WK_REAPING) &&
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

/* Fail the dead worker's announced in-flight task, unpark every waiter its
   orphaned deque names (their help scans then steal from it), and either
   wake a drainer or free the emptied slot. Read-only walk; resumable. */
static void pool_orphan_and_finalize(kio_pool *p, kio_wk_slot *w,
                                     uint32_t slot) {
  int32_t inf = atomic_load_explicit(&w->in_flight_rs, memory_order_acquire);
  if (inf >= 0 && (uint32_t) inf < p->hdr.result_slots) {
    kio_rs_hdr *rs = pool_rs(p, (uint32_t) inf);
    if (atomic_load_explicit(&rs->sequence, memory_order_relaxed) ==
        atomic_load_explicit(&w->in_flight_seq, memory_order_relaxed)) {
      int32_t expected = KIO_RS_PENDING;
      if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                  KIO_RS_DIED,
                                                  memory_order_seq_cst,
                                                  memory_order_relaxed)) {
        pool_unpark_result_waiter(p, rs);
      } else if (expected == KIO_RS_CANCEL) {
        /* handle already dropped: no collector waits; return the slot */
        atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                KIO_RS_FREE,
                                                memory_order_seq_cst,
                                                memory_order_relaxed);
      }
    }
    atomic_store_explicit(&w->in_flight_rs, -1, memory_order_relaxed);
  }
  int64_t b = atomic_load_explicit(&w->deque_bottom, memory_order_acquire);
  for (int64_t i = atomic_load_explicit(&w->deque_top, memory_order_acquire);
       i < b; i++) {
    kio_entry_hdr *eh = (kio_entry_hdr *) deque_entry_at(p, w, i);
    if (eh->rs_index < p->hdr.result_slots)
      pool_unpark_result_waiter(p, pool_rs(p, eh->rs_index));
  }
  if (atomic_load_explicit(&w->deque_top, memory_order_acquire) < b) {
    /* orphaned work must drain even when no waiter is parked */
    pool_wake_one_worker_from(p, slot);
  } else {
    int32_t expected = KIO_WK_REAPING;
    atomic_compare_exchange_strong_explicit(&w->status, &expected,
                                            KIO_WK_FREE,
                                            memory_order_seq_cst,
                                            memory_order_relaxed);
  }
}

/* Precondition: the caller holds the slot's liveness lock. */
static void pool_reap_worker(kio_pool *p, uint32_t slot) {
  kio_wk_slot *w = &p->wk[slot];
  for (;;) {
    int32_t expected = atomic_load_explicit(&w->status,
                                            memory_order_acquire);
    if (expected == KIO_WK_LIVE || expected == KIO_WK_LEAVING) {
      if (!atomic_compare_exchange_strong_explicit(&w->status, &expected,
                                                   KIO_WK_REAPING,
                                                   memory_order_seq_cst,
                                                   memory_order_relaxed))
        continue;
      pool_orphan_and_finalize(p, w, slot);
    } else if (expected == KIO_WK_REAPING) {
      /* predecessor reaper died mid-walk: re-running it is harmless */
      pool_orphan_and_finalize(p, w, slot);
    } else if (expected == KIO_WK_CLAIMING) {
      /* died between its lock acquire and CLAIMING -> LIVE: deque
         uninitialized, nothing in flight — lock-before-CAS in the join is
         what makes this state conclusive for a lock holder */
      if (!atomic_compare_exchange_strong_explicit(&w->status, &expected,
                                                   KIO_WK_FREE,
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
static int pool_probe_worker(kio_pool *p, uint32_t slot) {
  kio_wk_slot *w = &p->wk[slot];
  if (atomic_load_explicit(&w->status, memory_order_acquire) == KIO_WK_FREE)
    return 0;
  if (p->live_all != NULL) {
    if (kio_live_try(p->live_all[slot]) != KIO_LIVE_ACQUIRED) return 0;
    pool_reap_worker(p, slot);
    kio_live_unlock(p->live_all[slot]);
    return 1;
  }
  char path[1024];
  intptr_t h;
  if (pool_live_path(p, path, sizeof(path), "wk", slot) != 0) return 0;
  if (kio_live_open_existing(path, &h) != 0) return 0;
  uint64_t dev, ino;
  int dead = kio_live_ident(h, &dev, &ino) == 0 &&
    dev == w->live_dev && ino == w->live_ino &&
    kio_live_try(h) == KIO_LIVE_ACQUIRED;
  if (dead) pool_reap_worker(p, slot);
  kio_live_close(h);
  return dead;
}

/* Precondition: the caller holds the dead submitter's liveness lock.
   Cancels its PENDING slots (mid-execution workers observe the publish-CAS
   failure and discard), frees its published-but-uncollected results
   (releasing the producing workers' keepers), and leaves CANCEL slots
   alone — they free at pop, which is what keeps release-at-reuse safe for
   entries still queued in the dead submitter's ring. */
static void pool_reap_submitter(kio_pool *p, uint32_t j) {
  kio_sub_slot *s = &p->sub[j];
  int32_t expected = KIO_SUB_LIVE;
  if (!atomic_compare_exchange_strong_explicit(&s->status, &expected,
                                               KIO_SUB_REAPING,
                                               memory_order_seq_cst,
                                               memory_order_acquire) &&
      expected != KIO_SUB_REAPING)
    return;                                  /* FREE: nothing to do */
  for (uint32_t k = 0; k < s->rs_count; k++) {
    kio_rs_hdr *rs = pool_rs(p, s->rs_start + k);
    for (;;) {
      int32_t st = atomic_load_explicit(&rs->status, memory_order_acquire);
      if (st == KIO_RS_PENDING) {
        int32_t e2 = KIO_RS_PENDING;
        if (!atomic_compare_exchange_strong_explicit(&rs->status, &e2,
                                                     KIO_RS_CANCEL,
                                                     memory_order_seq_cst,
                                                     memory_order_relaxed))
          continue;
        pool_unpark_result_waiter(p, rs);
      } else if (st == KIO_RS_OK || st == KIO_RS_ERR || st == KIO_RS_DIED) {
        int32_t wk = atomic_load_explicit(&rs->worker_slot,
                                          memory_order_acquire);
        int32_t e2 = st;
        if (!atomic_compare_exchange_strong_explicit(&rs->status, &e2,
                                                     KIO_RS_FREE,
                                                     memory_order_seq_cst,
                                                     memory_order_relaxed))
          continue;
        if (wk >= 0 && (uint32_t) wk < p->hdr.max_workers)
          kio_unpark(pool_wk_pk(p, (uint32_t) wk));   /* keeper drop */
      }
      break;
    }
  }
  atomic_store_explicit(&s->status, KIO_SUB_FREE, memory_order_seq_cst);
}

static void pool_probe_submitter(kio_pool *p, uint32_t j) {
  if ((int) j == p->sub_slot) return;        /* our own held lock */
  kio_sub_slot *s = &p->sub[j];
  if (atomic_load_explicit(&s->status, memory_order_acquire) ==
      KIO_SUB_FREE)
    return;
  if (p->live_all != NULL) {
    intptr_t h = p->live_all[p->hdr.max_workers + j];
    if (kio_live_try(h) != KIO_LIVE_ACQUIRED) return;
    pool_reap_submitter(p, j);
    kio_live_unlock(h);
    return;
  }
  char path[1024];
  intptr_t h;
  if (pool_live_path(p, path, sizeof(path), "sub", j) != 0) return;
  if (kio_live_open_existing(path, &h) != 0) return;
  uint64_t dev, ino;
  if (kio_live_ident(h, &dev, &ino) == 0 &&
      dev == s->live_dev && ino == s->live_ino &&
      kio_live_try(h) == KIO_LIVE_ACQUIRED)
    pool_reap_submitter(p, j);
  kio_live_close(h);
}

/* The controller's per-worker death callback: OS notification -> lock
   verdict -> reap, entirely off the R main thread. A pid-reuse race is
   absorbed by the lock (the impostor holds nothing here). */
static void pool_wk_death_cb(void *arg) {
  struct kio_reap_ctx_s *c = arg;
  kio_pool *p = (kio_pool *) c->pool;
  if (kio_live_try(p->live_all[c->slot]) == KIO_LIVE_ACQUIRED) {
    pool_reap_worker(p, c->slot);
    kio_live_unlock(p->live_all[c->slot]);
  }
}

/* Owner-death cleanup: acquiring the owner lock (the kept fd from join)
   grants exclusive teardown; a caller finding it held knows teardown is in
   progress elsewhere and simply fails locally. The broadcast is exactly
   the stop broadcast; the tail is janitorial best-effort — liveness files
   by path, the region via the vendored dead-PID reaper (its name embeds
   the dead creator's pid). */
static void pool_remove_live_files(kio_pool *p) {
  char path[1024];
  for (uint32_t i = 0; i < p->hdr.max_workers; i++)
    if (pool_live_path(p, path, sizeof(path), "wk", i) == 0) remove(path);
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++)
    if (pool_live_path(p, path, sizeof(path), "sub", j) == 0) remove(path);
  if (pool_live_path(p, path, sizeof(path), "owner", 0) == 0) remove(path);
}

static void pool_orphan_teardown_try(kio_pool *p) {
  if (p->live_owner == 0 ||
      kio_live_try(p->live_owner) != KIO_LIVE_ACQUIRED)
    return;
  pool_shutdown_broadcast(p);
  pool_remove_live_files(p);
  int n = 0;
  char **list = mori_shm_reap(&n);
  for (int i = 0; i < n; i++) free(list[i]);
  free(list);
}

/* The publish tail shared by pool_execute and the unwind path
   (kio_pool_fail_inflight): stage the outcome into the result slot, CAS it
   OK/ERR, pin the keeper, wake the waiter — or consume a concurrent CANCEL
   and probe the (possibly dead) submitter. Retires the in-flight announce.
   Payload writes are plain stores into a slot no allocator can touch
   (status stays PENDING/CANCEL until the FREE transition); the publish CAS
   is the release barrier a collector's acquire load pairs with. */
static int pool_publish_result(kio_pool *p, SEXP xp, uint32_t rs_index,
                               uint16_t sub_slot, uint64_t seq, int ok,
                               SEXP value) {
  kio_rs_hdr *rs = pool_rs(p, rs_index);
  pool_rk_reserve(p);
  SEXP keep = PROTECT(kio_payload_stage(&rs->ph,
                                        (unsigned char *) rs +
                                        sizeof(kio_rs_hdr),
                                        p->inline_rs, value));
  int32_t expected = KIO_RS_PENDING;
  int published =
    atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                            ok ? KIO_RS_OK : KIO_RS_ERR,
                                            memory_order_seq_cst,
                                            memory_order_acquire);
  if (published) {
    SET_VECTOR_ELT(VECTOR_ELT(R_ExternalPtrProtected(xp), 0),
                   (R_xlen_t) rs_index, keep);
    pool_rk_add(p, rs_index, seq);
    pool_unpark_result_waiter(p, rs);
  } else {
    /* cancelled while we ran: drop the result, return the slot */
    expected = KIO_RS_CANCEL;
    atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                            KIO_RS_FREE,
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
  UNPROTECT(1);
  return published;
}

/* Executes the claimed, announced entry in scratch and publishes into its
   result slot. Reentrant: help-mode and nested-submit execution recurse
   through here from inside Rf_eval, and every claim path reuses scratch —
   so everything needed from the entry and the announce is copied out
   before the eval. */
static void pool_execute(kio_pool *p, SEXP xp, int catching) {
  (void) pool_eval_env(xp);
  kio_wk_slot *me = &p->wk[p->wk_slot];
  kio_entry_hdr *eh = (kio_entry_hdr *) p->scratch;
  uint32_t rs_index = eh->rs_index;
  uint16_t sub_slot = eh->submitter_slot;
  uint64_t task_id = eh->task_id;
  kio_rs_hdr *rs = pool_rs(p, rs_index);
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
      KIO_RS_CANCEL) {
    int32_t expected = KIO_RS_CANCEL;
    atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                            KIO_RS_FREE,
                                            memory_order_seq_cst,
                                            memory_order_relaxed);
    if (sub_slot < p->hdr.max_submitters)
      pool_probe_submitter(p, sub_slot);
    atomic_store_explicit(&me->in_flight_rs, -1, memory_order_relaxed);
    pool_trace_emit(xp, "drop", task_id);
    return;
  }

  /* A vanished out-of-line entry payload means the enqueuer died and its
     region went along (Win32 mappings cannot outlive their creator): the
     task can never run anywhere — it fails as DIED exactly like a claimed
     task whose worker died, and the drain continues in this thief. */
  int gone = 0;
  SEXP pl = PROTECT(kio_payload_read(&eh->ph,
                                     p->scratch + sizeof(kio_entry_hdr),
                                     p->inline_entry, &gone));
  if (gone) {
    int32_t expected = KIO_RS_PENDING;
    if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                KIO_RS_DIED,
                                                memory_order_seq_cst,
                                                memory_order_relaxed)) {
      pool_unpark_result_waiter(p, rs);
    } else if (expected == KIO_RS_CANCEL) {
      atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                              KIO_RS_FREE,
                                              memory_order_seq_cst,
                                              memory_order_relaxed);
      if (sub_slot < p->hdr.max_submitters)
        pool_probe_submitter(p, sub_slot);
    }
    atomic_store_explicit(&me->in_flight_rs, -1, memory_order_relaxed);
    UNPROTECT(1);
    pool_trace_emit(xp, "drop", task_id);
    return;
  }
  pool_trace_emit(xp, "start", task_id);
  /* scratch (and eh with it) is dead from here: the eval below may claim
     into it */
  int ok = 1;
  SEXP value = PROTECT(pool_eval_task(p, xp, pl, catching, &ok));
  p->st_tasks++;
  int published = pool_publish_result(p, xp, rs_index, sub_slot, seq, ok,
                                      value);
  UNPROTECT(2);
  pool_trace_emit(xp, published ? (ok ? "done" : "error") : "drop", task_id);
}

/* The unwind path's publisher, called from R — worker_main, or the test
   harness's step wrapper — with the condition caught after a task's eval
   longjmped out of kio_pool_step. Publishes it as that task's ERR result
   and reports TRUE; FALSE means the error did not come from inside a task
   eval and the caller must treat it as fatal infrastructure failure. The
   in_eval gate is what keeps errors from staging, payload reads, or trace
   hooks on the fatal path. */
SEXP kio_pool_fail_inflight(SEXP xp, SEXP cond) {
  kio_pool *p = pool_get(xp);
  if (p->role != KIO_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("kioto: not a worker handle");
  if (!p->in_eval)
    return Rf_ScalarLogical(0);
  p->in_eval = 0;
  p->st_tasks++;
  int published = pool_publish_result(p, xp, p->cur_rs_index,
                                      p->cur_sub_slot, p->cur_seq, 0, cond);
  pool_trace_emit(xp, published ? "error" : "drop", p->cur_task_id);
  return Rf_ScalarLogical(1);
}

/* One worker-loop iteration: reap keepers, check flags, claim + execute one
   task or park. Returns 1 after executing a task, 0 on timeout / spurious
   wake, -1 on shutdown or owner death. The R-level worker_main loops over
   this; the in-process test harness single-steps it. The evaluator comes
   from the handle (kio_pool_set_eval), checked up front so a claim can
   never outrun a missing evaluator. */
SEXP kio_pool_step(SEXP xp, SEXP timeout) {
  kio_pool *p = pool_get(xp);
  if (p->role != KIO_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("kioto: not a worker handle");
  (void) pool_eval_env(xp);
  SEXP keepers = VECTOR_ELT(R_ExternalPtrProtected(xp), 0);
  kio_wk_slot *me = &p->wk[p->wk_slot];
  uint64_t my_bit = 1ull << p->wk_slot;
  double timeout_s = Rf_asReal(timeout);
  double deadline = R_FINITE(timeout_s) ? kio_now() + timeout_s : -1;

  /* heal any announce (or unwind-path eval flag) left dangling by an
     interrupt longjmp out of a previous step: a stale bit costs the pusher
     one failed CAS */
  p->in_eval = 0;
  atomic_store_explicit(&me->park_state, KIO_WPK_RUNNING,
                        memory_order_relaxed);
  atomic_fetch_and_explicit(p->parked_workers, ~my_bit,
                            memory_order_seq_cst);

  for (;;) {
    pool_reap_quota(p, keepers);
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
      pool_execute(p, xp, 0);
      return Rf_ScalarInteger(1);
    }

    if (timeout_s <= 0) {
      pool_reap_result_keepers(p, keepers);
      pool_stats_publish(p);
      return Rf_ScalarInteger(0);
    }

    /* bounded spin before announcing: sub-µs submit gaps are absorbed
       without touching the parked_workers line */
    for (int i = 0; i < KIO_SPIN_ITERS; i++) {
      KIO_PAUSE();
      if (pool_work_hint(p)) break;
    }
    if (pool_work_hint(p))
      continue;

    /* going idle: sweep the whole keeper table, so a parked worker holds
       only what is genuinely uncollected */
    pool_reap_result_keepers(p, keepers);

    /* announce-then-rescan (the sleep race): either our rescan sees the
       push or the pusher's mask load sees our bit */
    uint32_t e = kio_parker_snapshot(pool_wk_pk(p, (uint32_t) p->wk_slot));
    atomic_store_explicit(&me->park_state, KIO_WPK_IDLE,
                          memory_order_relaxed);
    pool_stats_publish(p);
    atomic_fetch_or_explicit(p->parked_workers, my_bit,
                             memory_order_seq_cst);
    atomic_thread_fence(memory_order_seq_cst);
    if (pool_any_work(p) ||
        atomic_load_explicit(p->shutdown, memory_order_acquire) != 0 ||
        atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0) {
      atomic_fetch_and_explicit(p->parked_workers, ~my_bit,
                                memory_order_seq_cst);
      atomic_store_explicit(&me->park_state, KIO_WPK_RUNNING,
                            memory_order_relaxed);
      continue;
    }
    int32_t expected = KIO_WPK_IDLE;
    if (atomic_compare_exchange_strong_explicit(&me->park_state, &expected,
                                                KIO_WPK_PARKED,
                                                memory_order_seq_cst,
                                                memory_order_relaxed)) {
      long ms = -1;
      if (deadline >= 0) {
        double rem = deadline - kio_now();
        ms = rem <= 0 ? 0 : (long) (rem * 1000) + 1;
      }
      kio_park(pool_wk_pk(p, (uint32_t) p->wk_slot), e, ms);
      p->st_parks++;
    }
    atomic_fetch_and_explicit(p->parked_workers, ~my_bit,
                              memory_order_seq_cst);
    atomic_store_explicit(&me->park_state, KIO_WPK_RUNNING,
                          memory_order_relaxed);
    pool_stats_publish(p);
    R_CheckUserInterrupt();
    if (deadline >= 0 && kio_now() >= deadline) {
      pool_reap_result_keepers(p, keepers);
      return Rf_ScalarInteger(0);
    }
  }
}

/* Test-only: claim up to n injection entries and queue them on this
   worker's own deque instead of executing them — the deterministic way to
   populate a deque before Phase 3's nested submit exists. The full check
   precedes the claim (space only grows once we own the bottom), so a
   claimed entry can always be queued; the announce clears once the entry
   is safely in the deque, where worker death hands it to the REAPING
   consumption path instead of the in-flight reap. */
SEXP kio_pool_deque_pull(SEXP xp, SEXP n_sexp) {
  kio_pool *p = pool_get(xp);
  if (p->role != KIO_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("kioto: not a worker handle");
  kio_wk_slot *me = &p->wk[p->wk_slot];
  int n = Rf_asInteger(n_sexp);
  int was_empty = !deque_nonempty(me);
  int moved = 0;
  while (moved < n) {
    if (atomic_load_explicit(&me->deque_bottom, memory_order_relaxed) -
        atomic_load_explicit(&me->deque_top, memory_order_acquire) >=
        (int64_t) me->deque_cap)
      break;
    if (!pool_claim_rings(p, 1)) break;
    pool_deque_push(p, p->scratch);
    pool_announce_clear(p);
    moved++;
  }
  /* the nested-submit wake rule: a push taking the deque from empty to
     non-empty wakes one parked peer */
  if (moved > 0 && was_empty) pool_wake_one_worker(p);
  return Rf_ScalarInteger(moved);
}

// Collect and cancel ----------------------------------------------------------------------

static kio_task *task_get(SEXP xp, kio_pool **pool_out, SEXP *pool_xp_out) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != kio_task_tag)
    Rf_error("kioto: not a task handle");
  kio_task *t = (kio_task *) R_ExternalPtrAddr(xp);
  if (t == NULL) Rf_error("kioto: task handle is stale");
  SEXP pool_xp = R_ExternalPtrProtected(xp);
  kio_pool *p = pool_get(pool_xp);
  *pool_out = p;
  if (pool_xp_out != NULL) *pool_xp_out = pool_xp;
  return t;
}

SEXP kio_pool_collect(SEXP xp, SEXP timeout) {
  kio_pool *p;
  SEXP pool_xp;
  kio_task *t = task_get(xp, &p, &pool_xp);
  if (p->sub_slot < 0)
    Rf_error("kioto: not a submitter's task handle");
  kio_rs_hdr *rs = pool_rs(p, t->idx);
  double timeout_s = Rf_asReal(timeout);
  double deadline = -1;

  int32_t st;
  for (;;) {
    if (atomic_load_explicit(&rs->sequence, memory_order_relaxed) != t->seq)
      Rf_error("kioto: task handle already collected or invalidated");
    if (atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0)
      pool_orphan_teardown_try(p);   /* its cancel sweep ends this wait */
    st = atomic_load_explicit(&rs->status, memory_order_acquire);
    if (st != KIO_RS_PENDING) break;

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
    if (p->role == KIO_ROLE_WORKER && p->wk_slot >= 0) {
      int got = pool_deque_pop(p);
      if (!got)
        got = pool_steal_any(p, p->help_depth >= KIO_HELP_DEPTH_LIMIT);
      if (got) {
        p->st_helps++;
        p->help_depth++;
        pool_execute(p, pool_xp, 1);
        p->help_depth--;
        continue;
      }
    }

    if (timeout_s <= 0) return kio_sent_timeout;
    if (deadline < 0 && R_FINITE(timeout_s))
      deadline = kio_now() + timeout_s;

    /* announce -> fence -> re-check -> park bounded; the publishing worker
       reads waiter_slot after its publish CAS and unparks us */
    uint32_t e = kio_parker_snapshot(pool_sub_pk(p, (uint32_t) p->sub_slot));
    atomic_store_explicit(&rs->waiter_slot, p->sub_slot,
                          memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    if (atomic_load_explicit(&rs->status, memory_order_acquire) !=
        KIO_RS_PENDING)
      continue;
    long ms = KIO_INTERRUPT_BOUND_MS;
    if (deadline >= 0) {
      double rem = deadline - kio_now();
      if (rem <= 0) return kio_sent_timeout;
      long rem_ms = (long) (rem * 1000) + 1;
      if (rem_ms < ms) ms = rem_ms;
    }
    kio_park(pool_sub_pk(p, (uint32_t) p->sub_slot), e, ms);
    R_CheckUserInterrupt();
    if (atomic_load_explicit(&rs->status, memory_order_acquire) ==
        KIO_RS_PENDING) {
      /* backstop for a missed death notification, piggybacked on a wake
         that happened regardless — never a wakeup of its own */
      int32_t claimant = atomic_load_explicit(&rs->worker_slot,
                                              memory_order_acquire);
      if (claimant >= 0 && (uint32_t) claimant < p->hdr.max_workers)
        pool_probe_worker(p, (uint32_t) claimant);
      if (deadline >= 0 && kio_now() >= deadline &&
          atomic_load_explicit(&rs->status, memory_order_acquire) ==
          KIO_RS_PENDING)
        return kio_sent_timeout;
    }
  }

  switch (st) {
  case KIO_RS_OK:
  case KIO_RS_ERR: {
    /* materialize BEFORE the FREE transition — publication of FREE is what
       lets the worker's keeper reap unlink everything this payload
       references */
    SEXP v = PROTECT(kio_payload_read(&rs->ph,
                                      (unsigned char *) rs +
                                      sizeof(kio_rs_hdr), p->inline_rs,
                                      NULL));
    int32_t w = atomic_load_explicit(&rs->worker_slot, memory_order_acquire);
    if (t->idx >= p->sub[p->sub_slot].rs_start &&
        t->idx < p->sub[p->sub_slot].rs_start + p->sub[p->sub_slot].rs_count)
      SET_VECTOR_ELT(pool_task_keepers(p, pool_xp),
                     (R_xlen_t) (t->idx - p->sub[p->sub_slot].rs_start),
                     R_NilValue);
    int32_t expected = st;
    if (!atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                 KIO_RS_FREE,
                                                 memory_order_seq_cst,
                                                 memory_order_acquire)) {
      UNPROTECT(1);
      Rf_error("kioto: task handle already collected");
    }
    if (w >= 0 && (uint32_t) w < p->hdr.max_workers)
      kio_unpark(pool_wk_pk(p, (uint32_t) w));   /* keeper-drop signal */
    if (st == KIO_RS_ERR) {
      SEXP call = PROTECT(Rf_lang2(Rf_install("stop"), v));
      Rf_eval(call, R_BaseEnv);                  /* no return */
    }
    UNPROTECT(1);
    return v;
  }
  case KIO_RS_DIED: {
    /* terminal like ERR, but status-word only: the reaper wrote no
       payload (see the DIED note in kioto.h) */
    if (t->idx >= p->sub[p->sub_slot].rs_start &&
        t->idx < p->sub[p->sub_slot].rs_start + p->sub[p->sub_slot].rs_count)
      SET_VECTOR_ELT(pool_task_keepers(p, pool_xp),
                     (R_xlen_t) (t->idx - p->sub[p->sub_slot].rs_start),
                     R_NilValue);
    int32_t expected = KIO_RS_DIED;
    if (!atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                 KIO_RS_FREE,
                                                 memory_order_seq_cst,
                                                 memory_order_relaxed))
      Rf_error("kioto: task handle already collected");
    Rf_error("kioto: worker died while executing this task");
  }
  case KIO_RS_CANCEL:
    /* the task keeper releases at slot reuse, not here — the worker may not
       have materialized yet */
    Rf_error("kioto: task cancelled or pool stopped");
  case KIO_RS_FREE:
  default:
    Rf_error("kioto: task handle already collected");
  }
}

/* Advisory and discard-only, never preemptive: a task already executing runs
   to completion and its result is dropped by the worker's failed publish
   CAS. */
SEXP kio_pool_cancel(SEXP xp) {
  kio_pool *p;
  kio_task *t = task_get(xp, &p, NULL);
  kio_rs_hdr *rs = pool_rs(p, t->idx);
  if (atomic_load_explicit(&rs->sequence, memory_order_relaxed) != t->seq)
    return Rf_ScalarLogical(FALSE);
  int32_t expected = KIO_RS_PENDING;
  if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                              KIO_RS_CANCEL,
                                              memory_order_seq_cst,
                                              memory_order_relaxed)) {
    pool_unpark_result_waiter(p, rs);
    return Rf_ScalarLogical(TRUE);
  }
  return Rf_ScalarLogical(FALSE);
}

// Stop and introspection ---------------------------------------------------------------------

SEXP kio_pool_stop_call(SEXP xp, SEXP timeout) {
  kio_pool *p = pool_peek(xp);
  if (p == NULL) return Rf_ScalarLogical(TRUE);   /* stop is idempotent */
  if (p->role != KIO_ROLE_CONTROLLER)
    Rf_error("kioto: only the controller can stop a pool");
  SEXP prot = R_ExternalPtrProtected(xp);

  pool_shutdown_broadcast(p);

  /* wait for workers to take their clean-exit path; their liveness locks
     release with their fds either way */
  double deadline = kio_now() + Rf_asReal(timeout);
  int clean;
  for (;;) {
    clean = 1;
    for (uint32_t i = 0; i < p->hdr.max_workers; i++) {
      int32_t st = atomic_load_explicit(&p->wk[i].status,
                                        memory_order_acquire);
      if (st == KIO_WK_LIVE || st == KIO_WK_LEAVING || st == KIO_WK_CLAIMING)
        clean = 0;
    }
    if (clean || kio_now() >= deadline) break;
    for (uint32_t i = 0; i < p->hdr.max_workers; i++)
      kio_unpark(pool_wk_pk(p, i));
    uint32_t e = kio_parker_snapshot(pool_sub_pk(p, 0));
    kio_park(pool_sub_pk(p, 0), e, 50);
    R_CheckUserInterrupt();
  }

  /* teardown sweep: probe + reap whatever did not exit cleanly — dead
     workers (their in-flight tasks fail, their deques orphan) and dead or
     detached submitters (their result slots release). A live hung worker
     stays unreaped, correctly: the lock adjudicates exit, not stall. */
  for (uint32_t i = 0; i < p->hdr.max_workers; i++)
    pool_probe_worker(p, i);
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++)
    pool_probe_submitter(p, j);

  pool_unlink_names(p, prot);
  pool_release(p);
  return Rf_ScalarLogical(clean);
}

SEXP kio_pool_status_call(SEXP xp) {
  kio_pool *p = pool_get(xp);
  const char *names[] = {"name", "role", "max_workers", "max_submitters",
                         "injection_cap", "result_slots", "slot_size",
                         "workers", "parked", "submitters", "injection",
                         "tasks", "deque", "shutdown", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(p->shm.name));
  SET_VECTOR_ELT(out, 1, Rf_mkString(
    p->role == KIO_ROLE_CONTROLLER ? "controller" :
    p->role == KIO_ROLE_WORKER ? "worker" : "submitter"));
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
    if (st >= KIO_RS_PENDING && st <= KIO_RS_DIED)
      INTEGER(tasks)[st - KIO_RS_PENDING]++;
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
   stat_* fields (see the kio_wk_slot comment for their publish cadence);
   per-submitter injection totals are the ring positions themselves —
   tail = entries ever published, head = entries ever claimed, both
   monotonic from zero — so the wire state is the metric and the submit
   path writes nothing extra. */
SEXP kio_pool_stats_call(SEXP xp) {
  kio_pool *p = pool_get(xp);
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
    kio_wk_slot *w = &p->wk[i];
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

  const char *snames[] = {"status", "pid", "injected", "claimed", ""};
  SEXP sb = Rf_mkNamed(VECSXP, snames);
  SET_VECTOR_ELT(out, 1, sb);
  SET_VECTOR_ELT(sb, 0, Rf_allocVector(INTSXP, (R_xlen_t) ms));
  for (int f = 1; f < 4; f++)
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
  }
  UNPROTECT(1);
  return out;
}

/* Read-only region snapshot for debugging distributed state: per-slot
   registry detail, the three hot masks unpacked per slot, and every
   non-FREE result slot. States can move between the count pass and the
   fill pass; short rows are padded with NA and trimmed on the R side. */
SEXP kio_pool_dump_call(SEXP xp) {
  kio_pool *p = pool_get(xp);
  uint32_t mw = p->hdr.max_workers, ms = p->hdr.max_submitters;
  const char *names[] = {"name", "shutdown", "workers", "submitters",
                         "tasks", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(p->shm.name));
  SET_VECTOR_ELT(out, 1, Rf_ScalarLogical(
    (int) atomic_load_explicit(p->shutdown, memory_order_acquire)));

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
    kio_wk_slot *w = &p->wk[i];
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
    kio_sub_slot *s = &p->sub[j];
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
                              memory_order_acquire) != KIO_RS_FREE;
  const char *tnames[] = {"slot", "status", "sequence", "worker", "waiter",
                          ""};
  SEXP tk = Rf_mkNamed(VECSXP, tnames);
  SET_VECTOR_ELT(out, 4, tk);
  for (int f = 0; f < 5; f++)
    SET_VECTOR_ELT(tk, f, Rf_allocVector(f == 2 ? REALSXP : INTSXP,
                                         (R_xlen_t) n));
  uint32_t m = 0;
  for (uint32_t r = 0; r < p->hdr.result_slots && m < n; r++) {
    kio_rs_hdr *rs = pool_rs(p, r);
    int32_t st = atomic_load_explicit(&rs->status, memory_order_acquire);
    if (st == KIO_RS_FREE) continue;
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
