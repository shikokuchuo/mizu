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
   ipc-plan.md; the registry and slot structs in mov.h are its wire
   format. */

#include <stdlib.h>
#include <stdio.h>
#include "mov.h"
#include <R_ext/Utils.h>

enum { MOV_ROLE_CONTROLLER = 0, MOV_ROLE_WORKER, MOV_ROLE_SUBMITTER };

typedef struct mov_pool_s {
  mori_shm shm;                  /* our mapping; unmapped only in release */
  mov_pool_hdr hdr;
  unsigned char *base;
  int role;
  int released;
  long self_pid;                 /* fork guard */
  int wk_slot;                   /* our worker slot (-1 unless worker) */
  int sub_slot;                  /* our submitter slot (-1 unless we submit) */
  uint32_t inline_entry;         /* slot - sizeof(mov_entry_hdr) */
  uint32_t inline_rs;            /* slot - sizeof(mov_rs_hdr) */

  mov_wk_slot *wk;
  mov_sub_slot *sub;
  _Atomic uint64_t *inj_ready;
  _Atomic uint64_t *full_waiters;
  _Atomic uint32_t *shutdown;
  _Atomic uint64_t *parked_workers;
  unsigned char *rings;
  unsigned char *results;

  mov_parker *pks;               /* every entity: workers, then submitters */
  int pk_ok;

  intptr_t live_self;            /* our held lock (worker / submitter slot) */
  intptr_t live_owner;           /* kept fd on the owner file; 0 = not open */
  intptr_t *live_all;            /* controller: kept probe fds, wk then sub */
  char livedir[1024];

  /* submitter-local */
  uint32_t rs_cursor;
  uint64_t task_counter;
  int64_t inj_ltail;             /* producer-local tail */

  /* worker-local */
  unsigned char *scratch;        /* slot-sized claim copy buffer */
  uint32_t scan_start;           /* rotating ring-scan start */
  uint64_t claims;               /* fairness-tick counter (% 61) */
  uint64_t rng;                  /* xorshift state for victim selection */
  struct mov_rk_s { uint32_t idx; uint64_t seq; } *rk;
  uint32_t rk_n, rk_cap;

  _Atomic int owner_dead;        /* death-listener flag: wake trigger only */
  mov_death_watch *watch;
} mov_pool;

static SEXP mov_pool_tag;
static SEXP mov_task_tag;
static SEXP mov_class_pool;
static SEXP mov_class_task;

void mov_pool_init(void) {
  mov_pool_tag = Rf_install("mov_pool");
  mov_task_tag = Rf_install("mov_task");
  mov_class_pool = Rf_mkString("mov_pool");
  R_PreserveObject(mov_class_pool);
  mov_class_task = Rf_mkString("mov_task");
  R_PreserveObject(mov_class_task);
}

// Layout ----------------------------------------------------------------------------

static uint64_t pool_ring_bytes(const mov_pool_hdr *h) {
  return MOV_INJ_META_SIZE + (uint64_t) h->inj_cap * h->slot;
}

/* Region size implied by a header; the create sizes with it and the attach
   validator checks against it, so both sides share one piece of offset math. */
static uint64_t pool_fixed_size(const mov_pool_hdr *h) {
  return 64 +
    (uint64_t) h->max_workers * sizeof(mov_wk_slot) +
    (uint64_t) h->max_submitters * sizeof(mov_sub_slot) +
    128 +
    (uint64_t) h->max_submitters * pool_ring_bytes(h) +
    (uint64_t) h->max_workers * ((uint64_t) h->deque_cap * h->slot) +
    (uint64_t) h->result_slots * h->slot +
    128;
}

static void pool_wire(mov_pool *p) {
  unsigned char *b = (unsigned char *) p->shm.addr;
  const mov_pool_hdr *h = &p->hdr;
  p->base = b;
  p->inline_entry = h->slot - (uint32_t) sizeof(mov_entry_hdr);
  p->inline_rs = h->slot - (uint32_t) sizeof(mov_rs_hdr);

  size_t off = 64;
  p->wk = (mov_wk_slot *) (b + off);
  off += (size_t) h->max_workers * sizeof(mov_wk_slot);
  p->sub = (mov_sub_slot *) (b + off);
  off += (size_t) h->max_submitters * sizeof(mov_sub_slot);
  p->inj_ready = (_Atomic uint64_t *) (b + off + MOV_TIER_READY_OFF);
  p->full_waiters = (_Atomic uint64_t *) (b + off + MOV_TIER_FULL_OFF);
  off += 128;
  p->rings = b + off;
  off += (size_t) h->max_submitters * pool_ring_bytes(h);
  off += (size_t) h->max_workers * ((size_t) h->deque_cap * h->slot);
  p->results = b + off;
  off += (size_t) h->result_slots * h->slot;
  p->shutdown = (_Atomic uint32_t *) (b + off + MOV_CTRL_SHUTDOWN_OFF);
  p->parked_workers = (_Atomic uint64_t *) (b + off + MOV_CTRL_PARKED_OFF);
}

static unsigned char *pool_ring(mov_pool *p, uint32_t s) {
  return p->rings + (size_t) s * pool_ring_bytes(&p->hdr);
}

static _Atomic int64_t *ring_tail(unsigned char *r) {
  return (_Atomic int64_t *) (r + MOV_INJ_TAIL_OFF);
}

static _Atomic int64_t *ring_head(unsigned char *r) {
  return (_Atomic int64_t *) (r + MOV_INJ_HEAD_OFF);
}

static unsigned char *ring_entry(mov_pool *p, unsigned char *r, uint64_t i) {
  return r + MOV_INJ_META_SIZE +
    (i & ((uint64_t) p->hdr.inj_cap - 1)) * p->hdr.slot;
}

static mov_rs_hdr *pool_rs(mov_pool *p, uint32_t idx) {
  return (mov_rs_hdr *) (p->results + (size_t) idx * p->hdr.slot);
}

static unsigned char *deque_entry_at(mov_pool *p, mov_wk_slot *w, int64_t i) {
  return p->base + w->deque_buf_off +
    ((uint64_t) i & ((uint64_t) w->deque_cap - 1)) * p->hdr.slot;
}

static int deque_nonempty(mov_wk_slot *w) {
  return atomic_load_explicit(&w->deque_top, memory_order_acquire) <
    atomic_load_explicit(&w->deque_bottom, memory_order_acquire);
}

static const char *pool_hdr_validate(const void *region, size_t region_size,
                                     mov_pool_hdr *out) {
  if (region_size < 64)
    return "region is smaller than a pool header";
  mov_pool_hdr h;
  memcpy(&h, region, sizeof(h));
  if (h.magic != MOV_POOL_MAGIC)
    return "bad magic: not an mov pool region";
  if (h.version != MOV_ABI_VERSION)
    return "ABI version mismatch: participant and controller were built "
           "against different mov wire formats";
  if (h.max_workers == 0 || h.max_workers > 64 ||
      h.max_submitters == 0 || h.max_submitters > 64)
    return "registry capacities out of range";
  if ((h.inj_cap & (h.inj_cap - 1)) != 0 || h.inj_cap < 2 ||
      (h.deque_cap & (h.deque_cap - 1)) != 0 || h.deque_cap < 2 ||
      (h.slot & (h.slot - 1)) != 0 || h.slot < 64 || h.slot > (1u << 20))
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

static mov_parker *pool_wk_pk(mov_pool *p, uint32_t i) {
  return &p->pks[i];
}

static mov_parker *pool_sub_pk(mov_pool *p, uint32_t j) {
  return &p->pks[p->hdr.max_workers + j];
}

/* Entity numbering (the Windows event-name key): workers 0..MW-1, submitters
   MW..MW+MS-1. Every participant attaches every entity's parker up front —
   submitters unpark workers, workers unpark submitters — with create = 1 only
   on the controller, before any spawn. */
static int pool_parkers_attach(mov_pool *p, int create) {
  uint32_t mw = p->hdr.max_workers, ms = p->hdr.max_submitters;
  p->pks = calloc(mw + ms, sizeof(mov_parker));
  if (p->pks == NULL) return -1;
  for (uint32_t i = 0; i < mw + ms; i++) {
    _Atomic uint32_t *epoch = i < mw ? &p->wk[i].park_epoch
                                     : &p->sub[i - mw].park_epoch;
    if (mov_parker_attach(&p->pks[i], epoch, p->shm.name, (int) i,
                          create) != 0) {
      for (uint32_t k = 0; k < i; k++) mov_parker_detach(&p->pks[k]);
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
static int pool_live_path(mov_pool *p, char *buf, size_t size,
                          const char *kind, uint32_t idx) {
  const char *suffix = p->shm.name + strlen(MORI_PREFIX_LITERAL);
  int n = strcmp(kind, "owner") == 0 ?
    snprintf(buf, size, "%s/mov_%s.owner", p->livedir, suffix) :
    snprintf(buf, size, "%s/mov_%s.%s.%u", p->livedir, suffix, kind, idx);
  return (n > 0 && (size_t) n < size) ? 0 : -1;
}

// Release ----------------------------------------------------------------------------

/* Full teardown of a handle's process-local state, idempotent. Never
   unlinks: the region name and liveness files are removed only by the
   controller's stop / destroy protocol. Order is load-bearing, as in the
   channel: the death watch and parkers reference the mapping. */
static void pool_release(mov_pool *p) {
  if (p->released) return;
  p->released = 1;
  if (p->watch != NULL) {
    mov_death_watch_stop(p->watch);
    p->watch = NULL;
  }
  if (p->pk_ok) {
    uint32_t n = p->hdr.max_workers + p->hdr.max_submitters;
    for (uint32_t i = 0; i < n; i++) mov_parker_detach(&p->pks[i]);
    p->pk_ok = 0;
  }
  free(p->pks);
  p->pks = NULL;
  if (p->shm.addr != NULL) mori_shm_close(&p->shm, 0);
  p->base = NULL;
  if (p->live_self != 0) {
    mov_live_close(p->live_self);
    p->live_self = 0;
  }
  if (p->live_owner != 0) {
    mov_live_close(p->live_owner);
    p->live_owner = 0;
  }
  if (p->live_all != NULL) {
    uint32_t n = p->hdr.max_workers + p->hdr.max_submitters;
    for (uint32_t i = 0; i < n; i++)
      if (p->live_all[i] != 0) mov_live_close(p->live_all[i]);
    free(p->live_all);
    p->live_all = NULL;
  }
}

/* The controller half of teardown, shared by mov_pool_stop, the startup
   walk-back, and the handle finalizer: broadcast shutdown, wake everyone,
   cancel every pending result, unlink the names. Waiting for workers is the
   caller's business (the finalizer cannot wait). */
static void pool_shutdown_broadcast(mov_pool *p) {
  atomic_store_explicit(p->shutdown, 1u, memory_order_seq_cst);
  for (uint32_t i = 0; i < p->hdr.max_workers; i++)
    mov_unpark(pool_wk_pk(p, i));
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++)
    if (atomic_load_explicit(p->full_waiters, memory_order_acquire) &
        (1ull << j))
      mov_unpark(pool_sub_pk(p, j));
  for (uint32_t r = 0; r < p->hdr.result_slots; r++) {
    mov_rs_hdr *rs = pool_rs(p, r);
    int32_t expected = MOV_RS_PENDING;
    if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                MOV_RS_CANCEL,
                                                memory_order_seq_cst,
                                                memory_order_relaxed)) {
      int32_t ws = atomic_load_explicit(&rs->waiter_slot,
                                        memory_order_acquire);
      if (ws >= 0 && (uint32_t) ws < p->hdr.max_submitters)
        mov_unpark(pool_sub_pk(p, (uint32_t) ws));
    }
  }
}

static void pool_unlink_names(mov_pool *p, SEXP prot) {
  SEXP host_ptr = VECTOR_ELT(prot, 1);
  if (host_ptr != R_NilValue) mori_host_finalizer(host_ptr);
  char path[1024];
  for (uint32_t i = 0; i < p->hdr.max_workers; i++)
    if (pool_live_path(p, path, sizeof(path), "wk", i) == 0) remove(path);
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++)
    if (pool_live_path(p, path, sizeof(path), "sub", j) == 0) remove(path);
  if (pool_live_path(p, path, sizeof(path), "owner", 0) == 0) remove(path);
}

// Handle access ---------------------------------------------------------------------

static mov_pool *pool_peek(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mov_pool_tag)
    Rf_error("mov: not a pool handle");
  mov_pool *p = (mov_pool *) R_ExternalPtrAddr(xp);
  if (p == NULL || p->released) return NULL;
  if (p->self_pid != mov_self_pid())
    Rf_error("mov: pool handles do not survive fork()");
  return p;
}

static mov_pool *pool_get(SEXP xp) {
  mov_pool *p = pool_peek(xp);
  if (p == NULL) Rf_error("mov: pool handle is closed");
  return p;
}

static void mov_pool_finalizer(SEXP xp) {
  mov_pool *p = (mov_pool *) R_ExternalPtrAddr(xp);
  if (p == NULL) return;
  if (!p->released && p->role == MOV_ROLE_CONTROLLER && p->base != NULL) {
    pool_shutdown_broadcast(p);
    pool_unlink_names(p, R_ExternalPtrProtected(xp));
  }
  pool_release(p);
  free(p->scratch);
  free(p->rk);
  free(p);
  R_ClearExternalPtr(xp);
}

/* prot layout: [0] keepers — the submitter's task keepers (length rs_count)
   or the worker's result keepers (length result_slots); [1] the host unlink
   extptr (controller only). */
static SEXP pool_make_handle(mov_pool *p, SEXP keepers, SEXP host_ptr) {
  SEXP prot = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(prot, 0, keepers);
  SET_VECTOR_ELT(prot, 1, host_ptr);
  SEXP xp = PROTECT(R_MakeExternalPtr(p, mov_pool_tag, prot));
  R_RegisterCFinalizerEx(xp, mov_pool_finalizer, TRUE);
  Rf_setAttrib(xp, R_ClassSymbol, mov_class_pool);
  UNPROTECT(2);
  return xp;
}

// Create (controller) -----------------------------------------------------------------

static int mov_pow2_u64(uint64_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

SEXP mov_pool_create(SEXP maxw_sexp, SEXP maxs_sexp, SEXP inj_sexp,
                     SEXP deque_sexp, SEXP rslots_sexp, SEXP slot_sexp,
                     SEXP livedir_sexp) {
  uint64_t maxw = (uint64_t) Rf_asInteger(maxw_sexp);
  uint64_t maxs = (uint64_t) Rf_asInteger(maxs_sexp);
  uint64_t inj_cap = (uint64_t) Rf_asInteger(inj_sexp);
  uint64_t deque_cap = (uint64_t) Rf_asInteger(deque_sexp);
  uint64_t rslots = (uint64_t) Rf_asInteger(rslots_sexp);
  uint64_t slot = (uint64_t) Rf_asInteger(slot_sexp);
  if (maxw < 1 || maxw > 64)
    Rf_error("mov: max_workers must be between 1 and 64");
  if (maxs < 1 || maxs > 64)
    Rf_error("mov: max_submitters must be between 1 and 64");
  if (!mov_pow2_u64(inj_cap) || inj_cap < 2 || inj_cap > (1u << 24))
    Rf_error("mov: injection_cap must be a power of two between 2 and 2^24");
  if (!mov_pow2_u64(deque_cap) || deque_cap < 2 || deque_cap > (1u << 24))
    Rf_error("mov: per_worker_cap must be a power of two between 2 and 2^24");
  if (!mov_pow2_u64(slot) || slot < 64 || slot > (1u << 20))
    Rf_error("mov: slot_size must be a power of two between 64 and 2^20");
  if (rslots < maxs || rslots > (1u << 24))
    Rf_error("mov: result_slots must be between max_submitters and 2^24");
  rslots = (rslots + maxs - 1) / maxs * maxs;   /* per-submitter partition */
  if (TYPEOF(livedir_sexp) != STRSXP || XLENGTH(livedir_sexp) != 1)
    Rf_error("mov: expected a liveness directory path");
  const char *livedir = CHAR(STRING_ELT(livedir_sexp, 0));
  size_t livedir_len = strlen(livedir);
  if (livedir_len == 0 || livedir_len > 900)
    Rf_error("mov: liveness directory path too long");

  mov_pool_hdr h = {
    .magic = MOV_POOL_MAGIC,
    .version = MOV_ABI_VERSION,
    .max_workers = (uint32_t) maxw,
    .max_submitters = (uint32_t) maxs,
    .inj_cap = (uint32_t) inj_cap,
    .deque_cap = (uint32_t) deque_cap,
    .result_slots = (uint32_t) rslots,
    .slot = (uint32_t) slot,
    .owner_pid = (uint64_t) mov_self_pid(),
  };
  uint64_t fixed = pool_fixed_size(&h);
  h.livedir_offset = MORI_ALIGN64(fixed);
  h.livedir_size = livedir_len;
  uint64_t total = h.livedir_offset + livedir_len;
  if (total > ((uint64_t) 1 << 46))
    Rf_error("mov: pool region too large");

  mov_pool *p = calloc(1, sizeof(*p));
  if (p == NULL) Rf_error("mov: allocation failure");
  int rc = mori_shm_create(&p->shm, (size_t) total);
  if (rc != MORI_OK) {
    free(p);
    const char *summary, *hint;
    mori_err_describe(rc, &summary, &hint);
    Rf_error("mov: cannot create pool region (%llu bytes): %s%s%s",
             (unsigned long long) total, summary,
             hint[0] != '\0' ? ". " : "", hint);
  }
  p->role = MOV_ROLE_CONTROLLER;
  p->self_pid = mov_self_pid();
  p->wk_slot = -1;
  p->sub_slot = 0;
  p->hdr = h;
  memcpy(p->livedir, livedir, livedir_len + 1);

  /* From here cleanup is the finalizer's: build the handle before anything
     that can longjmp. */
  SEXP host_ptr = PROTECT(mov_shm_wrap_host(&p->shm));
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
  if (p->live_all == NULL) Rf_error("mov: allocation failure");
  for (uint32_t i = 0; i < h.max_workers + h.max_submitters; i++) {
    int is_wk = i < h.max_workers;
    if (pool_live_path(p, path, sizeof(path), is_wk ? "wk" : "sub",
                       is_wk ? i : i - h.max_workers) != 0)
      Rf_error("mov: liveness file path too long");
    if (mov_live_open(path, &p->live_all[i]) != 0)
      Rf_error("mov: cannot create liveness file '%s'", path);
  }
  if (pool_live_path(p, path, sizeof(path), "owner", 0) != 0)
    Rf_error("mov: liveness file path too long");
  if (mov_live_open(path, &p->live_owner) != 0 ||
      mov_live_try(p->live_owner) != MOV_LIVE_ACQUIRED)
    Rf_error("mov: cannot lock owner liveness file '%s'", path);

  /* Windows: every entity's named parker event must exist before any spawn */
  if (pool_parkers_attach(p, 1) != 0)
    Rf_error("mov: cannot attach pool parkers");

  /* Claim submitter slot 0 for the calling process: lock-before-CAS, as in
     every join. */
  p->live_self = p->live_all[h.max_workers + 0];
  if (mov_live_try(p->live_self) != MOV_LIVE_ACQUIRED)
    Rf_error("mov: cannot lock submitter liveness file");
  mov_sub_slot *s0 = &p->sub[0];
  int32_t expected = MOV_SUB_FREE;
  if (!atomic_compare_exchange_strong_explicit(&s0->status, &expected,
                                               MOV_SUB_LIVE,
                                               memory_order_seq_cst,
                                               memory_order_relaxed))
    Rf_error("mov: submitter slot 0 is not free in a fresh region");
  s0->pid = (int64_t) p->self_pid;
  s0->rs_start = 0;
  s0->rs_count = (uint32_t) (rslots / maxs);
  mov_live_ident(p->live_self, &s0->live_dev, &s0->live_ino);
  p->rs_cursor = 0;

  UNPROTECT(3);
  return xp;
}

SEXP mov_pool_suffix(SEXP xp) {
  mov_pool *p = pool_get(xp);
  return Rf_mkString(p->shm.name + strlen(MORI_PREFIX_LITERAL));
}

/* Startup rendezvous: park on the creator's submitter-0 parker, re-checking
   worker slots 0..n-1 for LIVE on each wake; each worker unparks the creator
   on reaching LIVE. FALSE on deadline expiry — the caller walks the pool
   back via mov_pool_destroy. */
SEXP mov_pool_ready_wait(SEXP xp, SEXP n_sexp, SEXP timeout) {
  mov_pool *p = pool_get(xp);
  uint32_t n = (uint32_t) Rf_asInteger(n_sexp);
  if (n > p->hdr.max_workers) Rf_error("mov: more workers than the registry");
  double deadline = mov_now() + Rf_asReal(timeout);
  for (;;) {
    uint32_t e = mov_parker_snapshot(pool_sub_pk(p, 0));
    uint32_t live = 0;
    for (uint32_t i = 0; i < n; i++)
      live += atomic_load_explicit(&p->wk[i].status, memory_order_acquire) ==
        MOV_WK_LIVE;
    if (live == n) return Rf_ScalarLogical(TRUE);
    double rem = deadline - mov_now();
    if (rem <= 0) return Rf_ScalarLogical(FALSE);
    long ms = (long) (rem * 1000) + 1;
    if (ms > MOV_INTERRUPT_BOUND_MS) ms = MOV_INTERRUPT_BOUND_MS;
    mov_park(pool_sub_pk(p, 0), e, ms);
    R_CheckUserInterrupt();
  }
}

/* Startup walk-back and finalizer-free explicit destroy: broadcast so a
   late-joining worker exits instead of parking against a pool that gave up,
   then unlink everything. */
SEXP mov_pool_destroy(SEXP xp) {
  mov_pool *p = pool_get(xp);
  if (p->role != MOV_ROLE_CONTROLLER)
    Rf_error("mov: only the controller can destroy a pool");
  pool_shutdown_broadcast(p);
  pool_unlink_names(p, R_ExternalPtrProtected(xp));
  pool_release(p);
  return R_NilValue;
}

// Attach helpers ----------------------------------------------------------------------

static mov_pool *pool_open_common(const char *suffix, SEXP *xp_out,
                                  int keeper_len_is_rs) {
  for (const char *q = suffix; *q != '\0'; q++)
    if (!((*q >= '0' && *q <= '9') || (*q >= 'a' && *q <= 'f') || *q == '_'))
      Rf_error("mov: malformed region-name suffix");
  char name[MORI_NAME_MAX];
  int nn = snprintf(name, sizeof(name), "%s%s", MORI_PREFIX_LITERAL, suffix);
  if (nn <= 0 || (size_t) nn >= sizeof(name))
    Rf_error("mov: malformed region-name suffix");

  mov_pool *p = calloc(1, sizeof(*p));
  if (p == NULL) Rf_error("mov: allocation failure");
  if (mov_shm_open_rw(&p->shm, name) != 0) {
    free(p);
    Rf_error("mov: cannot open pool region '%s'", name);
  }
  p->self_pid = mov_self_pid();
  p->wk_slot = -1;
  p->sub_slot = -1;

  /* validate before touching any other field */
  const char *err = pool_hdr_validate(p->shm.addr, p->shm.size, &p->hdr);
  if (err != NULL) {
    mori_shm_close(&p->shm, 0);
    free(p);
    Rf_error("mov: invalid pool region: %s", err);
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
static void pool_owner_check(mov_pool *p) {
  char path[1024];
  if (pool_live_path(p, path, sizeof(path), "owner", 0) != 0)
    Rf_error("mov: liveness file path too long");
  if (mov_live_open(path, &p->live_owner) != 0)
    Rf_error("mov: cannot open owner liveness file '%s'", path);
  if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0 ||
      mov_live_try(p->live_owner) == MOV_LIVE_ACQUIRED)
    Rf_error("mov: pool stopped or owner dead");
}

static void pool_watch_owner(mov_pool *p, mov_parker *own_pk) {
  if ((long) p->hdr.owner_pid == p->self_pid) return;   /* in-process join */
  p->watch = mov_death_watch_start((long) p->hdr.owner_pid, &p->owner_dead,
                                   own_pk);
  if (p->watch == NULL)
    Rf_error("mov: cannot watch owner process %llu",
             (unsigned long long) p->hdr.owner_pid);
}

// Worker join --------------------------------------------------------------------------

SEXP mov_pool_worker_join(SEXP suffix_sexp, SEXP slot_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("mov: expected a region-name suffix");
  SEXP xp;
  mov_pool *p = pool_open_common(CHAR(STRING_ELT(suffix_sexp, 0)), &xp, 1);
  PROTECT(xp);
  p->role = MOV_ROLE_WORKER;

  uint32_t slot = (uint32_t) Rf_asInteger(slot_sexp);
  if (slot >= p->hdr.max_workers)
    Rf_error("mov: worker slot index out of range");
  pool_owner_check(p);

  /* Lock-before-CAS: what makes "CLAIMING + free lock" a reliable dead-worker
     signal for the Phase 4 reaper, and what fail-fasts against a leftover
     ghost from a previous spawn. */
  char path[1024];
  if (pool_live_path(p, path, sizeof(path), "wk", slot) != 0)
    Rf_error("mov: liveness file path too long");
  if (mov_live_open(path, &p->live_self) != 0 ||
      mov_live_try(p->live_self) != MOV_LIVE_ACQUIRED)
    Rf_error("mov: worker slot %u already held — stale spawn?", slot);
  mov_wk_slot *me = &p->wk[slot];
  int32_t expected = MOV_WK_FREE;
  if (!atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                               MOV_WK_CLAIMING,
                                               memory_order_seq_cst,
                                               memory_order_relaxed))
    Rf_error("mov: worker slot %u not free — stale spawn?", slot);
  p->wk_slot = (int) slot;

  me->pid = (int64_t) p->self_pid;
  mov_live_ident(p->live_self, &me->live_dev, &me->live_ino);
  atomic_store_explicit(&me->in_flight_rs, -1, memory_order_relaxed);
  atomic_store_explicit(&me->park_state, MOV_WPK_RUNNING,
                        memory_order_relaxed);

  if (pool_parkers_attach(p, 0) != 0)
    Rf_error("mov: cannot attach pool parkers");
  p->scratch = malloc(p->hdr.slot);
  if (p->scratch == NULL) Rf_error("mov: allocation failure");
  p->rng = ((uint64_t) p->self_pid * 0x9E3779B97F4A7C15ull) ^
    ((uint64_t) (mov_now() * 1e9)) ^ ((uint64_t) slot << 32);
  if (p->rng == 0) p->rng = 1;
  pool_watch_owner(p, pool_wk_pk(p, slot));

  expected = MOV_WK_CLAIMING;
  atomic_compare_exchange_strong_explicit(&me->status, &expected, MOV_WK_LIVE,
                                          memory_order_seq_cst,
                                          memory_order_relaxed);
  mov_unpark(pool_sub_pk(p, 0));   /* the creator's startup wait */

  UNPROTECT(1);
  return xp;
}

static void pool_unpark_result_waiter(mov_pool *p, mov_rs_hdr *rs);
static void pool_wake_one_worker(mov_pool *p);
static int pool_reaping_free(mov_pool *p, mov_wk_slot *w);

/* Clean worker exit. A nonempty deque is never drained anywhere: it
   becomes an ordinary steal target while the slot reads REAPING, and the
   observer of the drained deque returns the slot to FREE. Kept results are
   abandoned: the Phase 2 exits are shutdown and owner death, both of which
   cancel or orphan every outstanding collect anyway. */
SEXP mov_pool_leave(SEXP xp) {
  mov_pool *p = pool_peek(xp);
  if (p == NULL) return R_NilValue;
  if (p->role != MOV_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("mov: not a worker handle");
  mov_wk_slot *me = &p->wk[p->wk_slot];
  atomic_fetch_and_explicit(p->parked_workers, ~(1ull << p->wk_slot),
                            memory_order_seq_cst);
  int32_t expected = MOV_WK_LIVE;
  if (atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                              MOV_WK_LEAVING,
                                              memory_order_seq_cst,
                                              memory_order_relaxed)) {
    /* walk the deque read-only, unparking each entry's result waiter;
       thieves may be advancing top concurrently — a wake for an
       already-stolen entry is a spurious wake, absorbed by the re-check */
    int64_t t = atomic_load_explicit(&me->deque_top, memory_order_acquire);
    int64_t b = atomic_load_explicit(&me->deque_bottom,
                                     memory_order_acquire);
    for (int64_t i = t; i < b; i++) {
      mov_entry_hdr *eh = (mov_entry_hdr *) deque_entry_at(p, me, i);
      if (eh->rs_index < p->hdr.result_slots)
        pool_unpark_result_waiter(p, pool_rs(p, eh->rs_index));
    }
    if (atomic_load_explicit(&me->deque_top, memory_order_acquire) >= b) {
      expected = MOV_WK_LEAVING;
      atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                              MOV_WK_FREE,
                                              memory_order_seq_cst,
                                              memory_order_relaxed);
    } else {
      expected = MOV_WK_LEAVING;
      atomic_compare_exchange_strong_explicit(&me->status, &expected,
                                              MOV_WK_REAPING,
                                              memory_order_seq_cst,
                                              memory_order_relaxed);
      /* a thief that emptied the deque while we still read LEAVING saw
         nothing to free: re-check now that REAPING is published */
      if (!pool_reaping_free(p, me))
        pool_wake_one_worker(p);
    }
  }
  pool_release(p);   /* closes the liveness fd: the lock releases */
  return R_NilValue;
}

// Submitter join ------------------------------------------------------------------------

SEXP mov_pool_attach_call(SEXP suffix_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("mov: expected a region-name suffix");
  SEXP xp;
  mov_pool *p = pool_open_common(CHAR(STRING_ELT(suffix_sexp, 0)), &xp, 0);
  PROTECT(xp);
  p->role = MOV_ROLE_SUBMITTER;
  pool_owner_check(p);

  /* Lock-first slot claim, mirroring the worker join: a dead submitter is
     recognisable by its free liveness lock regardless of which side of the
     CAS it died on. */
  char path[1024];
  uint32_t per = p->hdr.result_slots / p->hdr.max_submitters;
  for (uint32_t j = 0; j < p->hdr.max_submitters; j++) {
    if (atomic_load_explicit(&p->sub[j].status, memory_order_acquire) !=
        MOV_SUB_FREE)
      continue;
    if (pool_live_path(p, path, sizeof(path), "sub", j) != 0)
      Rf_error("mov: liveness file path too long");
    intptr_t h;
    if (mov_live_open(path, &h) != 0) continue;
    if (mov_live_try(h) != MOV_LIVE_ACQUIRED) {
      mov_live_close(h);
      continue;                    /* another claimant beat us */
    }
    int32_t expected = MOV_SUB_FREE;
    if (!atomic_compare_exchange_strong_explicit(&p->sub[j].status, &expected,
                                                 MOV_SUB_LIVE,
                                                 memory_order_seq_cst,
                                                 memory_order_relaxed)) {
      mov_live_close(h);           /* stale FREE reading */
      continue;
    }
    p->live_self = h;
    p->sub_slot = (int) j;
    break;
  }
  if (p->sub_slot < 0)
    Rf_error("mov: submitter registry full");

  mov_sub_slot *me = &p->sub[p->sub_slot];
  me->pid = (int64_t) p->self_pid;
  me->rs_start = (uint32_t) p->sub_slot * per;
  me->rs_count = per;
  mov_live_ident(p->live_self, &me->live_dev, &me->live_ino);

  if (pool_parkers_attach(p, 0) != 0)
    Rf_error("mov: cannot attach pool parkers");
  pool_watch_owner(p, pool_sub_pk(p, (uint32_t) p->sub_slot));
  p->inj_ltail = atomic_load_explicit(ring_tail(pool_ring(p,
    (uint32_t) p->sub_slot)), memory_order_acquire);

  UNPROTECT(1);
  return xp;
}

// Submit --------------------------------------------------------------------------------

static void pool_wake_one_worker(mov_pool *p) {
  /* pusher protocol: push, fence, then the mask load — either the parking
     worker's rescan sees the push or we see its bit */
  atomic_thread_fence(memory_order_seq_cst);
  uint64_t w = atomic_load_explicit(p->parked_workers, memory_order_relaxed);
  if (w == 0) return;
  uint32_t mw = p->hdr.max_workers;
  uint32_t start = p->scan_start++;
  for (uint32_t k = 0; k < mw; k++) {
    uint32_t i = (start + k) % mw;
    if (!(w & (1ull << i))) continue;
    int32_t expected = MOV_WPK_PARKED;
    if (atomic_compare_exchange_strong_explicit(&p->wk[i].park_state,
                                                &expected, MOV_WPK_WAKING,
                                                memory_order_seq_cst,
                                                memory_order_relaxed)) {
      mov_unpark(pool_wk_pk(p, i));
      return;
    }
    if (expected == MOV_WPK_IDLE) {
      /* announced but not yet parked — its rescan may already have missed
         this publish, so an uncontested wake is not safe to skip. Its epoch
         snapshot predates the announce, so this unpark turns the upcoming
         sleep into an immediate return; at worst one spurious wake. */
      mov_unpark(pool_wk_pk(p, i));
      return;
    }
    /* RUNNING or WAKING: the worker transitioned away or another pusher
       claimed the wake; try the next set bit */
  }
}

/* Block until the submitter's own ring has space (announce-then-rescan on
   full_waiters, parked on the submitter's own parker, woken directly by the
   worker whose pop freed a slot) or the deadline passes. */
static int pool_ring_space_wait(mov_pool *p, _Atomic int64_t *head,
                                double timeout_s) {
  if (p->inj_ltail - atomic_load_explicit(head, memory_order_acquire) <
      (int64_t) p->hdr.inj_cap)
    return 1;
  uint64_t bit = 1ull << p->sub_slot;
  double deadline = R_FINITE(timeout_s) ? mov_now() + timeout_s : -1;
  for (;;) {
    uint32_t e = mov_parker_snapshot(pool_sub_pk(p, (uint32_t) p->sub_slot));
    atomic_fetch_or_explicit(p->full_waiters, bit, memory_order_seq_cst);
    atomic_thread_fence(memory_order_seq_cst);
    if (p->inj_ltail - atomic_load_explicit(head, memory_order_acquire) <
        (int64_t) p->hdr.inj_cap) {
      atomic_fetch_and_explicit(p->full_waiters, ~bit, memory_order_seq_cst);
      return 1;
    }
    if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0) {
      atomic_fetch_and_explicit(p->full_waiters, ~bit, memory_order_seq_cst);
      Rf_error("mov: pool stopped");
    }
    long ms = MOV_INTERRUPT_BOUND_MS;
    if (deadline >= 0) {
      double rem = deadline - mov_now();
      if (rem <= 0) {
        atomic_fetch_and_explicit(p->full_waiters, ~bit,
                                  memory_order_seq_cst);
        return 0;
      }
      long rem_ms = (long) (rem * 1000) + 1;
      if (rem_ms < ms) ms = rem_ms;
    }
    mov_park(pool_sub_pk(p, (uint32_t) p->sub_slot), e, ms);
    atomic_fetch_and_explicit(p->full_waiters, ~bit, memory_order_seq_cst);
    R_CheckUserInterrupt();
  }
}

typedef struct mov_task_s {
  uint32_t idx;                  /* global result-slot index */
  uint64_t seq;
} mov_task;

static void pool_unpark_result_waiter(mov_pool *p, mov_rs_hdr *rs) {
  int32_t ws = atomic_load_explicit(&rs->waiter_slot, memory_order_acquire);
  if (ws >= 0 && (uint32_t) ws < p->hdr.max_submitters)
    mov_unpark(pool_sub_pk(p, (uint32_t) ws));
}

/* The handle finalizer's state machine (also invoked deliberately by
   mov_cancel's PENDING arm). A task keeper is never dropped here: CANCEL is
   not a release point — FREE strictly implies the worker is done with the
   entry, materialize included, so release-at-reuse stays safe. */
static void mov_task_finalizer(SEXP xp) {
  mov_task *t = (mov_task *) R_ExternalPtrAddr(xp);
  if (t == NULL) return;
  SEXP pool_xp = R_ExternalPtrProtected(xp);
  mov_pool *p = (mov_pool *) R_ExternalPtrAddr(pool_xp);
  if (p != NULL && !p->released && p->base != NULL &&
      p->self_pid == mov_self_pid()) {
    mov_rs_hdr *rs = pool_rs(p, t->idx);
    if (atomic_load_explicit(&rs->sequence, memory_order_relaxed) == t->seq) {
      for (;;) {
        int32_t st = atomic_load_explicit(&rs->status, memory_order_acquire);
        if (st == MOV_RS_PENDING) {
          int32_t expected = MOV_RS_PENDING;
          if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                      MOV_RS_CANCEL,
                                                      memory_order_seq_cst,
                                                      memory_order_acquire)) {
            pool_unpark_result_waiter(p, rs);
            break;
          }
        } else if (st == MOV_RS_OK || st == MOV_RS_ERR) {
          /* the deliberate "never collected" drop: FREE releases the
             producing worker's result keeper and any region unlinks */
          int32_t w = atomic_load_explicit(&rs->worker_slot,
                                           memory_order_acquire);
          int32_t expected = st;
          if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                      MOV_RS_FREE,
                                                      memory_order_seq_cst,
                                                      memory_order_acquire)) {
            if (w >= 0 && (uint32_t) w < p->hdr.max_workers)
              mov_unpark(pool_wk_pk(p, (uint32_t) w));
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

SEXP mov_pool_submit(SEXP xp, SEXP payload, SEXP timeout) {
  mov_pool *p = pool_get(xp);
  if (p->sub_slot < 0)
    Rf_error("mov: not a submitter handle");
  if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0)
    Rf_error("mov: pool stopped");
  SEXP keepers = VECTOR_ELT(R_ExternalPtrProtected(xp), 0);
  mov_sub_slot *me = &p->sub[p->sub_slot];

  unsigned char *ring = pool_ring(p, (uint32_t) p->sub_slot);
  if (!pool_ring_space_wait(p, ring_head(ring), Rf_asReal(timeout)))
    Rf_error("mov: submission timed out (injection ring full)");

  /* Result slot from our own subrange. Reusing a FREE slot drops its
     previous task keeper — the lazy backstop — by overwrite below. */
  uint32_t local = UINT32_MAX;
  for (uint32_t k = 0; k < me->rs_count; k++) {
    uint32_t cand = (p->rs_cursor + k) % me->rs_count;
    mov_rs_hdr *rs = pool_rs(p, me->rs_start + cand);
    if (atomic_load_explicit(&rs->status, memory_order_acquire) ==
        MOV_RS_FREE) {
      local = cand;
      break;
    }
  }
  if (local == UINT32_MAX)
    Rf_error("mov: result slots exhausted — collect or cancel outstanding "
             "tasks first");
  uint32_t rs_index = me->rs_start + local;
  mov_rs_hdr *rs = pool_rs(p, rs_index);

  /* Everything that can longjmp — payload staging (a possible region
     create) and handle allocation — runs before any observable mutation:
     an error here leaves the entry unpublished and the slot FREE. The
     handle carries the sequence about to be installed, so until the bump
     below it is simply stale. */
  unsigned char *e = ring_entry(p, ring, (uint64_t) p->inj_ltail);
  mov_entry_hdr *eh = (mov_entry_hdr *) e;
  SEXP keep = PROTECT(mov_payload_stage(&eh->ph, e + sizeof(mov_entry_hdr),
                                        p->inline_entry, payload));
  mov_task *t = malloc(sizeof(*t));
  if (t == NULL) Rf_error("mov: allocation failure");
  t->idx = rs_index;
  t->seq = atomic_load_explicit(&rs->sequence, memory_order_relaxed) + 1;
  SEXP txp = PROTECT(R_MakeExternalPtr(t, mov_task_tag, xp));
  R_RegisterCFinalizerEx(txp, mov_task_finalizer, TRUE);
  Rf_setAttrib(txp, R_ClassSymbol, mov_class_task);

  /* Pin unconditionally — whether a stream carries hook-emitted mori
     identifiers is not knowable without inspecting it — until collect
     observes OK/ERR or the slot is reused. */
  SET_VECTOR_ELT(keepers, (R_xlen_t) local, keep);
  p->rs_cursor = local + 1;
  atomic_fetch_add_explicit(&rs->sequence, 1, memory_order_relaxed);
  atomic_store_explicit(&rs->waiter_slot, -1, memory_order_relaxed);
  atomic_store_explicit(&rs->worker_slot, -1, memory_order_relaxed);
  atomic_store_explicit(&rs->status, MOV_RS_PENDING, memory_order_release);

  eh->task_id = ((uint64_t) p->sub_slot << 48) | ++p->task_counter;
  eh->rs_index = rs_index;
  eh->submitter_slot = (uint16_t) p->sub_slot;
  eh->pad = 0;
  p->inj_ltail++;
  atomic_store_explicit(ring_tail(ring), p->inj_ltail, memory_order_release);
  uint64_t bit = 1ull << p->sub_slot;
  if (!(atomic_load_explicit(p->inj_ready, memory_order_relaxed) & bit))
    atomic_fetch_or_explicit(p->inj_ready, bit, memory_order_seq_cst);
  pool_wake_one_worker(p);

  UNPROTECT(2);
  return txp;
}

// Worker step ----------------------------------------------------------------------------

/* Drop kept results whose slot has left OK/ERR (or been resequenced): the
   collector's or finalizer's FREE transition is the consumed-signal, its
   directed unpark what re-runs this on a parked worker. */
static void pool_reap_result_keepers(mov_pool *p, SEXP keepers) {
  uint32_t i = 0;
  while (i < p->rk_n) {
    mov_rs_hdr *rs = pool_rs(p, p->rk[i].idx);
    int32_t st = atomic_load_explicit(&rs->status, memory_order_acquire);
    uint64_t seq = atomic_load_explicit(&rs->sequence, memory_order_relaxed);
    if ((st == MOV_RS_OK || st == MOV_RS_ERR) && seq == p->rk[i].seq) {
      i++;
      continue;
    }
    SET_VECTOR_ELT(keepers, (R_xlen_t) p->rk[i].idx, R_NilValue);
    p->rk[i] = p->rk[--p->rk_n];
  }
}

/* Growth is split from recording so it can run before the publish CAS: an
   allocation failure after publish would leave a pinned keeper the reap
   never visits. */
static void pool_rk_reserve(mov_pool *p) {
  if (p->rk_n < p->rk_cap) return;
  uint32_t cap = p->rk_cap == 0 ? 64 : p->rk_cap * 2;
  struct mov_rk_s *rk = realloc(p->rk, cap * sizeof(*rk));
  if (rk == NULL) Rf_error("mov: allocation failure");
  p->rk = rk;
  p->rk_cap = cap;
}

static void pool_rk_add(mov_pool *p, uint32_t idx, uint64_t seq) {
  p->rk[p->rk_n].idx = idx;
  p->rk[p->rk_n].seq = seq;
  p->rk_n++;
}

/* Announce-before-claim: a worker dying after a claim CAS but before
   recording the task would otherwise vanish it. Recorded from the entry
   copied into scratch, before any claim (ring-head CAS, deque-bottom
   commit, or steal CAS) is attempted; plain stores suffice — the only
   reader is a post-mortem reaper serialized by the liveness lock. */
static void pool_announce(mov_pool *p) {
  mov_entry_hdr *eh = (mov_entry_hdr *) p->scratch;
  if (eh->rs_index >= p->hdr.result_slots)
    Rf_error("mov: corrupt pool entry");
  mov_wk_slot *me = &p->wk[p->wk_slot];
  atomic_store_explicit(&me->in_flight_rs, (int32_t) eh->rs_index,
                        memory_order_relaxed);
  atomic_store_explicit(&me->in_flight_seq,
                        atomic_load_explicit(&pool_rs(p, eh->rs_index)->
                                             sequence,
                                             memory_order_relaxed),
                        memory_order_relaxed);
}

static void pool_announce_clear(mov_pool *p) {
  atomic_store_explicit(&p->wk[p->wk_slot].in_flight_rs, -1,
                        memory_order_relaxed);
}

/* Copy-then-CAS claim over the ring scan, ready-mask-gated on the fast
   path (use_mask) and unfiltered on the fairness tick. The mask is a hint
   only: a stale clear bit is repaired by the pre-park full rescan, so it
   costs one trip to the park path, never a lost task. */
static int pool_claim_rings(mov_pool *p, int use_mask) {
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
        /* backpressure wake: ring and waiter correspond one-to-one */
        uint64_t bit = 1ull << s;
        if (atomic_load_explicit(p->full_waiters, memory_order_relaxed) &
            bit) {
          uint64_t w = atomic_fetch_and_explicit(p->full_waiters, ~bit,
                                                 memory_order_seq_cst);
          if (w & bit) mov_unpark(pool_sub_pk(p, s));
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
static int pool_deque_push(mov_pool *p, const unsigned char *entry) {
  mov_wk_slot *me = &p->wk[p->wk_slot];
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
static int pool_deque_pop(mov_pool *p) {
  mov_wk_slot *me = &p->wk[p->wk_slot];
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

enum { MOV_STEAL_EMPTY = 0, MOV_STEAL_GOT, MOV_STEAL_ABORT };

/* Attempt REAPING -> FREE. Conclusive only with top/bottom re-loaded after
   the status acquire (an earlier bottom read may predate the leaver's final
   writes): for an orphaned deque bottom is static and top monotonic, so
   top >= bottom then means truly drained, and any observer may free the
   slot — which closes the race where a thief empties the deque while the
   owner still reads LEAVING. Returns 1 when the deque is drained. */
static int pool_reaping_free(mov_pool *p, mov_wk_slot *w) {
  if (deque_nonempty(w)) return 0;
  int32_t expected = MOV_WK_REAPING;
  atomic_compare_exchange_strong_explicit(&w->status, &expected, MOV_WK_FREE,
                                          memory_order_seq_cst,
                                          memory_order_relaxed);
  return 1;
}

/* Chase-Lev steal: copy the entry at top, then CAS top to claim it; only
   the CAS publishes the theft, so a lost race or a torn copy from the
   owner lapping the buffer is discarded unobserved. An orphaned (REAPING)
   deque is consumed through this same path; whoever observes it drained
   returns the slot to FREE. */
static int pool_steal_from(mov_pool *p, uint32_t v) {
  mov_wk_slot *w = &p->wk[v];
  int64_t t = atomic_load_explicit(&w->deque_top, memory_order_acquire);
  atomic_thread_fence(memory_order_seq_cst);
  int64_t b = atomic_load_explicit(&w->deque_bottom, memory_order_acquire);
  if (t >= b) {
    if (atomic_load_explicit(&w->status, memory_order_acquire) ==
        MOV_WK_REAPING && !pool_reaping_free(p, w))
      return MOV_STEAL_ABORT;   /* orphaned and nonempty after all: retry */
    return MOV_STEAL_EMPTY;
  }
  memcpy(p->scratch, deque_entry_at(p, w, t), p->hdr.slot);
  pool_announce(p);
  if (!atomic_compare_exchange_strong_explicit(&w->deque_top, &t, t + 1,
                                               memory_order_seq_cst,
                                               memory_order_relaxed)) {
    pool_announce_clear(p);
    return MOV_STEAL_ABORT;
  }
  if (atomic_load_explicit(&w->status, memory_order_acquire) ==
      MOV_WK_REAPING)
    pool_reaping_free(p, w);
  return MOV_STEAL_GOT;
}

static uint64_t pool_rng(mov_pool *p) {   /* xorshift64 */
  uint64_t x = p->rng;
  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;
  return p->rng = x;
}

#define MOV_STEAL_ROUNDS 4

/* Random victim among LIVE and REAPING slots, one attempt per victim,
   bounded rounds; EMPTY and ABORT alike move to the next victim. Exhausting
   the rounds falls through to the injection scan and then the pre-park
   spin + announce-then-rescan, which is what makes a missed steal safe. */
static int pool_steal(mov_pool *p) {
  uint32_t mw = p->hdr.max_workers;
  if (mw <= 1) return 0;
  for (int r = 0; r < MOV_STEAL_ROUNDS; r++) {
    uint32_t start = (uint32_t) (pool_rng(p) % mw);
    for (uint32_t k = 0; k < mw; k++) {
      uint32_t i = (start + k) % mw;
      if ((int) i == p->wk_slot) continue;
      int32_t st = atomic_load_explicit(&p->wk[i].status,
                                        memory_order_acquire);
      if (st != MOV_WK_LIVE && st != MOV_WK_REAPING) continue;
      if (pool_steal_from(p, i) == MOV_STEAL_GOT) return 1;
    }
  }
  return 0;
}

/* The fairness tick's full scan: every injection ring unfiltered by the
   ready mask, then every REAPING slot's orphaned deque — the bound on
   external-submission (and ownerless-work) latency when a saturated pool
   never otherwise falls through its local tiers. */
static int pool_fairness_scan(mov_pool *p) {
  if (pool_claim_rings(p, 0)) return 1;
  for (uint32_t i = 0; i < p->hdr.max_workers; i++) {
    if ((int) i == p->wk_slot) continue;
    if (atomic_load_explicit(&p->wk[i].status, memory_order_acquire) ==
        MOV_WK_REAPING && pool_steal_from(p, i) == MOV_STEAL_GOT)
      return 1;
  }
  return 0;
}

/* One claim attempt in tier order. On success the entry is in scratch and
   announced; the caller executes it. */
static int pool_next_task(mov_pool *p) {
  if (++p->claims % 61 == 0 && pool_fairness_scan(p)) return 1;
  if (pool_deque_pop(p)) return 1;
  if (pool_steal(p)) return 1;
  return pool_claim_rings(p, 1);
}

/* Cheap work probe for the pre-announce spin: one load of the ready mask
   plus a sweep of the deque index lines. */
static int pool_work_hint(mov_pool *p) {
  if (atomic_load_explicit(p->inj_ready, memory_order_acquire) != 0)
    return 1;
  for (uint32_t i = 0; i < p->hdr.max_workers; i++) {
    int32_t st = atomic_load_explicit(&p->wk[i].status,
                                      memory_order_acquire);
    if ((st == MOV_WK_LIVE || st == MOV_WK_REAPING) &&
        deque_nonempty(&p->wk[i]))
      return 1;
  }
  return 0;
}

/* Unfiltered work check for the pre-park rescan, covering every claim
   source — injection rings (repairing a stale-clear ready bit as it goes)
   and every LIVE or REAPING deque, own included. */
static int pool_any_work(mov_pool *p) {
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
    if ((st == MOV_WK_LIVE || st == MOV_WK_REAPING) &&
        deque_nonempty(&p->wk[i]))
      any = 1;
  }
  return any;
}

static void pool_execute(mov_pool *p, SEXP keepers, SEXP eval_fun) {
  mov_wk_slot *me = &p->wk[p->wk_slot];
  mov_entry_hdr *eh = (mov_entry_hdr *) p->scratch;
  mov_rs_hdr *rs = pool_rs(p, eh->rs_index);
  uint64_t seq = atomic_load_explicit(&me->in_flight_seq,
                                      memory_order_relaxed);
  atomic_store_explicit(&rs->worker_slot, p->wk_slot, memory_order_relaxed);

  /* skip dead work — an optimization only: the check races the finalizer's
     CANCEL, and correctness rests on the publish CAS below either way */
  if (atomic_load_explicit(&rs->status, memory_order_acquire) ==
      MOV_RS_CANCEL) {
    int32_t expected = MOV_RS_CANCEL;
    atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                            MOV_RS_FREE,
                                            memory_order_seq_cst,
                                            memory_order_relaxed);
    atomic_store_explicit(&me->in_flight_rs, -1, memory_order_relaxed);
    return;
  }

  SEXP pl = PROTECT(mov_payload_read(&eh->ph,
                                     p->scratch + sizeof(mov_entry_hdr),
                                     p->inline_entry));
  /* worker_eval returns list(ok, value-or-condition); user errors are caught
     there, so an error out of this eval is infrastructure failure and
     propagates to worker_main */
  SEXP call = PROTECT(Rf_lang2(eval_fun, pl));
  SEXP res = PROTECT(Rf_eval(call, R_GlobalEnv));
  int ok = Rf_asLogical(VECTOR_ELT(res, 0)) == TRUE;
  SEXP value = VECTOR_ELT(res, 1);

  /* payload writes are plain stores into a slot no allocator can touch
     (status stays PENDING/CANCEL until the FREE transition); the publish CAS
     is the release barrier a collector's acquire load pairs with */
  pool_rk_reserve(p);
  SEXP keep = PROTECT(mov_payload_stage(&rs->ph,
                                        (unsigned char *) rs +
                                        sizeof(mov_rs_hdr),
                                        p->inline_rs, value));
  int32_t expected = MOV_RS_PENDING;
  if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                              ok ? MOV_RS_OK : MOV_RS_ERR,
                                              memory_order_seq_cst,
                                              memory_order_acquire)) {
    SET_VECTOR_ELT(keepers, (R_xlen_t) eh->rs_index, keep);
    pool_rk_add(p, eh->rs_index, seq);
    pool_unpark_result_waiter(p, rs);
  } else {
    /* cancelled while we ran: drop the result, return the slot */
    expected = MOV_RS_CANCEL;
    atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                            MOV_RS_FREE,
                                            memory_order_seq_cst,
                                            memory_order_relaxed);
  }
  atomic_store_explicit(&me->in_flight_rs, -1, memory_order_relaxed);
  UNPROTECT(4);
}

/* One worker-loop iteration: reap keepers, check flags, claim + execute one
   task or park. Returns 1 after executing a task, 0 on timeout / spurious
   wake, -1 on shutdown or owner death. The R-level worker_main loops over
   this; the in-process test harness single-steps it. */
SEXP mov_pool_step(SEXP xp, SEXP timeout, SEXP eval_fun) {
  mov_pool *p = pool_get(xp);
  if (p->role != MOV_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("mov: not a worker handle");
  if (TYPEOF(eval_fun) != CLOSXP)
    Rf_error("mov: expected an eval function");
  SEXP keepers = VECTOR_ELT(R_ExternalPtrProtected(xp), 0);
  mov_wk_slot *me = &p->wk[p->wk_slot];
  uint64_t my_bit = 1ull << p->wk_slot;
  double timeout_s = Rf_asReal(timeout);
  double deadline = R_FINITE(timeout_s) ? mov_now() + timeout_s : -1;

  /* heal any announce left dangling by an interrupt longjmp out of a
     previous step: a stale bit costs the pusher one failed CAS */
  atomic_store_explicit(&me->park_state, MOV_WPK_RUNNING,
                        memory_order_relaxed);
  atomic_fetch_and_explicit(p->parked_workers, ~my_bit,
                            memory_order_seq_cst);

  for (;;) {
    pool_reap_result_keepers(p, keepers);
    if (atomic_load_explicit(p->shutdown, memory_order_acquire) != 0 ||
        atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0)
      return Rf_ScalarInteger(-1);

    if (pool_next_task(p)) {
      pool_execute(p, keepers, eval_fun);
      return Rf_ScalarInteger(1);
    }

    if (timeout_s <= 0) return Rf_ScalarInteger(0);

    /* bounded spin before announcing: sub-µs submit gaps are absorbed
       without touching the parked_workers line */
    for (int i = 0; i < MOV_SPIN_ITERS; i++) {
      MOV_PAUSE();
      if (pool_work_hint(p)) break;
    }
    if (pool_work_hint(p))
      continue;

    /* announce-then-rescan (the sleep race): either our rescan sees the
       push or the pusher's mask load sees our bit */
    uint32_t e = mov_parker_snapshot(pool_wk_pk(p, (uint32_t) p->wk_slot));
    atomic_store_explicit(&me->park_state, MOV_WPK_IDLE,
                          memory_order_relaxed);
    atomic_fetch_or_explicit(p->parked_workers, my_bit,
                             memory_order_seq_cst);
    atomic_thread_fence(memory_order_seq_cst);
    if (pool_any_work(p) ||
        atomic_load_explicit(p->shutdown, memory_order_acquire) != 0 ||
        atomic_load_explicit(&p->owner_dead, memory_order_acquire) != 0) {
      atomic_fetch_and_explicit(p->parked_workers, ~my_bit,
                                memory_order_seq_cst);
      atomic_store_explicit(&me->park_state, MOV_WPK_RUNNING,
                            memory_order_relaxed);
      continue;
    }
    int32_t expected = MOV_WPK_IDLE;
    if (atomic_compare_exchange_strong_explicit(&me->park_state, &expected,
                                                MOV_WPK_PARKED,
                                                memory_order_seq_cst,
                                                memory_order_relaxed)) {
      long ms = -1;
      if (deadline >= 0) {
        double rem = deadline - mov_now();
        ms = rem <= 0 ? 0 : (long) (rem * 1000) + 1;
      }
      mov_park(pool_wk_pk(p, (uint32_t) p->wk_slot), e, ms);
    }
    atomic_fetch_and_explicit(p->parked_workers, ~my_bit,
                              memory_order_seq_cst);
    atomic_store_explicit(&me->park_state, MOV_WPK_RUNNING,
                          memory_order_relaxed);
    R_CheckUserInterrupt();
    if (deadline >= 0 && mov_now() >= deadline) {
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
SEXP mov_pool_deque_pull(SEXP xp, SEXP n_sexp) {
  mov_pool *p = pool_get(xp);
  if (p->role != MOV_ROLE_WORKER || p->wk_slot < 0)
    Rf_error("mov: not a worker handle");
  mov_wk_slot *me = &p->wk[p->wk_slot];
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

static mov_task *task_get(SEXP xp, mov_pool **pool_out, SEXP *pool_xp_out) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mov_task_tag)
    Rf_error("mov: not a task handle");
  mov_task *t = (mov_task *) R_ExternalPtrAddr(xp);
  if (t == NULL) Rf_error("mov: task handle is stale");
  SEXP pool_xp = R_ExternalPtrProtected(xp);
  mov_pool *p = pool_get(pool_xp);
  *pool_out = p;
  if (pool_xp_out != NULL) *pool_xp_out = pool_xp;
  return t;
}

SEXP mov_pool_collect(SEXP xp, SEXP timeout) {
  mov_pool *p;
  SEXP pool_xp;
  mov_task *t = task_get(xp, &p, &pool_xp);
  if (p->sub_slot < 0)
    Rf_error("mov: not a submitter's task handle");
  mov_rs_hdr *rs = pool_rs(p, t->idx);
  double timeout_s = Rf_asReal(timeout);
  double deadline = -1;

  int32_t st;
  for (;;) {
    if (atomic_load_explicit(&rs->sequence, memory_order_relaxed) != t->seq)
      Rf_error("mov: task handle already collected or invalidated");
    st = atomic_load_explicit(&rs->status, memory_order_acquire);
    if (st != MOV_RS_PENDING) break;
    if (timeout_s <= 0) return mov_sent_timeout;
    if (deadline < 0 && R_FINITE(timeout_s))
      deadline = mov_now() + timeout_s;

    /* announce -> fence -> re-check -> park bounded; the publishing worker
       reads waiter_slot after its publish CAS and unparks us */
    uint32_t e = mov_parker_snapshot(pool_sub_pk(p, (uint32_t) p->sub_slot));
    atomic_store_explicit(&rs->waiter_slot, p->sub_slot,
                          memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    if (atomic_load_explicit(&rs->status, memory_order_acquire) !=
        MOV_RS_PENDING)
      continue;
    long ms = MOV_INTERRUPT_BOUND_MS;
    if (deadline >= 0) {
      double rem = deadline - mov_now();
      if (rem <= 0) return mov_sent_timeout;
      long rem_ms = (long) (rem * 1000) + 1;
      if (rem_ms < ms) ms = rem_ms;
    }
    mov_park(pool_sub_pk(p, (uint32_t) p->sub_slot), e, ms);
    R_CheckUserInterrupt();
    if (deadline >= 0 && mov_now() >= deadline &&
        atomic_load_explicit(&rs->status, memory_order_acquire) ==
        MOV_RS_PENDING)
      return mov_sent_timeout;
  }

  switch (st) {
  case MOV_RS_OK:
  case MOV_RS_ERR: {
    /* materialize BEFORE the FREE transition — publication of FREE is what
       lets the worker's keeper reap unlink everything this payload
       references */
    SEXP v = PROTECT(mov_payload_read(&rs->ph,
                                      (unsigned char *) rs +
                                      sizeof(mov_rs_hdr), p->inline_rs));
    int32_t w = atomic_load_explicit(&rs->worker_slot, memory_order_acquire);
    if (t->idx >= p->sub[p->sub_slot].rs_start &&
        t->idx < p->sub[p->sub_slot].rs_start + p->sub[p->sub_slot].rs_count)
      SET_VECTOR_ELT(VECTOR_ELT(R_ExternalPtrProtected(pool_xp), 0),
                     (R_xlen_t) (t->idx - p->sub[p->sub_slot].rs_start),
                     R_NilValue);
    int32_t expected = st;
    if (!atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                                 MOV_RS_FREE,
                                                 memory_order_seq_cst,
                                                 memory_order_acquire)) {
      UNPROTECT(1);
      Rf_error("mov: task handle already collected");
    }
    if (w >= 0 && (uint32_t) w < p->hdr.max_workers)
      mov_unpark(pool_wk_pk(p, (uint32_t) w));   /* keeper-drop signal */
    if (st == MOV_RS_ERR) {
      SEXP call = PROTECT(Rf_lang2(Rf_install("stop"), v));
      Rf_eval(call, R_BaseEnv);                  /* no return */
    }
    UNPROTECT(1);
    return v;
  }
  case MOV_RS_CANCEL:
    /* the task keeper releases at slot reuse, not here — the worker may not
       have materialized yet */
    Rf_error("mov: task cancelled or pool stopped");
  case MOV_RS_FREE:
  default:
    Rf_error("mov: task handle already collected");
  }
}

/* Advisory and discard-only, never preemptive: a task already executing runs
   to completion and its result is dropped by the worker's failed publish
   CAS. */
SEXP mov_pool_cancel(SEXP xp) {
  mov_pool *p;
  mov_task *t = task_get(xp, &p, NULL);
  mov_rs_hdr *rs = pool_rs(p, t->idx);
  if (atomic_load_explicit(&rs->sequence, memory_order_relaxed) != t->seq)
    return Rf_ScalarLogical(FALSE);
  int32_t expected = MOV_RS_PENDING;
  if (atomic_compare_exchange_strong_explicit(&rs->status, &expected,
                                              MOV_RS_CANCEL,
                                              memory_order_seq_cst,
                                              memory_order_relaxed)) {
    pool_unpark_result_waiter(p, rs);
    return Rf_ScalarLogical(TRUE);
  }
  return Rf_ScalarLogical(FALSE);
}

// Stop and introspection ---------------------------------------------------------------------

SEXP mov_pool_stop_call(SEXP xp, SEXP timeout) {
  mov_pool *p = pool_peek(xp);
  if (p == NULL) return Rf_ScalarLogical(TRUE);   /* stop is idempotent */
  if (p->role != MOV_ROLE_CONTROLLER)
    Rf_error("mov: only the controller can stop a pool");
  SEXP prot = R_ExternalPtrProtected(xp);

  pool_shutdown_broadcast(p);

  /* wait for workers to take their clean-exit path; their liveness locks
     release with their fds either way */
  double deadline = mov_now() + Rf_asReal(timeout);
  int clean;
  for (;;) {
    clean = 1;
    for (uint32_t i = 0; i < p->hdr.max_workers; i++) {
      int32_t st = atomic_load_explicit(&p->wk[i].status,
                                        memory_order_acquire);
      if (st == MOV_WK_LIVE || st == MOV_WK_LEAVING || st == MOV_WK_CLAIMING)
        clean = 0;
    }
    if (clean || mov_now() >= deadline) break;
    for (uint32_t i = 0; i < p->hdr.max_workers; i++)
      mov_unpark(pool_wk_pk(p, i));
    uint32_t e = mov_parker_snapshot(pool_sub_pk(p, 0));
    mov_park(pool_sub_pk(p, 0), e, 50);
    R_CheckUserInterrupt();
  }

  pool_unlink_names(p, prot);
  pool_release(p);
  return Rf_ScalarLogical(clean);
}

SEXP mov_pool_status_call(SEXP xp) {
  mov_pool *p = pool_get(xp);
  const char *names[] = {"name", "role", "max_workers", "max_submitters",
                         "injection_cap", "result_slots", "slot_size",
                         "workers", "parked", "submitters", "injection",
                         "tasks", "deque", "shutdown", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(p->shm.name));
  SET_VECTOR_ELT(out, 1, Rf_mkString(
    p->role == MOV_ROLE_CONTROLLER ? "controller" :
    p->role == MOV_ROLE_WORKER ? "worker" : "submitter"));
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

  SEXP tasks = Rf_allocVector(INTSXP, 4);   /* pending, ok, err, cancel */
  SET_VECTOR_ELT(out, 11, tasks);
  memset(INTEGER(tasks), 0, 4 * sizeof(int));
  for (uint32_t r = 0; r < p->hdr.result_slots; r++) {
    int32_t st = atomic_load_explicit(&pool_rs(p, r)->status,
                                      memory_order_acquire);
    if (st >= MOV_RS_PENDING && st <= MOV_RS_CANCEL)
      INTEGER(tasks)[st - MOV_RS_PENDING]++;
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
