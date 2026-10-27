/* The R half of the notify endpoints (R/promise.R's engine room): per
   armed handle one mizu_r_notify record bridging the core's persistent
   pure-C watch and later's main-thread queue. The watch callback runs on
   the core's listener thread and touches only atomics: a coalescing CAS,
   a ref increment, and later's public execLaterNative2 (loaded once per
   process exactly as nanonext's nano_load_later) scheduling the drain.
   The drain runs on the main thread, where it may call R: it reads the
   state env's .drain closure and evals it under R_UnwindProtect so the
   scheduled-callback ref releases even on a longjmp (later itself
   contains the escaping error — its native callbacks are unwind-protected).
   The record's refs: 1 from create to handle teardown, +1 while armed
   (state env preserved/released on the main thread only), +1 per
   scheduled callback (released by the drain). All decrements are
   main-thread (the unwatch synchronizes with any in-flight callback
   first), so the count never races; the listener thread only increments.
   A pool record also owns the submitter's waiter registry (below): the
   enrolled async consumers, scanned in C per wake. */

#include <stdlib.h>
#include <string.h>
#include "mizu.h"

typedef void (*mizu_eln2_fn)(void (*)(void *), void *, double, int);

typedef struct mizu_r_notify_s {
  struct mizu_r_notify_s *next;   /* the live-record registry (unload fini) */
  mizu_r_handle *rh;              /* back-pointer, NULL after teardown */
  _Atomic int scheduled;          /* coalescing flag: one drain per burst */
  _Atomic int refs;               /* create + armed + scheduled callbacks */
  int is_pool;
  int watching;                   /* the core watch is active */
  int armed;                      /* the core arm and the armed ref are held */
  SEXP state;                     /* the R state env, preserved while armed */
  /* the pool waiter registry (main thread only): one entry per enrolled
     async consumer, the waiter envs in a parallel preserved VECSXP */
  struct ntfy_ent_s *ents;
  SEXP ws;                        /* VECSXP of waiter envs, preserved; NULL
                                     until the first enrollment */
  R_xlen_t n;                     /* live entries */
  R_xlen_t cap;
  size_t nwords;                  /* task words across every entry */
} mizu_r_notify;

/* One registry entry: the waiter's task words cached at enrollment (the
   scan never touches the extptrs again — mizu_pool_task_state is total on
   a stale word), the single word inline. A map entry keeps one word per
   runner slot (0 for a never-submitted runner, so the ordinal the morsel
   abandon takes is the slot's position) and the morsel wrap extptr, which
   the waiter env's map state keeps alive. */
typedef struct ntfy_ent_s {
  int mode;                       /* NTFY_* */
  uint32_t n;                     /* word count */
  uint64_t w1;                    /* the single word (words == NULL) */
  uint64_t *words;                /* malloc'd when n > 1 */
  SEXP wrap;                      /* map: the morsel wrap; else R_NilValue */
} ntfy_ent;

enum { NTFY_TASK = 0, NTFY_ANY = 1, NTFY_ALL = 2, NTFY_MAP = 3,
       NTFY_TASKH = 4 };

static const char *const ntfy_labels[5] = {
  "as.promise()", "mizu_collect_any_async()", "mizu_collect_all_async()",
  "a map promise", "a task handler"
};

static void ntfy_reg_free(mizu_r_notify *rec);

static mizu_eln2_fn mizu_eln2;
static mizu_r_notify *mizu_notify_registry;
static SEXP mizu_ntfy_chan_tag;
static SEXP mizu_ntfy_pool_tag;
static SEXP mizu_ntfy_task_tag;
static SEXP mizu_ntfy_drain_sym;

static void mizu_notify_drain(void *);

void mizu_notify_init(void) {
  mizu_ntfy_chan_tag = Rf_install("mizu_channel");
  mizu_ntfy_pool_tag = Rf_install("mizu_pool");
  mizu_ntfy_task_tag = Rf_install("mizu_task");
  mizu_ntfy_drain_sym = Rf_install(".drain");
}

/* later's execLaterNative2, loaded once (nanonext's nano_load_later
   pattern). Raises if the later namespace cannot be loaded — the R-side
   requireNamespace guard composes the friendlier message first. */
static void mizu_load_later(void) {
  if (mizu_eln2 != NULL) return;
  SEXP str = PROTECT(Rf_mkString("later"));
  SEXP call = PROTECT(Rf_lang2(Rf_install("loadNamespace"), str));
  Rf_eval(call, R_BaseEnv);
  UNPROTECT(2);
  mizu_eln2 = (mizu_eln2_fn) R_GetCCallable("later", "execLaterNative2");
}

// The record registry (R_unload_mizu stops every live watch) -------------------

static void mizu_r_notify_free(mizu_r_notify *rec) {
  if (mizu_notify_registry == rec) {
    mizu_notify_registry = rec->next;
  } else {
    for (mizu_r_notify *r = mizu_notify_registry; r != NULL; r = r->next) {
      if (r->next == rec) {
        r->next = rec->next;
        break;
      }
    }
  }
  free(rec);
}

static void mizu_r_notify_unref(mizu_r_notify *rec) {
  if (atomic_fetch_sub_explicit(&rec->refs, 1, memory_order_acq_rel) == 1)
    mizu_r_notify_free(rec);
}

/* Stop every live watch (before the death-listener teardown the watches
   ride on). Records stay; their handles' teardown unrefs as usual. */
void mizu_notify_fini(void) {
  for (mizu_r_notify *rec = mizu_notify_registry; rec != NULL;
       rec = rec->next) {
    if (rec->watching) {
      rec->watching = 0;
      if (rec->is_pool) {
        (void) mizu_pool_notify_unwatch((mizu_pool *) rec->rh->core);
      } else {
        (void) mizu_channel_notify_unwatch((mizu_channel *) rec->rh->core);
      }
    }
  }
}

/* Handle teardown (the finalizers and the destroy/close veneers): tombstone
   the record so a queued drain no-ops, stop the watch (synchronizes with
   any in-flight callback), release the state env and the held refs. Runs
   ahead of the core destroy, which is the backstop stop for everything
   here. A queued drain past this point sees rh NULL and frees the record
   when its own ref releases. */
void mizu_r_notify_teardown(mizu_r_handle *h) {
  mizu_r_notify *rec = h->notify;
  if (rec == NULL) return;
  h->notify = NULL;
  rec->rh = NULL;
  ntfy_reg_free(rec);
  if (rec->watching) {
    rec->watching = 0;
    if (rec->is_pool) {
      (void) mizu_pool_notify_unwatch((mizu_pool *) h->core);
    } else {
      (void) mizu_channel_notify_unwatch((mizu_channel *) h->core);
    }
  }
  if (rec->armed) {
    rec->armed = 0;
    if (rec->state != NULL) {
      R_ReleaseObject(rec->state);
      rec->state = NULL;
    }
    mizu_r_notify_unref(rec);
  }
  mizu_r_notify_unref(rec);   /* the create ref */
}

// The watch -> later bridge -----------------------------------------------------

/* The core's watch callback (any thread, no R API): coalesce the burst to
   one main-thread drain. The ref increment pairs with the drain's cleanup
   release, so the record outlives the handle's teardown when one races. */
static void mizu_r_notify_cb(void *ctx) {
  mizu_r_notify *rec = (mizu_r_notify *) ctx;
  int expected = 0;
  if (atomic_compare_exchange_strong_explicit(&rec->scheduled, &expected, 1,
                                              memory_order_acq_rel,
                                              memory_order_acquire)) {
    atomic_fetch_add_explicit(&rec->refs, 1, memory_order_relaxed);
    mizu_eln2(mizu_notify_drain, rec, 0.0, 0);
  }
}

/* The main-thread drain (later's queue): clear the coalescing flag first —
   a signal landing mid-drain schedules a fresh callback, which finds the
   state disarmed or empty and simply no-ops or re-arms. The R call is
   unwind-protected so the scheduled ref releases on a longjmp; later's
   own protection surfaces the escaping error at its run_now site. */
static SEXP mizu_notify_drain_run(void *d) {
  mizu_r_notify *rec = (mizu_r_notify *) d;
  if (rec->state != NULL) {
    /* the symbol eval is the C-API-compliant frame lookup (Rf_findVarInFrame
       is legacy non-API); a torn state's "not found" raise unwinds into
       later's own protection */
    SEXP fn = PROTECT(Rf_eval(mizu_ntfy_drain_sym, rec->state));
    if (TYPEOF(fn) == CLOSXP) {
      SEXP call = PROTECT(Rf_lang1(fn));
      Rf_eval(call, R_BaseEnv);
      UNPROTECT(1);
    }
    UNPROTECT(1);
  }
  return R_NilValue;
}

static void mizu_notify_drain_clean(void *d, Rboolean jump) {
  mizu_r_notify_unref((mizu_r_notify *) d);
}

static void mizu_notify_drain(void *data) {
  mizu_r_notify *rec = (mizu_r_notify *) data;
  atomic_store_explicit(&rec->scheduled, 0, memory_order_release);
  /* the cont contract: NULL (R makes the token), never R_NilValue — the
     epilogue SETCARs the result onto the token */
  R_UnwindProtect(mizu_notify_drain_run, rec, mizu_notify_drain_clean, rec,
                  NULL);
}

// Shared record plumbing ----------------------------------------------------------

/* First-arm record create: later loaded (raises), the record on the
   registry, the create ref held until teardown. */
static mizu_r_notify *mizu_r_notify_alloc(mizu_r_handle *h, int is_pool) {
  mizu_load_later();
  mizu_r_notify *rec = calloc(1, sizeof(*rec));
  if (rec == NULL) Rf_error("mizu: allocation failure");
  rec->rh = h;
  rec->is_pool = is_pool;
  atomic_init(&rec->scheduled, 0);
  atomic_init(&rec->refs, 1);
  rec->next = mizu_notify_registry;
  mizu_notify_registry = rec;
  h->notify = rec;
  return rec;
}

NORET static void ntfy_chan_raise(mizu_channel *c) {
  mizu_stop("mizu_error", "mizu: %s", mizu_channel_error(c));
}

NORET static void ntfy_pool_raise(mizu_pool *p) {
  mizu_errcat cat = mizu_pool_errcat(p);
  const char *msg = mizu_pool_error(p);
  SEXP cond = PROTECT(
    cat == MIZU_ERRCAT_STOPPED ?
      mizu_cond_make("mizu_error_stopped", "mizu: %s", msg) :
    cat == MIZU_ERRCAT_EXHAUSTED ?
      mizu_cond_make("mizu_error_slots_exhausted", "mizu: %s", msg) :
      mizu_cond_make("mizu_error", "mizu: %s", msg)
  );
  mizu_cond_signal(cond);
  UNPROTECT(1);                 /* unreachable: mizu_cond_signal is NORET */
}

/* The arm half shared by both handle kinds: ensure the record and the
   watch (watch first — its failure leaves the core unarmed), then hold
   the state env and the armed ref once per arm cycle. The core arm
   itself runs at the caller (its availability report differs). */
static void mizu_r_notify_ensure_armed(mizu_r_handle *h, SEXP state,
                                       int is_pool) {
  if (TYPEOF(state) != ENVSXP) Rf_error("mizu: expected a notify state env");
  mizu_r_notify *rec = h->notify;
  if (rec == NULL) rec = mizu_r_notify_alloc(h, is_pool);
  if (!rec->watching) {
    mizu_status st = is_pool ?
      mizu_pool_notify_watch((mizu_pool *) h->core, mizu_r_notify_cb, rec) :
      mizu_channel_notify_watch((mizu_channel *) h->core, mizu_r_notify_cb,
                                rec);
    if (st != MIZU_OK) {
      if (is_pool) ntfy_pool_raise((mizu_pool *) h->core);
      ntfy_chan_raise((mizu_channel *) h->core);
    }
    rec->watching = 1;
  }
  if (!rec->armed) {
    if (rec->state != state) {
      if (rec->state != NULL) R_ReleaseObject(rec->state);
      R_PreserveObject(state);
      rec->state = state;
    }
    rec->armed = 1;
    atomic_fetch_add_explicit(&rec->refs, 1, memory_order_relaxed);
  }
}

static void mizu_r_notify_disarm(mizu_r_handle *h) {
  mizu_r_notify *rec = h->notify;
  if (rec == NULL || !rec->armed) return;
  if (rec->is_pool) {
    (void) mizu_pool_notify_arm((mizu_pool *) h->core, 0);
  } else {
    (void) mizu_channel_notify_arm((mizu_channel *) h->core, 0, NULL);
  }
  rec->armed = 0;
  if (rec->state != NULL) {
    R_ReleaseObject(rec->state);
    rec->state = NULL;
  }
  mizu_r_notify_unref(rec);
}

// .Call entries: the state envs and the channel arm ------------------------------

/* The notify state env of a handle (prot slot), NULL when never armed.
   Lenient on purpose (no core access): the close/destroy flush paths read
   it after the core handle is gone. */
SEXP mizu_chan_notify_peek(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_ntfy_chan_tag)
    Rf_error("mizu: not a channel handle");
  SEXP prot = R_ExternalPtrProtected(xp);
  return VECTOR_ELT(prot, MIZU_CHAN_NTFY_SLOT);
}

SEXP mizu_chan_notify_set(SEXP xp, SEXP state) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_ntfy_chan_tag)
    Rf_error("mizu: not a channel handle");
  SET_VECTOR_ELT(R_ExternalPtrProtected(xp), MIZU_CHAN_NTFY_SLOT, state);
  return state;
}

SEXP mizu_pool_notify_peek(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_ntfy_pool_tag)
    Rf_error("mizu: not a pool handle");
  SEXP prot = R_ExternalPtrProtected(xp);
  return VECTOR_ELT(prot, MIZU_POOL_NTFY_SLOT);
}

SEXP mizu_pool_notify_set(SEXP xp, SEXP state) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_ntfy_pool_tag)
    Rf_error("mizu: not a pool handle");
  SET_VECTOR_ELT(R_ExternalPtrProtected(xp), MIZU_POOL_NTFY_SLOT, state);
  return state;
}

/* A task's pool extptr (its prot), for the pool's notify state lookup. */
SEXP mizu_task_pool_call(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_ntfy_task_tag)
    Rf_error("mizu: not a task handle");
  return R_ExternalPtrProtected(xp);
}

SEXP mizu_chan_notify_arm_call(SEXP xp, SEXP state) {
  mizu_r_handle *h =
    mizu_r_handle_get(xp, mizu_ntfy_chan_tag, "channel");
  mizu_channel *c = (mizu_channel *) h->core;
  mizu_r_notify_ensure_armed(h, state, 0);
  int avail = 0;
  if (mizu_channel_notify_arm(c, 1, &avail) != MIZU_OK) ntfy_chan_raise(c);
  return Rf_ScalarLogical(avail != 0);
}

SEXP mizu_chan_notify_disarm_call(SEXP xp) {
  mizu_r_handle *h =
    mizu_r_handle_peek(xp, mizu_ntfy_chan_tag, "channel");
  if (h == NULL) return R_NilValue;
  mizu_r_notify_disarm(h);
  return R_NilValue;
}

// The pool waiter registry -------------------------------------------------------

/* The submitter's enrolled async consumers (R/promise.R's pool state is
   the env holding the .drain closure; everything else lives here). An
   entry caches its task words, so the per-wake scan is a loop of atomic
   state loads with no R allocation; the waiter envs ride a parallel
   preserved VECSXP. Enrollment fuses the conflict check, the slot
   announces, the endpoint arm, and the announce -> fence -> re-check
   terminal probe into one call — and probes terminal FIRST, so a task
   that already published never arms the endpoint. Settled entries leave
   through the scan (announces withdrawn, the registry compacted); a
   timeout leaves through remove. Main thread only. */

static const uint64_t *ent_words(const ntfy_ent *e) {
  return e->words == NULL ? &e->w1 : e->words;
}

static void ntfy_reg_free(mizu_r_notify *rec) {
  for (R_xlen_t i = 0; i < rec->n; i++) free(rec->ents[i].words);
  free(rec->ents);
  rec->ents = NULL;
  rec->n = rec->cap = 0;
  rec->nwords = 0;
  if (rec->ws != NULL) {
    R_ReleaseObject(rec->ws);
    rec->ws = NULL;
  }
}

/* Room for one more entry: the only allocating step of an enrollment,
   run ahead of the core-side state changes. */
static void ntfy_reg_reserve(mizu_r_notify *rec) {
  if (rec->n < rec->cap) return;
  R_xlen_t cap = rec->cap == 0 ? 8 : rec->cap * 2;
  ntfy_ent *ents = realloc(rec->ents, (size_t) cap * sizeof(*ents));
  if (ents == NULL) Rf_error("mizu: allocation failure");
  rec->ents = ents;
  SEXP ws = PROTECT(Rf_allocVector(VECSXP, cap));
  for (R_xlen_t i = 0; i < rec->n; i++)
    SET_VECTOR_ELT(ws, i, VECTOR_ELT(rec->ws, i));
  R_PreserveObject(ws);
  if (rec->ws != NULL) R_ReleaseObject(rec->ws);
  rec->ws = ws;
  rec->cap = cap;
  UNPROTECT(1);
}

static int task_term(mizu_pool *p, uint64_t word) {
  mizu_task t = { word };
  return mizu_pool_task_state(p, &t) != MIZU_RS_PENDING;
}

/* The settle verdict: mode 0 the task, 1 any terminal, 2 all terminal
   (early exit), 3 every runner terminal or abandonable (map.c). */
static int ent_done(mizu_pool *p, const ntfy_ent *e) {
  const uint64_t *w = ent_words(e);
  switch (e->mode) {
  case NTFY_TASK:
  case NTFY_TASKH:
    return task_term(p, w[0]);
  case NTFY_ANY:
    for (uint32_t i = 0; i < e->n; i++)
      if (task_term(p, w[i])) return 1;
    return 0;
  case NTFY_ALL:
    for (uint32_t i = 0; i < e->n; i++)
      if (!task_term(p, w[i])) return 0;
    return 1;
  default:
    return mizu_map_runners_done(e->wrap, p, w, e->n);
  }
}

/* Withdraw the entry's announces. Total: a slot already consumed or
   recycled (FREE / an advanced sequence) is skipped, so a settle after a
   cancel never raises out of the drain. */
static void ent_withdraw(mizu_pool *p, const ntfy_ent *e) {
  const uint64_t *w = ent_words(e);
  for (uint32_t i = 0; i < e->n; i++) {
    if (w[i] == 0) continue;
    mizu_task t = { w[i] };
    if (mizu_pool_task_state(p, &t) != MIZU_RS_FREE)
      (void) mizu_pool_task_notify(p, &t, 1, 0);
  }
}

/* Unlink entry i: words freed, the tail shifted down, the registry
   disarmed when it empties. */
static void ntfy_reg_drop(mizu_r_handle *h, R_xlen_t i) {
  mizu_r_notify *rec = h->notify;
  ntfy_ent *e = &rec->ents[i];
  rec->nwords -= e->n;
  free(e->words);
  for (R_xlen_t j = i + 1; j < rec->n; j++) {
    rec->ents[j - 1] = rec->ents[j];
    SET_VECTOR_ELT(rec->ws, j - 1, VECTOR_ELT(rec->ws, j));
  }
  rec->n--;
  SET_VECTOR_ELT(rec->ws, rec->n, R_NilValue);
  if (rec->n == 0) mizu_r_notify_disarm(h);
}

/* The entry holding a task word, or -1. */
static R_xlen_t ntfy_reg_find(const mizu_r_notify *rec, uint64_t word) {
  for (R_xlen_t i = 0; i < rec->n; i++) {
    const ntfy_ent *e = &rec->ents[i];
    const uint64_t *w = ent_words(e);
    for (uint32_t j = 0; j < e->n; j++)
      if (w[j] == word) return i;
  }
  return -1;
}

/* The first registry entry holding any of nw[0..T): -1 when none. Linear
   for the common shapes (one task against the registry, a few tasks
   against a few); past the crossover a throwaway open-addressing table
   over the registry's words makes a bulk check O(W + T) — the
   back-to-back 10k collections case — with no index to maintain. */
static R_xlen_t ntfy_reg_conflict(const mizu_r_notify *rec,
                                  const uint64_t *nw, R_xlen_t T) {
  if (rec == NULL || rec->n == 0) return -1;
  const double W = (double) rec->nwords;
  if ((double) T * W <= 20.0 * ((double) T + W)) {
    for (R_xlen_t k = 0; k < T; k++) {
      if (nw[k] == 0) continue;
      R_xlen_t i = ntfy_reg_find(rec, nw[k]);
      if (i >= 0) return i;
    }
    return -1;
  }
  int bits = 1;
  while ((UINT64_C(1) << bits) < 2 * rec->nwords) bits++;
  size_t size = (size_t) 1 << bits;
  uint64_t *keys = (uint64_t *) R_alloc(size, sizeof(uint64_t));
  R_xlen_t *vals = (R_xlen_t *) R_alloc(size, sizeof(R_xlen_t));
  memset(keys, 0, size * sizeof(uint64_t));
  const size_t mask = size - 1;
#define NTFY_HASH(x) ((size_t) (((x) * UINT64_C(0x9E3779B97F4A7C15)) >> (64 - bits)))
  for (R_xlen_t i = 0; i < rec->n; i++) {
    const ntfy_ent *e = &rec->ents[i];
    const uint64_t *w = ent_words(e);
    for (uint32_t j = 0; j < e->n; j++) {
      if (w[j] == 0) continue;
      size_t s = NTFY_HASH(w[j]);
      while (keys[s] != 0) s = (s + 1) & mask;
      keys[s] = w[j];
      vals[s] = i;
    }
  }
  for (R_xlen_t k = 0; k < T; k++) {
    if (nw[k] == 0) continue;
    size_t s = NTFY_HASH(nw[k]);
    while (keys[s] != 0) {
      if (keys[s] == nw[k]) return vals[s];
      s = (s + 1) & mask;
    }
  }
#undef NTFY_HASH
  return -1;
}

/* Unpack one task handle of pool handle h. */
static uint64_t ntfy_task_word(SEXP xp, const mizu_r_handle *h) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_ntfy_task_tag)
    Rf_error("mizu: not a task handle");
  uintptr_t word = (uintptr_t) R_ExternalPtrAddr(xp);
  if (word == 0) Rf_error("mizu: task handle is closed");
  if ((mizu_r_handle *) R_ExternalPtrAddr(R_ExternalPtrProtected(xp)) != h)
    Rf_error("mizu: task handles must belong to the same pool handle");
  return (uint64_t) word;
}

NORET static void ntfy_refuse_enroll(int mode, int held) {
  if (mode == NTFY_TASK)
    Rf_error("mizu: task is enrolled in a pending %s and refuses a second "
             "promise — one consumer per result", ntfy_labels[held]);
  Rf_error("mizu: %s refuses a task with a pending consumer (%s) — one "
           "consumer per result", ntfy_labels[mode], ntfy_labels[held]);
}

/* The blocking collect verbs' refusal (verbs_pool.c, after the unpack): a
   task enrolled with an async consumer refuses, naming it. Free when
   nothing is enrolled — the common case costs one pointer test. */
void mizu_r_notify_refuse(mizu_r_handle *h, const mizu_task *ts, size_t n) {
  mizu_r_notify *rec = h->notify;
  if (rec == NULL || rec->n == 0) return;
  R_xlen_t i = ntfy_reg_conflict(rec, (const uint64_t *) ts, (R_xlen_t) n);
  if (i >= 0)
    Rf_error("mizu: task has a pending consumer (%s) and refuses a blocking "
             "collect — one consumer per result",
             ntfy_labels[rec->ents[i].mode]);
}

/* Enroll a waiter env on its pool's registry: tasks the handle list (a
   map passes its runner handles, NULL for a never-submitted slot, and
   the morsel wrap), mode NTFY_*. Call-site errors (a conflict, a stale
   or foreign handle, an unarmable submitter) raise here, ahead of the
   promise action. Returns TRUE when the waiter is terminal already and
   was NOT registered — the caller settles it directly — else FALSE with
   the waiter registered, announced, and the endpoint armed.
   Order: announce (the core validates every sequence) -> terminal probe
   (a publish that already landed: no arm, no entry) -> arm -> register
   -> re-check (the parker transplant: a publish between the probe and
   the arm saw the word unarmed and signalled nothing, so it is found
   here and the entry popped again). */
SEXP mizu_pool_notify_enroll(SEXP pool_xp, SEXP state, SEXP w, SEXP tasks,
                             SEXP mode_sexp, SEXP wrap) {
  mizu_r_handle *h = mizu_r_handle_get(pool_xp, mizu_ntfy_pool_tag, "pool");
  mizu_pool *p = (mizu_pool *) h->core;
  if (TYPEOF(state) != ENVSXP || TYPEOF(w) != ENVSXP)
    Rf_error("mizu: expected a notify state env and a waiter env");
  const int mode = Rf_asInteger(mode_sexp);
  if (mode < NTFY_TASK || mode > NTFY_TASKH)
    Rf_error("mizu: invalid waiter mode");
  if (TYPEOF(tasks) != VECSXP || XLENGTH(tasks) == 0)
    Rf_error("mizu: tasks must be a non-empty list of task handles");
  const R_xlen_t n = XLENGTH(tasks);
  if (n > UINT32_MAX) Rf_error("mizu: too many task handles");
  uint64_t *words = (uint64_t *) R_alloc((size_t) n, sizeof(uint64_t));
  R_xlen_t live = 0;
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP xp = VECTOR_ELT(tasks, i);
    if (mode == NTFY_MAP && xp == R_NilValue) {
      words[i] = 0;
      continue;
    }
    words[i] = ntfy_task_word(xp, h);
    live++;
  }
  R_xlen_t held = ntfy_reg_conflict(h->notify, words, n);
  if (held >= 0) ntfy_refuse_enroll(mode, h->notify->ents[held].mode);
  /* a map whose runners were all dropped: nothing can publish */
  if (live == 0) return Rf_ScalarLogical(1);
  ntfy_ent ent = { mode, (uint32_t) n, words[0], n > 1 ? words : NULL,
                   mode == NTFY_MAP ? wrap : R_NilValue };
  if (live == n) {
    if (mizu_pool_task_notify(p, (const mizu_task *) words, (size_t) n, 1) !=
        MIZU_OK)
      ntfy_pool_raise(p);
  } else {
    for (R_xlen_t i = 0; i < n; i++) {
      if (words[i] == 0) continue;
      mizu_task t = { words[i] };
      if (mizu_pool_task_notify(p, &t, 1, 1) != MIZU_OK) ntfy_pool_raise(p);
    }
  }
  if (ent_done(p, &ent)) {
    ent_withdraw(p, &ent);
    return Rf_ScalarLogical(1);
  }
  mizu_r_notify_ensure_armed(h, state, 1);
  mizu_r_notify *rec = h->notify;
  ntfy_reg_reserve(rec);
  if (mizu_pool_notify_arm(p, 1) != MIZU_OK) {
    ent_withdraw(p, &ent);
    ntfy_pool_raise(p);
  }
  if (n > 1) {
    ent.words = malloc((size_t) n * sizeof(uint64_t));
    if (ent.words == NULL) {
      ent.words = words;
      ent_withdraw(p, &ent);
      Rf_error("mizu: allocation failure");
    }
    memcpy(ent.words, words, (size_t) n * sizeof(uint64_t));
  }
  rec->ents[rec->n] = ent;
  SET_VECTOR_ELT(rec->ws, rec->n, w);
  rec->n++;
  rec->nwords += (size_t) n;
  if (ent_done(p, &rec->ents[rec->n - 1])) {
    ent_withdraw(p, &rec->ents[rec->n - 1]);
    ntfy_reg_drop(h, rec->n - 1);
    return Rf_ScalarLogical(1);
  }
  return Rf_ScalarLogical(0);
}

/* The per-wake scan (the drain): re-arm the edge the waking publish
   cleared, then collect up to max settled waiters in registry order —
   unlinked, their announces withdrawn — disarming when the registry
   empties. A re-arm of a continuously armed word is a load and a fence,
   so the drain's loop of scan-settle-scan ends on the empty scan that
   proves the re-armed word saw nothing. Lenient on a released pool
   (the empty list): a queued drain may outlive its handle. */
SEXP mizu_pool_notify_scan(SEXP pool_xp, SEXP max_sexp) {
  mizu_r_handle *h = mizu_r_handle_peek(pool_xp, mizu_ntfy_pool_tag, "pool");
  mizu_r_notify *rec = h == NULL ? NULL : h->notify;
  const int max = Rf_asInteger(max_sexp);
  if (rec == NULL || rec->n == 0 || max <= 0) {
    if (rec != NULL && rec->n == 0) mizu_r_notify_disarm(h);
    return Rf_allocVector(VECSXP, 0);
  }
  mizu_pool *p = (mizu_pool *) h->core;
  if (mizu_pool_notify_arm(p, 1) != MIZU_OK) ntfy_pool_raise(p);
  const R_xlen_t lim = max < rec->n ? max : rec->n;
  SEXP out = PROTECT(Rf_allocVector(VECSXP, lim));
  R_xlen_t k = 0, j = 0;
  for (R_xlen_t i = 0; i < rec->n; i++) {
    ntfy_ent *e = &rec->ents[i];
    if (k < lim && ent_done(p, e)) {
      SET_VECTOR_ELT(out, k++, VECTOR_ELT(rec->ws, i));
      ent_withdraw(p, e);
      rec->nwords -= e->n;
      free(e->words);
      continue;
    }
    if (j != i) {
      rec->ents[j] = *e;
      SET_VECTOR_ELT(rec->ws, j, VECTOR_ELT(rec->ws, i));
    }
    j++;
  }
  for (R_xlen_t i = j; i < rec->n; i++) SET_VECTOR_ELT(rec->ws, i, R_NilValue);
  rec->n = j;
  if (j == 0) mizu_r_notify_disarm(h);
  if (k < lim) out = Rf_lengthgets(out, (R_len_t) k);
  UNPROTECT(1);
  return out;
}

/* Unlink a waiter by identity (its timeout fired): announces withdrawn,
   TRUE; FALSE when it already settled. */
SEXP mizu_pool_notify_remove(SEXP pool_xp, SEXP w) {
  mizu_r_handle *h = mizu_r_handle_peek(pool_xp, mizu_ntfy_pool_tag, "pool");
  mizu_r_notify *rec = h == NULL ? NULL : h->notify;
  if (rec == NULL) return Rf_ScalarLogical(0);
  for (R_xlen_t i = 0; i < rec->n; i++) {
    if (VECTOR_ELT(rec->ws, i) != w) continue;
    ent_withdraw((mizu_pool *) h->core, &rec->ents[i]);
    ntfy_reg_drop(h, i);
    return Rf_ScalarLogical(1);
  }
  return Rf_ScalarLogical(0);
}

/* The waiter env enrolled on a task, or NULL (as.promise's cached-promise
   lookup). Lenient: a closed task or pool has no waiter. */
SEXP mizu_pool_notify_find(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_ntfy_task_tag)
    Rf_error("mizu: not a task handle");
  uintptr_t word = (uintptr_t) R_ExternalPtrAddr(xp);
  mizu_r_handle *h =
    (mizu_r_handle *) R_ExternalPtrAddr(R_ExternalPtrProtected(xp));
  if (word == 0 || h == NULL || h->notify == NULL) return R_NilValue;
  R_xlen_t i = ntfy_reg_find(h->notify, (uint64_t) word);
  return i < 0 ? R_NilValue : VECTOR_ELT(h->notify->ws, i);
}

/* The registry size (tests). */
SEXP mizu_pool_notify_count(SEXP pool_xp) {
  mizu_r_handle *h = mizu_r_handle_peek(pool_xp, mizu_ntfy_pool_tag, "pool");
  mizu_r_notify *rec = h == NULL ? NULL : h->notify;
  return Rf_ScalarInteger(rec == NULL ? 0 : (int) rec->n);
}
