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
   first), so the count never races; the listener thread only increments. */

#include <stdlib.h>
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
} mizu_r_notify;

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

// Task unpack (the task tag is a shared symbol; each TU installs its own) -------

static mizu_r_handle *ntfy_task_pool(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_ntfy_task_tag)
    Rf_error("mizu: not a task handle");
  SEXP pool_xp = R_ExternalPtrProtected(xp);
  return (mizu_r_handle *) R_ExternalPtrAddr(pool_xp);
}

static mizu_pool *ntfy_tasks_get(SEXP tasks, mizu_task **ts_out,
                                 mizu_r_handle **h_out, R_xlen_t *n_out) {
  if (TYPEOF(tasks) != VECSXP || XLENGTH(tasks) == 0)
    Rf_error("mizu: tasks must be a non-empty list of task handles");
  R_xlen_t n = XLENGTH(tasks);
  mizu_task *ts = (mizu_task *) R_alloc(n, sizeof(mizu_task));
  mizu_r_handle *h = NULL;
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP xp = VECTOR_ELT(tasks, i);
    if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_ntfy_task_tag)
      Rf_error("mizu: not a task handle");
    uintptr_t word = (uintptr_t) R_ExternalPtrAddr(xp);
    if (word == 0) Rf_error("mizu: task handle is closed");
    mizu_r_handle *hi =
      (mizu_r_handle *) R_ExternalPtrAddr(R_ExternalPtrProtected(xp));
    if (hi == NULL || hi->core == NULL)
      Rf_error("mizu: pool handle is closed");
    if (h == NULL) h = hi;
    else if (hi != h)
      Rf_error("mizu: task handles must belong to the same pool handle");
    ts[i].word = (uint64_t) word;
  }
  *ts_out = ts;
  *h_out = h;
  *n_out = n;
  return (mizu_pool *) h->core;
}

// .Call entries -----------------------------------------------------------------

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

/* The one-call lookup behind the collect verbs' refusal check: the task's
   pool's notify state env, or NULL (the common case — never armed). */
SEXP mizu_task_notify_state(SEXP xp) {
  mizu_r_handle *h = ntfy_task_pool(xp);
  if (h == NULL) return R_NilValue;
  return VECTOR_ELT(h->prot, MIZU_POOL_NTFY_SLOT);
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

SEXP mizu_pool_notify_arm_call(SEXP xp, SEXP state) {
  mizu_r_handle *h =
    mizu_r_handle_get(xp, mizu_ntfy_pool_tag, "pool");
  mizu_pool *p = (mizu_pool *) h->core;
  mizu_r_notify_ensure_armed(h, state, 1);
  if (mizu_pool_notify_arm(p, 1) != MIZU_OK) ntfy_pool_raise(p);
  return R_NilValue;
}

SEXP mizu_pool_notify_disarm_call(SEXP xp) {
  mizu_r_handle *h =
    mizu_r_handle_peek(xp, mizu_ntfy_pool_tag, "pool");
  if (h == NULL) return R_NilValue;
  mizu_r_notify_disarm(h);
  return R_NilValue;
}

/* The per-slot NOTIFY announce (on = TRUE) / withdraw (on = FALSE) — the
   core verb validates each task's sequence, so a stale handle raises the
   handle's recorded error. */
SEXP mizu_pool_task_notify_call(SEXP tasks, SEXP on) {
  mizu_task *ts;
  mizu_r_handle *h;
  R_xlen_t n;
  mizu_pool *p = ntfy_tasks_get(tasks, &ts, &h, &n);
  if (mizu_pool_task_notify(p, ts, (size_t) n,
                            Rf_asLogical(on) == TRUE) != MIZU_OK)
    ntfy_pool_raise(p);
  return R_NilValue;
}

/* The per-wake terminal scan (atomic loads only, one .Call for the whole
   armed set): TRUE per task whose result slot is no longer pending. */
SEXP mizu_pool_tasks_terminal(SEXP tasks) {
  mizu_task *ts;
  mizu_r_handle *h;
  R_xlen_t n;
  mizu_pool *p = ntfy_tasks_get(tasks, &ts, &h, &n);
  SEXP out = PROTECT(Rf_allocVector(LGLSXP, n));
  for (R_xlen_t i = 0; i < n; i++)
    LOGICAL(out)[i] = mizu_pool_task_state(p, &ts[i]) != MIZU_RS_PENDING;
  UNPROTECT(1);
  return out;
}

/* The per-waiter verdicts behind the drain's scans: one .Call, no
   per-task vector — mode 0 the first task, 1 any terminal, 2 all
   terminal (the collective forms early-exit). */
SEXP mizu_pool_waiter_terminal(SEXP tasks, SEXP mode) {
  mizu_task *ts;
  mizu_r_handle *h;
  R_xlen_t n;
  mizu_pool *p = ntfy_tasks_get(tasks, &ts, &h, &n);
  int done;
  switch (Rf_asInteger(mode)) {
  case 0:
    done = mizu_pool_task_state(p, &ts[0]) != MIZU_RS_PENDING;
    break;
  case 1:
    done = 0;
    for (R_xlen_t i = 0; i < n && !done; i++)
      done = mizu_pool_task_state(p, &ts[i]) != MIZU_RS_PENDING;
    break;
  default:
    done = 1;
    for (R_xlen_t i = 0; i < n && done; i++)
      done = mizu_pool_task_state(p, &ts[i]) != MIZU_RS_PENDING;
    break;
  }
  return Rf_ScalarLogical(done);
}
