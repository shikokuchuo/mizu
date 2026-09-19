/* The channel's .Call veneer (Part I): thin R entry points over the vendored
   core's mizu_channel_* verbs (vendor/libmizu). Arg validation, the extptr
   handle (a mizu_r_handle wrapping the opaque core handle), the drop's
   MIZU_DROP_R tagging, and the mizu_status -> sentinel / classed-error mapping
   live here; the transport (ring, arena, wakes, close rendezvous, peer-death
   verdict) is all core-side. */

#include <stdlib.h>
#include <string.h>
#include "mizu.h"

static SEXP mizu_chan_tag;
static SEXP mizu_class_channel;
SEXP mizu_sent_full, mizu_sent_timeout, mizu_sent_closed, mizu_sent_gone;

static SEXP mizu_make_sentinel(const char *value, const char *cls) {
  SEXP s = PROTECT(Rf_mkString(value));
  SEXP klass = PROTECT(Rf_allocVector(STRSXP, 2));
  SET_STRING_ELT(klass, 0, Rf_mkChar(cls));
  SET_STRING_ELT(klass, 1, Rf_mkChar("mizu_sentinel"));
  Rf_setAttrib(s, R_ClassSymbol, klass);
  R_PreserveObject(s);
  UNPROTECT(2);
  return s;
}

static void mizu_chan_finalizer(SEXP xp);

void mizu_channel_init(void) {
  mizu_chan_tag = Rf_install("mizu_channel");
  mizu_class_channel = Rf_mkString("mizu_channel");
  R_PreserveObject(mizu_class_channel);
  mizu_sent_full = mizu_make_sentinel("full", "mizu_full");
  mizu_sent_timeout = mizu_make_sentinel("timeout", "mizu_timeout");
  mizu_sent_closed = mizu_make_sentinel("closed", "mizu_closed");
  mizu_sent_gone = mizu_make_sentinel("peer_gone", "mizu_peer_gone");
}

void mizu_channel_fini(void) {
  R_ReleaseObject(mizu_sent_gone);
  R_ReleaseObject(mizu_sent_closed);
  R_ReleaseObject(mizu_sent_timeout);
  R_ReleaseObject(mizu_sent_full);
  R_ReleaseObject(mizu_class_channel);
}

// Handle access -------------------------------------------------------------------

/* NULL when the handle has already been released (closed / destroyed). */
static mizu_r_handle *chan_peek(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_chan_tag)
    Rf_error("mizu: not a channel handle");
  mizu_r_handle *h = (mizu_r_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL || h->core == NULL) return NULL;
  if (h->self_pid != mizu_self_pid())
    Rf_error("mizu: channel handles do not survive fork()");
  return h;
}

static mizu_r_handle *chan_get(SEXP xp) {
  mizu_r_handle *h = chan_peek(xp);
  if (h == NULL) Rf_error("mizu: channel handle is closed");
  return h;
}

static mizu_channel *chan_core(SEXP xp) {
  return (mizu_channel *) chan_get(xp)->core;
}

/* The R binding registered on every channel handle: the tier dispatch, the
   materialize, the interrupt poll, and the pin release. exec/park/sweep are
   NULL (a channel never evals; R has no global lock to bracket parks; no
   per-handle cache to sweep). */
static void chan_binding(mizu_r_handle *h, mizu_binding *b) {
  mizu_binding_init(b);
  b->stage = mizu_r_stage_channel;
  b->read = mizu_r_read_channel;
  b->check = mizu_r_check;
  b->drop = mizu_r_drop;
  b->ctx = h;
}

/* Build the extptr around a created/attached core handle: the prot chain
   ([0] the zc view cache's wrap table, [1] the pin chain) and the
   finalizer. */
static SEXP chan_wrap(mizu_r_handle *h) {
  SEXP prot = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(prot, 0, Rf_allocVector(VECSXP, MIZU_OPEN_CACHE_MAX));
  h->zoc.wraps = VECTOR_ELT(prot, 0);
  h->prot = prot;
  h->pin_slot = 1;
  SEXP xp = PROTECT(R_MakeExternalPtr(h, mizu_chan_tag, prot));
  R_RegisterCFinalizerEx(xp, mizu_chan_finalizer, TRUE);
  Rf_setAttrib(xp, R_ClassSymbol, mizu_class_channel);
  UNPROTECT(2);
  return xp;
}

static void mizu_chan_finalizer(SEXP xp) {
  mizu_r_handle *h = (mizu_r_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) return;
  if (h->core != NULL) {
    /* destroy signals close and runs the non-blocking rendezvous check —
       unlink only on the peer's bit or its confirmed death */
    mizu_channel_destroy((mizu_channel *) h->core);
    h->core = NULL;
  }
  free(h);
  R_ClearExternalPtr(xp);
}

// Small helpers -------------------------------------------------------------------

/* R-visible read of the shared clock: the map deadline R threads through
   the pool's _try entries is computed against the same timescale the C
   wait loops park against. */
SEXP mizu_now_call(void) {
  return Rf_ScalarReal(mizu_now());
}

static SEXP status_sentinel(mizu_status st) {
  switch (st) {
  case MIZU_FULL:      return mizu_sent_full;
  case MIZU_CLOSED:    return mizu_sent_closed;
  case MIZU_PEER_GONE: return mizu_sent_gone;
  case MIZU_TIMEOUT:   return mizu_sent_timeout;
  default:            return R_NilValue;
  }
}

/* Raise a MIZU_ERR from a handle verb as a classed error with the handle's
   recorded message. */
NORET static void chan_raise(mizu_channel *c) {
  mizu_stop("mizu_error", "mizu: %s", mizu_channel_error(c));
}

/* Raise a create/attach failure off the thread-local slot, where the core
   composes the full message (size + hint included). Space/existence
   failures carry the shm class; everything else is a plain error. */
NORET static void chan_raise_tls(void) {
  mizu_errcat cat = mizu_last_error_category();
  const char *msg = mizu_last_error_message();
  switch (cat) {
  case MIZU_ERRCAT_NOSPACE:
  case MIZU_ERRCAT_NOMEMORY:
  case MIZU_ERRCAT_EXISTS:
    mizu_stop_shm(NA_REAL, "mizu: %s", msg);
  default:
    Rf_error("mizu: %s", msg);
  }
}

/* Provenance, not class: TRUE only for the interned singletons themselves,
   so a payload merely carrying the class never passes. */
SEXP mizu_sentinel_check(SEXP x) {
  return Rf_ScalarLogical(x == mizu_sent_full || x == mizu_sent_timeout ||
                          x == mizu_sent_closed || x == mizu_sent_gone);
}

// Create (host) -------------------------------------------------------------------

static int mizu_pow2(uint64_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

SEXP mizu_channel_create_call(SEXP expr, SEXP cap_sexp, SEXP slot_sexp,
                         SEXP arena_sexp, SEXP spin) {
  uint64_t cap = (uint64_t) Rf_asInteger(cap_sexp);
  uint64_t slot = (uint64_t) Rf_asInteger(slot_sexp);
  double arena_in = Rf_asReal(arena_sexp);
  if (!mizu_pow2(cap) || cap < 2 || cap > (1u << 24))
    Rf_error("mizu: capacity must be a power of two between 2 and 2^24");
  if (!mizu_pow2(slot) || slot < 64 || slot > (1u << 20))
    Rf_error("mizu: slot_size must be a power of two between 64 and 2^20");
  if (!(arena_in >= 0) || arena_in > 1.1e12 ||
      (uint64_t) arena_in % 64 != 0)
    Rf_error("mizu: arena_size must be a non-negative multiple of 64");

  /* the drop: the peer's bootstrap, copied into the region at create. A
     quoted expression rides as an MIZU_DROP_R-tagged serialize stream; a
     character scalar is UTF-8 source text in the peer's language, tagged
     MIZU_DROP_SOURCE (the cross-language lingua franca). */
  int source = TYPEOF(expr) == STRSXP;
  size_t expr_size;
  unsigned char *drop;
  if (source) {
    if (XLENGTH(expr) != 1 || STRING_ELT(expr, 0) == NA_STRING)
      Rf_error("mizu: a source drop must be a single non-NA string");
    const char *src = Rf_translateCharUTF8(STRING_ELT(expr, 0));
    expr_size = strlen(src);
    drop = malloc(expr_size + 1);
    if (drop == NULL) Rf_error("mizu: allocation failure");
    drop[0] = MIZU_DROP_SOURCE;
    memcpy(drop + 1, src, expr_size);
  } else {
    expr_size = mizu_view_serialize_count(expr);
    drop = malloc(expr_size + 1);
    if (drop == NULL) Rf_error("mizu: allocation failure");
    drop[0] = MIZU_DROP_R;
    mizu_view_serialize_into(drop + 1, expr);
  }

  mizu_r_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) { free(drop); Rf_error("mizu: allocation failure"); }
  mizu_channel_opts opts;
  mizu_channel_opts_init(&opts);
  opts.capacity = (uint32_t) cap;
  opts.slot_size = (uint32_t) slot;
  opts.arena_size = (uint64_t) arena_in;
  opts.flags = Rf_asLogical(spin) == TRUE ? MIZU_FLAG_SPIN : 0;
  opts.drop = drop;
  opts.drop_size = (uint64_t) expr_size + 1;
  mizu_binding b;
  chan_binding(h, &b);

  mizu_channel *c;
  mizu_status st = mizu_channel_create(&c, &opts, &b);
  free(drop);
  if (st != MIZU_OK) {
    free(h);
    chan_raise_tls();
  }
  h->core = (mizu_handle *) c;
  h->self_pid = mizu_self_pid();
  return chan_wrap(h);
}

SEXP mizu_channel_suffix(SEXP xp) {
  mizu_channel *c = chan_core(xp);
  char buf[64];
  if (mizu_channel_token(c, buf, sizeof(buf)) != MIZU_OK)
    chan_raise(c);
  return Rf_mkString(buf);
}

/* Startup rendezvous: the host waits for the peer's ready word. Returns
   FALSE on deadline expiry — the caller walks the channel back. */
SEXP mizu_channel_ready_wait_call(SEXP xp, SEXP timeout) {
  mizu_channel *c = chan_core(xp);
  mizu_status st = mizu_channel_ready_wait(c, mizu_timeout_ms(Rf_asReal(timeout)));
  if (st == MIZU_ERR) chan_raise(c);
  return Rf_ScalarLogical(st == MIZU_OK);
}

/* Startup walk-back: signal close so a late-attaching peer exits instead of
   parking against a host that gave up, then unlink everything. */
SEXP mizu_channel_destroy_call(SEXP xp) {
  mizu_r_handle *h = chan_get(xp);
  mizu_channel_destroy((mizu_channel *) h->core);
  h->core = NULL;
  return R_NilValue;
}

// Attach (peer) -------------------------------------------------------------------

SEXP mizu_channel_attach_call(SEXP suffix_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("mizu: expected a region-name suffix");
  const char *suffix = CHAR(STRING_ELT(suffix_sexp, 0));

  mizu_r_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) Rf_error("mizu: allocation failure");
  mizu_binding b;
  chan_binding(h, &b);
  mizu_channel *c;
  if (mizu_channel_attach(&c, suffix, &b) != MIZU_OK) {
    free(h);
    chan_raise_tls();
  }
  h->core = (mizu_handle *) c;
  h->self_pid = mizu_self_pid();

  SEXP xp = PROTECT(chan_wrap(h));

  /* materialize-before-ready: the host's frame keeps the expression — and
     through the view layer's keeper chains every region its identifiers name — alive
     exactly until ready is observed. An MIZU_DROP_R drop unserializes to
     the expression; an MIZU_DROP_SOURCE drop is UTF-8 source text, returned
     as a string for the caller to parse (kept opaque here so the tag
     dispatch is explicit at both ends). */
  const uint8_t *bytes;
  uint64_t n;
  mizu_channel_drop(c, &bytes, &n);
  if (n == 0 || (bytes[0] != MIZU_DROP_R && bytes[0] != MIZU_DROP_SOURCE))
    Rf_error("mizu: foreign channel drop (not an R bootstrap)");
  SEXP drop = bytes[0] == MIZU_DROP_R ?
    PROTECT(mizu_view_unserialize_from((unsigned char *) (bytes + 1),
                                  (size_t) n - 1)) :
    PROTECT(Rf_ScalarString(Rf_mkCharLenCE((const char *) (bytes + 1),
                                           (int) (n - 1), CE_UTF8)));
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(out, 0, xp);
  SET_VECTOR_ELT(out, 1, drop);
  UNPROTECT(3);
  return out;
}

SEXP mizu_channel_ready_set_call(SEXP xp) {
  mizu_channel *c = chan_core(xp);
  if (mizu_channel_ready_set(c) != MIZU_OK) chan_raise(c);
  return R_NilValue;
}

// Verbs ---------------------------------------------------------------------------

SEXP mizu_channel_send_call(SEXP xp, SEXP x) {
  mizu_channel *c = chan_core(xp);
  mizu_status st = mizu_channel_send(c, (void *) x);
  if (st == MIZU_ERR) chan_raise(c);
  return st == MIZU_OK ? Rf_ScalarLogical(TRUE) : status_sentinel(st);
}

/* One .Call; the core batches the tail store and the wake. Returns the count
   accepted (short on ring-full or close midway — probe why with mizu_send). */
SEXP mizu_channel_send_batch_call(SEXP xp, SEXP xs) {
  mizu_channel *c = chan_core(xp);
  if (TYPEOF(xs) != VECSXP)
    Rf_error("mizu: expected a list of payloads");
  R_xlen_t n = XLENGTH(xs);
  void **objs = (void **) R_alloc(n, sizeof(void *));
  for (R_xlen_t i = 0; i < n; i++)
    objs[i] = (void *) VECTOR_ELT(xs, i);
  size_t accepted = 0;
  mizu_status st = mizu_channel_send_batch(c, objs, (size_t) n, &accepted);
  if (st == MIZU_ERR) chan_raise(c);
  return Rf_ScalarInteger((int) accepted);
}

SEXP mizu_channel_recv_call(SEXP xp, SEXP timeout) {
  mizu_r_handle *h = chan_get(xp);
  mizu_channel *c = (mizu_channel *) h->core;
  h->saw_foreign = 0;
  void *obj = NULL;
  mizu_status st = mizu_channel_recv(c, &obj, mizu_timeout_ms(Rf_asReal(timeout)));
  /* the read hook flags a foreign payload on the handle and fails the read
     with MIZU_READ_CONSUME: the slot is already consumed, so the informative
     error costs the message, not the channel */
  if (st == MIZU_ERR) {
    if (h->saw_foreign) mizu_stop_python_payload();
    chan_raise(c);
  }
  if (st != MIZU_OK) return status_sentinel(st);
  return (SEXP) obj;
}

/* Up to n messages; waits only for the first, then drains already-published
   ones without waiting further. The sentinel discipline matches recv. The
   sink form anchors each message in out as it is read — the array form
   would hold n unprotected SEXPs across the remaining reads. */
SEXP mizu_channel_recv_batch_call(SEXP xp, SEXP n_sexp, SEXP timeout) {
  mizu_r_handle *h = chan_get(xp);
  mizu_channel *c = (mizu_channel *) h->core;
  int n = Rf_asInteger(n_sexp);
  if (n < 1) Rf_error("mizu: n must be at least 1");
  SEXP out = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) n));
  size_t count = 0;
  h->saw_foreign = 0;
  mizu_status st = mizu_channel_recv_batch_fn(c, (size_t) n, &count,
                                            mizu_vec_sink, out,
                                            mizu_timeout_ms(Rf_asReal(timeout)));
  /* a foreign payload in the batch: the read hook flags the handle and its
     slot is consumed (MIZU_READ_CONSUME), so the informative error costs the
     message, not the channel */
  if (st == MIZU_ERR) {
    if (h->saw_foreign) {
      UNPROTECT(1);
      mizu_stop_python_payload();
    }
    chan_raise(c);
  }
  if (st != MIZU_OK) {
    UNPROTECT(1);
    return status_sentinel(st);
  }
  if (count < (size_t) n) {
    SEXP res = PROTECT(Rf_lengthgets(out, (R_xlen_t) count));
    UNPROTECT(2);
    return res;
  }
  UNPROTECT(1);
  return out;
}

// Close protocol ------------------------------------------------------------------

/* The peer half of the protocol, run by peer_main's epilogue: set our bit,
   wake the host. No rendezvous — process exit releases everything else. */
SEXP mizu_channel_close_signal_call(SEXP xp) {
  mizu_r_handle *h = chan_peek(xp);
  if (h == NULL) return R_NilValue;
  mizu_channel_close_signal((mizu_channel *) h->core);
  return R_NilValue;
}

SEXP mizu_channel_close_call(SEXP xp, SEXP timeout) {
  mizu_r_handle *h = chan_peek(xp);
  if (h == NULL) return Rf_ScalarLogical(TRUE);   /* close is idempotent */
  mizu_channel *c = (mizu_channel *) h->core;
  mizu_status st = mizu_channel_close(c, mizu_timeout_ms(Rf_asReal(timeout)));
  if (st == MIZU_ERR) chan_raise(c);
  if (st == MIZU_OK) {
    /* rendezvoused: the handle is dead — destroy releases it */
    mizu_channel_destroy(c);
    h->core = NULL;
    return Rf_ScalarLogical(TRUE);
  }
  return Rf_ScalarLogical(FALSE);   /* MIZU_TIMEOUT: destroy retries it */
}

// Introspection -------------------------------------------------------------------

/* Reports peer *process* liveness (the fd-scoped lock verdict): an orderly
   close with the process still running is alive; a released handle is not. */
SEXP mizu_channel_alive_call(SEXP xp) {
  mizu_r_handle *h = chan_peek(xp);
  if (h == NULL) return Rf_ScalarLogical(FALSE);
  return Rf_ScalarLogical(mizu_channel_alive((mizu_channel *) h->core));
}

SEXP mizu_channel_stat(SEXP xp) {
  mizu_r_handle *h = chan_get(xp);
  mizu_channel *c = (mizu_channel *) h->core;
  mizu_channel_info info;
  if (mizu_channel_info_get(c, &info) != MIZU_OK) chan_raise(c);
  const char *names[] = {"name", "side", "capacity", "slot_size",
                         "arena_size", "inline_max", "spin", "ready",
                         "closed", "peer_pid", "tx_sent", "tx_published",
                         "tx_consumed", "rx_consumed", "rx_published",
                         "fl_entries", "fl_hits", "open_hits",
                         "open_misses", "ledger_entries", "zc_open_hits",
                         "zc_open_misses", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(info.name));
  SET_VECTOR_ELT(out, 1, Rf_mkString(info.side == MIZU_ENTITY_HOST ?
                                     "host" : "peer"));
  SET_VECTOR_ELT(out, 2, Rf_ScalarReal((double) info.capacity));
  SET_VECTOR_ELT(out, 3, Rf_ScalarReal((double) info.slot_size));
  SET_VECTOR_ELT(out, 4, Rf_ScalarReal((double) info.arena_size));
  SET_VECTOR_ELT(out, 5, Rf_ScalarReal((double) info.inline_max));
  SET_VECTOR_ELT(out, 6, Rf_ScalarLogical(info.spin));
  SET_VECTOR_ELT(out, 7, Rf_ScalarLogical(info.ready));
  SET_VECTOR_ELT(out, 8, Rf_ScalarInteger(info.closed));
  SET_VECTOR_ELT(out, 9, Rf_ScalarReal((double) info.peer_pid));
  SET_VECTOR_ELT(out, 10, Rf_ScalarReal((double) info.tx_sent));
  SET_VECTOR_ELT(out, 11, Rf_ScalarReal((double) info.tx_published));
  SET_VECTOR_ELT(out, 12, Rf_ScalarReal((double) info.tx_consumed));
  SET_VECTOR_ELT(out, 13, Rf_ScalarReal((double) info.rx_consumed));
  SET_VECTOR_ELT(out, 14, Rf_ScalarReal((double) info.rx_published));
  SET_VECTOR_ELT(out, 15, Rf_ScalarInteger((int) info.fl_entries));
  SET_VECTOR_ELT(out, 16, Rf_ScalarReal((double) info.fl_hits));
  SET_VECTOR_ELT(out, 17, Rf_ScalarReal((double) info.open_hits));
  SET_VECTOR_ELT(out, 18, Rf_ScalarReal((double) info.open_misses));
  SET_VECTOR_ELT(out, 19, Rf_ScalarInteger((int) info.ledger_entries));
  /* the zc view cache is binding-owned: the core fills 0, the R-side cache
     patches its own counters */
  SET_VECTOR_ELT(out, 20, Rf_ScalarReal((double) h->zoc.hits));
  SET_VECTOR_ELT(out, 21, Rf_ScalarReal((double) h->zoc.misses));
  UNPROTECT(1);
  return out;
}
