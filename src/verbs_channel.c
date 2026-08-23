/* The channel's .Call veneer (Part I): thin R entry points over the vendored
   core's rei_channel_* verbs (vendor/librei). Arg validation, the extptr
   handle (a sora_handle wrapping the opaque core handle), the drop's
   REI_DROP_R tagging, and the rei_status -> sentinel / classed-error mapping
   live here; the transport (ring, arena, wakes, close rendezvous, peer-death
   verdict) is all core-side. */

#include <stdlib.h>
#include <string.h>
#include "sora.h"

static SEXP sora_chan_tag;
static SEXP sora_class_channel;
SEXP sora_sent_full, sora_sent_timeout, sora_sent_closed, sora_sent_gone;

static SEXP sora_make_sentinel(const char *value, const char *cls) {
  SEXP s = PROTECT(Rf_mkString(value));
  SEXP klass = PROTECT(Rf_allocVector(STRSXP, 2));
  SET_STRING_ELT(klass, 0, Rf_mkChar(cls));
  SET_STRING_ELT(klass, 1, Rf_mkChar("sora_sentinel"));
  Rf_setAttrib(s, R_ClassSymbol, klass);
  R_PreserveObject(s);
  UNPROTECT(2);
  return s;
}

static void sora_chan_finalizer(SEXP xp);

void sora_channel_init(void) {
  sora_chan_tag = Rf_install("sora_channel");
  sora_class_channel = Rf_mkString("sora_channel");
  R_PreserveObject(sora_class_channel);
  sora_sent_full = sora_make_sentinel("full", "sora_full");
  sora_sent_timeout = sora_make_sentinel("timeout", "sora_timeout");
  sora_sent_closed = sora_make_sentinel("closed", "sora_closed");
  sora_sent_gone = sora_make_sentinel("peer_gone", "sora_peer_gone");
}

void sora_channel_fini(void) {
  R_ReleaseObject(sora_sent_gone);
  R_ReleaseObject(sora_sent_closed);
  R_ReleaseObject(sora_sent_timeout);
  R_ReleaseObject(sora_sent_full);
  R_ReleaseObject(sora_class_channel);
}

// Handle access -------------------------------------------------------------------

/* NULL when the handle has already been released (closed / destroyed). */
static sora_handle *chan_peek(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != sora_chan_tag)
    Rf_error("sora: not a channel handle");
  sora_handle *h = (sora_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL || h->core == NULL) return NULL;
  if (h->self_pid != rei_self_pid())
    Rf_error("sora: channel handles do not survive fork()");
  return h;
}

static sora_handle *chan_get(SEXP xp) {
  sora_handle *h = chan_peek(xp);
  if (h == NULL) Rf_error("sora: channel handle is closed");
  return h;
}

static rei_channel *chan_core(SEXP xp) {
  return (rei_channel *) chan_get(xp)->core;
}

/* The R binding registered on every channel handle: the tier dispatch, the
   materialize, the interrupt poll, and the pin release. exec/park/sweep are
   NULL (a channel never evals; R has no global lock to bracket parks; no
   per-handle cache to sweep). */
static void chan_binding(sora_handle *h, rei_binding *b) {
  rei_binding_init(b);
  b->stage = sora_r_stage_channel;
  b->read = sora_r_read_channel;
  b->check = sora_r_check;
  b->drop = sora_r_drop;
  b->ctx = h;
}

/* Build the extptr around a created/attached core handle: the prot chain
   ([0] the zc view cache's wrap table) and the finalizer. */
static SEXP chan_wrap(sora_handle *h) {
  SEXP prot = PROTECT(Rf_allocVector(VECSXP, 1));
  SET_VECTOR_ELT(prot, 0, Rf_allocVector(VECSXP, REI_OPEN_CACHE_MAX));
  h->zoc.wraps = VECTOR_ELT(prot, 0);
  h->prot = prot;
  SEXP xp = PROTECT(R_MakeExternalPtr(h, sora_chan_tag, prot));
  R_RegisterCFinalizerEx(xp, sora_chan_finalizer, TRUE);
  Rf_setAttrib(xp, R_ClassSymbol, sora_class_channel);
  UNPROTECT(2);
  return xp;
}

static void sora_chan_finalizer(SEXP xp) {
  sora_handle *h = (sora_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) return;
  if (h->core != NULL) {
    /* destroy signals close and runs the non-blocking rendezvous check —
       unlink only on the peer's bit or its confirmed death */
    rei_channel_destroy((rei_channel *) h->core);
    h->core = NULL;
  }
  free(h);
  R_ClearExternalPtr(xp);
}

// Small helpers -------------------------------------------------------------------

/* R-visible read of the shared clock: the map deadline R threads through
   the pool's _try entries is computed against the same timescale the C
   wait loops park against. */
SEXP sora_now_call(void) {
  return Rf_ScalarReal(rei_now());
}

/* seconds (the R convention; <= 0 polls, non-finite waits indefinitely) to
   the core's timeout_ms (0 polls, < 0 indefinite). */
static double timeout_ms_of(SEXP timeout) {
  double t = Rf_asReal(timeout);
  if (!R_FINITE(t)) return -1;
  return t <= 0 ? 0 : t * 1000;
}

static SEXP status_sentinel(rei_status st) {
  switch (st) {
  case REI_FULL:      return sora_sent_full;
  case REI_CLOSED:    return sora_sent_closed;
  case REI_PEER_GONE: return sora_sent_gone;
  case REI_TIMEOUT:   return sora_sent_timeout;
  default:            return R_NilValue;
  }
}

/* Raise a REI_ERR from a handle verb as a classed error with the handle's
   recorded message. */
static void chan_raise(rei_channel *c) {
  sora_stop("sora_error", "sora: %s", rei_channel_error(c));
}

/* Raise a create/attach failure off the thread-local slot, where the core
   composes the full message (size + hint included). Space/existence
   failures carry the shm class; everything else is a plain error. */
static void chan_raise_tls(void) {
  rei_errcat cat = rei_last_error_category();
  const char *msg = rei_last_error_message();
  switch (cat) {
  case REI_ERRCAT_NOSPACE:
  case REI_ERRCAT_NOMEMORY:
  case REI_ERRCAT_EXISTS:
    sora_stop_shm(NA_REAL, "sora: %s", msg);
  default:
    Rf_error("sora: %s", msg);
  }
}

/* Provenance, not class: TRUE only for the interned singletons themselves,
   so a payload merely carrying the class never passes. */
SEXP sora_sentinel_check(SEXP x) {
  return Rf_ScalarLogical(x == sora_sent_full || x == sora_sent_timeout ||
                          x == sora_sent_closed || x == sora_sent_gone);
}

// Create (host) -------------------------------------------------------------------

static int sora_pow2(uint64_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

SEXP sora_channel_create(SEXP expr, SEXP cap_sexp, SEXP slot_sexp,
                         SEXP arena_sexp, SEXP spin) {
  uint64_t cap = (uint64_t) Rf_asInteger(cap_sexp);
  uint64_t slot = (uint64_t) Rf_asInteger(slot_sexp);
  double arena_in = Rf_asReal(arena_sexp);
  if (!sora_pow2(cap) || cap < 2 || cap > (1u << 24))
    Rf_error("sora: capacity must be a power of two between 2 and 2^24");
  if (!sora_pow2(slot) || slot < 64 || slot > (1u << 20))
    Rf_error("sora: slot_size must be a power of two between 64 and 2^20");
  if (!(arena_in >= 0) || arena_in > 1.1e12 ||
      (uint64_t) arena_in % 64 != 0)
    Rf_error("sora: arena_size must be a non-negative multiple of 64");

  /* the drop: the peer's bootstrap expression as an REI_DROP_R-tagged
     serialize stream (the core copies it into the region at create) */
  size_t expr_size = mori_serialize_count(expr);
  unsigned char *drop = malloc(expr_size + 1);
  if (drop == NULL) Rf_error("sora: allocation failure");
  drop[0] = REI_DROP_R;
  mori_serialize_into(drop + 1, expr);

  sora_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) { free(drop); Rf_error("sora: allocation failure"); }
  rei_channel_opts opts;
  rei_channel_opts_init(&opts);
  opts.capacity = (uint32_t) cap;
  opts.slot_size = (uint32_t) slot;
  opts.arena_size = (uint64_t) arena_in;
  opts.flags = Rf_asLogical(spin) == TRUE ? REI_FLAG_SPIN : 0;
  opts.drop = drop;
  opts.drop_size = (uint64_t) expr_size + 1;
  rei_binding b;
  chan_binding(h, &b);

  rei_channel *c;
  rei_status st = rei_channel_create(&c, &opts, &b);
  free(drop);
  if (st != REI_OK) {
    free(h);
    chan_raise_tls();
  }
  h->core = (rei_handle *) c;
  h->self_pid = rei_self_pid();
  return chan_wrap(h);
}

SEXP sora_channel_suffix(SEXP xp) {
  rei_channel *c = chan_core(xp);
  char buf[64];
  if (rei_channel_token(c, buf, sizeof(buf)) != REI_OK)
    chan_raise(c);
  return Rf_mkString(buf);
}

/* Startup rendezvous: the host waits for the peer's ready word. Returns
   FALSE on deadline expiry — the caller walks the channel back. */
SEXP sora_channel_ready_wait(SEXP xp, SEXP timeout) {
  rei_channel *c = chan_core(xp);
  rei_status st = rei_channel_ready_wait(c, timeout_ms_of(timeout));
  if (st == REI_ERR) chan_raise(c);
  return Rf_ScalarLogical(st == REI_OK);
}

/* Startup walk-back: signal close so a late-attaching peer exits instead of
   parking against a host that gave up, then unlink everything. */
SEXP sora_channel_destroy(SEXP xp) {
  sora_handle *h = chan_get(xp);
  rei_channel_destroy((rei_channel *) h->core);
  h->core = NULL;
  return R_NilValue;
}

// Attach (peer) -------------------------------------------------------------------

SEXP sora_channel_attach(SEXP suffix_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("sora: expected a region-name suffix");
  const char *suffix = CHAR(STRING_ELT(suffix_sexp, 0));

  sora_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) Rf_error("sora: allocation failure");
  rei_binding b;
  chan_binding(h, &b);
  rei_channel *c;
  if (rei_channel_attach(&c, suffix, &b) != REI_OK) {
    free(h);
    chan_raise_tls();
  }
  h->core = (rei_handle *) c;
  h->self_pid = rei_self_pid();

  SEXP xp = PROTECT(chan_wrap(h));

  /* materialize-before-ready: the host's frame keeps the expression — and
     through mori's keeper chains every region its identifiers name — alive
     exactly until ready is observed. The drop is REI_DROP_R-tagged. */
  const uint8_t *bytes;
  uint64_t n;
  rei_channel_drop(c, &bytes, &n);
  if (n == 0 || bytes[0] != REI_DROP_R)
    Rf_error("sora: foreign channel drop (not an R bootstrap)");
  SEXP drop = PROTECT(mori_unserialize_from(bytes + 1, (size_t) n - 1));
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(out, 0, xp);
  SET_VECTOR_ELT(out, 1, drop);
  UNPROTECT(3);
  return out;
}

SEXP sora_channel_ready_set(SEXP xp) {
  rei_channel *c = chan_core(xp);
  if (rei_channel_ready_set(c) != REI_OK) chan_raise(c);
  return R_NilValue;
}

// Verbs ---------------------------------------------------------------------------

SEXP sora_channel_send(SEXP xp, SEXP x) {
  rei_channel *c = chan_core(xp);
  rei_status st = rei_channel_send(c, (void *) x);
  if (st == REI_ERR) chan_raise(c);
  return st == REI_OK ? Rf_ScalarLogical(TRUE) : status_sentinel(st);
}

/* One .Call; the core batches the tail store and the wake. Returns the count
   accepted (short on ring-full or close midway — probe why with sora_send). */
SEXP sora_channel_send_batch(SEXP xp, SEXP xs) {
  rei_channel *c = chan_core(xp);
  if (TYPEOF(xs) != VECSXP)
    Rf_error("sora: expected a list of payloads");
  R_xlen_t n = XLENGTH(xs);
  void **objs = (void **) R_alloc(n, sizeof(void *));
  for (R_xlen_t i = 0; i < n; i++)
    objs[i] = (void *) VECTOR_ELT(xs, i);
  size_t accepted = 0;
  rei_status st = rei_channel_send_batch(c, objs, (size_t) n, &accepted);
  if (st == REI_ERR) chan_raise(c);
  return Rf_ScalarInteger((int) accepted);
}

SEXP sora_channel_recv(SEXP xp, SEXP timeout) {
  rei_channel *c = chan_core(xp);
  void *obj = NULL;
  rei_status st = rei_channel_recv(c, &obj, timeout_ms_of(timeout));
  if (st == REI_ERR) chan_raise(c);
  if (st != REI_OK) return status_sentinel(st);
  return (SEXP) obj;
}

/* Up to n messages; waits only for the first, then drains already-published
   ones without waiting further. The sentinel discipline matches recv. */
SEXP sora_channel_recv_batch(SEXP xp, SEXP n_sexp, SEXP timeout) {
  rei_channel *c = chan_core(xp);
  int n = Rf_asInteger(n_sexp);
  if (n < 1) Rf_error("sora: n must be at least 1");
  void **objs = (void **) R_alloc(n, sizeof(void *));
  size_t count = 0;
  rei_status st =
    rei_channel_recv_batch(c, objs, (size_t) n, &count, timeout_ms_of(timeout));
  if (st == REI_ERR) chan_raise(c);
  if (st != REI_OK) return status_sentinel(st);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) count));
  for (size_t i = 0; i < count; i++)
    SET_VECTOR_ELT(out, (R_xlen_t) i, (SEXP) objs[i]);
  UNPROTECT(1);
  return out;
}

// Close protocol ------------------------------------------------------------------

/* The peer half of the protocol, run by peer_main's epilogue: set our bit,
   wake the host. No rendezvous — process exit releases everything else. */
SEXP sora_channel_close_signal(SEXP xp) {
  sora_handle *h = chan_peek(xp);
  if (h == NULL) return R_NilValue;
  rei_channel_close_signal((rei_channel *) h->core);
  return R_NilValue;
}

SEXP sora_channel_close(SEXP xp, SEXP timeout) {
  sora_handle *h = chan_peek(xp);
  if (h == NULL) return Rf_ScalarLogical(TRUE);   /* close is idempotent */
  rei_channel *c = (rei_channel *) h->core;
  rei_status st = rei_channel_close(c, timeout_ms_of(timeout));
  if (st == REI_ERR) chan_raise(c);
  if (st == REI_OK) {
    /* rendezvoused: the handle is dead — destroy releases it */
    rei_channel_destroy(c);
    h->core = NULL;
    return Rf_ScalarLogical(TRUE);
  }
  return Rf_ScalarLogical(FALSE);   /* REI_TIMEOUT: destroy retries it */
}

// Introspection -------------------------------------------------------------------

/* Reports peer *process* liveness (the fd-scoped lock verdict): an orderly
   close with the process still running is alive; a released handle is not. */
SEXP sora_channel_alive(SEXP xp) {
  sora_handle *h = chan_peek(xp);
  if (h == NULL) return Rf_ScalarLogical(FALSE);
  return Rf_ScalarLogical(rei_channel_alive((rei_channel *) h->core));
}

SEXP sora_channel_stat(SEXP xp) {
  sora_handle *h = chan_get(xp);
  rei_channel *c = (rei_channel *) h->core;
  rei_channel_info info;
  if (rei_channel_info_get(c, &info) != REI_OK) chan_raise(c);
  const char *names[] = {"name", "side", "capacity", "slot_size",
                         "arena_size", "inline_max", "spin", "ready",
                         "closed", "peer_pid", "tx_sent", "tx_published",
                         "tx_consumed", "rx_consumed", "rx_published",
                         "fl_entries", "fl_hits", "open_hits",
                         "open_misses", "ledger_entries", "zc_open_hits",
                         "zc_open_misses", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(info.name));
  SET_VECTOR_ELT(out, 1, Rf_mkString(info.side == REI_ENTITY_HOST ?
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
