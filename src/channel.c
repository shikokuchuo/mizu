/* Part I SPSC channel: one producer, one consumer, one ring per direction,
   over a single host-created region mapped read-write by both sides. The hot
   path is entirely in user space — cached index snapshots, batched
   publication, a spill arena for mid-size payloads — with directed
   parker wakes at the edges and event-driven peer-death detection. The
   channel region layout is the table in ipc-plan.md; kioto.h's KIO_OFF_*
   constants are its offsets. */

#include <stdlib.h>
#include <stdio.h>
#include "kioto.h"
#include <R_ext/Utils.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <unistd.h>
#include <time.h>
#include <sys/mman.h>
#endif

/* Consumer head publication cadence: publish every K messages, on
   drain-empty, and before parking. The sender sees at most K slots less
   free space than truly exists and its keeper release trails by at most K
   slots — both benign. */
#define KIO_HEAD_PUBLISH_K 32

/* Slot framing is the shared kio_slot_hdr wire form (kioto.h / payload.c):
   every slot is a 16-byte header then the inline region (slot - 16 bytes),
   with the channel adding the ARENA tier around the shared kinds. */

// Channel state -------------------------------------------------------------------

/* One direction's ring. Shared pointers alias the mapped region; everything
   below them is process-local — producer-local cursors on the side that
   produces, consumer-local on the side that consumes; neither is ever
   mirrored into shared memory. */
typedef struct kio_chan_ring_s {
  _Atomic int64_t *tail;         /* shared: producer-published */
  _Atomic int64_t *head;         /* shared: consumer-published */
  unsigned char *slots;
  unsigned char *arena;          /* NULL when arena_size == 0 */
  uint64_t arena_size;
  uint64_t mask;
  uint32_t cap;
  uint32_t slot;
  /* producer-local */
  int64_t ltail;                 /* next slot to write */
  int64_t ptail;                 /* last published tail */
  int64_t cached_head;
  int64_t reaped_head;
  uint64_t aalloc, afree;        /* monotonic arena byte cursors */
  uint64_t *aend;                /* per-slot aalloc after that send */
  /* consumer-local */
  int64_t lhead;                 /* next slot to read */
  int64_t phead;                 /* last published head */
  int64_t cached_tail;
  uint32_t unpublished;
} kio_chan_ring;

typedef struct kio_chan_s {
  mori_shm shm;                  /* our mapping; unmapped only in release */
  kio_preamble pre;
  unsigned char *base;
  int side;                      /* KIO_ENTITY_HOST or KIO_ENTITY_PEER */
  int spin;
  int released;                  /* full teardown ran; handle is dead */
  int verdict_dead;              /* sticky flock-confirmed peer death */
  int names_unlinked;            /* survivor cleanup already ran */
  int pk_ok;
  long self_pid;                 /* fork guard */
  uint32_t inline_max;

  _Atomic uint32_t *ready;
  _Atomic uint32_t *closedw;     /* bit 1 = host closed, bit 2 = peer closed */
  _Atomic uint64_t *peer_pid;
  _Atomic uint32_t *self_parked, *peer_parked;
  _Atomic uint32_t *self_reg, *peer_reg;

  kio_parker self_pk;            /* we park here; the peer unparks it */
  kio_parker peer_pk;            /* we unpark this */
  intptr_t live_self, live_peer; /* kept liveness fds/handles; 0 = not open */
  char live_self_path[1024];
  char live_peer_path[1024];
  _Atomic int peer_dead;         /* death-listener flag: wake trigger only */
  kio_death_watch *watch;

  /* SHM_RAW arena-overflow fallback reuse, as on pool handles: producer
     spill free list and consumer mapping cache, wraps pinned by prot[2] /
     prot[3] */
  kio_spill_fl fl;
  kio_open_cache oc;

  kio_chan_ring tx, rx;
} kio_chan;

enum {
  KIO_ST_OK = 0,
  KIO_ST_FULL,
  KIO_ST_CLOSED,
  KIO_ST_GONE,
  KIO_ST_TIMEOUT
};

static SEXP kio_chan_tag;
static SEXP kio_class_channel;
SEXP kio_sent_full, kio_sent_timeout, kio_sent_closed, kio_sent_gone;

static SEXP kio_make_sentinel(const char *value, const char *cls) {
  SEXP s = PROTECT(Rf_mkString(value));
  SEXP klass = PROTECT(Rf_allocVector(STRSXP, 2));
  SET_STRING_ELT(klass, 0, Rf_mkChar(cls));
  SET_STRING_ELT(klass, 1, Rf_mkChar("kio_sentinel"));
  Rf_setAttrib(s, R_ClassSymbol, klass);
  R_PreserveObject(s);
  UNPROTECT(2);
  return s;
}

void kio_channel_init(void) {
  kio_chan_tag = Rf_install("kio_channel");
  kio_class_channel = Rf_mkString("kio_channel");
  R_PreserveObject(kio_class_channel);
  kio_sent_full = kio_make_sentinel("full", "kio_full");
  kio_sent_timeout = kio_make_sentinel("timeout", "kio_timeout");
  kio_sent_closed = kio_make_sentinel("closed", "kio_closed");
  kio_sent_gone = kio_make_sentinel("peer_gone", "kio_peer_gone");
}

// Small helpers -------------------------------------------------------------------

double kio_now(void) {
#ifdef _WIN32
  return (double) GetTickCount64() / 1000.0;
#else
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double) ts.tv_sec + (double) ts.tv_nsec / 1e9;
#endif
}

long kio_self_pid(void) {
#ifdef _WIN32
  return (long) GetCurrentProcessId();
#else
  return (long) getpid();
#endif
}

static void kio_unlink_region_name(const char *name) {
#if defined(_WIN32)
  (void) name;                   /* kernel object: no unlink step */
#elif defined(__linux__)
  char path[64];
  snprintf(path, sizeof(path), "/dev/shm%s", name);
  unlink(path);
#else
  shm_unlink(name);
#endif
}

static uint32_t kio_closed_bit(const kio_chan *c) {
  return c->side == KIO_ENTITY_HOST ? 1u : 2u;
}

static SEXP kio_status_sentinel(int st) {
  switch (st) {
  case KIO_ST_FULL:    return kio_sent_full;
  case KIO_ST_CLOSED:  return kio_sent_closed;
  case KIO_ST_GONE:    return kio_sent_gone;
  case KIO_ST_TIMEOUT: return kio_sent_timeout;
  }
  return R_NilValue;
}

/* Provenance, not class: TRUE only for the interned singletons themselves,
   so a payload merely carrying the class never passes. */
SEXP kio_sentinel_check(SEXP x) {
  return Rf_ScalarLogical(x == kio_sent_full || x == kio_sent_timeout ||
                          x == kio_sent_closed || x == kio_sent_gone);
}

// Handle access -------------------------------------------------------------------

/* NULL when the handle has already been released (closed / destroyed). */
static kio_chan *chan_peek(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != kio_chan_tag)
    Rf_error("kioto: not a channel handle");
  kio_chan *c = (kio_chan *) R_ExternalPtrAddr(xp);
  if (c == NULL || c->released) return NULL;
  if (c->self_pid != kio_self_pid())
    Rf_error("kioto: channel handles do not survive fork()");
  return c;
}

static kio_chan *chan_get(SEXP xp) {
  kio_chan *c = chan_peek(xp);
  if (c == NULL) Rf_error("kioto: channel handle is closed");
  return c;
}

static SEXP chan_keepers(SEXP xp) {
  return VECTOR_ELT(R_ExternalPtrProtected(xp), 0);
}

// Wiring --------------------------------------------------------------------------

/* Build all region pointers from a validated preamble. Local cursors are
   initialised from the shared indices (zero on a fresh region; correct
   either way). */
static void chan_wire(kio_chan *c, const kio_preamble *p) {
  unsigned char *b = (unsigned char *) c->shm.addr;
  c->pre = *p;
  c->base = b;
  c->inline_max = p->slot - (uint32_t) sizeof(kio_slot_hdr);
  c->spin = (*(uint32_t *) (b + KIO_OFF_FLAGS) & KIO_FLAG_SPIN) != 0;

  c->ready = (_Atomic uint32_t *) (b + KIO_OFF_READY);
  c->closedw = (_Atomic uint32_t *) (b + KIO_OFF_CLOSED);
  c->peer_pid = (_Atomic uint64_t *) (b + KIO_OFF_PEER_PID);

  int self = c->side, peer = 1 - c->side;
  c->self_parked =
    (_Atomic uint32_t *) (b + KIO_ENTITY_OFFSET(self) + KIO_ENTITY_PARKED);
  c->peer_parked =
    (_Atomic uint32_t *) (b + KIO_ENTITY_OFFSET(peer) + KIO_ENTITY_PARKED);
  c->self_reg =
    (_Atomic uint32_t *) (b + KIO_ENTITY_OFFSET(self) + KIO_ENTITY_REG);
  c->peer_reg =
    (_Atomic uint32_t *) (b + KIO_ENTITY_OFFSET(peer) + KIO_ENTITY_REG);

  uint64_t ring_bytes = (uint64_t) p->cap * p->slot;
  kio_chan_ring hp = {0}, ph = {0};
  hp.tail = (_Atomic int64_t *) (b + KIO_OFF_HP_TAIL);
  hp.head = (_Atomic int64_t *) (b + KIO_OFF_HP_HEAD);
  ph.tail = (_Atomic int64_t *) (b + KIO_OFF_PH_TAIL);
  ph.head = (_Atomic int64_t *) (b + KIO_OFF_PH_HEAD);
  hp.slots = b + KIO_FIXED_LAYOUT_SIZE;
  ph.slots = hp.slots + ring_bytes;
  if (p->arena_size > 0) {
    hp.arena = ph.slots + ring_bytes;
    ph.arena = hp.arena + p->arena_size;
  }
  hp.arena_size = ph.arena_size = p->arena_size;
  hp.cap = ph.cap = p->cap;
  hp.slot = ph.slot = p->slot;
  hp.mask = ph.mask = (uint64_t) p->cap - 1;

  c->tx = c->side == KIO_ENTITY_HOST ? hp : ph;
  c->rx = c->side == KIO_ENTITY_HOST ? ph : hp;

  c->tx.ltail = c->tx.ptail = c->tx.cached_head = c->tx.reaped_head =
    atomic_load_explicit(c->tx.tail, memory_order_acquire);
  c->rx.lhead = c->rx.phead = c->rx.cached_tail =
    atomic_load_explicit(c->rx.head, memory_order_acquire);
}

/* Both liveness files live in the host-chosen directory recorded in the
   preamble; neither side ever resolves the path independently (a launcher
   that scrubs TMPDIR must not be able to split the death protocol). */
static int chan_live_paths(kio_chan *c, const char *livedir) {
  const char *suffix = c->shm.name + strlen(MORI_PREFIX_LITERAL);
  int n1 = snprintf(c->live_self_path, sizeof(c->live_self_path),
                    "%s/kio_%s.live.%s", livedir, suffix,
                    c->side == KIO_ENTITY_HOST ? "host" : "peer");
  int n2 = snprintf(c->live_peer_path, sizeof(c->live_peer_path),
                    "%s/kio_%s.live.%s", livedir, suffix,
                    c->side == KIO_ENTITY_HOST ? "peer" : "host");
  return (n1 > 0 && (size_t) n1 < sizeof(c->live_self_path) &&
          n2 > 0 && (size_t) n2 < sizeof(c->live_peer_path)) ? 0 : -1;
}

// Peer-death ----------------------------------------------------------------------

/* The listener's flag is only ever a wake trigger; this fd-scoped probe is
   the verdict, and acquiring the dead side's lock serializes survivor
   cleanup. The verdict is sticky: once confirmed, no re-probe. */
static void chan_survivor_unlink(kio_chan *c, SEXP prot) {
  if (c->names_unlinked) return;
  c->names_unlinked = 1;
  if (c->side == KIO_ENTITY_HOST) {
    SEXP host_ptr = VECTOR_ELT(prot, 1);
    if (host_ptr != R_NilValue) mori_host_finalizer(host_ptr);
  } else {
    kio_unlink_region_name(c->shm.name);
  }
  if (c->live_self_path[0] != '\0') remove(c->live_self_path);
  if (c->live_peer_path[0] != '\0') remove(c->live_peer_path);
}

static int chan_probe_dead(kio_chan *c, SEXP prot) {
  if (c->verdict_dead) return 1;
  if (c->live_peer == 0) return 0;
  if (kio_live_try(c->live_peer) != KIO_LIVE_ACQUIRED) return 0;
  c->verdict_dead = 1;
  chan_survivor_unlink(c, prot);
  return 1;
}

// Release -------------------------------------------------------------------------

/* Full teardown, idempotent. The order is load-bearing: the death watch and
   parkers reference the mapping (the watch's unpark target is the epoch word
   inside it), so both stop before the munmap. */
static void chan_release(kio_chan *c, SEXP prot, int unlink_names) {
  if (c->released) return;
  c->released = 1;
  if (c->watch != NULL) {
    kio_death_watch_stop(c->watch);
    c->watch = NULL;
  }
  if (c->pk_ok) {
    kio_parker_detach(&c->self_pk);
    kio_parker_detach(&c->peer_pk);
    c->pk_ok = 0;
  }
  if (unlink_names) chan_survivor_unlink(c, prot);
  if (c->shm.addr != NULL) mori_shm_close(&c->shm, 0);
  c->base = NULL;
  if (c->live_self != 0) {
    kio_live_close(c->live_self);
    c->live_self = 0;
  }
  if (c->live_peer != 0) {
    kio_live_close(c->live_peer);
    c->live_peer = 0;
  }
}

static void kio_chan_finalizer(SEXP xp) {
  kio_chan *c = (kio_chan *) R_ExternalPtrAddr(xp);
  if (c == NULL) return;
  SEXP prot = R_ExternalPtrProtected(xp);
  if (!c->released) {
    /* the close protocol's rendezvous re-check: unlink only once the peer
       has set its bit or its death is confirmed — a live peer's in-flight
       drain is never raced */
    int unlink_names = 0;
    if (c->base != NULL) {
      uint32_t cw = atomic_load_explicit(c->closedw, memory_order_acquire);
      uint32_t other = 3u ^ kio_closed_bit(c);
      unlink_names = (cw & other) != 0 || c->verdict_dead ||
        (c->live_peer != 0 && kio_live_try(c->live_peer) == KIO_LIVE_ACQUIRED);
    }
    chan_release(c, prot, unlink_names);
  }
  free(c->tx.aend);
  free(c);
  R_ClearExternalPtr(xp);
}

// Keeper reap ---------------------------------------------------------------------

/* Walk the shared head forward, clearing keepers the consumer has drained
   and advancing the arena free cursor past their chunks. Also refreshes the
   producer's cached head, so the full check and the reap ride one load.
   Head-advanced is the channel's consumer-done signal (recv materializes
   before publishing), so a drained SHM_RAW keeper's region surrenders to
   the free list here — the only sender-side release point: the send-slot
   overwrite never sees a live keeper, since the ring's full check keeps
   ltail within cap of the reaped head. */
static void chan_reap(kio_chan *c, SEXP keepers) {
  kio_chan_ring *r = &c->tx;
  int64_t head = atomic_load_explicit(r->head, memory_order_acquire);
  r->cached_head = head;
  if (head <= r->reaped_head) return;
  for (int64_t i = r->reaped_head; i < head; i++)
    kio_spill_fl_surrender(&c->fl, keepers,
                           (R_xlen_t) ((uint64_t) i & r->mask));
  r->afree = r->aend[(uint64_t) (head - 1) & r->mask];
  r->reaped_head = head;
}

// Spill arena ---------------------------------------------------------------------

/* Producer-local FIFO byte-ring: bump-allocated in slot order, freed in slot
   order by the reap, contiguous always (a chunk that would straddle the end
   pads to the start; the pad is accounted to the monotonic cursor and freed
   with the chunk). The consumer writes no arena state. */
static int chan_arena_alloc(kio_chan *c, SEXP keepers, uint64_t n,
                            uint64_t *out) {
  kio_chan_ring *r = &c->tx;
  if (r->arena == NULL || n == 0 || n > r->arena_size) return 0;
  for (int attempt = 0; ; attempt++) {
    uint64_t off = r->aalloc % r->arena_size;
    uint64_t pad = off + n <= r->arena_size ? 0 : r->arena_size - off;
    if (r->aalloc + pad + n - r->afree <= r->arena_size) {
      r->aalloc += pad;
      *out = r->aalloc % r->arena_size;
      r->aalloc += n;
      return 1;
    }
    if (attempt > 0) return 0;
    chan_reap(c, keepers);       /* full -> reap -> retry, then give up */
  }
}

// Send ----------------------------------------------------------------------------

static int chan_send1(kio_chan *c, SEXP prot, SEXP x) {
  SEXP keepers = VECTOR_ELT(prot, 0);
  kio_chan_ring *r = &c->tx;

  if (atomic_load_explicit(c->closedw, memory_order_acquire) != 0)
    return KIO_ST_CLOSED;
  if (c->verdict_dead) return KIO_ST_GONE;

  if (r->ltail - r->cached_head >= (int64_t) r->cap) {
    chan_reap(c, keepers);
    if (r->ltail - r->cached_head >= (int64_t) r->cap)
      /* full is off the hot path and exactly where "peer stopped draining"
         needs disambiguating */
      return chan_probe_dead(c, prot) ? KIO_ST_GONE : KIO_ST_FULL;
  }

  uint64_t idx = (uint64_t) r->ltail & r->mask;
  unsigned char *sl = r->slots + idx * r->slot;
  kio_slot_hdr *hdr = (kio_slot_hdr *) sl;
  unsigned char *payload = sl + sizeof(kio_slot_hdr);
  SEXP keep = x;
  int nprotect = 0;

  size_t rawlen;
  if (kio_raw_eligible(x, c->inline_max, &rawlen)) {
    memcpy(payload, kio_vec_ptr(x), rawlen);
    hdr->kind = KIO_KIND_RAWVEC;
    hdr->len = (uint32_t) rawlen;
    hdr->aux = (uint64_t) TYPEOF(x);
  } else {
    size_t n = kio_serialize_bounded(payload, c->inline_max, x);
    if (n <= c->inline_max) {
      hdr->kind = KIO_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = 0;
    } else {
      uint64_t off;
      if (chan_arena_alloc(c, keepers, MORI_ALIGN64(n), &off)) {
        mori_serialize_into(r->arena + off, n, x);
        hdr->kind = KIO_KIND_ARENA;
        hdr->len = 0;
        hdr->aux = off;
        uint64_t n64 = (uint64_t) n;
        memcpy(payload, &n64, sizeof(n64));
      } else {
        /* reap before staging: the consumer's latest head publish may have
           released a fitting region for this very spill to pop */
        chan_reap(c, keepers);
        keep = PROTECT(kio_payload_spill_shm(hdr, payload, x, n, &c->fl));
        nprotect++;
      }
    }
  }

  /* Pin unconditionally — whether a stream carries hook-emitted mori
     identifiers is not knowable without inspecting it, so INLINE and RAWVEC
     pin too; the store costs nothing and the keeper story stays free of
     kind analysis. */
  SET_VECTOR_ELT(keepers, (R_xlen_t) idx, keep);
  r->aend[idx] = r->aalloc;
  r->ltail++;
  UNPROTECT(nprotect);
  return KIO_ST_OK;
}

/* Publication: shared stores are an order of magnitude dearer than local
   ones, so the shared tail moves only here — after every send, once per
   batch. The wake-register OR is transition-only and the unpark
   parked-gated, so the steady-state publish is a release store plus two
   loads and no syscall. */
static void chan_flush(kio_chan *c) {
  kio_chan_ring *r = &c->tx;
  if (r->ptail == r->ltail) return;
  atomic_store_explicit(r->tail, r->ltail, memory_order_release);
  r->ptail = r->ltail;
  if (!(atomic_load_explicit(c->peer_reg, memory_order_relaxed) & 1u))
    atomic_fetch_or_explicit(c->peer_reg, 1u, memory_order_release);
  if (!c->spin) {
    /* Dekker with the consumer's announce: its parked-flag store and our
       tail store are each fenced before the cross-check, so either it sees
       the data in its re-check or we see the flag. */
    atomic_thread_fence(memory_order_seq_cst);
    if (atomic_load_explicit(c->peer_parked, memory_order_relaxed) != 0)
      kio_unpark(&c->peer_pk);
  }
}

// Recv ----------------------------------------------------------------------------

static int chan_rx_avail(kio_chan *c) {
  kio_chan_ring *r = &c->rx;
  if (r->lhead < r->cached_tail) return 1;
  r->cached_tail = atomic_load_explicit(r->tail, memory_order_acquire);
  return r->lhead < r->cached_tail;
}

/* Publication is what signals the sender "keepers may drop"; the tx-keeper
   reap piggybacks on the same cadence — one extra shared load per batch. */
static void chan_publish_head(kio_chan *c, SEXP keepers) {
  kio_chan_ring *r = &c->rx;
  if (r->phead != r->lhead) {
    atomic_store_explicit(r->head, r->lhead, memory_order_release);
    r->phead = r->lhead;
  }
  r->unpublished = 0;
  chan_reap(c, keepers);
}

static SEXP chan_materialize(kio_chan *c, const unsigned char *sl) {
  const kio_slot_hdr *hdr = (const kio_slot_hdr *) sl;
  const unsigned char *payload = sl + sizeof(kio_slot_hdr);

  if (hdr->kind == KIO_KIND_ARENA) {
    uint64_t off = hdr->aux, n;
    memcpy(&n, payload, sizeof(n));
    if (c->rx.arena == NULL || off > c->rx.arena_size ||
        n > c->rx.arena_size - off)
      Rf_error("kioto: corrupt payload slot");
    /* already mapped: no open, no syscall */
    return mori_unserialize_from(c->rx.arena + off, (size_t) n);
  }
  return kio_payload_read(hdr, payload, c->inline_max, NULL, &c->oc);
}

/* Block until a message is available at rx.lhead (KIO_ST_OK) or a verdict.
   Drain-before-verdict: closed and peer-death are reported only through an
   empty ring, with one final tail refresh after the flag read so a
   publish-then-signal sequence is never inverted. */
static int chan_wait_msg(kio_chan *c, SEXP prot, double timeout_s) {
  SEXP keepers = VECTOR_ELT(prot, 0);
  double deadline = -1;
  chan_reap(c, keepers);         /* recv is a reap trigger: the quiet-sender
                                    case pins at most cap payloads otherwise */
  for (;;) {
    if (chan_rx_avail(c)) return KIO_ST_OK;

    /* our wake-register bit: clear, then re-check — the producer's OR
       follows its tail publish, so data ORed before the clear is caught */
    if (atomic_load_explicit(c->self_reg, memory_order_relaxed) & 1u) {
      atomic_fetch_and_explicit(c->self_reg, ~1u, memory_order_acq_rel);
      if (chan_rx_avail(c)) return KIO_ST_OK;
    }

    if (atomic_load_explicit(c->closedw, memory_order_acquire) != 0)
      return chan_rx_avail(c) ? KIO_ST_OK : KIO_ST_CLOSED;
    if (c->verdict_dead)
      return chan_rx_avail(c) ? KIO_ST_OK : KIO_ST_GONE;
    if (atomic_load_explicit(&c->peer_dead, memory_order_acquire) &&
        chan_probe_dead(c, prot))
      return chan_rx_avail(c) ? KIO_ST_OK : KIO_ST_GONE;

    if (timeout_s <= 0) {
      chan_publish_head(c, keepers);
      return KIO_ST_TIMEOUT;
    }
    if (deadline < 0 && R_FINITE(timeout_s))
      deadline = kio_now() + timeout_s;

    for (int i = 0; i < KIO_SPIN_ITERS; i++) {
      KIO_PAUSE();
      if (chan_rx_avail(c)) return KIO_ST_OK;
    }

    if (c->spin) {
      /* pure-spin mode: the producer skips wakes, so never park */
      R_CheckUserInterrupt();
      if (deadline >= 0 && kio_now() >= deadline) {
        chan_publish_head(c, keepers);
        return KIO_ST_TIMEOUT;
      }
      continue;
    }

    /* Park: snapshot -> announce -> re-check -> sleep bounded. Snapshot-
       before-announce means any unpark that observes the flag bumps the
       epoch after our snapshot and the sleep returns immediately. A stale
       flag left by an interrupt longjmp costs one wasted wake. */
    uint32_t e = kio_parker_snapshot(&c->self_pk);
    atomic_store_explicit(c->self_parked, 1u, memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    if (chan_rx_avail(c) ||
        atomic_load_explicit(c->closedw, memory_order_acquire) != 0 ||
        atomic_load_explicit(&c->peer_dead, memory_order_acquire) != 0) {
      atomic_store_explicit(c->self_parked, 0u, memory_order_relaxed);
      continue;
    }
    chan_publish_head(c, keepers);     /* always publish before parking */
    long ms = KIO_INTERRUPT_BOUND_MS;
    if (deadline >= 0) {
      double rem = deadline - kio_now();
      if (rem <= 0) {
        atomic_store_explicit(c->self_parked, 0u, memory_order_relaxed);
        return KIO_ST_TIMEOUT;
      }
      long rem_ms = (long) (rem * 1000) + 1;
      if (rem_ms < ms) ms = rem_ms;
    }
    kio_park(&c->self_pk, e, ms);
    atomic_store_explicit(c->self_parked, 0u, memory_order_relaxed);
    R_CheckUserInterrupt();
    if (deadline >= 0 && kio_now() >= deadline)
      return chan_rx_avail(c) ? KIO_ST_OK : KIO_ST_TIMEOUT;
  }
}

/* Materialize the message at rx.lhead (chan_wait_msg returned KIO_ST_OK),
   then advance and publish per the batching rule. Materialize-before-publish
   is the whole correctness story for large-message transport: publication
   is what lets the sender's reap drop the keeper pinning every region this
   message references. */
static SEXP chan_consume1(kio_chan *c, SEXP keepers) {
  kio_chan_ring *r = &c->rx;
  SEXP y = chan_materialize(c, r->slots + ((uint64_t) r->lhead & r->mask) *
                            r->slot);
  r->lhead++;
  r->unpublished++;
  if (r->unpublished >= KIO_HEAD_PUBLISH_K || !chan_rx_avail(c))
    chan_publish_head(c, keepers);
  return y;
}

// Create (host) -------------------------------------------------------------------

static int kio_pow2(uint64_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

SEXP kio_channel_create(SEXP expr, SEXP cap_sexp, SEXP slot_sexp,
                        SEXP arena_sexp, SEXP livedir_sexp, SEXP spin) {
  uint64_t cap = (uint64_t) Rf_asInteger(cap_sexp);
  uint64_t slot = (uint64_t) Rf_asInteger(slot_sexp);
  double arena_in = Rf_asReal(arena_sexp);
  if (!kio_pow2(cap) || cap < 2 || cap > (1u << 24))
    Rf_error("kioto: capacity must be a power of two between 2 and 2^24");
  if (!kio_pow2(slot) || slot < 64 || slot > (1u << 20))
    Rf_error("kioto: slot_size must be a power of two between 64 and 2^20");
  if (!(arena_in >= 0) || arena_in > 1.1e12 ||
      (uint64_t) arena_in % 64 != 0)
    Rf_error("kioto: arena_size must be a non-negative multiple of 64");
  uint64_t arena = (uint64_t) arena_in;
  if (TYPEOF(livedir_sexp) != STRSXP || XLENGTH(livedir_sexp) != 1)
    Rf_error("kioto: expected a liveness directory path");
  const char *livedir = CHAR(STRING_ELT(livedir_sexp, 0));
  size_t livedir_len = strlen(livedir);
  if (livedir_len == 0 || livedir_len > 900)
    Rf_error("kioto: liveness directory path too long");

  /* sized by the vendored count pass: the drop slot has no fixed budget and
     no truncation case */
  size_t expr_size = mori_serialize_count(expr);
  uint64_t ring_bytes = cap * slot;
  uint64_t drop_off = KIO_FIXED_LAYOUT_SIZE + 2 * ring_bytes + 2 * arena;
  uint64_t livedir_off = MORI_ALIGN64(drop_off + expr_size);
  uint64_t total = livedir_off + livedir_len;
  if (total > ((uint64_t) 1 << 46))
    Rf_error("kioto: channel region too large");

  kio_chan *c = calloc(1, sizeof(*c));
  if (c == NULL) Rf_error("kioto: allocation failure");
  int rc = kio_shm_create_populate(&c->shm, (size_t) total);
  if (rc != MORI_OK) {
    free(c);
    const char *summary, *hint;
    mori_err_describe(rc, &summary, &hint);
    kio_stop_shm((double) total,
                 "kioto: cannot create channel region (%llu bytes): %s%s%s",
                 (unsigned long long) total, summary,
                 hint[0] != '\0' ? ". " : "", hint);
  }
  c->side = KIO_ENTITY_HOST;
  c->self_pid = kio_self_pid();

  /* From here cleanup is the finalizer's: build the handle before anything
     that can longjmp. */
  SEXP host_ptr = PROTECT(kio_shm_wrap_host(&c->shm));
  SEXP keepers = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) cap));
  SEXP prot = PROTECT(Rf_allocVector(VECSXP, 4));
  SET_VECTOR_ELT(prot, 0, keepers);
  SET_VECTOR_ELT(prot, 1, host_ptr);
  SET_VECTOR_ELT(prot, 2, Rf_allocVector(VECSXP, KIO_SPILL_FL_MAX));
  c->fl.wraps = VECTOR_ELT(prot, 2);
  SET_VECTOR_ELT(prot, 3, Rf_allocVector(VECSXP, KIO_OPEN_CACHE_MAX));
  c->oc.wraps = VECTOR_ELT(prot, 3);
  SEXP xp = PROTECT(R_MakeExternalPtr(c, kio_chan_tag, prot));
  R_RegisterCFinalizerEx(xp, kio_chan_finalizer, TRUE);
  Rf_setAttrib(xp, R_ClassSymbol, kio_class_channel);

  kio_preamble p = {
    .magic = KIO_MAGIC,
    .version = KIO_ABI_VERSION,
    .cap = (uint32_t) cap,
    .slot = (uint32_t) slot,
    .host_pid = (uint64_t) c->self_pid,
    .arena_size = arena,
    .drop_offset = drop_off,
    .drop_size = expr_size,
    .livedir_offset = livedir_off,
    .livedir_size = livedir_len,
  };
  unsigned char *b = (unsigned char *) c->shm.addr;
  kio_preamble_write(b, &p);
  if (Rf_asLogical(spin) == TRUE)
    *(uint32_t *) (b + KIO_OFF_FLAGS) = KIO_FLAG_SPIN;
  mori_serialize_into(b + drop_off, expr_size, expr);
  memcpy(b + livedir_off, livedir, livedir_len);
  chan_wire(c, &p);
  c->tx.aend = calloc((size_t) cap, sizeof(uint64_t));
  if (c->tx.aend == NULL) Rf_error("kioto: allocation failure");

  if (chan_live_paths(c, livedir) != 0)
    Rf_error("kioto: liveness file path too long");
  if (kio_live_open(c->live_self_path, &c->live_self) != 0 ||
      kio_live_try(c->live_self) != KIO_LIVE_ACQUIRED)
    Rf_error("kioto: cannot lock host liveness file '%s'", c->live_self_path);
  if (kio_live_open(c->live_peer_path, &c->live_peer) != 0)
    Rf_error("kioto: cannot create peer liveness file '%s'", c->live_peer_path);

  /* Windows: the named parker events must exist before any peer opens them */
  if (kio_parker_attach(&c->self_pk,
                        (_Atomic uint32_t *) (b + KIO_ENTITY_OFFSET(0)),
                        c->shm.name, KIO_ENTITY_HOST, 1) != 0 ||
      kio_parker_attach(&c->peer_pk,
                        (_Atomic uint32_t *) (b + KIO_ENTITY_OFFSET(1)),
                        c->shm.name, KIO_ENTITY_PEER, 1) != 0)
    Rf_error("kioto: cannot attach channel parkers");
  c->pk_ok = 1;

  UNPROTECT(4);
  return xp;
}

SEXP kio_channel_suffix(SEXP xp) {
  kio_chan *c = chan_get(xp);
  return Rf_mkString(c->shm.name + strlen(MORI_PREFIX_LITERAL));
}

/* Startup rendezvous: park on the host parker re-checking the ready word;
   the peer unparks after setting it. On success the death listener starts
   watching the pid the peer wrote into the control block. Returns FALSE on
   deadline expiry — the caller walks the channel back. */
SEXP kio_channel_ready_wait(SEXP xp, SEXP timeout) {
  kio_chan *c = chan_get(xp);
  double deadline = kio_now() + Rf_asReal(timeout);
  for (;;) {
    uint32_t e = kio_parker_snapshot(&c->self_pk);
    if (atomic_load_explicit(c->ready, memory_order_acquire) != 0) break;
    double rem = deadline - kio_now();
    if (rem <= 0) return Rf_ScalarLogical(FALSE);
    long ms = (long) (rem * 1000) + 1;
    if (ms > KIO_INTERRUPT_BOUND_MS) ms = KIO_INTERRUPT_BOUND_MS;
    kio_park(&c->self_pk, e, ms);
    R_CheckUserInterrupt();
  }
  uint64_t pid = atomic_load_explicit(c->peer_pid, memory_order_acquire);
  c->watch = kio_death_watch_start((long) pid, &c->peer_dead, &c->self_pk);
  if (c->watch == NULL)
    Rf_error("kioto: cannot watch peer process %llu", (unsigned long long) pid);
  return Rf_ScalarLogical(TRUE);
}

/* Startup walk-back: signal close so a late-attaching peer exits instead of
   parking against a host that gave up, then unlink everything. */
SEXP kio_channel_destroy(SEXP xp) {
  kio_chan *c = chan_get(xp);
  SEXP prot = R_ExternalPtrProtected(xp);
  atomic_fetch_or_explicit(c->closedw, kio_closed_bit(c),
                           memory_order_seq_cst);
  kio_unpark(&c->peer_pk);
  chan_release(c, prot, 1);
  return R_NilValue;
}

// Attach (peer) -------------------------------------------------------------------

SEXP kio_channel_attach(SEXP suffix_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("kioto: expected a region-name suffix");
  const char *suffix = CHAR(STRING_ELT(suffix_sexp, 0));
  for (const char *q = suffix; *q != '\0'; q++)
    if (!((*q >= '0' && *q <= '9') || (*q >= 'a' && *q <= 'f') || *q == '_'))
      Rf_error("kioto: malformed region-name suffix");
  char name[MORI_NAME_MAX];
  int nn = snprintf(name, sizeof(name), "%s%s", MORI_PREFIX_LITERAL, suffix);
  if (nn <= 0 || (size_t) nn >= sizeof(name))
    Rf_error("kioto: malformed region-name suffix");

  kio_chan *c = calloc(1, sizeof(*c));
  if (c == NULL) Rf_error("kioto: allocation failure");
  if (kio_shm_open_rw(&c->shm, name, 1) != 0) {
    free(c);
    kio_stop_shm(NA_REAL, "kioto: cannot open channel region '%s'", name);
  }
  c->side = KIO_ENTITY_PEER;
  c->self_pid = kio_self_pid();

  /* validate before touching any other field */
  kio_preamble p;
  const char *err = kio_preamble_validate(c->shm.addr, c->shm.size, &p);
  if (err == NULL && (p.livedir_size == 0 || p.livedir_size > 900))
    err = "liveness directory path is missing or too long";
  if (err != NULL) {
    mori_shm_close(&c->shm, 0);
    free(c);
    Rf_error("kioto: invalid channel region: %s", err);
  }

  SEXP keepers = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) p.cap));
  SEXP prot = PROTECT(Rf_allocVector(VECSXP, 4));
  SET_VECTOR_ELT(prot, 0, keepers);
  SET_VECTOR_ELT(prot, 1, R_NilValue);   /* no host extptr on the peer side */
  SET_VECTOR_ELT(prot, 2, Rf_allocVector(VECSXP, KIO_SPILL_FL_MAX));
  c->fl.wraps = VECTOR_ELT(prot, 2);
  SET_VECTOR_ELT(prot, 3, Rf_allocVector(VECSXP, KIO_OPEN_CACHE_MAX));
  c->oc.wraps = VECTOR_ELT(prot, 3);
  SEXP xp = PROTECT(R_MakeExternalPtr(c, kio_chan_tag, prot));
  R_RegisterCFinalizerEx(xp, kio_chan_finalizer, TRUE);
  Rf_setAttrib(xp, R_ClassSymbol, kio_class_channel);

  chan_wire(c, &p);
  c->tx.aend = calloc(p.cap, sizeof(uint64_t));
  if (c->tx.aend == NULL) Rf_error("kioto: allocation failure");

  char livedir[1024];
  memcpy(livedir, c->base + p.livedir_offset, (size_t) p.livedir_size);
  livedir[p.livedir_size] = '\0';
  if (chan_live_paths(c, livedir) != 0)
    Rf_error("kioto: liveness file path too long");
  if (kio_live_open(c->live_self_path, &c->live_self) != 0 ||
      kio_live_try(c->live_self) != KIO_LIVE_ACQUIRED)
    Rf_error("kioto: cannot lock peer liveness file '%s'", c->live_self_path);
  if (kio_live_open(c->live_peer_path, &c->live_peer) != 0)
    Rf_error("kioto: cannot open host liveness file '%s'", c->live_peer_path);
  if (kio_live_try(c->live_peer) == KIO_LIVE_ACQUIRED) {
    c->verdict_dead = 1;
    chan_survivor_unlink(c, prot);
    Rf_error("kioto: host died before the channel was established");
  }

  if (kio_parker_attach(&c->self_pk,
                        (_Atomic uint32_t *) (c->base + KIO_ENTITY_OFFSET(1)),
                        c->shm.name, KIO_ENTITY_PEER, 0) != 0 ||
      kio_parker_attach(&c->peer_pk,
                        (_Atomic uint32_t *) (c->base + KIO_ENTITY_OFFSET(0)),
                        c->shm.name, KIO_ENTITY_HOST, 0) != 0)
    Rf_error("kioto: cannot attach channel parkers");
  c->pk_ok = 1;

  /* getppid() is unreliable through the sh -c exec chain: watch the pid the
     host wrote into the preamble */
  c->watch = kio_death_watch_start((long) p.host_pid, &c->peer_dead,
                                   &c->self_pk);
  if (c->watch == NULL)
    Rf_error("kioto: cannot watch host process %llu",
             (unsigned long long) p.host_pid);

  /* materialize-before-ready: the host's kio_channel frame keeps the
     expression — and through mori's keeper chains every region its
     identifiers name — alive exactly until ready is observed */
  SEXP drop = PROTECT(mori_unserialize_from(c->base + p.drop_offset,
                                            (size_t) p.drop_size));
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(out, 0, xp);
  SET_VECTOR_ELT(out, 1, drop);
  UNPROTECT(5);
  return out;
}

SEXP kio_channel_ready_set(SEXP xp) {
  kio_chan *c = chan_get(xp);
  atomic_store_explicit(c->peer_pid, (uint64_t) c->self_pid,
                        memory_order_release);
  atomic_store_explicit(c->ready, 1u, memory_order_release);
  kio_unpark(&c->peer_pk);
  return R_NilValue;
}

// Verbs ---------------------------------------------------------------------------

SEXP kio_channel_send(SEXP xp, SEXP x) {
  kio_chan *c = chan_get(xp);
  int st = chan_send1(c, R_ExternalPtrProtected(xp), x);
  chan_flush(c);
  chan_reap(c, chan_keepers(xp));
  return st == KIO_ST_OK ? Rf_ScalarLogical(TRUE) : kio_status_sentinel(st);
}

/* One .Call, one flush, one wake check, one reap; stops at the first message
   the ring refuses and returns the count accepted (probe why with kio_send). */
SEXP kio_channel_send_batch(SEXP xp, SEXP xs) {
  kio_chan *c = chan_get(xp);
  SEXP prot = R_ExternalPtrProtected(xp);
  if (TYPEOF(xs) != VECSXP)
    Rf_error("kioto: expected a list of payloads");
  R_xlen_t n = XLENGTH(xs), i;
  for (i = 0; i < n; i++)
    if (chan_send1(c, prot, VECTOR_ELT(xs, i)) != KIO_ST_OK) break;
  chan_flush(c);
  chan_reap(c, chan_keepers(xp));
  return Rf_ScalarInteger((int) i);
}

SEXP kio_channel_recv(SEXP xp, SEXP timeout) {
  kio_chan *c = chan_get(xp);
  SEXP prot = R_ExternalPtrProtected(xp);
  int st = chan_wait_msg(c, prot, Rf_asReal(timeout));
  if (st != KIO_ST_OK) return kio_status_sentinel(st);
  return chan_consume1(c, VECTOR_ELT(prot, 0));
}

/* Up to n messages under a single park cycle and a single batched head
   publication. Waits only for the first message; whatever else has already
   been published comes along, and the sentinel discipline matches recv. */
SEXP kio_channel_recv_batch(SEXP xp, SEXP n_sexp, SEXP timeout) {
  kio_chan *c = chan_get(xp);
  SEXP prot = R_ExternalPtrProtected(xp);
  SEXP keepers = VECTOR_ELT(prot, 0);
  int n = Rf_asInteger(n_sexp);
  if (n < 1) Rf_error("kioto: n must be at least 1");
  int st = chan_wait_msg(c, prot, Rf_asReal(timeout));
  if (st != KIO_ST_OK) return kio_status_sentinel(st);

  int64_t avail = c->rx.cached_tail - c->rx.lhead;
  int count = avail < n ? (int) avail : n;
  SEXP out = PROTECT(Rf_allocVector(VECSXP, count));
  for (int i = 0; i < count; i++)
    SET_VECTOR_ELT(out, i, chan_consume1(c, keepers));
  UNPROTECT(1);
  return out;
}

// Close protocol ------------------------------------------------------------------

/* The peer half of the protocol, run by peer_main's epilogue: flush, set our
   bit, wake the host. No rendezvous — process exit releases everything else.
   A no-op when the expression already closed the channel itself. */
SEXP kio_channel_close_signal(SEXP xp) {
  kio_chan *c = chan_peek(xp);
  if (c == NULL) return R_NilValue;
  chan_flush(c);
  atomic_fetch_or_explicit(c->closedw, kio_closed_bit(c),
                           memory_order_seq_cst);
  kio_unpark(&c->peer_pk);
  return R_NilValue;
}

SEXP kio_channel_close(SEXP xp, SEXP timeout) {
  kio_chan *c = chan_peek(xp);
  if (c == NULL) return Rf_ScalarLogical(TRUE);   /* close is idempotent */
  SEXP prot = R_ExternalPtrProtected(xp);
  SEXP keepers = VECTOR_ELT(prot, 0);
  uint32_t own = kio_closed_bit(c), other = 3u ^ own;

  /* 1. flush — close never silently discards sent messages */
  chan_flush(c);
  /* 2. signal */
  atomic_fetch_or_explicit(c->closedw, own, memory_order_seq_cst);
  kio_unpark(&c->peer_pk);

  /* 3. rendezvous: the other side's bit, or its death confirmed */
  double deadline = kio_now() + Rf_asReal(timeout);
  int ok = 0;
  for (;;) {
    uint32_t e = kio_parker_snapshot(&c->self_pk);
    if ((atomic_load_explicit(c->closedw, memory_order_acquire) & other) !=
        0 || c->verdict_dead || chan_probe_dead(c, prot)) {
      ok = 1;
      break;
    }
    double rem = deadline - kio_now();
    if (rem <= 0) break;
    long ms = (long) (rem * 1000) + 1;
    if (ms > KIO_INTERRUPT_BOUND_MS) ms = KIO_INTERRUPT_BOUND_MS;
    kio_park(&c->self_pk, e, ms);
    R_CheckUserInterrupt();
  }
  /* 4. release on rendezvous; on timeout keepers are retained and the
     handle finalizer re-runs this check */
  if (!ok) return Rf_ScalarLogical(FALSE);
  for (R_xlen_t i = 0; i < XLENGTH(keepers); i++)
    SET_VECTOR_ELT(keepers, i, R_NilValue);
  chan_release(c, prot, 1);
  return Rf_ScalarLogical(TRUE);
}

// Introspection -------------------------------------------------------------------

/* Reports peer *process* liveness (the fd-scoped lock verdict): an orderly
   close with the process still running is alive; a released handle is not. */
SEXP kio_channel_alive(SEXP xp) {
  kio_chan *c = chan_peek(xp);
  if (c == NULL || c->verdict_dead) return Rf_ScalarLogical(FALSE);
  return Rf_ScalarLogical(!chan_probe_dead(c, R_ExternalPtrProtected(xp)));
}

SEXP kio_channel_stat(SEXP xp) {
  kio_chan *c = chan_get(xp);
  const char *names[] = {"name", "side", "capacity", "slot_size",
                         "arena_size", "inline_max", "spin", "ready",
                         "closed", "peer_pid", "tx_sent", "tx_published",
                         "tx_consumed", "rx_consumed", "rx_published",
                         "fl_entries", "fl_hits", "open_hits",
                         "open_misses", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_mkString(c->shm.name));
  SET_VECTOR_ELT(out, 1, Rf_mkString(c->side == KIO_ENTITY_HOST ?
                                     "host" : "peer"));
  SET_VECTOR_ELT(out, 2, Rf_ScalarReal((double) c->pre.cap));
  SET_VECTOR_ELT(out, 3, Rf_ScalarReal((double) c->pre.slot));
  SET_VECTOR_ELT(out, 4, Rf_ScalarReal((double) c->pre.arena_size));
  SET_VECTOR_ELT(out, 5, Rf_ScalarReal((double) c->inline_max));
  SET_VECTOR_ELT(out, 6, Rf_ScalarLogical(c->spin));
  SET_VECTOR_ELT(out, 7, Rf_ScalarLogical(
    (int) atomic_load_explicit(c->ready, memory_order_acquire)));
  SET_VECTOR_ELT(out, 8, Rf_ScalarInteger(
    (int) atomic_load_explicit(c->closedw, memory_order_acquire)));
  SET_VECTOR_ELT(out, 9, Rf_ScalarReal(
    (double) atomic_load_explicit(c->peer_pid, memory_order_acquire)));
  SET_VECTOR_ELT(out, 10, Rf_ScalarReal((double) c->tx.ltail));
  SET_VECTOR_ELT(out, 11, Rf_ScalarReal((double) c->tx.ptail));
  SET_VECTOR_ELT(out, 12, Rf_ScalarReal((double)
    atomic_load_explicit(c->tx.head, memory_order_acquire)));
  SET_VECTOR_ELT(out, 13, Rf_ScalarReal((double) c->rx.lhead));
  SET_VECTOR_ELT(out, 14, Rf_ScalarReal((double) c->rx.phead));
  /* handle-local spill-reuse machinery, as in kio_pool_dump's local */
  SET_VECTOR_ELT(out, 15, Rf_ScalarInteger((int) c->fl.n));
  SET_VECTOR_ELT(out, 16, Rf_ScalarReal((double) c->fl.hits));
  SET_VECTOR_ELT(out, 17, Rf_ScalarReal((double) c->oc.hits));
  SET_VECTOR_ELT(out, 18, Rf_ScalarReal((double) c->oc.misses));
  UNPROTECT(1);
  return out;
}
