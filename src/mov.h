#ifndef MOV_H
#define MOV_H

#include "vendor/mori.h"
#include <stdatomic.h>

// Preamble --------------------------------------------------------------------

/* First 64-byte line of every mov channel region. Host-written before spawn
   and immutable thereafter; validated by the peer before any shared atomic is
   read or written. The version governs the ring layout specifically and is
   independent of R's serialize version. */

#define MOV_MAGIC        0x4D4F5643u   /* "MOVC" */
#define MOV_ABI_VERSION  1u

typedef struct mov_preamble_s {
  uint32_t magic;
  uint32_t version;
  uint32_t cap;              /* slots per ring, power of two */
  uint32_t slot;             /* slot size in bytes, power of two */
  uint64_t host_pid;         /* the peer death listener's watch target */
  uint64_t arena_size;       /* bytes per direction, multiple of 64, 0 disables */
  uint64_t drop_offset;      /* serialized peer expression, exact-sized */
  uint64_t drop_size;
  uint64_t livedir_offset;   /* directory holding the two liveness files */
  uint64_t livedir_size;
} mov_preamble;

/* The struct is the wire format: it must own the region's first line exactly. */
typedef char mov_preamble_assert[(sizeof(mov_preamble) == 64) ? 1 : -1];

/* Fixed channel layout: preamble (0), rendezvous line (64), one entity block
   per side (128 host, 192 peer; park epoch (4), parked flag (4),
   wake-register (4)), then the four ring index lines from 256. */
#define MOV_ENTITY_HOST  0
#define MOV_ENTITY_PEER  1
#define MOV_ENTITY_OFFSET(i)  ((size_t) 128 + 64 * (size_t) (i))
#define MOV_FIXED_LAYOUT_SIZE ((size_t) 512)

/* Rendezvous line fields. ready and closed (one bit per side) are the only
   atomics; peer_pid is peer-written at attach, before ready. flags is
   host-written before spawn and immutable thereafter, like the preamble —
   bit 0 opts the channel into pure-spin waiting (consumers never park, so
   producers skip the wake fence + parked-flag load on publish). */
#define MOV_OFF_READY      ((size_t) 64)
#define MOV_OFF_CLOSED     ((size_t) 68)
#define MOV_OFF_PEER_PID   ((size_t) 72)
#define MOV_OFF_FLAGS      ((size_t) 80)
#define MOV_FLAG_SPIN      1u

/* Entity block fields, offsets within MOV_ENTITY_OFFSET(i). */
#define MOV_ENTITY_EPOCH   0
#define MOV_ENTITY_PARKED  4
#define MOV_ENTITY_REG     8

/* Ring index lines: one full cache line per shared index, producer and
   consumer writes never sharing a line. H->P is the ring the host produces. */
#define MOV_OFF_HP_TAIL    ((size_t) 256)
#define MOV_OFF_HP_HEAD    ((size_t) 320)
#define MOV_OFF_PH_TAIL    ((size_t) 384)
#define MOV_OFF_PH_HEAD    ((size_t) 448)

void mov_preamble_write(void *region, const mov_preamble *p);
/* Returns NULL and fills *out on success, else a static error message. */
const char *mov_preamble_validate(const void *region, size_t region_size,
                                  mov_preamble *out);

// Writable attach (peer side; both sides write ring indices) -------------------

int mov_shm_open_rw(mori_shm *shm, const char *name);
mori_shm *mov_shm_open_rw_heap(const char *name);

// Bounded single-pass serialize -------------------------------------------------

/* Serializes object, writing bytes into dst while they fit within limit and
   flipping to count-only mode on overflow. Returns the exact total serialized
   size n; dst holds the complete stream iff n <= limit (an overflowed prefix
   is discarded by the caller). */
size_t mov_serialize_bounded(unsigned char *dst, size_t limit, SEXP object);

// Payload framing (payload.c) ----------------------------------------------------

/* Shared between Part I channel slots and Part II pool entries / result
   slots: a 16-byte header then the payload bytes. INLINE carries a complete
   serialized stream; RAWVEC the bare bytes of an attribute-free non-ALTREP
   atomic vector (aux = SEXPTYPE) — byte-identical round-trip at allocVector +
   memcpy cost; ARENA (channel-only) one chunk in the channel's spill arena
   (aux = chunk offset, chunk byte length as a uint64 in the payload);
   SHM_RAW the name of a fresh mov region holding the stream (len = name
   length, name bytes in the payload — root-form, bounded by MORI_NAME_MAX). */

enum {
  MOV_KIND_INLINE = 0,
  MOV_KIND_ARENA,
  MOV_KIND_SHM_RAW,
  MOV_KIND_RAWVEC
};

typedef struct mov_slot_hdr_s {
  uint32_t kind;
  uint32_t len;
  uint64_t aux;
} mov_slot_hdr;

typedef char mov_slot_hdr_assert[(sizeof(mov_slot_hdr) == 16) ? 1 : -1];

void *mov_vec_ptr(SEXP x);
int mov_raw_eligible(SEXP x, uint32_t inline_max, size_t *out_len);
/* Serialize x into a fresh mov region of exactly n bytes (the bounded pass
   supplied n) and frame it as SHM_RAW. Returns the keeper — list(x, producer
   wrapper) — freshly allocated: the caller must protect it. */
SEXP mov_payload_spill_shm(mov_slot_hdr *hdr, unsigned char *payload, SEXP x,
                           size_t n);
/* Stage x as RAWVEC, INLINE, or (past the inline budget) SHM_RAW — the pool
   framing, with no arena tier. Returns the keeper to pin: x itself, or the
   fresh SHM_RAW list; the caller must protect it. */
SEXP mov_payload_stage(mov_slot_hdr *hdr, unsigned char *payload,
                       uint32_t inline_max, SEXP x);
/* Materialize an INLINE / RAWVEC / SHM_RAW payload (errors on ARENA — the
   channel resolves its own arena chunks). */
/* gone: NULL raises on a vanished out-of-line region; else set to 1 with a
   NULL-value return, for callers that can turn it into a task verdict */
SEXP mov_payload_read(const mov_slot_hdr *hdr, const unsigned char *payload,
                      uint32_t inline_max, int *gone);

/* Terminal-state sentinels (channel.c), shared across the verb surface. */
extern SEXP mov_sent_full, mov_sent_timeout, mov_sent_closed, mov_sent_gone;

// Per-entity parker ------------------------------------------------------------

/* One parker per waiting entity: a 32-bit monotonic epoch word in the shared
   region plus, on Windows only, one named auto-reset event (the epoch compare
   is not atomic with the sleep there; auto-reset stickiness substitutes).
   Park sites follow snapshot -> announce -> re-check -> sleep-bounded; any
   unpark that observes the announcement bumps the epoch after the snapshot,
   so the sleep returns immediately. Spurious wakes are absorbed by the
   caller's re-check. */

typedef struct mov_parker_s {
  _Atomic uint32_t *epoch;   /* in the shared region */
#ifdef _WIN32
  void *event;               /* named auto-reset event handle */
#endif
} mov_parker;

enum { MOV_PARK_WOKEN = 0, MOV_PARK_TIMEOUT = 1, MOV_PARK_INTR = 2 };

/* POSIX parks are always timed: an untimed FUTEX_WAIT is silently restarted
   under SA_RESTART (which R's signal()-installed SIGINT handler implies) and
   would swallow Ctrl-C until the next genuine wake, so "indefinite"
   (timeout_ms < 0) parks use this nominal bound and rely on directed unparks.
   Windows uses INFINITE there: console-control cannot interrupt the wait
   either way. Fits in a uint32 of microseconds (the __ulock_wait argument). */
#define MOV_PARK_NOMINAL_MS 3600000L

/* Interrupt-latency bound on parks from R verbs run on interactive processes
   (see *Hybrid wait*): on POSIX a SIGINT EINTRs the timed wait, so the bound
   only covers front-ends that set R's interrupt flag without a signal and can
   be lazy; Windows console-control cannot interrupt WaitForSingleObject, so
   the bound is the Ctrl-C latency and stays short. */
#ifdef _WIN32
#define MOV_INTERRUPT_BOUND_MS 100L
#else
#define MOV_INTERRUPT_BOUND_MS 2000L
#endif

/* Bounded pause-hinted spin over the work sources before announcing a park:
   sub-µs publish gaps are absorbed without touching the entity line. */
#define MOV_SPIN_ITERS 256

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__)
#define MOV_PAUSE() __builtin_ia32_pause()
#elif defined(__aarch64__) || defined(__arm64__) || defined(_M_ARM64)
#define MOV_PAUSE() __asm__ __volatile__("isb" ::: "memory")
#else
#define MOV_PAUSE() do { } while (0)
#endif

/* region_name/entity name the Windows event ("<region>.pk.<entity>"), created
   by the region's host (create = 1) and opened by name by attachers; unused
   on POSIX. Returns 0 on success. */
int mov_parker_attach(mov_parker *pk, _Atomic uint32_t *epoch,
                      const char *region_name, int entity, int create);
void mov_parker_detach(mov_parker *pk);

/* Monotonic seconds / current pid (channel.c). */
double mov_now(void);
long mov_self_pid(void);

static inline uint32_t mov_parker_snapshot(const mov_parker *pk) {
  return atomic_load_explicit(pk->epoch, memory_order_acquire);
}

/* Sleeps while the epoch still equals snapshot, up to timeout_ms
   (0 = poll: never sleeps; < 0 = indefinite, see above). */
int mov_park(mov_parker *pk, uint32_t snapshot, long timeout_ms);
void mov_unpark(mov_parker *pk);

// Per-process death listener -----------------------------------------------------

/* Translates a watched pid's exit into *flag = 1 plus a directed unpark of
   pk (optional, copied). A pid that is already dead fires immediately. The
   flag target and the parker's epoch word / event must stay valid until
   mov_death_watch_stop returns: stop synchronizes with any in-flight
   callback (mutex / serial-queue drain / blocking UnregisterWaitEx), so
   after it returns nothing touches them. Detection is a wake trigger only —
   the liveness lock is the verdict; pid-reuse races are absorbed there. */

typedef struct mov_death_watch_s mov_death_watch;

mov_death_watch *mov_death_watch_start(long pid, _Atomic int *flag,
                                       const mov_parker *pk);
/* As above plus a generic callback invoked after the flag store and
   unpark, on the listener's callback thread (or synchronously from start
   when the pid is already dead): pure C only — no R API, and the
   callback's targets must stay valid until mov_death_watch_stop returns.
   The pool's worker reap rides this. */
mov_death_watch *mov_death_watch_start2(long pid, _Atomic int *flag,
                                        const mov_parker *pk,
                                        void (*cb)(void *), void *cb_arg);
void mov_death_watch_stop(mov_death_watch *w);

/* Package-unload teardown; joins the Linux epoll thread (no-op elsewhere:
   macOS dispatch sources and Windows thread-pool waits are per-watch). */
void mov_death_listener_teardown(void);

// Liveness lock -----------------------------------------------------------------

/* Exclusive flock (POSIX) / LockFileEx (Windows) held for a process's entire
   lifetime and released by the kernel on any exit path. fd-scoped, not
   PID-scoped: pid reuse cannot produce a false "alive". A probe is a
   non-blocking acquire on the fd kept from open — ACQUIRED means the
   previous holder is dead (and the caller now holds the lock, serializing
   survivor cleanup); HELD means alive. The fd is opened close-on-exec so
   spawned children cannot inherit the open file description and keep a dead
   host's lock alive. */

enum { MOV_LIVE_ACQUIRED = 0, MOV_LIVE_HELD = 1 };

int mov_live_open(const char *path, intptr_t *out);
/* Open without creating: ENOENT reads as "indeterminate, treat as alive",
   never a verdict — the probe-by-path discipline (see the pool's worker
   death detection). */
int mov_live_open_existing(const char *path, intptr_t *out);
int mov_live_try(intptr_t h);
/* Release an acquired lock while keeping the fd — the kept-fd prober's
   epilogue after a reap, so a respawned holder can lock the same file. */
void mov_live_unlock(intptr_t h);
void mov_live_close(intptr_t h);
/* The locked file's identity — (dev, inode) on POSIX, (volume serial, file
   index) on Windows — recorded in registry slots at join so a path-opened
   prober can discard a probe whose file was unlinked and recreated out from
   under the lock. Returns 0 on success. */
int mov_live_ident(intptr_t h, uint64_t *dev, uint64_t *ino);

// Pool region (Part II) -----------------------------------------------------------

/* One pool SHM region: header, worker + submitter registries, injection tier
   metadata, per-submitter injection rings, per-worker deques, result slot
   pool, control block, liveness-dir string. The structs below are the wire
   format; every section is 64-byte aligned and the layout is fixed from
   Phase 1 so later phases add capability without moving anything. */

#define MOV_POOL_MAGIC  0x4D4F5650u   /* "MOVP" */

typedef struct mov_pool_hdr_s {
  uint32_t magic;
  uint32_t version;
  uint32_t max_workers;      /* <= 64: parked_workers is one bit per slot */
  uint32_t max_submitters;   /* <= 64: inj_ready_sub / full_waiters bits */
  uint32_t inj_cap;          /* entries per submitter ring, power of two */
  uint32_t deque_cap;        /* entries per worker deque, power of two */
  uint32_t result_slots;     /* total; a multiple of max_submitters */
  uint32_t slot;             /* bytes per entry / result slot, power of two */
  uint64_t owner_pid;        /* the workers' death-listener watch target */
  uint64_t livedir_offset;   /* directory holding the liveness files */
  uint64_t livedir_size;
  uint8_t  pad[8];
} mov_pool_hdr;

typedef char mov_pool_hdr_assert[(sizeof(mov_pool_hdr) == 64) ? 1 : -1];

/* Worker registry slot: two cache lines. Line 0 is admin + owner-written
   fields; deque_top sits apart on line 1 so thief CAS traffic never pingpongs
   with the owner's high-rate deque_bottom writes. in_flight_rs/_seq are
   recorded before any claim is attempted (announce-before-claim) and read
   only post-mortem by a reaper serialized by the liveness lock. The stat_*
   counters are cumulative per incarnation (reset at join), owner-published
   from process-local counters only at park/fairness-tick cadence — never
   per task, which would reintroduce the line-1 pingpong deque_top's
   placement exists to avoid — so under load they lag by up to one fairness
   tick and are exact whenever the worker is parked or departed. */
typedef struct mov_wk_slot_s {
  _Atomic int32_t  status;        /* FREE, CLAIMING, LIVE, LEAVING, REAPING */
  int32_t          id;            /* slot index (redundant, for debugging) */
  int64_t          pid;           /* informational; never a liveness signal */
  _Atomic int32_t  park_state;    /* RUNNING, IDLE, PARKED, WAKING */
  _Atomic uint32_t park_epoch;    /* parker epoch word */
  int64_t          deque_buf_off; /* offset from region base */
  int32_t          deque_cap;
  _Atomic int32_t  in_flight_rs;  /* claimed task's result slot (-1 none) */
  _Atomic uint64_t in_flight_seq; /* rs.sequence recorded with in_flight_rs */
  _Atomic int64_t  deque_bottom;  /* owner stores; thieves load */
  _Atomic int32_t  retire;        /* controller-set clean-exit request */
  uint8_t          pad0[4];
  _Atomic int64_t  deque_top;     /* thieves CAS; owner loads */
  uint64_t         live_dev;      /* liveness-file identity, written once */
  uint64_t         live_ino;      /*  at join before LIVE */
  _Atomic uint64_t stat_tasks;    /* task evals run (help/nested included) */
  _Atomic uint64_t stat_steals;   /* entries claimed from peers' deques */
  _Atomic uint64_t stat_inj;      /* entries claimed from injection rings */
  _Atomic uint64_t stat_parks;    /* kernel parks in the worker loop */
  _Atomic uint64_t stat_helps;    /* claims run in nested-collect help mode */
} mov_wk_slot;

typedef char mov_wk_slot_assert[(sizeof(mov_wk_slot) == 128) ? 1 : -1];

/* Submitter registry slot: one cache line. The result-slot subrange is the
   static partition result_slots / max_submitters, stored for introspection. */
typedef struct mov_sub_slot_s {
  _Atomic int32_t  status;        /* FREE, LIVE, REAPING */
  _Atomic uint32_t park_epoch;    /* parker epoch word */
  int64_t          pid;
  uint32_t         rs_start;
  uint32_t         rs_count;
  uint64_t         live_dev;
  uint64_t         live_ino;
  uint8_t          pad[24];
} mov_sub_slot;

typedef char mov_sub_slot_assert[(sizeof(mov_sub_slot) == 64) ? 1 : -1];

/* Result slot header; the payload framing header sits at offset 24 and
   payload bytes at 40. status is the condition collect re-checks around its
   park; waiter_slot routes the publish-side unpark; sequence increments on
   every reuse so stale handles are detected; worker_slot is the keeper-drop
   unpark target. */
typedef struct mov_rs_hdr_s {
  _Atomic int32_t  status;        /* FREE, PENDING, OK, ERR, CANCEL */
  _Atomic int32_t  waiter_slot;   /* submitter slot parked on this (-1) */
  _Atomic uint64_t sequence;
  _Atomic int32_t  worker_slot;   /* executing worker (-1 until claimed) */
  uint32_t         pad;
  mov_slot_hdr     ph;
} mov_rs_hdr;

typedef char mov_rs_hdr_assert[(sizeof(mov_rs_hdr) == 40) ? 1 : -1];

/* Injection ring / deque entry header; payload framing at offset 16 and
   payload bytes at 32. task_id is submitter slot in the high 16 bits, a
   per-submitter counter below — debug/tracing only. */
typedef struct mov_entry_hdr_s {
  uint64_t task_id;
  uint32_t rs_index;
  uint16_t submitter_slot;
  uint16_t pad;
  mov_slot_hdr ph;
} mov_entry_hdr;

typedef char mov_entry_hdr_assert[(sizeof(mov_entry_hdr) == 32) ? 1 : -1];

enum { MOV_WK_FREE = 0, MOV_WK_CLAIMING, MOV_WK_LIVE, MOV_WK_LEAVING,
       MOV_WK_REAPING };
enum { MOV_SUB_FREE = 0, MOV_SUB_LIVE, MOV_SUB_REAPING };
/* DIED is the reaper's terminal: status-word only, no payload — a reap
   cannot write payload bytes without racing a live worker's concurrent
   publish of the same slot (the benign died-before-claim-committed race),
   so the "worker died" message lives in collect, keyed off the status. */
enum { MOV_RS_FREE = 0, MOV_RS_PENDING, MOV_RS_OK, MOV_RS_ERR, MOV_RS_CANCEL,
       MOV_RS_DIED };
enum { MOV_WPK_RUNNING = 0, MOV_WPK_IDLE, MOV_WPK_PARKED, MOV_WPK_WAKING };

/* Per-submitter injection ring metadata: the shared tail (submitter-
   published) and head (worker-CAS'd) each own a full cache line, as in the
   channel; the ring's entry bytes follow. */
#define MOV_INJ_META_SIZE   ((size_t) 128)
#define MOV_INJ_TAIL_OFF    ((size_t) 0)
#define MOV_INJ_HEAD_OFF    ((size_t) 64)

/* Injection tier metadata (128 B): inj_ready_sub and full_waiters are two
   unrelated hot words, one line each. Control block (128 B): shutdown word
   and parked_workers, one line each. */
#define MOV_TIER_READY_OFF  ((size_t) 0)
#define MOV_TIER_FULL_OFF   ((size_t) 64)
#define MOV_CTRL_SHUTDOWN_OFF ((size_t) 0)
#define MOV_CTRL_PARKED_OFF   ((size_t) 64)

// GC extptr wrappers (wrap.c) ------------------------------------------------------

mori_shm *mov_region(SEXP xp);
SEXP mov_shm_wrap_producer(mori_shm *shm);
SEXP mov_shm_wrap_consumer(mori_shm *shm);
SEXP mov_shm_wrap_host(mori_shm *shm);

// init hooks ----------------------------------------------------------------------

void mov_wrap_init(void);
void mov_entity_init(void);
void mov_channel_init(void);
void mov_pool_init(void);

#endif /* MOV_H */
