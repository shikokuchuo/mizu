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

/* region_name/entity name the Windows event ("<region>.pk.<entity>"), created
   by the region's host (create = 1) and opened by name by attachers; unused
   on POSIX. Returns 0 on success. */
int mov_parker_attach(mov_parker *pk, _Atomic uint32_t *epoch,
                      const char *region_name, int entity, int create);
void mov_parker_detach(mov_parker *pk);

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
int mov_live_try(intptr_t h);
void mov_live_close(intptr_t h);

// GC extptr wrappers (wrap.c) ------------------------------------------------------

mori_shm *mov_region(SEXP xp);
SEXP mov_shm_wrap_producer(mori_shm *shm);
SEXP mov_shm_wrap_consumer(mori_shm *shm);
SEXP mov_shm_wrap_host(mori_shm *shm);

// init hooks ----------------------------------------------------------------------

void mov_wrap_init(void);
void mov_entity_init(void);
void mov_channel_init(void);

#endif /* MOV_H */
