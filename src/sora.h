#ifndef SORA_H
#define SORA_H

#include "vendor/mori.h"
#include <stdatomic.h>

// Preamble --------------------------------------------------------------------

/* First 64-byte line of every sora channel region. Host-written before spawn
   and immutable thereafter; validated by the peer before any shared atomic is
   read or written. The version governs the ring layout specifically and is
   independent of R's serialize version. */

#define SORA_MAGIC        0x534F5243u   /* "SORC" */
#define SORA_ABI_VERSION  1u

typedef struct sora_preamble_s {
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
} sora_preamble;

/* The struct is the wire format: it must own the region's first line exactly. */
typedef char sora_preamble_assert[(sizeof(sora_preamble) == 64) ? 1 : -1];

/* Fixed channel layout: preamble (0), rendezvous line (64), one entity block
   per side (128 host, 192 peer; park epoch (4), parked flag (4),
   wake-register (4)), then the four ring index lines from 256. */
#define SORA_ENTITY_HOST  0
#define SORA_ENTITY_PEER  1
#define SORA_ENTITY_OFFSET(i)  ((size_t) 128 + 64 * (size_t) (i))
#define SORA_FIXED_LAYOUT_SIZE ((size_t) 512)

/* Rendezvous line fields. ready and closed (one bit per side) are the only
   atomics; peer_pid is peer-written at attach, before ready. flags is
   host-written before spawn and immutable thereafter, like the preamble —
   bit 0 opts the channel into pure-spin waiting (consumers never park, so
   producers skip the wake fence + parked-flag load on publish). */
#define SORA_OFF_READY      ((size_t) 64)
#define SORA_OFF_CLOSED     ((size_t) 68)
#define SORA_OFF_PEER_PID   ((size_t) 72)
#define SORA_OFF_FLAGS      ((size_t) 80)
#define SORA_FLAG_SPIN      1u

/* Entity block fields, offsets within SORA_ENTITY_OFFSET(i). */
#define SORA_ENTITY_EPOCH   0
#define SORA_ENTITY_PARKED  4
#define SORA_ENTITY_REG     8

/* Ring index lines: one full cache line per shared index, producer and
   consumer writes never sharing a line. H->P is the ring the host produces. */
#define SORA_OFF_HP_TAIL    ((size_t) 256)
#define SORA_OFF_HP_HEAD    ((size_t) 320)
#define SORA_OFF_PH_TAIL    ((size_t) 384)
#define SORA_OFF_PH_HEAD    ((size_t) 448)

void sora_preamble_write(void *region, const sora_preamble *p);
/* Returns NULL and fills *out on success, else a static error message. */
const char *sora_preamble_validate(const void *region, size_t region_size,
                                  sora_preamble *out);

// Writable attach (peer side; both sides write ring indices) -------------------

/* populate pre-faults the whole mapping (MAP_POPULATE on Linux, a read-touch
   pass on macOS / Windows). The channel and pool attaches populate — the
   whole ring is hot there; sora_map's template-path contexts don't, so a
   large RAWVEC x still demand-pages per worker. */
int sora_shm_open_rw(mori_shm *shm, const char *name, int populate);
mori_shm *sora_shm_open_rw_heap(const char *name, int populate);

/* Read-only open for SHM_RAW payload reads: the vendored consumer open,
   except populated on Linux — the stream is unserialized in full
   immediately, so eager PTE install beats a fault per page. Elsewhere it
   defers to the vendored open. NULL on failure (the gone path). */
mori_shm *sora_shm_open_ro_heap(const char *name);

/* Create for the pool / channel control regions: pre-faulted on every
   platform (a page-touch pass where mmap has no populate flag), so slot
   walks never zero-fill-fault on the hot path. Payload regions use the
   vendored mori_shm_create — written in full at stage time. */
int sora_shm_create_populate(mori_shm *shm, size_t size);

/* The survivor-unlink half of a control region's teardown (spill.c):
   releases the name / creator handle, keeping the mapping (the death watch
   and parkers reference it until the handle's release). */
void sora_region_unlink(mori_shm *shm);

// Bounded single-pass serialize -------------------------------------------------

/* Serializes object, writing bytes into dst while they fit within limit and
   flipping to count-only mode on overflow. Returns the exact total serialized
   size n; dst holds the complete stream iff n <= limit (an overflowed prefix
   is discarded by the caller). */
size_t sora_serialize_bounded(unsigned char *dst, size_t limit, SEXP object);

// Compact codec (codec.c) --------------------------------------------------------

/* The sora-native binary framing for the hot-path payload subset: NULL,
   symbols, atomic vectors (attributes included), strings, list/vector
   trees, and calls. Streams are self-describing — the first byte is
   SORA_CODEC_MAGIC where an R binary stream carries 'B', so readers
   dispatch on it and the slot header is untouched. The writer rejects
   ALTREP anywhere in the graph, so a codec stream carries no mori
   identifier and needs no keeper pin. */
#define SORA_CODEC_MAGIC 0x53u   /* 'S'; R's binary and xdr streams are 'B'/'X' */

/* Bounded single-pass write with count-only flip on overflow (the
   sora_serialize_bounded discipline). Returns the exact total stream size
   (complete in dst iff <= limit), or 0 when object is outside the subset
   and must fall back to R_Serialize. */
size_t sora_codec_write(unsigned char *dst, size_t limit, SEXP object);
/* Read a codec stream (magic included); raises on any malformation. */
SEXP sora_codec_read(const unsigned char *buf, size_t len);
int sora_codec_read_task(const unsigned char *buf, size_t len, SEXP *expr,
                         SEXP *args);
SEXP sora_empty_args(void);

// Payload framing (payload.c) ----------------------------------------------------

/* Shared between Part I channel slots and Part II pool entries / result
   slots: a 16-byte header then the payload bytes. INLINE carries a complete
   serialized stream; RAWVEC the bare bytes of an attribute-free non-ALTREP
   atomic vector (aux = SEXPTYPE) — byte-identical round-trip at allocVector +
   memcpy cost; ARENA (channel-only) one chunk in the channel's spill arena
   (aux = chunk offset, chunk byte length as a uint64 in the payload);
   SHM_RAW the name of a fresh sora region holding the stream (len = name
   length, name bytes in the payload — root-form, bounded by MORI_NAME_MAX);
   SHM_VEC the name of a spill region holding a mori-layout object (aux =
   layout SEXPTYPE | exact used bytes << 8) — the consumer wraps it as an
   ALTREP view instead of copying (zc.c); REF the /sora_ identifier of an
   object already in shared memory (a view being passed on) — zero payload
   bytes beyond the identifier move, resolved via the consumer's zc cache;
   NIL is R_NilValue immediate in the header — no bytes move either way;
   STR1 is a length-1 string: the CHARSXP bytes in the payload, aux the
   cetype (NA_character_ is len 0 + aux = SORA_STR1_NA); RAWSPILL is RAWVEC
   out of line — the bare bytes of the same eligible vectors, in a channel
   arena chunk (payload = uint64 chunk offset) or a pool spill region
   (payload = name bytes, name length in aux >> 8) — len is the byte count
   and aux & 0xff the SEXPTYPE in both. */

enum {
  SORA_KIND_INLINE = 0,
  SORA_KIND_ARENA,
  SORA_KIND_SHM_RAW,
  SORA_KIND_RAWVEC,
  SORA_KIND_SHM_VEC,
  SORA_KIND_REF,
  SORA_KIND_NIL,       /* R_NilValue, immediate: no bytes move either way */
  SORA_KIND_STR1,      /* length-1 STRSXP: bytes + encoding, no serialize */
  SORA_KIND_RAWSPILL   /* RAWVEC out of line: arena chunk / spill region */
};

/* STR1's NA marker in aux (a cetype is 0-3, so this cannot alias one). */
#define SORA_STR1_NA UINT64_MAX

/* Whether a staged payload created no keeper record, so the collect-side
   keeper-drop wake is pure cost: the immediate kinds, or a self-contained
   codec stream inline (payload byte 0 is the magic — an INLINE stream is
   never empty). Must match sora_payload_stage's keeper discipline. */
static inline int sora_keeperless(uint32_t kind,
                                 const unsigned char *payload) {
  return kind == SORA_KIND_NIL || kind == SORA_KIND_RAWVEC ||
    kind == SORA_KIND_STR1 ||
    (kind == SORA_KIND_INLINE && payload[0] == SORA_CODEC_MAGIC);
}

typedef struct sora_slot_hdr_s {
  uint32_t kind;
  uint32_t len;
  uint64_t aux;                    /* SHM_RAW: exact stream length — regions
                                      recycled from a free list carry slack */
} sora_slot_hdr;

typedef char sora_slot_hdr_assert[(sizeof(sora_slot_hdr) == 16) ? 1 : -1];

/* Producer spill-region free list: retired SHM_RAW regions recycled by the
   handle that spilled them, so steady-state spill traffic is region-churn-
   free (no create / open / unlink / zero-fill per payload). Regions are
   owned by the handle outright — explicit lifetime, not GC-inherited:
   eviction and handle teardown close + unlink them in place. Reuse is safe
   only because a region is offered exclusively at the protocol's
   consumer-done release points (collect, result-slot reuse, the worker
   keeper sweep). Regions are created at power-of-two sizes (floor 4 KiB) so
   nearby payload sizes hit; caps are per size class and total bytes, the
   latter sized to admit one 8 MiB entry. */
#define SORA_SPILL_FL_MAX    16
#define SORA_SPILL_FL_CLASS  2
#define SORA_SPILL_FL_BYTES  ((size_t) 32 << 20)
#define SORA_SPILL_FL_FLOOR  ((size_t) 4096)

/* Zero-copy view protocol (zc.c): a SHM_VEC region holds a mori-layout
   object (64-byte header, then data). sora owns header bytes [24-31] —
   reserved [24-63] in every mori layout — as a view refcount at [24-27]
   and a flags word at [28-31] (bit 0: staged as REF at least once, so the
   holder set may be wider than the direct peer and the death backstop
   leaks + unlinks instead of force-reclaiming). The producer stores 1 at
   every stage (its own loan — fresh create or free-list pop alike); the
   consumer adds 1 at view wrap, before its consumer-done signal; view
   release (COW materialize or finalizer — finalizer only for list views,
   whose extracted elements keep referencing the region) subs 1. The
   producer drops its
   loan at the existing keeper release points; regions at count 0 rejoin
   the spill free list, others wait in the lent-region ledger below. */
#define SORA_ZC_REFCOUNT_OFF ((size_t) 24)
#define SORA_ZC_FLAGS_OFF    ((size_t) 28)
#define SORA_ZC_FLAG_REFHELD 1u

typedef char sora_zc_off_assert[
  (SORA_ZC_FLAGS_OFF + 4 <= MORI_HEADER_SIZE) ? 1 : -1];

/* SHM_VEC escalation floor: below max(inline budget, this) the copy tiers
   win — Phase 0 measured the wrap-vs-copy crossover in the 16-64 KiB band
   (ARENA ~2 µs flat vs a fresh-region spill ~6-7 µs under churn). */
#define SORA_ZC_FLOOR ((size_t) 32768)

/* The channel's raw-vector floor, higher: the arena's bare-bytes copy has
   no region machinery to amortize, so it beats SHM_VEC well past the
   serialize-era floor — measured crossover in the 256-512 KiB band on the
   echo round trip, zc's friendliest pattern (the REF return ride is free
   there). The pool keeps SORA_ZC_FLOOR: its raw spill is a region too, so
   the view's no-copy receive decides from 64 KiB. Under the (Linux-only)
   churn signal the arena stays the churn-immune tier at any size, so the
   gate lifts. */
#define SORA_ZC_FLOOR_RAW ((size_t) (256 << 10))

/* Lent-region ledger: producer wraps of SHM_VEC regions with views
   outstanding, pinned until the refcount hits 0 (then free-listed) or the
   consumer's death is confirmed (then force-reclaimed; REFHELD entries
   leak + unlink instead). Full at SORA_LEDGER_MAX the wrap simply drops to
   GC — the name unlinks, live views keep their own mappings, and only
   recycling is forfeited. key names the consumer: pool result regions the
   submitter slot, pool task-arg regions the consuming worker slot (set at
   the release point), channel regions unused (-1: the peer is the only
   possible holder); -1 entries are never force-reclaimed. */
#define SORA_LEDGER_MAX 64

typedef struct sora_spill_fl_s {
  mori_shm *regions[SORA_SPILL_FL_MAX]; /* owned; evict/teardown = close+unlink */
  size_t size[SORA_SPILL_FL_MAX];    /* region size; 0 = empty entry */
  uint64_t stamp[SORA_SPILL_FL_MAX]; /* push order: largest-oldest eviction */
  uint64_t tick;
  size_t total;
  uint32_t n;
  int last_reused;                  /* whether the last spill popped an entry */
  uint64_t hits;                    /* process-local reuse count (dump-only) */
  mori_shm *led_regions[SORA_LEDGER_MAX]; /* lent: views outstanding */
  int32_t led_key[SORA_LEDGER_MAX];
  uint32_t led_n;
  /* the one uncommitted staging checkout: set by sora_spill_region_get,
     committed to the retain table (or discarded) at publish, rolled back
     to the free list at the next staging verb or handle teardown — a
     mid-stage raise never leaks a region */
  mori_shm *staging;
  /* set when a spill pop misses with lent regions outstanding (the sweep
     just proved consumer-side views outlive their traffic): the signal
     for the copy-tier fallback in sora_payload_stage and chan_send1;
     cleared when a ledger sweep or force-reclaim returns a lent region
     to the free list. Raised on Linux only (spill_fl_pop): fresh
     regions pre-fault there; macOS/Windows creates are lazy and
     SHM_VEC wins even under churn */
  int churn;
} sora_spill_fl;

/* Per-slot retain table — the explicit successor of the keeper VECSXP.
   Staging always copies, so the only retained objects are the regions a
   staged payload references plus, for the serialize tiers, the staged
   object itself: a serialized stream may carry hook-emitted mori
   identifiers whose views the pin keeps alive until consumer-done. The
   table holds the region half (handle-owned); the pin half rides a
   parallel VECSXP in the handle's prot chain (GC-visible, nil'd at the
   release points) — the two are always the same length and index. kind:
   SPILL — the region surrenders to the free list at release; ZC — the
   producer-loan refcount sub, then free list or lent ledger by key;
   PIN — no region, the pin alone. */
enum { SORA_KEEP_FREE = 0, SORA_KEEP_SPILL, SORA_KEEP_ZC, SORA_KEEP_PIN };

typedef struct sora_keeper_s {
  mori_shm *region;  /* SPILL/ZC: the staged region, owned until release */
  SEXP pin;          /* the staged object to pin (serialize tiers) */
  int32_t key;       /* ZC: the consumer's identity for the lent ledger */
  uint8_t kind;
} sora_keeper;

/* Release one committed entry at a consumer-done point: surrender the
   region per its kind, drop the pin, mark the slot FREE. */
void sora_keeper_release(sora_spill_fl *fl, sora_keeper *tab, SEXP pins,
                         R_xlen_t at);
/* Commit a staged entry to a (FREE) slot: store the region half and the
   pin, clearing the uncommitted checkout. */
void sora_keeper_commit(sora_spill_fl *fl, sora_keeper *tab, SEXP pins,
                        R_xlen_t at, const sora_keeper *k);
/* Discard a staged entry that was never committed (a cancelled publish):
   surrender the region, clear the checkout; the pin was never stored. */
void sora_keeper_discard(sora_spill_fl *fl, sora_keeper *k);
/* Roll back an abandoned staging checkout (a mid-stage raise): the region
   rejoins the free list. */
void sora_stage_rollback(sora_spill_fl *fl);
/* Handle teardown: close + unlink every retained region, drop every pin. */
void sora_keepers_teardown(sora_keeper *tab, SEXP pins, uint32_t n);
void sora_spill_fl_teardown(sora_spill_fl *fl);

/* Consumer-side mapping cache, the read counterpart of the free list: once
   producers repeat region names, a name -> mapping table skips the
   open / fstat / mmap per SHM_RAW payload. Mappings are owned by the
   handle — eviction and teardown close them in place; the SHM_RAW tiers
   copy out before consumer-done, so no view outlives a mapping here (the
   zc view cache below is the GC-pinned exception). Names never alias
   (mori's counter never regenerates one) and a region's size is fixed for
   its lifetime, so entries cannot go stale — one whose region was evicted
   producer-side just never matches again and ages out (LRU). Producer
   death leaves hits readable (the mapping — and on Windows the cached
   handle — outlives the name); the gone path only ever ran on misses and
   is unchanged. Counters are process-local, sora_pool_dump-only. */
#define SORA_OPEN_CACHE_MAX 16

typedef struct sora_open_cache_s {
  mori_shm *maps[SORA_OPEN_CACHE_MAX];  /* owned; evict/teardown = close */
  char names[SORA_OPEN_CACHE_MAX][MORI_NAME_MAX];
  uint8_t name_len[SORA_OPEN_CACHE_MAX];   /* 0 = empty entry */
  uint64_t stamp[SORA_OPEN_CACHE_MAX];
  uint64_t tick;
  uint64_t hits, misses;
} sora_open_cache;

/* The zc view cache: name -> split-mapped (page 0 RW) consumer mapping,
   wrap-pinned. Unlike the SHM_RAW cache, eviction only drops the reference
   — a live view keeps its mapping through its own chain, so a cache
   eviction must never unmap. Stays R-side (keyed on SEXP views). */
typedef struct sora_zc_cache_s {
  SEXP wraps;                       /* VECSXP(SORA_OPEN_CACHE_MAX), handle-pinned */
  char names[SORA_OPEN_CACHE_MAX][MORI_NAME_MAX];
  uint8_t name_len[SORA_OPEN_CACHE_MAX];
  uint64_t stamp[SORA_OPEN_CACHE_MAX];
  uint64_t tick;
  uint64_t hits, misses;
} sora_zc_cache;

void *sora_vec_ptr(SEXP x);
int sora_raw_eligible(SEXP x, uint32_t inline_max, size_t *out_len);
/* Raw-bytes eligibility with no budget gate (the spill tiers size-gate
   themselves): attribute-free, non-ALTREP, non-S4 atomic vector; *out_len
   receives the byte length. */
int sora_raw_type(SEXP x, size_t *out_len);
/* STR1 staging, shared by the channel and pool send paths: frames a
   length-1, attribute-free, non-ALTREP, non-S4 string whose bytes fit the
   inline budget (views were already filtered by the REF check upstream).
   Returns 1 when staged, 0 to fall through to the serialize tiers. */
int sora_str1_stage(sora_slot_hdr *hdr, unsigned char *payload,
                   uint32_t inline_max, SEXP x);
/* Pop the smallest fitting free-list region (a full ledger sweep first on
   a miss) or create one fresh — at the pow2 size class when a free list is
   in play, exact otherwise. The checkout is recorded in fl->staging until
   the caller commits or discards it (sora_keeper_commit / _discard, or
   sora_stage_rollback on a mid-stage raise). Raises sora_error_shm on
   create failure. */
mori_shm *sora_spill_region_get(sora_spill_fl *fl, size_t n);
/* Insert a producer region into the free list under the size-class and
   byte caps (evicting largest-oldest), or close + unlink it in place when
   it doesn't fit. */
void sora_spill_fl_insert(sora_spill_fl *fl, mori_shm *shm);
/* Serialize x into a sora region — popped from fl when an entry fits,
   created fresh otherwise — and frame it as SHM_RAW, filling the retain
   entry: the region plus the pin of x (a stream may carry hook-emitted
   mori identifiers). */
void sora_payload_spill_shm(sora_slot_hdr *hdr, unsigned char *payload, SEXP x,
                           size_t n, sora_spill_fl *fl, sora_keeper *out);
/* The raw-bytes counterpart for RAWSPILL: copies x's bytes into a spill
   region instead of serializing. The entry pins the region only (bare
   bytes can carry no hook-emitted identifiers). */
void sora_payload_spill_raw(sora_slot_hdr *hdr, unsigned char *payload, SEXP x,
                           size_t n, sora_spill_fl *fl, sora_keeper *out);
/* The SHM_RAW spill of a codec stream (n from the counting first pass):
   the region alone is pinned — the writer rejected ALTREP, so no
   hook-emitted identifier can ride along. */
void sora_payload_spill_codec(sora_slot_hdr *hdr, unsigned char *payload,
                             SEXP x, size_t n, sora_spill_fl *fl,
                             sora_keeper *out);
/* Stage x as REF (a sora view), RAWVEC, INLINE, SHM_VEC (a mori-layout-
   eligible object past the inline budget and the zc floor), or SHM_RAW —
   the pool framing, with no arena tier — filling the retain entry: the
   self-contained kinds (NIL, RAWVEC, STR1, codec streams) retain nothing,
   the serialize tiers pin x, the spill tiers pin the region. */
void sora_payload_stage(sora_slot_hdr *hdr, unsigned char *payload,
                       uint32_t inline_max, SEXP x, sora_spill_fl *fl,
                       sora_keeper *out);
/* Materialize an INLINE / RAWVEC / SHM_RAW payload (errors on ARENA — the
   channel resolves its own arena chunks), or wrap a SHM_VEC / REF payload
   as an ALTREP view. oc is the handle's SHM_RAW mapping cache; zoc is the
   consumer cache for the view tiers — split-mapped (page 0 RW for the
   refcount word) and lazy, unlike the SHM_RAW cache. */
/* gone: NULL raises on a vanished out-of-line region; else set to 1 with a
   NULL-value return, for callers that can turn it into a task verdict */
SEXP sora_payload_read(const sora_slot_hdr *hdr, const unsigned char *payload,
                      uint32_t inline_max, int *gone, sora_open_cache *oc,
                      sora_zc_cache *zoc);

/* The SHM_RAW open-cache primitives: the name-keyed mapping lookup (NULL
   on a miss) and the LRU store (takes ownership; an evicted entry is
   closed). */
mori_shm *sora_oc_lookup(sora_open_cache *oc, const unsigned char *name,
                        uint32_t len);
void sora_oc_store(sora_open_cache *oc, mori_shm *shm);
void sora_oc_teardown(sora_open_cache *oc);
/* The zc view cache counterparts (wrap-pinned mappings): the wrap SEXP on
   a hit (stamp bumped), R_NilValue on a miss or a finalized entry. */
SEXP sora_zc_lookup_wrap(sora_zc_cache *oc, const unsigned char *name,
                        uint32_t len);
void sora_zc_cache_store(sora_zc_cache *oc, const unsigned char *name,
                        uint32_t len, SEXP wrap);

// Zero-copy payload tiers (zc.c) ---------------------------------------------

/* The refcount / flags words of a SHM_VEC region header (the sora-owned
   bytes [24-31] of the reserved band, above). Shared by the core release
   machinery (spill.c) and the R-side stage/read (zc.c). */
static inline _Atomic uint32_t *sora_zc_rc(void *base) {
  return (_Atomic uint32_t *) ((unsigned char *) base + SORA_ZC_REFCOUNT_OFF);
}
static inline _Atomic uint32_t *sora_zc_flags(void *base) {
  return (_Atomic uint32_t *) ((unsigned char *) base + SORA_ZC_FLAGS_OFF);
}

void sora_zc_init(void);
/* SHM_VEC eligibility: a mori-layout-eligible object (non-ALTREP, non-S4
   atomic vector; string vector; list tree) whose layout bytes exceed both
   the inline budget and SORA_ZC_FLOOR — cheap lower-bound probes keep the
   layout-size walk off the inline path, and a sora view nested in a list
   tree rejects it (nested views cross by reference on the serialize-hook
   path). *out_total receives the exact layout size (header + data +
   attrs). */
int sora_zc_eligible(SEXP x, uint32_t inline_max, size_t *out_total);
/* Stage x as SHM_VEC into a spill region, filling the retain entry: the
   region, the pin of x, and the consumer key cell (-1) the release point
   may re-stamp with the consumer's identity (pool keying). */
void sora_zc_stage(sora_slot_hdr *hdr, unsigned char *payload, SEXP x,
                  size_t total, sora_spill_fl *fl, sora_keeper *out);
/* Stage a sora-native view as REF (its identifier as the payload), marking
   the region REFHELD. Returns 1 on success, 0 to fall through to the copy
   tiers (not a view, materialized view, or an identifier past the budget). */
int sora_zc_ref_stage(sora_slot_hdr *hdr, unsigned char *payload,
                     uint32_t inline_max, SEXP x);
/* The receive sides: wrap the SHM_VEC region / resolve the REF identifier
   as an ALTREP view over the shared pages, refcounted per the zc protocol
   (zc.c). gone as in sora_payload_read. */
SEXP sora_zc_read(const sora_slot_hdr *hdr, const unsigned char *payload,
                 int *gone, sora_zc_cache *oc);
SEXP sora_zc_ref_read(const sora_slot_hdr *hdr, const unsigned char *payload,
                     int *gone, sora_zc_cache *oc);
/* The producer-loan release for a ZC retain entry: refcount sub, then free
   list on 0 or the lent-region ledger otherwise. */
void sora_zc_release(sora_spill_fl *fl, mori_shm *shm, int32_t key);
/* Move zero-count ledger entries to the free list, up to quota
   (SORA_LEDGER_MAX = full sweep). */
void sora_ledger_sweep(sora_spill_fl *fl, uint32_t quota);
/* Force-reclaim ledger entries after a confirmed consumer death: key >= 0
   matches that consumer only, key < 0 all entries (the channel's single
   peer). REFHELD entries leak + unlink; the rest rejoin the free list. */
void sora_ledger_force(sora_spill_fl *fl, int32_t key);
/* Test / debug surface: is x a sora-native view; c(refcount, flags) of
   the region behind a view; c(free-list, ledger) entry counts for a
   handle. */
SEXP sora_zc_view_check_call(SEXP x);
SEXP sora_zc_refcount_call(SEXP x);
SEXP sora_zc_fl_info(sora_spill_fl *fl);

/* Terminal-state sentinels (channel.c), shared across the verb surface. */
extern SEXP sora_sent_full, sora_sent_timeout, sora_sent_closed, sora_sent_gone;

/* Classed error conditions (condition.c): signal an R condition of class
   c(subclass, "sora_error", "error", "condition") with a NULL call, so
   handlers dispatch on class instead of parsing messages. The typed
   variants add structured fields: _shm carries the requested byte count
   (NA_REAL on opens, where the size is unknown), _died the result slot's
   claimant record (negative = unknown -> NA) — informational reads, racy
   against slot reuse exactly as sora_pool_dump is. Never return. */
#ifndef R_PRINTF_FORMAT                        /* added in R 4.4.0 */
#define R_PRINTF_FORMAT(M, N)
#endif
NORET void sora_stop(const char *subclass, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
NORET void sora_stop_shm(double bytes, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
NORET void sora_stop_died(int slot, double pid, const char *fmt, ...)
  R_PRINTF_FORMAT(3, 4);
/* Signal an already-built condition via stop(cond): the raise longjmps
   out of the Rf_eval. */
NORET void sora_cond_signal(SEXP cond);
/* Sentinel-mode variants: the same conditions returned boxed in a
   length-1 list of class "sora_caught" instead of signalled, for hot
   loops that branch on class rather than arm a tryCatch handler. Only C
   boxes, so a task value that is itself a condition stays bare. */
SEXP sora_caught(SEXP cond);
SEXP sora_caught_cond(const char *subclass, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
SEXP sora_caught_died(int slot, double pid, const char *fmt, ...)
  R_PRINTF_FORMAT(3, 4);
/* Task-error transport (the pool's ERR publish): flatten a caught
   condition into a transport condition that is safe by construction —
   message (truncated at a UTF-8 boundary past half the budget, else a
   fixed fallback), class verbatim, and call + every named field the codec
   can carry within the whole-condition budget; drops named in
   dropped_fields. Fits the budget (the result slot's inline budget)
   wherever a classed condition fits at all, so the publish frames INLINE
   and cannot fail. Returns UNPROTECTED: protect before any allocation. */
SEXP sora_condition_flatten(SEXP cond, size_t budget);

// Per-entity parker ------------------------------------------------------------

/* One parker per waiting entity: a 32-bit monotonic epoch word in the shared
   region plus, on Windows only, one named auto-reset event (the epoch compare
   is not atomic with the sleep there; auto-reset stickiness substitutes).
   Park sites follow snapshot -> announce -> re-check -> sleep-bounded; any
   unpark that observes the announcement bumps the epoch after the snapshot,
   so the sleep returns immediately. Spurious wakes are absorbed by the
   caller's re-check. */

typedef struct sora_parker_s {
  _Atomic uint32_t *epoch;   /* in the shared region */
#ifdef _WIN32
  void *event;               /* named auto-reset event handle */
#endif
} sora_parker;

enum { SORA_PARK_WOKEN = 0, SORA_PARK_TIMEOUT = 1, SORA_PARK_INTR = 2 };

/* POSIX parks are always timed: an untimed FUTEX_WAIT is silently restarted
   under SA_RESTART (which R's signal()-installed SIGINT handler implies) and
   would swallow Ctrl-C until the next genuine wake, so "indefinite"
   (timeout_ms < 0) parks use this nominal bound and rely on directed unparks.
   Windows uses INFINITE there: console-control cannot interrupt the wait
   either way. Fits in a uint32 of microseconds (the __ulock_wait argument). */
#define SORA_PARK_NOMINAL_MS 3600000L

/* Interrupt-latency bound on parks from R verbs run on interactive processes
   (see *Hybrid wait*): on POSIX a SIGINT EINTRs the timed wait, so the bound
   only covers front-ends that set R's interrupt flag without a signal and can
   be lazy; Windows console-control cannot interrupt WaitForSingleObject, so
   the bound is the Ctrl-C latency and stays short. */
#ifdef _WIN32
#define SORA_INTERRUPT_BOUND_MS 100L
#else
#define SORA_INTERRUPT_BOUND_MS 2000L
#endif

/* Time-boxed pre-park spins: a park/wake round trip costs microseconds,
   so a wait that would park first spins in userspace against a
   nanosecond budget, sized ~2x the measured park/wake round trip.
   Budgets adapt per handle (process-local words, never shared): an
   episode whose spin comes up empty halves the budget
   (floor SORA_SPIN_FLOOR_NS); a completed wait learns its measured
   turnaround via sora_spin_learn. SORA_SPIN_BUDGET_NS covers the worker
   pre-park scan and the channel recv wait, absorbing sub-µs publish
   gaps without touching the entity line. */
#define SORA_SPIN_BUDGET_NS 16000

/* Collect's pre-announce spin budget, sized for a short task's whole
   submit -> publish turnaround rather than a publish gap: waiter_slot
   stays unannounced through the spin, so a fast result caught here costs
   neither side a syscall — the publisher skips its wake, the collector
   its park. That syscall-skip invariant is pre-first-park only: the spin
   re-runs after every bounded park wake, and waiter_slot stays announced
   from the first park until the collect returns. */
#define SORA_COLLECT_SPIN_BUDGET_NS 32000

/* Decay floor for both budgets: genuine idleness converges here, so an
   idle pool or channel parks instead of burning a core. */
#define SORA_SPIN_FLOOR_NS 1000

/* Adaptation ceiling for the wait budgets: sora_spin_learn grows the
   next budget to 1.5x the just-measured turnaround plus headroom (never
   below the site constant), so a repeat wait catches its publish in the
   spin — no syscall either side — and halves toward the floor past the
   cap, where a catching spin would burn more than the park/wake pair it
   saves. 64 us: where the pair (~8 us macOS, ~16 us virtualized Linux;
   dev/bench/notes.md) stops being a double-digit share of the period. */
#define SORA_SPIN_CAP_NS 64000

static inline uint64_t sora_spin_learn(double gap_ns, uint64_t budget,
                                       uint64_t base) {
  if (gap_ns <= (double) SORA_SPIN_CAP_NS) {
    uint64_t b = (uint64_t) (1.5 * gap_ns) + 8000;
    if (b < base) b = base;
    return b > (uint64_t) SORA_SPIN_CAP_NS ? (uint64_t) SORA_SPIN_CAP_NS
                                           : b;
  }
  budget = budget / 2;
  return budget < SORA_SPIN_FLOOR_NS ? SORA_SPIN_FLOOR_NS : budget;
}

/* Pause iterations between sora_now() deadline checks at the spin
   sites. The predicates are 1-2 loads (collect, channel) or an
   O(max_workers) scan (worker): a clock read per iteration measurably
   dominates the small scans (macOS has no vDSO; the commpage read is
   ~25 ns), and the worst-case budget overshoot is one stride of scans.
   Two tiers: the default for O(n) scan predicates, and LIGHT for the
   1-2-load predicates — there a stride of 8 makes the clock half the
   spin (measured ~1/3 of a channel round trip's host CPU), while 64
   iterations of a 2-4 ns predicate bound the overshoot at ~200 ns. */
#define SORA_SPIN_CLOCK_EVERY 8
#define SORA_SPIN_CLOCK_EVERY_LIGHT 64

/* Busy-path bound on result-keeper reap visits per worker step: keeps the
   per-task reap cost O(1) against any number of results outstanding. The
   idle-path full sweep clears any residue before a park. */
#define SORA_REAP_QUOTA 32

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__)
#define SORA_PAUSE() __builtin_ia32_pause()
#elif defined(__aarch64__) || defined(__arm64__) || defined(_M_ARM64)
#define SORA_PAUSE() __asm__ __volatile__("yield" ::: "memory")
#else
#define SORA_PAUSE() do { } while (0)
#endif

/* Time-boxed pause-hinted spin, the shared pre-announce wait layer:
   evaluate cond each iteration until it holds (out = 1) or the
   sora_now()-scale deadline `until` passes (out = 0), reading the clock
   every `stride` iterations. A macro, not an inline: the predicate must
   inline at each site. Callers clamp `until` to any outer wait deadline
   themselves. */
#define SORA_SPIN_WAIT(cond, until, stride, out)                         \
  do {                                                                  \
    (out) = 0;                                                          \
    for (;;) {                                                          \
      int sora_sw_i = 0;                                                 \
      for (; sora_sw_i < (stride); sora_sw_i++) {                         \
        SORA_PAUSE();                                                    \
        if (cond) { (out) = 1; break; }                                 \
      }                                                                 \
      if ((out) || sora_now() >= (until)) break;                         \
    }                                                                   \
  } while (0)

/* region_name/entity name the Windows event ("<region>.pk.<entity>"), created
   by the region's host (create = 1) and opened by name by attachers; unused
   on POSIX. Returns 0 on success. */
int sora_parker_attach(sora_parker *pk, _Atomic uint32_t *epoch,
                      const char *region_name, int entity, int create);
void sora_parker_detach(sora_parker *pk);

/* Monotonic seconds / current pid (channel.c). */
double sora_now(void);
long sora_self_pid(void);

/* One 2^127-step CMRG stream jump in place (rng.c); the map batch loop's
   per-element step. */
void sora_rng_jump(int *seed);

static inline uint32_t sora_parker_snapshot(const sora_parker *pk) {
  return atomic_load_explicit(pk->epoch, memory_order_acquire);
}

/* Sleeps while the epoch still equals snapshot, up to timeout_ms
   (0 = poll: never sleeps; < 0 = indefinite, see above). */
int sora_park(sora_parker *pk, uint32_t snapshot, long timeout_ms);
void sora_unpark(sora_parker *pk);

// Binding hooks (the language seam) --------------------------------------------

/* The language-binding callback set every handle carries, registered at
   create/attach/join: the transport invokes these where the R API calls
   sat before the seam carve. check polls for interruption at
   abandon-safe points only (no shared-state mutation in progress, no
   cleanup pending); a nonzero return means abandon — R's hook never
   returns nonzero because R_CheckUserInterrupt longjmps first. park
   brackets each bounded park's sleep (entering nonzero before, zero
   after), around the sleep only, for runtimes with a global lock to
   drop; NULL for R. ctx is opaque to the core. All fire only on the
   verb-calling thread. */
typedef int (*sora_check_fn)(void *ctx);
typedef void (*sora_park_fn)(void *ctx, int entering);
/* stage frames obj as (hdr, payload) — payload capacity inline_max —
   filling out with the retain entry the core commits on success
   (stager-initialized: SORA_KEEP_FREE / no pin / key -1, then what the
   tier retains; the core mutates no shared state before stage returns,
   so a mid-stage raise abandons cleanly). The staging services it rides:
   sora_stage_arena_alloc (channel arena chunks), sora_chan_reap (the
   pre-spill keeper reap), sora_spill_region_get (the region checkout,
   via the zc/payload spill helpers). Returns 0 on success; the R stager
   raises on failure, never returns nonzero. */
typedef int (*sora_stage_fn)(void *obj, sora_slot_hdr *hdr,
                             unsigned char *payload, uint32_t inline_max,
                             void *handle, sora_keeper *out);
/* read materializes a received frame. payload is always a
   dereferenceable byte range: the core resolves its arena-referencing
   kinds (ARENA, channel RAWSPILL) against the arena base before the
   call, so the callback never learns arena mechanics. limit is the
   validated byte capacity of the range — inline_max for slot-resident
   frames, the arena-validated length for resolved ones. gone as in
   sora_payload_read. */
typedef SEXP (*sora_read_fn)(const sora_slot_hdr *hdr,
                             const unsigned char *payload, size_t limit,
                             int *gone, void *handle);

typedef struct sora_binding_s {
  sora_check_fn check;
  sora_park_fn park;
  sora_stage_fn stage;
  sora_read_fn read;
  void *ctx;
} sora_binding;

/* The R binding's check hook (spill.c). */
int sora_r_check(void *ctx);

static inline int sora_check_interrupt(const sora_binding *b) {
  return b->check != NULL ? b->check(b->ctx) : 0;
}

static inline void sora_park_bracket(const sora_binding *b, int entering) {
  if (b->park != NULL) b->park(b->ctx, entering);
}

// Per-process death listener -----------------------------------------------------

/* Translates a watched pid's exit into *flag = 1 plus a directed unpark of
   pk (optional, copied). A pid that is already dead fires immediately. The
   flag target and the parker's epoch word / event must stay valid until
   sora_death_watch_stop returns: stop synchronizes with any in-flight
   callback (mutex / serial-queue drain / blocking UnregisterWaitEx), so
   after it returns nothing touches them. Detection is a wake trigger only —
   the liveness lock is the verdict; pid-reuse races are absorbed there. */

typedef struct sora_death_watch_s sora_death_watch;

sora_death_watch *sora_death_watch_start(long pid, _Atomic int *flag,
                                       const sora_parker *pk);
/* As above plus a generic callback invoked after the flag store and
   unpark, on the listener's callback thread (or synchronously from start
   when the pid is already dead): pure C only — no R API, and the
   callback's targets must stay valid until sora_death_watch_stop returns.
   The pool's worker reap rides this. */
sora_death_watch *sora_death_watch_start2(long pid, _Atomic int *flag,
                                        const sora_parker *pk,
                                        void (*cb)(void *), void *cb_arg);
void sora_death_watch_stop(sora_death_watch *w);

/* Package-unload teardown; joins the Linux epoll thread (no-op elsewhere:
   macOS dispatch sources and Windows thread-pool waits are per-watch). */
void sora_death_listener_teardown(void);

// Channel handle (channel.c; the R binding's stage/read live in stage_r.c) ------

/* One direction's ring. Shared pointers alias the mapped region; everything
   below them is process-local — producer-local cursors on the side that
   produces, consumer-local on the side that consumes; neither is ever
   mirrored into shared memory. */
typedef struct sora_chan_ring_s {
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
} sora_chan_ring;

typedef struct sora_chan_s {
  mori_shm shm;                  /* our mapping; unmapped only in release */
  sora_preamble pre;
  unsigned char *base;
  int side;                      /* SORA_ENTITY_HOST or SORA_ENTITY_PEER */
  int spin;
  sora_binding binding;          /* the language binding's hooks */
  int released;                  /* full teardown ran; handle is dead */
  int verdict_dead;              /* sticky flock-confirmed peer death */
  int names_unlinked;            /* survivor cleanup already ran */
  int pk_ok;
  long self_pid;                 /* fork guard */
  uint32_t inline_max;
  /* process-local adaptive recv-wait spin budget (ns): halved when an
     episode's spin comes up empty, reset to SORA_SPIN_BUDGET_NS on any
     message acquired; never shared */
  uint64_t wait_budget_ns;
  /* tx keepers pinned and not yet reaped: the gate that lets keeperless
     traffic (the immediate kinds, codec streams) skip the reap's shared
     head load entirely */
  int64_t keep_out;

  _Atomic uint32_t *ready;
  _Atomic uint32_t *closedw;     /* bit 1 = host closed, bit 2 = peer closed */
  _Atomic uint64_t *peer_pid;
  _Atomic uint32_t *self_parked, *peer_parked;
  _Atomic uint32_t *self_reg, *peer_reg;

  sora_parker self_pk;            /* we park here; the peer unparks it */
  sora_parker peer_pk;            /* we unpark this */
  intptr_t live_self, live_peer; /* kept liveness fds/handles; 0 = not open */
  char live_self_path[1024];
  char live_peer_path[1024];
  _Atomic int peer_dead;         /* death-listener flag: wake trigger only */
  sora_death_watch *watch;

  /* Payload lifetime: the per-slot retain table (malloc'd, cap entries)
     with its GC-visible pin store (pins, pinned by the extptr's prot);
     the producer spill free list + lent-region ledger and the consumer
     SHM_RAW mapping cache are owned outright (spill.c); the prot also
     pins the zc view cache's wraps (split-mapped, lazy — unlike the
     SHM_RAW cache) */
  SEXP pins;
  sora_keeper *keepers;
  sora_spill_fl fl;
  sora_open_cache oc;
  sora_zc_cache zoc;

  sora_chan_ring tx, rx;
} sora_chan;

/* The channel's staging services for the binding's stage_fn (stage_r.c):
   reserve n bytes in the tx spill arena, returning the chunk (NULL when
   full or disabled) with *off set for the frame's aux — the arena base
   stays core-private; and the pre-spill keeper reap, so a region checkout
   sees the freshest consumer-done surrenders. */
unsigned char *sora_stage_arena_alloc(sora_chan *c, uint64_t n, uint64_t *off);
void sora_chan_reap(sora_chan *c, int force);

/* The R binding's channel stage/read (stage_r.c), registered on every
   channel handle at create/attach. */
int sora_r_stage_channel(void *obj, sora_slot_hdr *hdr,
                         unsigned char *payload, uint32_t inline_max,
                         void *handle, sora_keeper *out);
SEXP sora_r_read_channel(const sora_slot_hdr *hdr,
                         const unsigned char *payload, size_t limit,
                         int *gone, void *handle);

// Liveness lock -----------------------------------------------------------------

/* Exclusive flock (POSIX) / LockFileEx (Windows) held for a process's entire
   lifetime and released by the kernel on any exit path. fd-scoped, not
   PID-scoped: pid reuse cannot produce a false "alive". A probe is a
   non-blocking acquire on the fd kept from open — ACQUIRED means the
   previous holder is dead (and the caller now holds the lock, serializing
   survivor cleanup); HELD means alive. The fd is opened close-on-exec so
   spawned children cannot inherit the open file description and keep a dead
   host's lock alive. */

enum { SORA_LIVE_ACQUIRED = 0, SORA_LIVE_HELD = 1 };

/* Directory for liveness lock files: the SORA_LIVENESS_DIR override
   (read-through, checked on every call) else a per-platform default
   resolved once — /dev/shm on Linux, the per-user temp dir on macOS and
   Windows. Trailing separators trimmed; NULL if unresolvable. Only region
   creators call this: participants read the embedded copy. */
const char *sora_live_dir(void);

int sora_live_open(const char *path, intptr_t *out);
/* Open without creating: ENOENT reads as "indeterminate, treat as alive",
   never a verdict — the probe-by-path discipline (see the pool's worker
   death detection). */
int sora_live_open_existing(const char *path, intptr_t *out);
int sora_live_try(intptr_t h);
/* Release an acquired lock while keeping the fd — the kept-fd prober's
   epilogue after a reap, so a respawned holder can lock the same file. */
void sora_live_unlock(intptr_t h);
void sora_live_close(intptr_t h);
/* The locked file's identity — (dev, inode) on POSIX, (volume serial, file
   index) on Windows — recorded in registry slots at join so a path-opened
   prober can discard a probe whose file was unlinked and recreated out from
   under the lock. Returns 0 on success. */
int sora_live_ident(intptr_t h, uint64_t *dev, uint64_t *ino);

// Pool region (Part II) -----------------------------------------------------------

/* One pool SHM region: header, worker + submitter registries, injection tier
   metadata, per-submitter injection rings, per-worker deques, result slot
   pool, control block, liveness-dir string. The structs below are the wire
   format; every section is 64-byte aligned and the layout is fixed from
   Phase 1 so later phases add capability without moving anything. */

#define SORA_POOL_MAGIC  0x534F5250u   /* "SORP" */

/* The worker-slot bound: parked_workers is one bit per slot, and sora_map
   stages its CLAIM array (one word per runner ordinal) at this count. */
#define SORA_MAX_WORKERS 64

typedef struct sora_pool_hdr_s {
  uint32_t magic;
  uint32_t version;
  uint32_t max_workers;      /* <= SORA_MAX_WORKERS */
  uint32_t max_submitters;   /* <= 64: inj_ready_sub / full_waiters bits */
  uint32_t inj_cap;          /* entries per submitter ring, power of two */
  uint32_t deque_cap;        /* entries per worker deque, power of two */
  uint32_t result_slots;     /* total; a multiple of max_submitters */
  uint32_t slot;             /* bytes per entry / result slot, power of two */
  uint64_t owner_pid;        /* the workers' death-listener watch target */
  uint64_t livedir_offset;   /* directory holding the liveness files */
  uint64_t livedir_size;
  uint8_t  pad[8];
} sora_pool_hdr;

typedef char sora_pool_hdr_assert[(sizeof(sora_pool_hdr) == 64) ? 1 : -1];

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
typedef struct sora_wk_slot_s {
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
} sora_wk_slot;

typedef char sora_wk_slot_assert[(sizeof(sora_wk_slot) == 128) ? 1 : -1];

/* Submitter registry slot: one cache line. The result-slot subrange is the
   static partition result_slots / max_submitters, stored for introspection.
   stat_spills counts payloads staged past the inline budget onto the
   SHM_RAW tier — task payloads at submit, result payloads at publish, both
   attributed to the task's submitter — the visible signal that slot_size
   is undersized for the traffic. stat_spill_reuse counts the subset that
   recycled a region from the producer's free list instead of creating one:
   spills - reuse is the region-churn rate. Spill-path-only writes (even a
   recycled region dwarfs the cross-process fetch_add); reset at claim. */
typedef struct sora_sub_slot_s {
  _Atomic int32_t  status;        /* FREE, LIVE, REAPING */
  _Atomic uint32_t park_epoch;    /* parker epoch word */
  int64_t          pid;
  uint32_t         rs_start;
  uint32_t         rs_count;
  uint64_t         live_dev;
  uint64_t         live_ino;
  _Atomic uint64_t stat_spills;
  _Atomic uint64_t stat_spill_reuse;
  uint8_t          pad[8];
} sora_sub_slot;

typedef char sora_sub_slot_assert[(sizeof(sora_sub_slot) == 64) ? 1 : -1];

/* Result slot header; the payload framing header sits at offset 24 and
   payload bytes at 40. status is the condition collect re-checks around its
   park; waiter_slot routes the publish-side unpark; sequence increments on
   every reuse so stale handles are detected; worker_slot is the keeper-drop
   unpark target. */
typedef struct sora_rs_hdr_s {
  _Atomic int32_t  status;        /* FREE, PENDING, OK, ERR, CANCEL */
  _Atomic int32_t  waiter_slot;   /* submitter slot parked on this (-1) */
  _Atomic uint64_t sequence;
  _Atomic int32_t  worker_slot;   /* executing worker (-1 until claimed) */
  uint32_t         pad;
  sora_slot_hdr     ph;
} sora_rs_hdr;

typedef char sora_rs_hdr_assert[(sizeof(sora_rs_hdr) == 40) ? 1 : -1];

/* Injection ring / deque entry header; payload framing at offset 16 and
   payload bytes at 32. task_id is submitter slot in the high 16 bits, a
   per-submitter counter below — debug/tracing only. flags was the always-
   zeroed pad word, so repurposing it is backward-consistent (the ABI
   version gate rejects mixed builds regardless). */
typedef struct sora_entry_hdr_s {
  uint64_t task_id;
  uint32_t rs_index;
  uint16_t submitter_slot;
  uint16_t flags;
  sora_slot_hdr ph;
} sora_entry_hdr;

typedef char sora_entry_hdr_assert[(sizeof(sora_entry_hdr) == 32) ? 1 : -1];

/* RUNNER marks a map runner task — a map's join ticket: a worker joins the
   map's shared cursor exactly by executing one runner, so a doorbell help
   beat that claims one re-homes it onto the helper's own deque (stealable
   by idle peers) instead of executing it nested, which would silently
   serialize the map. Runner-only by design: a runner lost in the re-home
   death window is backstopped by sora_map_abandon's lane trim; an ordinary
   task has no such backstop and must execute inline where it is claimed. */
#define SORA_ENTRY_RUNNER 1u

enum { SORA_WK_FREE = 0, SORA_WK_CLAIMING, SORA_WK_LIVE, SORA_WK_LEAVING,
       SORA_WK_REAPING };
enum { SORA_SUB_FREE = 0, SORA_SUB_LIVE, SORA_SUB_REAPING };
/* DIED is the reaper's terminal: status-word only, no payload — a reap
   cannot write payload bytes without racing a live worker's concurrent
   publish of the same slot (the benign died-before-claim-committed race),
   so the "worker died" message lives in collect, keyed off the status. */
enum { SORA_RS_FREE = 0, SORA_RS_PENDING, SORA_RS_OK, SORA_RS_ERR, SORA_RS_CANCEL,
       SORA_RS_DIED };
enum { SORA_WPK_RUNNING = 0, SORA_WPK_IDLE, SORA_WPK_PARKED, SORA_WPK_WAKING };

/* Per-submitter injection ring metadata: the shared tail (submitter-
   published) and head (worker-CAS'd) each own a full cache line, as in the
   channel; the ring's entry bytes follow. */
#define SORA_INJ_META_SIZE   ((size_t) 128)
#define SORA_INJ_TAIL_OFF    ((size_t) 0)
#define SORA_INJ_HEAD_OFF    ((size_t) 64)

/* Injection tier metadata (128 B): inj_ready_sub and full_waiters are two
   unrelated hot words, one line each. Control block (192 B): shutdown
   word, parked_workers, and the help_wanted doorbell — set by a publish
   that finds every worker busy, polled once per batch transition by map
   runners — one line each. */
#define SORA_TIER_READY_OFF  ((size_t) 0)
#define SORA_TIER_FULL_OFF   ((size_t) 64)
#define SORA_CTRL_SHUTDOWN_OFF ((size_t) 0)
#define SORA_CTRL_PARKED_OFF   ((size_t) 64)
#define SORA_CTRL_HELP_OFF     ((size_t) 128)

/* The opaque pool-signal trio a map runner loads relaxed once per batch
   transition (sora_pool_signals mints it; map.c dereferences the words and
   stays pool-layout-free): the help_wanted doorbell, the pool's shared
   shutdown word, and the handle's process-local listener-written
   owner_dead flag — set independently in every worker process, which is
   what keeps owner death visible when no worker is in its step loop to
   broadcast it. */
typedef struct sora_pool_sig_s {
  _Atomic uint32_t *help_wanted;
  _Atomic uint32_t *shutdown;
  _Atomic int      *owner_dead;
} sora_pool_sig;

/* The struct behind a sora_pool_signals extptr, or an error for anything
   else — pool.c owns the tag; sora_map_next is the consumer. */
sora_pool_sig *sora_pool_sig_get(SEXP xp);

// GC extptr wrappers (wrap.c) ------------------------------------------------------

mori_shm *sora_region(SEXP xp);
SEXP sora_shm_wrap_producer(mori_shm *shm);
SEXP sora_shm_wrap_consumer(mori_shm *shm);
SEXP sora_shm_wrap_host(mori_shm *shm);
/* The region behind a sora_shm-tagged wrap, or NULL — keeps the tag private
   to wrap.c (a finalized wrap also reads NULL: its region is gone). */
mori_shm *sora_shm_unwrap(SEXP x);

// init hooks ----------------------------------------------------------------------

void sora_wrap_init(void);
void sora_payload_init(void);
void sora_entity_init(void);
void sora_channel_init(void);
void sora_pool_init(void);
void sora_map_init(void);

/* map.c's srcref strip, shared with the codec's closure and task
   expression writes. */
extern SEXP sora_srcref_sym;
SEXP sora_strip_srcref(SEXP f);
SEXP sora_strip_lang(SEXP x);
void sora_tune_malloc(void);
/* Unload counterparts (R_unload_sora): release the objects the inits
   preserve — only payload, channel, pool and zc hold any. */
void sora_payload_fini(void);
void sora_channel_fini(void);
void sora_pool_fini(void);

#endif /* SORA_H */
