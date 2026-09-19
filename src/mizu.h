/* mizu.h — the R-internal header: the R binding's declarations on top of the
   vendored libmizu core. The wire format and the core verbs live in
   vendor/libmizu/mizu.h (the authority); the binding-author surface (the seam,
   the stager services, the promoted parker/liveness/preamble internals) in
   vendor/libmizu/mizu_ext.h — never vendor/libmizu/internal.h, which only the
   vendored core TUs and the view layer include (the view layer manages
   embedder-owned regions through the heap-form region API). The view layer
   itself — the ALTREP consumer classes, the MIZH/MIZS/MIZL layout writer,
   and the exact-size serialize streams — is first-class here: view.h,
   view.c, serialize.c (mizu is its upstream; mori vendors it from this
   repo). Only R-coupled declarations live here. */

#ifndef MIZU_H
#define MIZU_H

#include "vendor/libmizu/mizu.h"
#include "vendor/libmizu/mizu_ext.h"
#include "view.h"
#include <stdatomic.h>

/* Cold-call annotation for raise-only helpers: keeps their call sites from
   perturbing the hot verbs' code layout (the 2026-08-23 cold-recorder
   record in dev/bench/notes.md). */
#if defined(__GNUC__) || defined(__clang__)
#define MIZU_COLD __attribute__((cold))
#else
#define MIZU_COLD
#endif

// The R binding's per-handle context ----------------------------------------------

/* The zc view cache: name -> split-mapped (page 0 RW) consumer mapping,
   wrap-pinned. Unlike the SHM_RAW cache (core-side, mizu_open_cache), eviction
   only drops the reference — a live view keeps its mapping through its own
   chain, so a cache eviction must never unmap. */
typedef struct mizu_zc_cache_s {
  SEXP wraps;                       /* VECSXP(MIZU_OPEN_CACHE_MAX), prot-pinned */
  char names[MIZU_OPEN_CACHE_MAX][MIZU_NAME_MAX];
  uint8_t name_len[MIZU_OPEN_CACHE_MAX];
  uint64_t stamp[MIZU_OPEN_CACHE_MAX];
  uint64_t tick;
  uint64_t hits, misses;
} mizu_zc_cache;

/* The channel/pool extptr's address and the binding ctx. The core handle
   (a mizu_channel or mizu_pool) is opaque; everything R-side rides here: the
   prot chain (GC-visible slots) and the zc view cache — keyed on SEXP views,
   so it cannot live in the R-free core. Channel prot: [0] zoc wraps,
   [1] pin chain. Pool prot: [0] eval env, [1] trace fn, [2] map cache,
   [3] zoc wraps, [4] pin chain. */
typedef struct mizu_r_handle_s {
  mizu_handle *core;         /* the core handle; NULL after destroy */
  SEXP prot;                /* the extptr's prot chain */
  long self_pid;            /* fork guard */
  int role;                 /* pool: MIZU_ROLE_*; channel: -1 */
  int saw_foreign;          /* channel: a read flagged a foreign payload */
  int pin_slot;             /* prot slot of the pin chain */
  uint32_t pins_dead;       /* tombstoned pin cells awaiting splice */
  uint32_t pins_total;      /* pin chain length (live + dead) */
  mizu_zc_cache zoc;        /* the R-side zc view cache */
} mizu_r_handle;

/* Terminal-state sentinels (the channel/pool veneer), shared across the verb
   surface. */
extern SEXP mizu_sent_full, mizu_sent_timeout, mizu_sent_closed, mizu_sent_gone;

// Bounded single-pass serialize (bounded.c) ---------------------------------------

size_t mizu_serialize_bounded(unsigned char *dst, size_t limit, SEXP object);

// Compact codec (codec.c) ---------------------------------------------------------

/* The mizu-native binary framing for the hot-path payload subset. Streams are
   self-describing — the first byte is MIZU_CODEC_MAGIC where an R binary
   stream carries 'B', so readers dispatch on it and the slot header is
   untouched. The writer rejects ALTREP anywhere in the graph, so a codec
   stream carries no view identifier and needs no pin. */
size_t mizu_codec_write(unsigned char *dst, size_t limit, SEXP object);
SEXP mizu_codec_read(const unsigned char *buf, size_t len);
int mizu_codec_read_task(const unsigned char *buf, size_t len, SEXP *expr,
                         SEXP *args);
SEXP mizu_empty_args(void);

// Payload framing (payload.c) -----------------------------------------------------

void *mizu_vec_ptr(SEXP x);
void mizu_vec_sink(void *ctx, size_t i, void *obj);
/* The single raw gate: the wire type code (0 = ineligible) and byte length;
   class-only integer64 reports MIZU_TYPE_INT64. Callers apply their own size
   gate. */
int mizu_raw_type(SEXP x, size_t *out_len);
/* Wire type -> fresh vector: MIZU_TYPE_INT64 lands as bit64's layout, every
   other code is a SEXPTYPE. */
SEXP mizu_wire_alloc(int type, R_xlen_t n);
int mizu_str1_stage(mizu_slot_hdr *hdr, unsigned char *payload,
                    uint32_t inline_max, SEXP x);
/* The service-form spill checkout, raising mizu_error_shm on create
   failure (the stager's raise-on-failure discipline). Shared by the
   payload and zc spill paths. */
mizu_shm *mizu_spill_get_raise(mizu_handle *h, size_t n);
/* The per-handle pin (stage_r.c): one cons cell pushed on the prot-anchored
   pin chain and registered as the core's opaque token via mizu_stage_pin;
   the drop hook tombstones the cell. The cons precedes mizu_stage_pin, so a
   failed allocation abandons the stage with nothing pinned. ctx is the
   stage hook's binding ctx (the mizu_r_handle). Cold: called
   only on the pinned tiers (which already pay the serialize pass) — the
   annotation keeps it out of the hot verbs' code layout (the 2026-08-23
   cold-recorder record in dev/bench/notes.md). */
void mizu_r_pin(mizu_handle *h, void *ctx, SEXP x) MIZU_COLD;
/* The spill tiers, staged through the handle's services (mizu_stage_spill_get
   / mizu_stage_retain / mizu_stage_pin): the region checkout is the handle's,
   the pin is the staged object where a stream may carry hook-emitted view
   identifiers (the codec stream is ALTREP-free and pins nothing). */
void mizu_payload_spill_shm(mizu_slot_hdr *hdr, unsigned char *payload, SEXP x,
                            size_t n, mizu_handle *h, void *ctx);
void mizu_payload_spill_codec(mizu_slot_hdr *hdr, unsigned char *payload,
                              SEXP x, size_t n, mizu_handle *h);
/* The pool framing (no arena tier): REF, RAWVEC, STR1, INLINE, SHM_VEC, or
   SHM_RAW. ctx is the stage hook's binding ctx. */
void mizu_payload_stage(mizu_slot_hdr *hdr, unsigned char *payload,
                        uint32_t inline_max, SEXP x, mizu_handle *h,
                        void *ctx);
/* Materialize an INLINE / RAWVEC / SHM_RAW payload, or wrap a SHM_VEC / REF
   payload as an ALTREP view. ctx carries the handle's open cache (via
   mizu_read_region) and the R-side view cache; a vanished out-of-line region
   sets ctx->gone and the read returns NULL. A foreign (Python) stream on a
   serialize tier: with consume_foreign (the channel), sets the handle's
   saw_foreign and fails the read with MIZU_READ_CONSUME, so the slot is
   consumed before the veneer raises; without it (the pool), raises. */
SEXP mizu_payload_read(const mizu_slot_hdr *hdr, const unsigned char *payload,
                       uint32_t inline_max, mizu_read_ctx *ctx,
                       int consume_foreign);
/* Foreign-stream detection on the serialize tiers: a pymizu compact-codec
   stream opens with 'P' (DESIGN.md's codec registry allocates 'R' to mizu
   and 'P' to pymizu), and anything past its subset rides pickle (0x80 then a
   protocol byte >= 2). No R stream opens with either ('B'/'X'/'A' are
   ASCII, MIZU_CODEC_MAGIC is 'R'). Inline: it sits on the serialize-tier
   read dispatch of both read hooks. */
#define MIZU_PYMIZU_CODEC_MAGIC 0x50u   /* 'P' */
static inline int mizu_is_python_payload(const unsigned char *p, size_t n) {
  return n >= 1 &&
    (p[0] == MIZU_PYMIZU_CODEC_MAGIC || (n >= 2 && p[0] == 0x80 && p[1] >= 2));
}

// Zero-copy payload tiers (zc.c) ---------------------------------------------------

void mizu_zc_init(void);
/* The consumer split open (page 0 RW, the tail RO): registered as the view
   layer's embedder open hook, so the wire-resolve cache's mappings carry the
   same protection split as the prep path's, which calls it directly. */
mizu_shm *mizu_zc_open(const char *name);
int mizu_zc_eligible(SEXP x, uint32_t inline_max, size_t *out_total);
/* Stage x as SHM_VEC through the handle's services: the region checkout, the
   zc producer-loan retain (mizu_stage_retain_zc performs the refcount store),
   and the pin of x. ctx is the stage hook's binding ctx. */
void mizu_zc_stage(mizu_slot_hdr *hdr, unsigned char *payload, SEXP x,
                   size_t total, mizu_handle *h, void *ctx);
int mizu_zc_ref_stage(mizu_slot_hdr *hdr, unsigned char *payload,
                      uint32_t inline_max, SEXP x);
SEXP mizu_zc_read(const mizu_slot_hdr *hdr, const unsigned char *payload,
                  int *gone, mizu_zc_cache *oc);
SEXP mizu_zc_ref_read(const mizu_slot_hdr *hdr, const unsigned char *payload,
                      int *gone, mizu_zc_cache *oc);
SEXP mizu_zc_lookup_wrap(mizu_zc_cache *oc, const unsigned char *name,
                         uint32_t len);
void mizu_zc_cache_store(mizu_zc_cache *oc, const unsigned char *name,
                         uint32_t len, SEXP wrap);
SEXP mizu_zc_view_check_call(SEXP x);
SEXP mizu_zc_refcount_call(SEXP x);
SEXP mizu_zc_fl_info(mizu_handle *h);

// Classed error conditions (condition.c) -------------------------------------------

#ifndef R_PRINTF_FORMAT                        /* added in R 4.4.0 */
#define R_PRINTF_FORMAT(M, N)
#endif
NORET void mizu_stop(const char *subclass, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
NORET void mizu_stop_shm(double bytes, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
NORET void mizu_stop_died(int slot, double pid, const char *fmt, ...)
  R_PRINTF_FORMAT(3, 4);
NORET void mizu_stop_python_payload(void) MIZU_COLD;
NORET void mizu_cond_signal(SEXP cond);
SEXP mizu_cond_set_index(SEXP cond, int index);
SEXP mizu_caught(SEXP cond);
SEXP mizu_caught_cond(const char *subclass, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
SEXP mizu_caught_died(int slot, double pid, const char *fmt, ...)
  R_PRINTF_FORMAT(3, 4);
SEXP mizu_condition_flatten(SEXP cond, size_t budget);

// The R binding's seam callbacks (stage_r.c) ---------------------------------------

/* Registered on every handle at create/attach/join. stage/read/exec are the
   tier dispatch, the materialize, and the task evaluator; check polls
   R_CheckUserInterrupt; drop tombstones the staged object's pin-chain cell
   (no allocation — finalizer-safe); sweep drops the pool worker's map cache
   at idle/depart. */
int mizu_r_stage_channel(void *obj, mizu_slot_hdr *hdr,
                         unsigned char *payload, uint32_t inline_max,
                         mizu_handle *h, void *ctx);
void *mizu_r_read_channel(const mizu_slot_hdr *hdr,
                          const unsigned char *payload, size_t limit,
                          mizu_read_ctx *ctx);
int mizu_r_stage_pool(void *obj, mizu_slot_hdr *hdr,
                      unsigned char *payload, uint32_t inline_max,
                      mizu_handle *h, void *ctx);
void *mizu_r_read_pool(const mizu_slot_hdr *hdr, const unsigned char *payload,
                       size_t limit, mizu_read_ctx *ctx);
int mizu_r_exec_pool(const mizu_slot_hdr *hdr, const unsigned char *payload,
                     size_t limit, mizu_result_sink *sink, int catching,
                     mizu_read_ctx *ctx);
void mizu_r_publish_err(mizu_result_sink *sink, SEXP cond);
void mizu_r_trace(mizu_trace_event event, uint64_t task_id, void *ctx);
int mizu_r_check(void *ctx);
void mizu_r_drop(void *ctx, void *pin);
void mizu_r_sweep(void *ctx);

// Pool-signal unwrap (verbs_pool.c; map.c's runner reads the words) -------

mizu_pool_sig *mizu_pool_sig_get(SEXP xp);

// GC extptr wrappers (wrap.c) -------------------------------------------------------

mizu_shm *mizu_region(SEXP xp);
SEXP mizu_shm_wrap_producer(mizu_shm *shm);
SEXP mizu_shm_wrap_consumer(mizu_shm *shm);
SEXP mizu_shm_wrap_host(mizu_shm *shm);
mizu_shm *mizu_shm_unwrap(SEXP x);

// init / fini hooks -----------------------------------------------------------------

void mizu_wrap_init(void);
void mizu_payload_init(void);
void mizu_entity_init(void);
void mizu_channel_init(void);
void mizu_pool_init(void);
void mizu_map_init(void);
void mizu_zc_init(void);
void mizu_payload_fini(void);
void mizu_channel_fini(void);
void mizu_pool_fini(void);

/* map.c's srcref strip, shared with the codec's closure and task writes. */
extern SEXP mizu_srcref_sym;
SEXP mizu_strip_srcref(SEXP f);
SEXP mizu_strip_lang(SEXP x);

#endif /* MIZU_H */
