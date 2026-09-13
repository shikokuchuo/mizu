/* rei.h — the R-internal header: the R binding's declarations on top of the
   vendored librei core. The wire format and the core verbs live in
   vendor/librei/rei.h (the authority); the binding-author surface (the seam,
   the stager services, the promoted parker/liveness/preamble internals) in
   vendor/librei/rei_ext.h — never vendor/librei/internal.h, which only the
   vendored core TUs and the view layer include (the view layer manages
   embedder-owned regions through the heap-form region API). The view layer
   itself — the ALTREP consumer classes, the REIH/REIS/REIL layout writer,
   and the exact-size serialize streams — is first-class here: view.h,
   view.c, serialize.c (rei is its upstream; mori vendors it from this
   repo). Only R-coupled declarations live here. */

#ifndef REI_H
#define REI_H

#include "vendor/librei/rei.h"
#include "vendor/librei/rei_ext.h"
#include "view.h"
#include <stdatomic.h>

/* Cold-call annotation for raise-only helpers: keeps their call sites from
   perturbing the hot verbs' code layout (the 2026-08-23 cold-recorder
   record in dev/bench/notes.md). */
#if defined(__GNUC__) || defined(__clang__)
#define REI_COLD __attribute__((cold))
#else
#define REI_COLD
#endif

// The R binding's per-handle context ----------------------------------------------

/* The zc view cache: name -> split-mapped (page 0 RW) consumer mapping,
   wrap-pinned. Unlike the SHM_RAW cache (core-side, rei_open_cache), eviction
   only drops the reference — a live view keeps its mapping through its own
   chain, so a cache eviction must never unmap. */
typedef struct rei_zc_cache_s {
  SEXP wraps;                       /* VECSXP(REI_OPEN_CACHE_MAX), prot-pinned */
  char names[REI_OPEN_CACHE_MAX][REI_NAME_MAX];
  uint8_t name_len[REI_OPEN_CACHE_MAX];
  uint64_t stamp[REI_OPEN_CACHE_MAX];
  uint64_t tick;
  uint64_t hits, misses;
} rei_zc_cache;

/* The channel/pool extptr's address and the binding ctx. The core handle
   (a rei_channel or rei_pool) is opaque; everything R-side rides here: the
   prot chain (GC-visible slots) and the zc view cache — keyed on SEXP views,
   so it cannot live in the R-free core. Channel prot: [0] zoc wraps,
   [1] pin chain. Pool prot: [0] eval env, [1] trace fn, [2] map cache,
   [3] zoc wraps, [4] pin chain. */
typedef struct rei_r_handle_s {
  rei_handle *core;         /* the core handle; NULL after destroy */
  SEXP prot;                /* the extptr's prot chain */
  long self_pid;            /* fork guard */
  int role;                 /* pool: REI_ROLE_*; channel: -1 */
  int saw_foreign;          /* channel: a read flagged a foreign payload */
  int pin_slot;             /* prot slot of the pin chain */
  uint32_t pins_dead;       /* tombstoned pin cells awaiting splice */
  uint32_t pins_total;      /* pin chain length (live + dead) */
  rei_zc_cache zoc;        /* the R-side zc view cache */
} rei_r_handle;

/* Terminal-state sentinels (the channel/pool veneer), shared across the verb
   surface. */
extern SEXP rei_sent_full, rei_sent_timeout, rei_sent_closed, rei_sent_gone;

// Bounded single-pass serialize (bounded.c) ---------------------------------------

size_t rei_serialize_bounded(unsigned char *dst, size_t limit, SEXP object);

// Compact codec (codec.c) ---------------------------------------------------------

/* The rei-native binary framing for the hot-path payload subset. Streams are
   self-describing — the first byte is REI_CODEC_MAGIC where an R binary
   stream carries 'B', so readers dispatch on it and the slot header is
   untouched. The writer rejects ALTREP anywhere in the graph, so a codec
   stream carries no view identifier and needs no pin. */
size_t rei_codec_write(unsigned char *dst, size_t limit, SEXP object);
SEXP rei_codec_read(const unsigned char *buf, size_t len);
int rei_codec_read_task(const unsigned char *buf, size_t len, SEXP *expr,
                         SEXP *args);
SEXP rei_empty_args(void);

// Payload framing (payload.c) -----------------------------------------------------

void *rei_vec_ptr(SEXP x);
void rei_vec_sink(void *ctx, size_t i, void *obj);
/* The single raw gate: the wire type code (0 = ineligible) and byte length;
   class-only integer64 reports REI_TYPE_INT64. Callers apply their own size
   gate. */
int rei_raw_type(SEXP x, size_t *out_len);
/* Wire type -> fresh vector: REI_TYPE_INT64 lands as bit64's layout, every
   other code is a SEXPTYPE. */
SEXP rei_wire_alloc(int type, R_xlen_t n);
int rei_str1_stage(rei_slot_hdr *hdr, unsigned char *payload,
                    uint32_t inline_max, SEXP x);
/* The service-form spill checkout, raising rei_error_shm on create
   failure (the stager's raise-on-failure discipline). Shared by the
   payload and zc spill paths. */
rei_shm *rei_spill_get_raise(rei_handle *h, size_t n);
/* The per-handle pin (stage_r.c): one cons cell pushed on the prot-anchored
   pin chain and registered as the core's opaque token via rei_stage_pin;
   the drop hook tombstones the cell. The cons precedes rei_stage_pin, so a
   failed allocation abandons the stage with nothing pinned. ctx is the
   stage hook's binding ctx (the rei_r_handle). Cold: called
   only on the pinned tiers (which already pay the serialize pass) — the
   annotation keeps it out of the hot verbs' code layout (the 2026-08-23
   cold-recorder record in dev/bench/notes.md). */
void rei_r_pin(rei_handle *h, void *ctx, SEXP x) REI_COLD;
/* The spill tiers, staged through the handle's services (rei_stage_spill_get
   / rei_stage_retain / rei_stage_pin): the region checkout is the handle's,
   the pin is the staged object where a stream may carry hook-emitted view
   identifiers (the codec stream is ALTREP-free and pins nothing). */
void rei_payload_spill_shm(rei_slot_hdr *hdr, unsigned char *payload, SEXP x,
                            size_t n, rei_handle *h, void *ctx);
void rei_payload_spill_codec(rei_slot_hdr *hdr, unsigned char *payload,
                              SEXP x, size_t n, rei_handle *h);
/* The pool framing (no arena tier): REF, RAWVEC, STR1, INLINE, SHM_VEC, or
   SHM_RAW. ctx is the stage hook's binding ctx. */
void rei_payload_stage(rei_slot_hdr *hdr, unsigned char *payload,
                        uint32_t inline_max, SEXP x, rei_handle *h,
                        void *ctx);
/* Materialize an INLINE / RAWVEC / SHM_RAW payload, or wrap a SHM_VEC / REF
   payload as an ALTREP view. ctx carries the handle's open cache (via
   rei_read_region) and the R-side view cache; a vanished out-of-line region
   sets ctx->gone and the read returns NULL. A foreign (Python) stream on a
   serialize tier: with consume_foreign (the channel), sets the handle's
   saw_foreign and fails the read with REI_READ_CONSUME, so the slot is
   consumed before the veneer raises; without it (the pool), raises. */
SEXP rei_payload_read(const rei_slot_hdr *hdr, const unsigned char *payload,
                       uint32_t inline_max, rei_read_ctx *ctx,
                       int consume_foreign);
/* Foreign-stream detection on the serialize tiers: a pyrei compact-codec
   stream opens with 'P' (DESIGN.md's codec registry allocates 'R' to rei
   and 'P' to pyrei), and anything past its subset rides pickle (0x80 then a
   protocol byte >= 2). No R stream opens with either ('B'/'X'/'A' are
   ASCII, REI_CODEC_MAGIC is 'R'). Inline: it sits on the serialize-tier
   read dispatch of both read hooks. */
#define REI_PYREI_CODEC_MAGIC 0x50u   /* 'P' */
static inline int rei_is_python_payload(const unsigned char *p, size_t n) {
  return n >= 1 &&
    (p[0] == REI_PYREI_CODEC_MAGIC || (n >= 2 && p[0] == 0x80 && p[1] >= 2));
}

// Zero-copy payload tiers (zc.c) ---------------------------------------------------

void rei_zc_init(void);
/* The consumer split open (page 0 RW, the tail RO): registered as the view
   layer's embedder open hook, so the wire-resolve cache's mappings carry the
   same protection split as the prep path's, which calls it directly. */
rei_shm *rei_zc_open(const char *name);
int rei_zc_eligible(SEXP x, uint32_t inline_max, size_t *out_total);
/* Stage x as SHM_VEC through the handle's services: the region checkout, the
   zc producer-loan retain (rei_stage_retain_zc performs the refcount store),
   and the pin of x. ctx is the stage hook's binding ctx. */
void rei_zc_stage(rei_slot_hdr *hdr, unsigned char *payload, SEXP x,
                   size_t total, rei_handle *h, void *ctx);
int rei_zc_ref_stage(rei_slot_hdr *hdr, unsigned char *payload,
                      uint32_t inline_max, SEXP x);
SEXP rei_zc_read(const rei_slot_hdr *hdr, const unsigned char *payload,
                  int *gone, rei_zc_cache *oc);
SEXP rei_zc_ref_read(const rei_slot_hdr *hdr, const unsigned char *payload,
                      int *gone, rei_zc_cache *oc);
SEXP rei_zc_lookup_wrap(rei_zc_cache *oc, const unsigned char *name,
                         uint32_t len);
void rei_zc_cache_store(rei_zc_cache *oc, const unsigned char *name,
                         uint32_t len, SEXP wrap);
SEXP rei_zc_view_check_call(SEXP x);
SEXP rei_zc_refcount_call(SEXP x);
SEXP rei_zc_fl_info(rei_handle *h);

// Classed error conditions (condition.c) -------------------------------------------

#ifndef R_PRINTF_FORMAT                        /* added in R 4.4.0 */
#define R_PRINTF_FORMAT(M, N)
#endif
NORET void rei_stop(const char *subclass, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
NORET void rei_stop_shm(double bytes, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
NORET void rei_stop_died(int slot, double pid, const char *fmt, ...)
  R_PRINTF_FORMAT(3, 4);
NORET void rei_stop_python_payload(void) REI_COLD;
NORET void rei_cond_signal(SEXP cond);
SEXP rei_cond_set_index(SEXP cond, int index);
SEXP rei_caught(SEXP cond);
SEXP rei_caught_cond(const char *subclass, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
SEXP rei_caught_died(int slot, double pid, const char *fmt, ...)
  R_PRINTF_FORMAT(3, 4);
SEXP rei_condition_flatten(SEXP cond, size_t budget);

// The R binding's seam callbacks (stage_r.c) ---------------------------------------

/* Registered on every handle at create/attach/join. stage/read/exec are the
   tier dispatch, the materialize, and the task evaluator; check polls
   R_CheckUserInterrupt; drop tombstones the staged object's pin-chain cell
   (no allocation — finalizer-safe); sweep drops the pool worker's map cache
   at idle/depart. */
int rei_r_stage_channel(void *obj, rei_slot_hdr *hdr,
                         unsigned char *payload, uint32_t inline_max,
                         rei_handle *h, void *ctx);
void *rei_r_read_channel(const rei_slot_hdr *hdr,
                          const unsigned char *payload, size_t limit,
                          rei_read_ctx *ctx);
int rei_r_stage_pool(void *obj, rei_slot_hdr *hdr,
                      unsigned char *payload, uint32_t inline_max,
                      rei_handle *h, void *ctx);
void *rei_r_read_pool(const rei_slot_hdr *hdr, const unsigned char *payload,
                       size_t limit, rei_read_ctx *ctx);
int rei_r_exec_pool(const rei_slot_hdr *hdr, const unsigned char *payload,
                     size_t limit, rei_result_sink *sink, int catching,
                     rei_read_ctx *ctx);
void rei_r_publish_err(rei_result_sink *sink, SEXP cond);
void rei_r_trace(rei_trace_event event, uint64_t task_id, void *ctx);
int rei_r_check(void *ctx);
void rei_r_drop(void *ctx, void *pin);
void rei_r_sweep(void *ctx);

// Pool-signal unwrap (verbs_pool.c; map.c's runner reads the words) -------

rei_pool_sig *rei_pool_sig_get(SEXP xp);

// GC extptr wrappers (wrap.c) -------------------------------------------------------

rei_shm *rei_region(SEXP xp);
SEXP rei_shm_wrap_producer(rei_shm *shm);
SEXP rei_shm_wrap_consumer(rei_shm *shm);
SEXP rei_shm_wrap_host(rei_shm *shm);
rei_shm *rei_shm_unwrap(SEXP x);

// init / fini hooks -----------------------------------------------------------------

void rei_wrap_init(void);
void rei_payload_init(void);
void rei_entity_init(void);
void rei_channel_init(void);
void rei_pool_init(void);
void rei_map_init(void);
void rei_zc_init(void);
void rei_payload_fini(void);
void rei_channel_fini(void);
void rei_pool_fini(void);

/* map.c's srcref strip, shared with the codec's closure and task writes. */
extern SEXP rei_srcref_sym;
SEXP rei_strip_srcref(SEXP f);
SEXP rei_strip_lang(SEXP x);

#endif /* REI_H */
