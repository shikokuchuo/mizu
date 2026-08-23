/* sora.h — the R-internal header: the R binding's declarations on top of the
   vendored librei core. The wire format, the core verbs, and the region layer
   live in vendor/librei/rei.h (the authority) and vendor/librei/internal.h;
   the ALTREP consumer classes and exact-size serialize streams come from
   vendor/mori.h. Only R-coupled declarations live here. */

#ifndef SORA_H
#define SORA_H

#include "vendor/librei/rei.h"
#include "vendor/librei/internal.h"
#include "vendor/mori.h"
#include <stdatomic.h>

// The R binding's per-handle context ----------------------------------------------

/* The zc view cache: name -> split-mapped (page 0 RW) consumer mapping,
   wrap-pinned. Unlike the SHM_RAW cache (core-side, rei_open_cache), eviction
   only drops the reference — a live view keeps its mapping through its own
   chain, so a cache eviction must never unmap. */
typedef struct sora_zc_cache_s {
  SEXP wraps;                       /* VECSXP(REI_OPEN_CACHE_MAX), prot-pinned */
  char names[REI_OPEN_CACHE_MAX][REI_NAME_MAX];
  uint8_t name_len[REI_OPEN_CACHE_MAX];
  uint64_t stamp[REI_OPEN_CACHE_MAX];
  uint64_t tick;
  uint64_t hits, misses;
} sora_zc_cache;

/* The channel/pool extptr's address and the binding ctx. The core handle
   (a rei_channel or rei_pool) is opaque; everything R-side rides here: the
   prot chain (GC-visible slots) and the zc view cache — keyed on SEXP views,
   so it cannot live in the R-free core. Channel prot: [0] zoc wraps. Pool
   prot: [0] eval env, [1] trace fn, [2] map cache, [3] zoc wraps. */
typedef struct sora_handle_s {
  rei_handle *core;         /* the core handle; NULL after destroy */
  SEXP prot;                /* the extptr's prot chain */
  long self_pid;            /* fork guard */
  int role;                 /* pool: REI_ROLE_*; channel: -1 */
  sora_zc_cache zoc;        /* the R-side zc view cache */
} sora_handle;

/* Terminal-state sentinels (the channel/pool veneer), shared across the verb
   surface. */
extern SEXP sora_sent_full, sora_sent_timeout, sora_sent_closed, sora_sent_gone;

// Bounded single-pass serialize (bounded.c) ---------------------------------------

size_t sora_serialize_bounded(unsigned char *dst, size_t limit, SEXP object);

// Compact codec (codec.c) ---------------------------------------------------------

/* The sora-native binary framing for the hot-path payload subset. Streams are
   self-describing — the first byte is REI_CODEC_MAGIC where an R binary
   stream carries 'B', so readers dispatch on it and the slot header is
   untouched. The writer rejects ALTREP anywhere in the graph, so a codec
   stream carries no mori identifier and needs no pin. */
size_t sora_codec_write(unsigned char *dst, size_t limit, SEXP object);
SEXP sora_codec_read(const unsigned char *buf, size_t len);
int sora_codec_read_task(const unsigned char *buf, size_t len, SEXP *expr,
                         SEXP *args);
SEXP sora_empty_args(void);

// Payload framing (payload.c) -----------------------------------------------------

void *sora_vec_ptr(SEXP x);
int sora_raw_eligible(SEXP x, uint32_t inline_max, size_t *out_len);
int sora_raw_type(SEXP x, size_t *out_len);
int sora_str1_stage(rei_slot_hdr *hdr, unsigned char *payload,
                    uint32_t inline_max, SEXP x);
/* The service-form spill checkout, raising sora_error_shm on create
   failure (the stager's raise-on-failure discipline). Shared by the
   payload and zc spill paths. */
rei_shm *sora_spill_get_raise(rei_handle *h, size_t n);
/* The spill tiers, staged through the handle's services (rei_stage_spill_get
   / rei_stage_retain / rei_stage_pin): the region checkout is the handle's,
   the pin is the staged object where a stream may carry hook-emitted mori
   identifiers (the codec stream is ALTREP-free and pins nothing). */
void sora_payload_spill_shm(rei_slot_hdr *hdr, unsigned char *payload, SEXP x,
                            size_t n, rei_handle *h);
void sora_payload_spill_raw(rei_slot_hdr *hdr, unsigned char *payload, SEXP x,
                            size_t n, rei_handle *h);
void sora_payload_spill_codec(rei_slot_hdr *hdr, unsigned char *payload,
                              SEXP x, size_t n, rei_handle *h);
/* The pool framing (no arena tier): REF, RAWVEC, STR1, INLINE, SHM_VEC, or
   SHM_RAW. */
void sora_payload_stage(rei_slot_hdr *hdr, unsigned char *payload,
                        uint32_t inline_max, SEXP x, rei_handle *h);
/* Materialize an INLINE / RAWVEC / SHM_RAW payload, or wrap a SHM_VEC / REF
   payload as an ALTREP view. ctx carries the handle's open cache (via
   rei_read_region) and the R-side view cache; a vanished out-of-line region
   sets ctx->gone and the read returns NULL. */
SEXP sora_payload_read(const rei_slot_hdr *hdr, const unsigned char *payload,
                       uint32_t inline_max, rei_read_ctx *ctx);

// Zero-copy payload tiers (zc.c) ---------------------------------------------------

void sora_zc_init(void);
int sora_zc_eligible(SEXP x, uint32_t inline_max, size_t *out_total);
/* Stage x as SHM_VEC through the handle's services: the region checkout, the
   zc producer-loan retain (rei_stage_retain_zc performs the refcount store),
   and the pin of x. */
void sora_zc_stage(rei_slot_hdr *hdr, unsigned char *payload, SEXP x,
                   size_t total, rei_handle *h);
int sora_zc_ref_stage(rei_slot_hdr *hdr, unsigned char *payload,
                      uint32_t inline_max, SEXP x);
SEXP sora_zc_read(const rei_slot_hdr *hdr, const unsigned char *payload,
                  int *gone, sora_zc_cache *oc);
SEXP sora_zc_ref_read(const rei_slot_hdr *hdr, const unsigned char *payload,
                      int *gone, sora_zc_cache *oc);
SEXP sora_zc_lookup_wrap(sora_zc_cache *oc, const unsigned char *name,
                         uint32_t len);
void sora_zc_cache_store(sora_zc_cache *oc, const unsigned char *name,
                         uint32_t len, SEXP wrap);
SEXP sora_zc_view_check_call(SEXP x);
SEXP sora_zc_refcount_call(SEXP x);
SEXP sora_zc_fl_info(rei_spill_fl *fl);

// Classed error conditions (condition.c) -------------------------------------------

#ifndef R_PRINTF_FORMAT                        /* added in R 4.4.0 */
#define R_PRINTF_FORMAT(M, N)
#endif
NORET void sora_stop(const char *subclass, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
NORET void sora_stop_shm(double bytes, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
NORET void sora_stop_died(int slot, double pid, const char *fmt, ...)
  R_PRINTF_FORMAT(3, 4);
NORET void sora_cond_signal(SEXP cond);
SEXP sora_caught(SEXP cond);
SEXP sora_caught_cond(const char *subclass, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
SEXP sora_caught_died(int slot, double pid, const char *fmt, ...)
  R_PRINTF_FORMAT(3, 4);
SEXP sora_condition_flatten(SEXP cond, size_t budget);

// The R binding's seam callbacks (stage_r.c) ---------------------------------------

/* Registered on every handle at create/attach/join. stage/read/exec are the
   tier dispatch, the materialize, and the task evaluator; check polls
   R_CheckUserInterrupt; drop releases the staged object's R_PreserveObject;
   sweep drops the pool worker's map cache at idle/depart. */
int sora_r_stage_channel(void *obj, rei_slot_hdr *hdr,
                         unsigned char *payload, uint32_t inline_max,
                         rei_handle *h);
void *sora_r_read_channel(const rei_slot_hdr *hdr,
                          const unsigned char *payload, size_t limit,
                          rei_read_ctx *ctx);
int sora_r_stage_pool(void *obj, rei_slot_hdr *hdr,
                      unsigned char *payload, uint32_t inline_max,
                      rei_handle *h);
void *sora_r_read_pool(const rei_slot_hdr *hdr, const unsigned char *payload,
                       size_t limit, rei_read_ctx *ctx);
int sora_r_exec_pool(const rei_slot_hdr *hdr, const unsigned char *payload,
                     size_t limit, rei_result_sink *sink, int catching,
                     void *ctx);
void sora_r_publish_err(rei_result_sink *sink, SEXP cond);
void sora_r_trace(rei_trace_event event, uint64_t task_id, void *ctx);
int sora_r_check(void *ctx);
void sora_r_drop(void *ctx, void *pin);
void sora_r_sweep(void *ctx);

// Pool-signal unwrap (verbs_pool.c; map.c's runner reads the words) -------

rei_pool_sig *sora_pool_sig_get(SEXP xp);

// GC extptr wrappers (wrap.c) -------------------------------------------------------

rei_shm *sora_region(SEXP xp);
SEXP sora_shm_wrap_producer(rei_shm *shm);
SEXP sora_shm_wrap_consumer(rei_shm *shm);
SEXP sora_shm_wrap_host(rei_shm *shm);
rei_shm *sora_shm_unwrap(SEXP x);

// init / fini hooks -----------------------------------------------------------------

void sora_wrap_init(void);
void sora_payload_init(void);
void sora_entity_init(void);
void sora_channel_init(void);
void sora_pool_init(void);
void sora_map_init(void);
void sora_zc_init(void);
void sora_payload_fini(void);
void sora_channel_fini(void);
void sora_pool_fini(void);

/* map.c's srcref strip, shared with the codec's closure and task writes. */
extern SEXP sora_srcref_sym;
SEXP sora_strip_srcref(SEXP f);
SEXP sora_strip_lang(SEXP x);

#endif /* SORA_H */
