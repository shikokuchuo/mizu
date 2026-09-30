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
   [1] pin chain, [2] decline record. Pool prot: [0] eval env, [1] trace fn,
   [2] map cache, [3] zoc wraps, [4] pin chain. */
typedef struct mizu_r_handle_s {
  mizu_handle *core;         /* the core handle; NULL after destroy */
  SEXP prot;                /* the extptr's prot chain */
  SEXP xp;                  /* pool: weak back-ref to the own extptr, read by
                               the current-pool accessor; NULL on channels */
  long self_pid;            /* fork guard */
  int role;                 /* pool: MIZU_ROLE_*; channel: -1 */
  int exec_fail;            /* pool, test-only: fail the next task-frame
                               decode (mizu_pool_exec_fail) */
  int pin_slot;             /* prot slot of the pin chain */
  int decline_slot;         /* channel: prot slot of the decline record (a
                               stashed condition, R_NilValue when empty) */
  uint32_t pins_dead;       /* tombstoned pin cells awaiting splice */
  uint32_t pins_total;      /* pin chain length (live + dead) */
  uint32_t peer_lang;       /* channel: the peer's MIZU_LANG_* byte (0 unset).
                               Pool: the §4.2 foreign-result policy, set from
                               the task stream's submitter identity around
                               one publish, 0 otherwise */
  uint32_t peer_caps;       /* channel: the peer's reader-capability mask
                               (pool: the submitter's, same discipline) */
  uint64_t worker_ident;    /* pool: the cached pool word (mizu_pool_worker_ident,
                               0 until a worker joins; re-read while 0) */
  SEXP err_cond;            /* channel: the peer shim's err-send condition
                               (mizu_channel_send_error); the stage hook
                               pointer-matches it and frames the err stream.
                               NULL when idle; set and cleared within the
                               send veneer, whose argument roots it. */
  SEXP spec;                /* pool submitter: the mizu_pool_submit_spec spec
                               under staging, pointer-matched by the stage
                               hook (the err_cond pattern). NULL when idle;
                               set and cleared within the submit veneer,
                               whose argument roots it. */
  uint64_t spec_ident;      /* the submitter identity stamped on the spec's
                               task stream (this build's word, or the submit
                               veneer's test-only trailing override) */
  mizu_zc_cache zoc;        /* the R-side zc view cache */
} mizu_r_handle;

/* This build's identity word, the one binding fill (channel and pool). */
#define MIZU_R_IDENT \
  MIZU_IDENT(MIZU_LANG_R, MIZU_CAP_MIZS | MIZU_CAP_MIZL | MIZU_CAP_ATTRS)

/* Terminal-state sentinels (the channel/pool veneer), shared across the verb
   surface. */
extern SEXP mizu_sent_full, mizu_sent_timeout, mizu_sent_closed, mizu_sent_gone;

/* The evaluating worker's own pool extptr (stage_r.c): save/restored around
   each task eval by the exec hook and cleared by the pool finalizer —
   borrowed, never precious-listed; between tasks on a live worker the
   extptr is anchored by worker_main's handle. mizu_current_pool() reads
   it. */
extern SEXP mizu_curpool_xp;

/* The executing task's submitter identity word (stage_r.c): 0 for a
   private frame (reads as same-language), else the task stream's header
   word, set at decode before any field is read — the ERR-format and
   result-policy key of §4.2 (foreign = nonzero with a non-R language
   byte). Left dangling on the catching = 0 longjmp exactly like
   mizu_curpool_xp; read by mizu_r_publish_err on both its paths. */
extern uint64_t mizu_curpool_ident;

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

// Interchange codec (interop.c) ----------------------------------------------------

/* The 'I' interchange stream (DESIGN.md's Interchange codec section), R
   half over the core's cursor/emitters: the writer runs on foreign
   handles only, where a decline (0) raises mizu_error_not_portable with
   the recorded path and reason; the reader builds over the core cursor.
   The attr qualification is the inline writer's gate and the §3.5
   layout blob's; the attr builder has a validating mode (the inline
   tag's whitelisted shapes) and an apply-as-is mode (the layout blob). */
typedef struct mizu_ix_decline_s {
  int decline;
  char path[128];
  char reason[160];
  char remedy[96];         /* empty when no one-line rewrite exists */
} mizu_ix_decline;

/* The attr qualification results: 0 not encodable; the whitelisted
   shapes; MIZU_IXQ_ENCODABLE an encodable attribute set outside the
   whitelist (the §3.5 layout blob's R-peer answer — the inline writer,
   foreign-only, requires a whitelisted shape). */
enum {
  MIZU_IXQ_NONE = 0,
  MIZU_IXQ_FACTOR,
  MIZU_IXQ_FRAME,
  MIZU_IXQ_DIM,
  MIZU_IXQ_DATE,
  MIZU_IXQ_POSIXCT,
  MIZU_IXQ_ENCODABLE
};

size_t mizu_interop_write(unsigned char *dst, size_t limit, SEXP x,
                          mizu_ix_decline *rec);
SEXP mizu_interop_read(const unsigned char *buf, size_t len);
/* The err tag (0x11) framer: cond as the bounded top-level error value —
   truncated to fit inline_max by construction, so the caller stamps INLINE
   with the keeperless claim (the writer cannot fail). A mizu_error_remote
   keeps its origin fields; any other condition writes its most-specific
   class, raw message field, and call text. */
size_t mizu_interop_write_err(unsigned char *dst, uint32_t inline_max,
                              SEXP cond);
int mizu_interop_attrs_qualify(SEXP x);
SEXP mizu_interop_attrs_build(SEXP value, SEXP attrs, int validate);
/* The dict-key rules on a names vector (non-NA, UTF-8-writable, unique
   after translation), and the foreign MIZS string gate (every element
   ASCII, CE_UTF8, or native that validates as UTF-8) — shared with the
   zero-copy filter in zc.c. */
int mizu_interop_names_ok(SEXP names);
int mizu_interop_strings_utf8(SEXP x);
/* The foreign STR1: the top-level length-1 string tier, normalized to
   UTF-8 (latin1 translated, CE_BYTES declined). 1 staged, 0 not a
   length-1 string, -1 decline (the record filled). */
int mizu_interop_str1_foreign(mizu_slot_hdr *hdr, unsigned char *payload,
                              uint32_t inline_max, SEXP x,
                              mizu_ix_decline *rec);
/* The task stream (0x12) writer: emits off the spec's components (code,
   positional, named) — two-pass, NULL dst sizes and qualifies (0 = a
   non-portable argument, the record filled). */
size_t mizu_interop_write_task(unsigned char *dst, size_t limit, SEXP spec,
                               uint32_t target, uint64_t ident,
                               mizu_ix_decline *rec);
/* The exec-hook decode of a task stream: validates the header and the
   per-kind shape off the cursor, stashes the submitter identity in
   mizu_curpool_ident, and builds in place — name kind: a LANGSXP (the
   qualified name resolved); source kind: list(parsed exprs, env) with the
   arguments bound as names (positional as "..1", "..2", ...) in a fresh
   frame under base. kind_out takes the kind byte. Raises the informative
   shape errors (the exec hook contains them). */
SEXP mizu_interop_exec_task(const unsigned char *buf, size_t len, SEXP base,
                            int *kind_out);

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
   serialize tier: with consume_foreign (the channel), stash the interned
   decline condition on the handle and fail the read with MIZU_READ_CONSUME,
   so the slot is consumed before the veneer signals it; without it (the
   pool), raise in place. */
SEXP mizu_payload_read(const mizu_slot_hdr *hdr, const unsigned char *payload,
                       uint32_t inline_max, mizu_read_ctx *ctx,
                       int consume_foreign);
/* The one first-byte dispatch of the serialize tiers (the codec registry
   in DESIGN.md): 'I' the interchange stream, 'R' the compact codec, 'B' /
   'X' / 'A' R native streams, anything else ('P', pickle, or unlisted) the
   informative decline — consumed on the channel, raised in place on the
   pool. Covers the INLINE / ARENA / SHM_RAW read sites and Phase 4's task
   decode (its tag check branches ahead of the 'I' case). */
SEXP mizu_stream_read(const unsigned char *buf, size_t len,
                      mizu_read_ctx *ctx, int consume_foreign);
void mizu_payload_spill_interop(mizu_slot_hdr *hdr, unsigned char *payload,
                                SEXP x, size_t n, mizu_handle *h);
/* The SHM_RAW spill of a task stream (a pool has no arena): the ordinary
   SHM_RAW retain, no keeperless claim (§4.0). */
void mizu_payload_spill_task(mizu_slot_hdr *hdr, unsigned char *payload,
                             SEXP spec, uint32_t target, uint64_t ident,
                             size_t n, mizu_handle *h);
/* Foreign-stream detection on the serialize tiers: a pymizu compact-codec
   stream opens with 'P' (DESIGN.md's codec registry allocates 'R' to mizu
   and 'P' to pymizu), and anything past its subset rides pickle (0x80 then a
   protocol byte >= 2). No R stream opens with either ('B'/'X'/'A' are
   ASCII, MIZU_CODEC_MAGIC is 'R'). Inline: it sits on the serialize-tier
   read dispatch of both read hooks. */
static inline int mizu_is_python_payload(const unsigned char *p, size_t n) {
  return n >= 1 &&
    (p[0] == MIZU_PYMIZU_CODEC_MAGIC || (n >= 2 && p[0] == 0x80 && p[1] >= 2));
}

/* The interned foreign-payload decline condition (condition.c; preserved
   at load): the general decline record a consumed read failure hands the
   veneer through the handle's prot slot. */
extern SEXP mizu_decline_python;

/* The channel's foreign-payload decline: stash the interned condition on
   the handle's decline slot (SET_VECTOR_ELT, no allocation) and consume
   the slot — the recv veneer signals the record ahead of chan_raise, and
   the core records nothing for a consumed read. */
static inline void mizu_decline_foreign(mizu_read_ctx *ctx) {
  mizu_r_handle *rh = (mizu_r_handle *) ctx->binding_ctx;
  SET_VECTOR_ELT(rh->prot, rh->decline_slot, mizu_decline_python);
  ctx->flags |= MIZU_READ_CONSUME;
}

// Zero-copy payload tiers (zc.c) ---------------------------------------------------

void mizu_zc_init(void);
/* The REF-used flag: whether the emit hook fired since the last reset — a
   serialize pass then in flight carried a view by reference, so its stage
   must pin and cannot claim keeperless (MIZU_AUX_F_KEEPERLESS). Reset
   before a stage's first serialize pass; read after it. */
void mizu_zc_ref_reset(void);
int mizu_zc_ref_fired(void);
/* The consumer split open (page 0 RW, the tail RO): registered as the view
   layer's embedder open hook, so the wire-resolve cache's mappings carry the
   same protection split as the prep path's, which calls it directly. */
mizu_shm *mizu_zc_open(const char *name);
int mizu_zc_eligible(SEXP x, uint32_t inline_max, size_t *out_total,
                     int foreign);
/* Stage x as SHM_VEC through the handle's services: the region checkout, the
   zc producer-loan retain (mizu_stage_retain_zc performs the refcount store),
   and the pin of x. ctx is the stage hook's binding ctx. foreign is the
   peer-language signal: the layout write builds the validity-bitmap
   section and aux carries the write's actual total (the size pass's
   reservation counts clean leaves' unspent bitmap bytes). */
void mizu_zc_stage(mizu_slot_hdr *hdr, unsigned char *payload, SEXP x,
                   size_t total, mizu_handle *h, void *ctx, int foreign);
int mizu_zc_ref_stage(mizu_slot_hdr *hdr, unsigned char *payload,
                      uint32_t inline_max, SEXP x);
/* The foreign-handle SHM_VEC gate: the baseline layouts (an
   attribute-free atomic — ALTREP admitted, the layout write copying
   through *_GET_REGION — or a class-only integer64) plus whatever the
   peer's capability mask advertises. A value past the floor outside the
   peer's set takes the interop writer instead. */
int mizu_zc_eligible_foreign(SEXP x, uint32_t inline_max, size_t *out_total,
                             uint32_t caps);
/* The REF half of the filter: a view re-sent top-level crosses by
   reference only when the peer wraps its layout. */
int mizu_zc_ref_foreign_ok(SEXP x, uint32_t caps);
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
NORET void mizu_stop_not_portable(const char *path, const char *reason,
                                  const char *remedy) MIZU_COLD;
SEXP mizu_cond_python_payload(void);
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
void mizu_interop_init(void);
void mizu_payload_fini(void);
void mizu_channel_fini(void);
void mizu_pool_fini(void);

/* map.c's srcref strip, shared with the codec's closure and task writes. */
extern SEXP mizu_srcref_sym;
SEXP mizu_strip_srcref(SEXP f);
SEXP mizu_strip_lang(SEXP x);

#endif /* MIZU_H */
