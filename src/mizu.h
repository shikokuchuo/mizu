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

/* R-version backports, one copy for the package's TUs (Writing R
   Extensions, "Moving into C API compliance"): ANY_ATTRIB(), the closure
   slot accessors, and R_mkClosure() joined the C API in R 4.5.0 (the
   constructor's single definition is map.c's); R_getAttributes() in
   R 4.6.0 (the named-list form — the fallback reads the pairlist, and
   every consumer handles both forms). view.c carries its own ANY_ATTRIB
   copy: mori vendors it without this header. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif
#if R_VERSION < R_Version(4, 5, 0)
#define R_ClosureFormals(x) FORMALS(x)
#define R_ClosureBody(x)    BODY(x)
#define R_ClosureEnv(x)     CLOENV(x)
SEXP R_mkClosure(SEXP formals, SEXP body, SEXP env);
#endif
#if R_VERSION < R_Version(4, 6, 0)
#define R_getAttributes(x) ATTRIB(x)
#endif

/* Wire-relevant numeric bounds as named constants: the largest integer a
   double represents exactly (2^53), the int64 magnitude bound (2^63), the
   uint64 ceiling as a double (2^64), and R's L'Ecuyer-CMRG kind word
   (.Random.seed's first element). */
#define MIZU_DBL_SAFE_INT 0x1p53
#define MIZU_DBL_I64      0x1p63
#define MIZU_DBL_U64      0x1p64
#define MIZU_RNG_CMRG_KIND 10407

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
   [2] map cache, [3] zoc wraps, [4] pin chain, [5] decline record. */
typedef struct mizu_r_handle_s {
  mizu_handle *core;         /* the core handle; NULL after destroy */
  SEXP prot;                /* the extptr's prot chain */
  SEXP xp;                  /* pool: weak back-ref to the own extptr, read by
                               the current-pool accessor; NULL on channels */
  long self_pid;            /* fork guard */
  int role;                 /* pool: MIZU_ROLE_*; channel: -1 */
  int exec_fail;            /* pool, test-only: fail the next task-frame
                               decode (mizu_pool_exec_fail) */
  int eval_marked;          /* pool worker: the binding's copy of the eval
                               mark's armed state, for the trace thunk's
                               lift-and-restore (the core has no read API) */
  int pin_slot;             /* prot slot of the pin chain */
  int decline_slot;         /* prot slot of the decline record (a stashed
                               condition, R_NilValue when empty) */
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

/* This build's identity word, the one binding fill (channel and pool).
   MIZU_CAP_MIZL_TSN deliberately absent: R lands ns counts as double
   seconds either way (ix_tsn_landing), so the bit would only cost the
   receive-side copy and the view. */
#define MIZU_R_IDENT \
  MIZU_IDENT(MIZU_LANG_R, MIZU_CAP_MIZS | MIZU_CAP_MIZL | MIZU_CAP_ATTRS | \
             MIZU_CAP_TASKREF | MIZU_CAP_MIZL_REF | MIZU_CAP_MIZL_U64V | \
             MIZU_CAP_LABELED)

/* A same-language stage's own capability word (pymizu's precedent: a task
   frame is same-language by construction, so this build's caps is the
   real word). The pool Arrow gate's word (stage_r.c) and the codec frame
   tier's (codec.c) — both run same-language only. */
#define MIZU_R_CAPS ((uint32_t) (MIZU_R_IDENT >> 32))

/* The SHM_VEC floor gate: a payload past max(inline budget,
   MIZU_ZC_FLOOR) is a zero-copy candidate; at or under it the copy tiers
   win. */
static inline size_t mizu_zc_gate(uint32_t inline_max) {
  return (size_t) inline_max > MIZU_ZC_FLOOR ?
    (size_t) inline_max : MIZU_ZC_FLOOR;
}

/* The reader-language state (one predicted branch per stage): a foreign
   handle — a channel whose peer is not R, or the §4.2 foreign result
   policy armed around a pool publish — stages through the interop paths;
   same-language handles run the private tiers. */
static inline int mizu_r_peer_foreign(const mizu_r_handle *rh) {
  return rh->peer_lang != 0 && rh->peer_lang != MIZU_LANG_R;
}

/* Terminal-state sentinels (the channel/pool veneer), shared across the verb
   surface. */
extern SEXP mizu_sent_full, mizu_sent_timeout, mizu_sent_closed, mizu_sent_gone;

/* The evaluating worker's own pool extptr (stage_r.c): save/restored around
   each task eval by the exec hook and cleared by the pool finalizer —
   borrowed, never precious-listed; between tasks on a live worker the
   extptr is anchored by worker_main's handle. mizu_current_pool() reads
   it. */
extern SEXP mizu_curpool_xp;

/* The outermost executing task's submitter identity word (stage_r.c): 0
   for a private frame (reads as same-language), else the task stream's
   header word, set at decode before any field is read — the ERR-format and
   result-policy key of §4.2 (foreign = nonzero with a non-R language
   byte). Written only through the catching = 0 exec's identp, so a nested
   exec (help mode, a nested collect) never clobbers it; read by
   mizu_pool_run_outcome's ERR publish after the catching = 0 longjmp, for
   which it stays dangling exactly like mizu_curpool_xp. */
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
/* The frame-aware codec tier (codec.c), called from the two same-language
   codec sites after the plain pass declined: an ArrowTabular nested in
   the payload crosses as one frame leaf (the tag, a u8 length, the region
   name) — at most one per payload, its stream always INLINE (the core's
   single-checkout rule). 1 = hdr filled (INLINE or SHM_RAW, aux stamped);
   0 = no ArrowTabular found, past the candidate cap, or the rest of the
   tree outside the codec subset — the caller's serialize fallback runs,
   exactly as today and with no Arrow stream consumed. */
int mizu_frame_tier(mizu_slot_hdr *hdr, unsigned char *payload,
                    uint32_t inline_max, SEXP x, mizu_handle *h);

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
  MIZU_IXQ_DIFFTIME,
  MIZU_IXQ_ENCODABLE,
  MIZU_IXQ_NAMES,    /* attrs exactly {names} on an atomic non-character */
  MIZU_IXQ_DIMNAMES, /* {dim, dimnames[, class = "table"]} */
  MIZU_IXQ_FRAME_EXT /* the extended frame: class suffix + extras */
};

/* The capability bits a verdict needs: MIZU_CAP_ATTRS for the baseline
   shapes, plus MIZU_CAP_LABELED for the three labeled ones (zc.c's
   foreign filters key on it so a labeled shape never stages to a peer
   that cannot read it). */
uint32_t ix_q_caps(int q);

size_t mizu_interop_write(unsigned char *dst, size_t limit, SEXP x,
                          mizu_ix_decline *rec, uint32_t caps);
/* The in-hook forms (payload.c, stage_r.c): the caller's stage scope
   owns the verdict-cache generation — the public entries' arm would
   reseat it, splitting the size and write passes' shared verdicts. */
size_t mizu_interop_write_scoped(unsigned char *dst, size_t limit, SEXP x,
                                 mizu_ix_decline *rec, uint32_t caps);
SEXP mizu_interop_read(const unsigned char *buf, size_t len);
/* The refs mode of the value reader: with refs, a 0x13 leaf resolves to
   a view (the task decode, the map descriptor reader, and the pool's
   collect-side result reader); without, it raises the builder decline
   (the channel value reader). */
SEXP mizu_interop_read_mode(const unsigned char *buf, size_t len, int refs);
/* The err tag (0x11) framer: cond as the bounded top-level error value —
   truncated to fit inline_max by construction, so the caller stamps INLINE
   with the keeperless claim (the writer cannot fail). A mizu_error_remote
   keeps its origin fields; any other condition writes its most-specific
   class, raw message field, and call text. */
size_t mizu_interop_write_err(unsigned char *dst, uint32_t inline_max,
                              SEXP cond);
size_t mizu_interop_write_err_scoped(unsigned char *dst,
                                     uint32_t inline_max, SEXP cond);
int mizu_interop_attrs_qualify(SEXP x);
/* The per-stage attr-qualification verdict cache (interop.c): a cached
   verdict is consulted only under a live armed generation whose scope
   anchors the qualified trees — the stage hooks arm per invocation
   (stage_r.c's guard; per element loop under an opted-in batch verb),
   the off-hook writer entries arm unconditionally — and every arm is
   eager, so a raise-orphaned generation is replaced before the next
   consultation. */
void mizu_interop_stage_arm(void);
void mizu_interop_stage_disarm(void);
/* A saved cache scope (the live generation and its insert budget),
   pushed/popped around a converted-temporary re-stage inside a batch
   verb (arrow.c's decline re-entries), so the temporary's verdict keys
   die with it instead of riding the batch's generation. */
typedef struct {
  uint64_t gen;
  unsigned n;
} mizu_qc_scope;
void mizu_interop_stage_push(mizu_qc_scope *s);
void mizu_interop_stage_pop(const mizu_qc_scope *s);
/* The batch verbs' wide scope (stage_r.c's guard): begin arms one
   generation for the whole element loop; end voids it (the veneer's
   unwind cleanup covers the raise path — mizu_stage_batch_clean is the
   R_ExecWithCleanup arm). The temp pair scopes a converted-temporary
   re-stage within it. */
void mizu_stage_batch_begin(void);
void mizu_stage_batch_end(void);
void mizu_stage_batch_clean(void *ignored);
void mizu_stage_temp_begin(mizu_qc_scope *s);
void mizu_stage_temp_end(const mizu_qc_scope *s);
SEXP mizu_interop_attrs_build(SEXP value, SEXP attrs, int validate);
/* The dict-key rules on a names vector (non-NA, UTF-8-writable, unique
   after translation), and the foreign MIZS string gate (every element
   ASCII, CE_UTF8, or native that validates as UTF-8) — shared with the
   zero-copy filter in zc.c. */
int mizu_interop_names_ok(SEXP names);
int mizu_interop_strings_utf8(SEXP x, size_t *bytes);
/* The foreign STR1: the top-level length-1 string tier, normalized to
   UTF-8 (latin1 translated, CE_BYTES declined). 1 staged, 0 not a
   length-1 string, -1 decline (the record filled). */
int mizu_interop_str1_foreign(mizu_slot_hdr *hdr, unsigned char *payload,
                              uint32_t inline_max, SEXP x,
                              mizu_ix_decline *rec);
/* The zc ref leaf's conservative reservation in a plan/count walk: the
   fresh region's name length is known only at the checkout, so the fit
   decision uses the bound and the actual write never exceeds it. */
#define MIZU_IX_ZC_RESERVE (2 + (MIZU_NAME_MAX - 1))
/* The W2 plan record, filled by mizu_interop_write_task's record mode:
   the first 16 SHM_VEC candidates in document order, each with its
   by-value subtree size (a recorded candidate's subtree walks with
   detection suppressed — the never-nest rule), and has_ref set when a
   ref leaf emitted (the D2 detector). zc_size is the candidate's exact
   layout size from the candidacy probe: the write pass's region size,
   sparing a second eligibility evaluation. The selection is arithmetic:
   the first candidate with total - size[i] + MIZU_IX_ZC_RESERVE <=
   inline_max is the zc node — the record walk's total is the no_zc
   stream's, REF-able views emitting ref leaves in every mode. */
typedef struct {
  SEXP cand[16];
  size_t size[16];
  size_t zc_size[16];
  int ncand;
  int has_ref;
} mizu_ix_plan;
/* The task stream (0x12) writer: emits off the spec's components (code,
   positional, named) — two-pass, NULL dst sizes and qualifies (0 = a
   non-portable argument, the record filled). Refs are always on (F1): a
   REF-qualifying view in the argument trees emits a ref leaf (0x13) and
   marks the region REFHELD. With h != NULL the selected zc_node stages
   one SHM_VEC checkout mid-write (the write pass only — the count walks
   size a conservative reservation); a mid-write checkout failure returns
   SIZE_MAX, the caller re-running with no_zc = 1. inline_max is the zc
   floor gate. plan non-NULL runs the walk in record mode (zc_node
   R_NilValue): the optimistic write flips to count-only at the first
   candidate, the plan filled for the caller's arithmetic selection.
   zc_size is the plan-recorded layout size of zc_node (0 re-probes
   eligibility). */
size_t mizu_interop_write_task(unsigned char *dst, size_t limit, SEXP spec,
                               uint32_t target, uint64_t ident,
                               mizu_ix_decline *rec, mizu_handle *h,
                               uint32_t caps, uint32_t inline_max,
                               SEXP zc_node, size_t zc_size, int no_zc,
                               mizu_ix_plan *plan);
size_t mizu_interop_write_task_scoped(unsigned char *dst, size_t limit,
                                      SEXP spec, uint32_t target,
                                      uint64_t ident, mizu_ix_decline *rec,
                                      mizu_handle *h, uint32_t caps,
                                      uint32_t inline_max, SEXP zc_node,
                                      size_t zc_size, int no_zc,
                                      mizu_ix_plan *plan);
/* The exec-hook decode of a task stream: validates the header and the
   per-kind shape off the cursor, returns the submitter identity through
   ident_out ahead of every field read (so even a torn stream fails the
   task in the submitter's own format), and builds in place — name kind: a
   LANGSXP (the qualified name resolved); source kind: list(parsed exprs,
   env) with the arguments bound as names (positional as "..1", "..2", ...)
   in a fresh frame under base. kind_out takes the kind byte. Raises the
   informative shape errors (the exec hook contains them). */
SEXP mizu_interop_exec_task(const unsigned char *buf, size_t len, SEXP base,
                            int *kind_out, uint64_t *ident_out);
/* The map descriptor's 'I' form (Phase 5): one stream, list[task, x | nil]
   — the f spec nested as a kind 0/1 task tag, the list-x a bare list (nil
   when the raw section carries it). Two-pass; 0 = decline. */
size_t mizu_interop_write_map_desc(unsigned char *dst, size_t limit,
                                   SEXP spec, SEXP x, uint32_t target,
                                   uint64_t ident, mizu_ix_decline *rec);
/* The kind-2 (runner) task stream: region name, the packed
   ordinal+generation i64 (ordinal the high 32 bits, the morsel generation
   the low 32), the seed nil or the (seed, offset) i64v[2]. */
size_t mizu_interop_write_runner(unsigned char *dst, size_t limit,
                                 SEXP name, double gen_field, SEXP seed,
                                 uint32_t target, uint64_t ident);
/* The exec-hook kind-2 decode: list(name, gen_field, seed|NULL); the
   submitter identity returns through ident_out as the call kinds'. */
SEXP mizu_interop_exec_runner(const unsigned char *buf, size_t len,
                              uint64_t *ident_out);
/* The map descriptor read: list(kind, product, x|NULL) — name kind: the
   consed LANGSXP; source kind: list(exprs, env) parented on base. */
SEXP mizu_interop_read_map_desc(const unsigned char *buf, size_t len,
                                SEXP base);

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
                                SEXP x, size_t n, mizu_handle *h,
                                uint32_t caps);
/* The SHM_RAW spill of a task stream (a pool has no arena): the ordinary
   SHM_RAW retain, no keeperless claim (§4.0). Refs stay on (a REF leaf
   needs no checkout) with no_zc forced — the fresh values cross by
   value. */
void mizu_payload_spill_task(mizu_slot_hdr *hdr, unsigned char *payload,
                             SEXP spec, uint32_t target, uint64_t ident,
                             size_t n, mizu_handle *h, uint32_t caps,
                             uint32_t inline_max);
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

/* The boundary-collection gate (zc.c): region bytes armed since the last
   boundary collection that trigger a minor gc at the pool task boundaries
   (the worker's exec entry, the submitter's collect entry). A view is
   ~100 B of R heap over MBs of shm, so R's own gc cadence never sees the
   pin pressure. The gate sizes the reclaim drip against the spill free
   list's 2-per-class cap: a firing hands back one or two regions per class
   (they rejoin the list); a bigger gate would burst past the cap, the
   excess destroyed at insert and re-created next task. */
#define MIZU_R_GC_BYTES ((size_t) 8 << 20)

void mizu_zc_init(void);
/* The by-reference emission record: the mark list holds every view a
   by-reference emission marked REFHELD since the last reset — the stage's
   pin-and-no-keeperless signal (a payload carrying a reference must pin
   and cannot claim MIZU_AUX_F_KEEPERLESS). The list rides the precious
   root's MIZU_PRIV_ZC_MARKS slot. Reset at a stage's entry and again once
   a worker's outcome write has consumed it; read after it. */
void mizu_zc_ref_reset(void);
int mizu_zc_ref_fired(void);
/* The funnel every by-reference emission goes through: mark the view's
   region REFHELD and record it on the mark list. The REF tier, the
   serialize fallback's wire hooks, and the 'I' ref gate all call it. */
void mizu_zc_ref_mark(SEXP x);
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
/* The byte-gated minor collection at a pool task boundary: releases the
   views the finished task left behind while they are still in New space.
   Past MIZU_R_GC_BYTES of armed view bytes only — a task that wrapped
   nothing or only small views pays one load. The idle sweep hook
   deliberately never collects: at idle the previous REF result is still
   retained, and a collection there would promote it to Old[0], slipping
   its release to the level-1 cadence (the generation trap). */
void mizu_zc_gc_boundary(void);
SEXP mizu_zc_view_check_call(SEXP x);
SEXP mizu_zc_refcount_call(SEXP x);
SEXP mizu_zc_armed_bytes_call(void);
SEXP mizu_zc_fl_info(mizu_handle *h);

// Classed error conditions (condition.c) -------------------------------------------

#ifndef R_PRINTF_FORMAT                        /* added in R 4.4.0 */
#define R_PRINTF_FORMAT(M, N)
#endif
NORET void mizu_stop(const char *subclass, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
NORET void mizu_stop_shm(double bytes, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
NORET void mizu_stop_python_payload(void) MIZU_COLD;
NORET void mizu_stop_not_portable(const char *path, const char *reason,
                                  const char *remedy) MIZU_COLD;
SEXP mizu_cond_python_payload(void);
NORET void mizu_cond_signal(SEXP cond);
SEXP mizu_cond_make(const char *subclass, const char *fmt, ...)
  R_PRINTF_FORMAT(2, 3);
void mizu_cond_preserve_index(SEXP cond);
SEXP mizu_cond_set_field(SEXP cond, const char *field, SEXP value);
SEXP mizu_cond_set_index(SEXP cond, int index);
SEXP mizu_caught(SEXP cond);
int mizu_caught_check(SEXP x);
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
void mizu_r_publish_err(mizu_result_sink *sink, SEXP cond, uint64_t ident);
void mizu_r_trace(mizu_trace_event event, uint64_t task_id, void *ctx);
int mizu_r_check(void *ctx);
void mizu_r_drop(void *ctx, void *pin);
void mizu_r_sweep(void *ctx);

// The channel/pool veneer's shared helpers (verbs.c) -----------------------

/* Raise a create/attach/join failure off the core's thread-local slot. */
NORET void mizu_r_raise_tls(void) MIZU_COLD;
int mizu_r_pow2(uint64_t v);
double mizu_r_size_arg(SEXP x, const char *what);
const char *mizu_r_suffix_arg(SEXP suffix_sexp);
uint64_t mizu_r_ident_override(SEXP ident_sexp, uint64_t dflt);
/* The seconds argument of every verb's timeout: NA (from NA input or any
   non-numeric coercion) rejected — the core maps it to an indefinite wait. */
double mizu_r_timeout_arg(SEXP timeout_sexp);
/* The handle unwrap: tag check, released check (peek returns NULL), fork
   guard. `what` ("channel" / "pool") composes the messages. */
mizu_r_handle *mizu_r_handle_peek(SEXP xp, SEXP tag, const char *what);
mizu_r_handle *mizu_r_handle_get(SEXP xp, SEXP tag, const char *what);
/* The recv/collect entry discipline of both veneer's wait verbs: the
   consumer-side boundary collection (dead received views release here
   instead of piling up as lent bytes the caller's own gc cadence cannot
   see), skipped inside a pool task — a collection there would only
   promote the task's live views; then the decline slot cleared, so the
   verb's read hook stashes a fresh caught condition there on MIZU_ERR. */
void mizu_r_recv_begin(mizu_r_handle *h);

// Pool-signal unwrap (verbs_pool.c; map.c's runner reads the words) -------

mizu_pool_sig *mizu_pool_sig_get(SEXP xp);

// GC extptr wrappers (wrap.c) -------------------------------------------------------

mizu_shm *mizu_region(SEXP xp);
SEXP mizu_shm_wrap_producer(mizu_shm *shm);
SEXP mizu_shm_wrap_consumer(mizu_shm *shm);
SEXP mizu_shm_wrap_host(mizu_shm *shm);
mizu_shm *mizu_shm_unwrap(SEXP x);

// The private precious root ---------------------------------------------------------

/* The private precious root (init.c): the package's one entry on R's
   precious list beyond the read-only singletons and sentinels. Every
   mutable or cached package-level SEXP rides a slot here; the root's
   identity never changes (slot contents may — the zc mark list grows, the
   view cache wraps rotate). Preserved first at load, released last at
   unload. */
enum {
  MIZU_PRIV_ZC_MARKS,          /* zc.c: REFHELD mark list (growable) */
  MIZU_PRIV_VIEW_WRAPS,        /* view.c: consumer-mapping cache wraps
                                  (borrowed there) */
  MIZU_PRIV_MIZU_NS,           /* arrow.c: the mizu namespace */
  MIZU_PRIV_FN_ARROW_NS,
  MIZU_PRIV_FN_ARROW_TO_VALUE,
  MIZU_PRIV_FN_BATCHES_TO_VALUE,
  MIZU_PRIV_NANO_NS,           /* arrow.c: the nanoarrow namespace */
  MIZU_PRIV_FN_AS_STREAM,
  MIZU_PRIV_FN_ALLOC_SCHEMA,
  MIZU_PRIV_FN_ALLOC_ARRAY,
  MIZU_PRIV_RUNNER_NS,         /* stage_r.c: the mizu namespace (runner path) */
  MIZU_PRIV_FN_MAP_RUNNER,
  MIZU_PRIV_GC_MINOR,          /* zc.c: the cached .Internal(gc(FALSE, FALSE,
                                  FALSE)) of the boundary collection */
  MIZU_PRIV_MAX
};

extern SEXP mizu_precious;
static inline SEXP mizu_priv(int slot) {
  return VECTOR_ELT(mizu_precious, slot);
}
static inline void mizu_priv_set(int slot, SEXP x) {
  SET_VECTOR_ELT(mizu_precious, slot, x);
}

// init / fini hooks -----------------------------------------------------------------

void mizu_cond_init(void);
void mizu_wrap_init(void);
void mizu_payload_init(void);
void mizu_entity_init(void);
void mizu_live_init(void);
void mizu_channel_init(void);
void mizu_pool_init(void);
void mizu_map_init(void);
void mizu_zc_init(void);
void mizu_interop_init(void);
void mizu_interop_fini(void);
void mizu_zc_fini(void);
void mizu_payload_fini(void);
void mizu_channel_fini(void);
void mizu_pool_fini(void);
void mizu_cond_fini(void);

/* map.c's srcref strip, shared with the codec's closure and task writes. */
extern SEXP mizu_srcref_sym;
SEXP mizu_strip_srcref(SEXP f);
SEXP mizu_strip_lang(SEXP x);

// The .Call entry points (registered in init.c) ---------------------------------

/* Declared here, not at the registration site: the defining TU then
   compiles against its own prototype, so a signature drift is a compile
   error rather than a DL_FUNC-cast mismatch. Sectioned by defining TU. */

/* bounded.c */
SEXP mizu_bounded_call(SEXP, SEXP);
SEXP mizu_unserialize_call(SEXP);
/* codec.c */
SEXP mizu_codec_write_call(SEXP);
SEXP mizu_codec_read_call(SEXP);
SEXP mizu_codec_cycle_call(void);
/* interop.c */
SEXP mizu_interop_write_call(SEXP);
SEXP mizu_interop_read_call(SEXP);
SEXP mizu_interop_write_err_call(SEXP, SEXP);
SEXP mizu_interop_write_task_call(SEXP, SEXP, SEXP, SEXP);
SEXP mizu_interop_map_desc_call(SEXP, SEXP, SEXP);
SEXP mizu_interop_runner_call(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_interop_read_task_call(SEXP);
SEXP mizu_stream_read_call(SEXP);
/* wrap.c */
SEXP mizu_region_create(SEXP);
SEXP mizu_region_open(SEXP, SEXP);
SEXP mizu_region_name(SEXP);
SEXP mizu_region_size(SEXP);
SEXP mizu_peek(SEXP, SEXP, SEXP);
SEXP mizu_poke(SEXP, SEXP, SEXP);
SEXP mizu_prune_call(void);
/* preamble.c */
SEXP mizu_preamble_write_call(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_preamble_validate_call(SEXP);
/* entity.c */
SEXP mizu_park_call(SEXP, SEXP, SEXP, SEXP);
SEXP mizu_unpark_call(SEXP, SEXP, SEXP);
SEXP mizu_epoch_call(SEXP, SEXP);
/* liveness.c */
SEXP mizu_live_open_call(SEXP);
SEXP mizu_live_try_call(SEXP);
SEXP mizu_live_close_call(SEXP);
SEXP mizu_live_dir_call(void);
SEXP mizu_death_watch_call(SEXP, SEXP, SEXP, SEXP);
SEXP mizu_death_fired_call(SEXP);
SEXP mizu_death_stop_call(SEXP);
/* verbs_channel.c */
SEXP mizu_channel_create_call(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_channel_suffix(SEXP);
SEXP mizu_channel_ready_wait_call(SEXP, SEXP);
SEXP mizu_channel_destroy_call(SEXP);
SEXP mizu_channel_attach_call(SEXP, SEXP);
SEXP mizu_channel_ready_set_call(SEXP);
SEXP mizu_channel_send_call(SEXP, SEXP);
SEXP mizu_channel_send_error_call(SEXP, SEXP);
SEXP mizu_channel_send_batch_call(SEXP, SEXP);
SEXP mizu_channel_recv_call(SEXP, SEXP);
SEXP mizu_channel_recv_batch_call(SEXP, SEXP, SEXP);
SEXP mizu_channel_close_call(SEXP, SEXP);
SEXP mizu_channel_close_signal_call(SEXP);
SEXP mizu_channel_alive_call(SEXP);
SEXP mizu_channel_keep_out_call(SEXP);
SEXP mizu_channel_stat(SEXP);
SEXP mizu_sentinel_check(SEXP);
SEXP mizu_now_call(void);
/* verbs_pool.c */
SEXP mizu_pool_create_call(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_pool_suffix(SEXP);
SEXP mizu_pool_ready_wait_call(SEXP, SEXP, SEXP);
SEXP mizu_pool_destroy_call(SEXP);
SEXP mizu_pool_worker_join_call(SEXP, SEXP, SEXP);
SEXP mizu_pool_leave_call(SEXP);
SEXP mizu_pool_lame_duck_call(SEXP);
SEXP mizu_pool_retire_call(SEXP, SEXP);
SEXP mizu_pool_attach_call(SEXP);
SEXP mizu_pool_ident_call(SEXP);
SEXP mizu_pool_submit_spec(SEXP, SEXP, SEXP, SEXP);
SEXP mizu_pool_submit_call(SEXP, SEXP, SEXP, SEXP);
SEXP mizu_pool_submit_batch_call(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_pool_submit_expr(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_pool_submit_try(SEXP, SEXP, SEXP, SEXP);
SEXP mizu_pool_submit_map_runner(SEXP, SEXP, SEXP);
SEXP mizu_pool_step_call(SEXP, SEXP);
SEXP mizu_pool_run(SEXP, SEXP);
SEXP mizu_pool_run_outcome(SEXP, SEXP);
SEXP mizu_pool_set_eval(SEXP);
SEXP mizu_current_pool_call(void);
SEXP mizu_pool_set_trace_call(SEXP, SEXP);
SEXP mizu_pool_deque_pull_call(SEXP, SEXP);
SEXP mizu_pool_exec_fail_call(SEXP, SEXP);
SEXP mizu_pool_dump_call(SEXP);
SEXP mizu_pool_collect_call(SEXP, SEXP);
SEXP mizu_pool_collect_try(SEXP, SEXP);
SEXP mizu_pool_collect_any_call(SEXP, SEXP);
SEXP mizu_pool_collect_all_call(SEXP, SEXP);
SEXP mizu_pool_cancel_call(SEXP);
SEXP mizu_pool_task_state_call(SEXP);
SEXP mizu_pool_stop_call(SEXP, SEXP);
SEXP mizu_pool_status_call(SEXP);
SEXP mizu_pool_stats_call(SEXP);
SEXP mizu_pool_map_caps_call(SEXP);
SEXP mizu_pool_map_cache(SEXP);
SEXP mizu_pool_signals_call(SEXP);
SEXP mizu_pool_help_once_call(SEXP);
SEXP mizu_pool_zc_info(SEXP);
/* map.c */
SEXP mizu_map_eligible(SEXP);
SEXP mizu_map_stage(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_map_open(SEXP, SEXP);
SEXP mizu_map_desc(SEXP);
SEXP mizu_map_is_template(SEXP);
SEXP mizu_map_slice(SEXP, SEXP, SEXP);
SEXP mizu_map_xslice(SEXP, SEXP, SEXP);
SEXP mizu_map_write(SEXP, SEXP, SEXP);
SEXP mizu_map_gather(SEXP);
SEXP mizu_map_gather_view(SEXP, SEXP, SEXP);
SEXP mizu_map_dim_call(SEXP, SEXP);
SEXP mizu_map_splice(SEXP, SEXP, SEXP);
SEXP mizu_map_vsplice(SEXP, SEXP, SEXP);
SEXP mizu_map_splice_ix(SEXP, SEXP, SEXP);
SEXP mizu_map_asplice(SEXP, SEXP, SEXP);
SEXP mizu_map_lost(SEXP, SEXP);
SEXP mizu_map_next(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_map_batch(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP,
                    SEXP);
SEXP mizu_map_abandon(SEXP, SEXP);
SEXP mizu_map_cancel_set(SEXP);
SEXP mizu_map_close(SEXP);
SEXP mizu_map_cancel_get(SEXP);
SEXP mizu_map_reset(SEXP);
SEXP mizu_map_swap_x(SEXP, SEXP);
SEXP mizu_map_info(SEXP);
SEXP mizu_map_claim_state(SEXP, SEXP);
SEXP mizu_map_timeout_call(void);
/* rng.c */
SEXP mizu_map_rng_base(SEXP);
SEXP mizu_map_rng_seek(SEXP, SEXP);
SEXP mizu_map_rng_install(SEXP);
/* zc.c */
SEXP mizu_zc_view_name_call(SEXP);

#endif /* MIZU_H */
