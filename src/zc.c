/* Zero-copy payload tiers (zc.c): SHM_VEC — a view-layout object (atomic
   vector, string vector, or list tree) in a spill region, wrapped ALTREP
   at receive (no allocVector, no memcpy, no parse) — and REF — the /mizu_
   identifier of an object already in shared memory, resolved to a view of
   the same pages. The receive machinery is the view layer (view.c);
   what zc.c adds is the cross-process release protocol: a
   refcount in the region header's reserved bytes (mizu.h), a per-handle
   lent-region ledger on the producer, and a once-only release callback
   per view (the layer's embedder release hook — fired at COW materialization or at the
   view finalizer, whichever comes first; list views fire at the finalizer
   only, since extracted element views keep referencing the region).

   Ordering invariant: the consumer's refcount add happens at wrap, before
   its consumer-done signal (head publish / result-slot release), which
   happens-before the producer's loan drop at the keeper release points —
   so refcount 0 means no live views and reuse can't disturb a consumer.
   An add any later (e.g. lazily at first access) would race the sweep.

   Views nested inside a larger payload (a pool task's argument list, a
   list crossing a channel) never reach the top-level tier selection: R's
   serializer emits their identifiers via the vendored Serialized_state
   hooks and the far side resolves them inside R_Unserialize. Mori's wire
   hooks (mizu_view_set_wire_hooks, set at load) keep that path inside the
   protocol: emit marks the region REFHELD (the holder set widens beyond
   the direct peer), resolve does the counted add and arms the release
   callback before the read returns — which happens-before the
   consumer-done signal, the sender's keeper pinning the payload (and
   with it the view) until then. */

#include <stdlib.h>
#include "mizu.h"

/* ANY_ATTRIB() joined the C API in R 4.5.0; equivalent fallback for
   earlier R, where ATTRIB() was still the sanctioned spelling. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

static SEXP mizu_rel_tag;      /* the release-record extptr */
static SEXP mizu_shm_tag_sym;  /* installed MIZU_VIEW_TAG_SHM: the chain terminus */

/* The by-reference traffic record (F1's retain protocol). Two small
   process-global lists, preserved containers refilled per pass — safe
   because R staging is single-threaded per process (pool workers are
   separate processes):

   - the mark list: every view a by-reference emission marked REFHELD
     since the last reset (the REF tier's claim, the serialize fallback's
     wire-hook emissions, the 'I' ref gate's). Non-empty is the stage's
     pin-and-no-keeperless signal (conservative — pin when unsure — is
     the correct failure direction), and the worker's emitted-set for the
     argument-loan release pass: the release pass keeps a loan whose view
     the outcome write emitted, from whatever graph position, by whichever
     writer, at whatever depth — coverage is definitional because every
     emission funnels through the one mark.
   - the resolve list: every view the wire resolve hook armed while
     recording (armed at a task-stream decode, sealed at its end). The
     worker's release pass force-fires the once-only release record of
     each decoded argument view absent from the mark list — R's GC would
     otherwise linger the counted adds across tasks, accumulating lent
     regions. Membership is pointer identity, not region identity: two
     view objects over one region each hold a count, so force-firing the
     argument's object is safe even when the write emitted a different
     object over the same region.

   Reset clears the count only; stale slots beyond it stay valid (the
   container is preserved) and once-only release records make a stale
   entry's re-fire a no-op, so a nested task's reset degrades the outer
   task's pass to the GC-gated behavior, never to a wrong fire. */
static SEXP mizu_zc_marks;
static int mizu_zc_nmarks;
static SEXP mizu_zc_resolved;
static int mizu_zc_nresolved;
static int mizu_zc_resolved_armed;

static void zc_list_clear(SEXP list, int *pn) {
  const int n = *pn;
  for (int i = 0; i < n; i++)
    SET_VECTOR_ELT(list, i, R_NilValue);
  *pn = 0;
}

void mizu_zc_ref_reset(void) { zc_list_clear(mizu_zc_marks, &mizu_zc_nmarks); }
int mizu_zc_ref_fired(void) { return mizu_zc_nmarks > 0; }

void mizu_zc_resolved_reset(void) {
  zc_list_clear(mizu_zc_resolved, &mizu_zc_nresolved);
  mizu_zc_resolved_armed = 1;
}

int mizu_zc_resolved_count(void) { return mizu_zc_nresolved; }

/* Append to a preserved list, doubling on growth. The pushed view is
   always anchored elsewhere already (the decode product, the staged
   object), so the list is a record, never the last reference. */
static SEXP zc_list_push(SEXP list, int *pn, SEXP x) {
  const int n = *pn;
  if (n >= XLENGTH(list)) {
    SEXP bigger = PROTECT(Rf_allocVector(VECSXP, n == 0 ? 8 : 2 * n));
    for (int i = 0; i < n; i++)
      SET_VECTOR_ELT(bigger, i, VECTOR_ELT(list, i));
    R_PreserveObject(bigger);
    R_ReleaseObject(list);
    UNPROTECT(1);
    list = bigger;
  }
  SET_VECTOR_ELT(list, n, x);
  *pn = n + 1;
  return list;
}

static int zc_list_has(SEXP list, int n, SEXP x) {
  for (int i = 0; i < n; i++)
    if (VECTOR_ELT(list, i) == x) return 1;
  return 0;
}

void mizu_zc_ref_mark(SEXP x);
static void mizu_zc_wire_resolve(SEXP view, mizu_shm *shm);

/* The outcome-write pass (D5): force-fire the once-only release record
   of each decoded argument view the write did not emit, [0, seal) of the
   resolve list (the decode-end snapshot — eval-time resolves, a nested
   collect's, are not argument loans). ALTLIST views keep their
   finalizer-only discipline: extracted element views ride the list's
   single loan, so an early fire could drop the count under a live
   element — they stay GC-gated. Ends disarmed with both lists cleared:
   the marks' last consumer is this pass (the stage's pin decision and
   the emitted-set diff are both done), and the records hold views — a
   count-only reset would keep them alive indefinitely. */
void mizu_zc_args_release(int seal) {
  const int n = seal < mizu_zc_nresolved ? seal : mizu_zc_nresolved;
  for (int i = 0; i < n; i++) {
    SEXP v = VECTOR_ELT(mizu_zc_resolved, i);
    if (TYPEOF(v) == VECSXP) continue;
    if (!zc_list_has(mizu_zc_marks, mizu_zc_nmarks, v))
      mizu_view_release_now(v);
  }
  zc_list_clear(mizu_zc_resolved, &mizu_zc_nresolved);
  zc_list_clear(mizu_zc_marks, &mizu_zc_nmarks);
  mizu_zc_resolved_armed = 0;
}

void mizu_zc_init(void) {
  mizu_rel_tag = Rf_install("mizu_view_release");
  mizu_shm_tag_sym = Rf_install(MIZU_VIEW_TAG_SHM);
  mizu_zc_marks = Rf_allocVector(VECSXP, 0);
  R_PreserveObject(mizu_zc_marks);
  mizu_zc_resolved = Rf_allocVector(VECSXP, 0);
  R_PreserveObject(mizu_zc_resolved);
  mizu_view_set_wire_hooks(mizu_zc_ref_mark, mizu_zc_wire_resolve);
  /* the vendored resolve cache opens through mizu's split open, so every
     consumer mapping — cached or prep-path — has a writable page 0 */
  mizu_view_set_open_hook(mizu_zc_open);
}

// View release ------------------------------------------------------------------

/* One per wrapped view: the refcount sub, armed only after the add lands
   (a longjmp between wrap and add must not sub a count it never added).
   pid is the fork guard (the view layer's host-finalizer pattern): views are
   ordinary R objects that cross mclapply forks, and a child-side GC must
   not sub a count it never added. shm is the view's mapping handle —
   the prep path's own, or the vendored resolve cache's shared one (pinned
   by the view's keeper chain there, never by the record). */
typedef struct mizu_zc_rel_s {
  mizu_shm *shm;
  long pid;
  int armed;
} mizu_zc_rel;

static void mizu_rel_finalizer(SEXP ptr) {
  mizu_zc_rel *rel = (mizu_zc_rel *) R_ExternalPtrAddr(ptr);
  if (rel != NULL) {
    if (rel->armed && rel->pid == mizu_self_pid())
      mizu_zc_unref(rel->shm);
    free(rel);
    R_ClearExternalPtr(ptr);
  }
}

/* The mizu_view_owned release hook: run the finalizer early (it clears the
   extptr, so the GC pass is a no-op). Fires at COW materialization — the
   shared pages are dead weight from there. */
static void mizu_zc_rel_fire(void *arg) {
  mizu_rel_finalizer((SEXP) arg);
}

// Consumer open: split mapping ---------------------------------------------------

/* The core's flags-form view open: page 0 read-write (the refcount word
   needs it), remaining pages read-only, lazy everywhere — a view is touched
   on demand, so eager PTE install would prefault never-read pages on the
   recv hot path (the SHM_RAW cache's populated open exists because a
   stream is unserialized in full immediately; a view is not). NOCOUNT:
   an ALTREP wrap can longjmp between map and count, so the counted add
   lands at the wrap (before the consumer-done signal), not here.
   Two call paths: registered as the view layer's embedder open hook
   (mizu_zc_init) — the wire-resolve cache's misses — and called directly by
   the prep path below (the per-handle cache). */
mizu_shm *mizu_zc_open(const char *name) {
  mizu_shm *shm;
  if (mizu_shm_open_view_flags(&shm, name, MIZU_OPEN_VIEW_NOCOUNT) != MIZU_OK)
    return NULL;
  return shm;
}

// Eligibility --------------------------------------------------------------------

/* Lower bound on the MIZS layout size (header + the string block's fixed
   sections + packed string bytes; attrs excluded): 64 + the geometry's
   data offset + the CHARSXP bytes. Walks string lengths only — no
   allocation, no serialize count. The budget caps the walk: exact while
   the running sum stays within it, a partial sum past it — the only
   consumers are gate comparisons, and the fixed sections alone clear a
   zero budget, so no caller needs the walk it skips. */
static size_t mizu_zc_str_probe_budget(SEXP x, size_t budget) {
  R_xlen_t n = XLENGTH(x);
  const SEXP *base = mizu_view_str_base(x);
  size_t total = MIZU_HEADER_SIZE + mizu_view_str_geometry((size_t) n).data;
  for (R_xlen_t i = 0; i < n && total <= budget; i++) {
    SEXP s = mizu_view_str_elt(x, i, base);
    if (s != NA_STRING) total += (size_t) LENGTH(s);
  }
  return total;
}

static size_t mizu_zc_str_probe(SEXP x) {
  return mizu_zc_str_probe_budget(x, SIZE_MAX);
}

/* The string-leaf body sums an eligibility walk has already measured,
   handed to mizu_view_layout_size_sums so the size pass's string walks
   retire (the foreign walks are verdict-bound — they visit every string
   element regardless — so recording is free there; the same-language
   probe keeps its budget cap and records nothing). exact goes 0 on any
   leaf the size pass would walk but the walk didn't measure — a lazy
   ALTREP container or string leaf the probe skips, or the buffer
   filling — and the caller falls back to the plain size pass. */
#define MIZU_ZC_SUMS_MAX 128
typedef struct {
  size_t v[MIZU_ZC_SUMS_MAX];
  size_t n;
  int exact;
} mizu_zc_sums;

static void mizu_zc_sums_record(mizu_zc_sums *sums, size_t body) {
  if (sums->n == MIZU_ZC_SUMS_MAX) {
    sums->exact = 0;
    return;
  }
  sums->v[sums->n++] = body;
}

/* Lower bound on the MIZL layout size: layout-eligible leaf bytes only
   (headers, directory, attrs, and serialized leaves all excluded), so the
   exact mizu_view_layout_size always exceeds it. A mizu view anywhere in the
   tree rejects it outright: nested views must cross by reference on the
   serialize-hook path (the wire hooks keep them counted) — the layout
   writer would copy their bytes, and a copied view can never be REF'd
   back. Foreign ALTREP leaves failing mizu_view_altrep_readable cost 0 here;
   the oracle rejects the tree later. Serialized leaves cost 0 too,
   so a tree whose bulk is
   non-eligible (a big environment, a call) stays on the serialize tiers —
   MIZL's win is the eligible leaves.
   The return feeds only a `<= gate` comparison, so the budget (seeded
   with that gate) caps it: exact while the tree's sum stays within
   budget, a partial sum once it clears — both sides of the boundary
   exact. The cap shortens string-leaf walks only; every node is still
   visited, so a non-REF-able view past the clearing point rejects as the
   uncapped walk did. */
static size_t mizu_zc_tree_probe(SEXP x, size_t budget, int *reject) {
  if (*reject) return 0;
  if (ALTREP(x)) {
    if (mizu_view_check(x)) {
      /* a REF-able view element crosses as a remote leaf (F2.5): the
         identifier span. A materialized, locally attributed or broken-
         identifier view keeps rejecting — the serialize path's wire
         hooks carry those, by reference or by value as before. */
      if (mizu_view_refable(x)) {
        SEXP id = PROTECT(mizu_view_shm_name(x));
        if (id != R_NilValue) {
          const size_t span = (size_t) LENGTH(STRING_ELT(id, 0));
          UNPROTECT(1);
          return span;
        }
        UNPROTECT(1);
      }
      *reject = 1;
      return 0;
    }
    if (!mizu_view_altrep_readable(x)) return 0;  /* lazy: the oracle vetoes */
  }
  int type = TYPEOF(x);
  size_t elt = mizu_view_sizeof_elt(type);
  if (elt != 0) return (size_t) XLENGTH(x) * elt;
  if (type == STRSXP) return mizu_zc_str_probe_budget(x, budget);
  if (type == VECSXP) {
    R_xlen_t n = XLENGTH(x);
    size_t total = 0;
    for (R_xlen_t i = 0; i < n && !*reject; i++)
      total += mizu_zc_tree_probe(VECTOR_ELT(x, i),
                                  total < budget ? budget - total : 0,
                                  reject);
    return total;
  }
  if (type == LISTSXP) {
    size_t total = 0;
    for (SEXP s = x; s != R_NilValue && !*reject; s = CDR(s))
      total += mizu_zc_tree_probe(CAR(s),
                                  total < budget ? budget - total : 0,
                                  reject);
    return total;
  }
  return 0;
}

/* SHM_VEC eligibility: an object the view layouts cover whose layout
   bytes exceed both the inline budget and MIZU_ZC_FLOOR. Atomic vectors
   gate on the O(1) data size (a lazy ALTREP input must never stage:
   staging grabs DATAPTR and materializes it — 1:1e8 would become an
   800 MB memcpy against the ~100-byte stream its serialized-state hook
   emits. The mizu_view_altrep_readable probe admits directly readable,
   keeper-free data without materializing: R's S4 data-part wrappers
   forward to their data part).
   Strings and list trees have no O(1) size, and mizu_view_layout_size's walk
   serialize-counts non-eligible leaves — a per-send tax the pool's small
   task payloads must not pay — so a cheap lower-bound probe gates the
   exact walk, which then doubles as region size and oracle (0 rejects
   foreign ALTREP nodes). Top-level LISTSXP stays on the serialize
   tiers: the layout coerces it to VECSXP, a type change a transport must
   not make. */
int mizu_zc_eligible(SEXP x, uint32_t inline_max, size_t *out_total,
                     int foreign) {
  int type = TYPEOF(x);
  size_t elt = mizu_view_sizeof_elt(type);
  if (elt != 0) {
    if (!foreign && ALTREP(x) && !mizu_view_altrep_readable(x)) return 0;
    size_t data = (size_t) XLENGTH(x) * elt;
    if (data <= (size_t) inline_max || data < MIZU_ZC_FLOOR) return 0;
    size_t total = mizu_view_layout_size(x, foreign);
    if (total == 0 || total <= (size_t) inline_max) return 0;
    *out_total = total;
    return 1;
  }
  size_t gate = (size_t) inline_max > MIZU_ZC_FLOOR ?
    (size_t) inline_max : MIZU_ZC_FLOOR;
  if (type == STRSXP) {
    /* a foreign ALTREP string's Elt may materialize it — the probe must
       not touch it (a mizu view is safe: its accessors read the shared
       pages or the materialized copy). The mizu_view_altrep_readable probe
       admits directly readable, keeper-free data (R's S4 data-part
       wrappers) without touching elements. */
    if (ALTREP(x) && !mizu_view_check(x) && !mizu_view_altrep_readable(x))
      return 0;
    const size_t probe = mizu_zc_str_probe(x);
    if (probe <= gate) return 0;
    if (!ANY_ATTRIB(x)) {
      /* the probe's sum is the exact layout size here: header + geometry +
         packed bytes is mizs_size with zero attrs, and the writer's
         return — the size walk adds nothing */
      *out_total = probe;
      return 1;
    }
  } else if (type == VECSXP) {
    int reject = 0;
    if (mizu_zc_tree_probe(x, gate, &reject) <= gate || reject) return 0;
  } else {
    return 0;
  }
  size_t total = mizu_view_layout_size(x, foreign);
  if (total == 0) return 0;
  *out_total = total;
  return 1;
}

// The foreign zero-copy gate ---------------------------------------------------------

/* The generic-tree half of the filter, fused with the floor-gate probe:
   every leaf an attribute-free atomic, a class-only integer64, a string
   vector (MIZS-gated), a plain factor (ATTRS-gated), or such a tree;
   names under the dict-key rules (ATTRS-gated); a serialized leaf or a
   nested view rejects the tree (the interop writer takes it as a copy,
   or declines it at send). Verdict before sum per leaf — a caps
   rejection never pays a size walk — and the string verdict's walk
   doubles as the probe's byte sum, so the tree is walked once where the
   separate caps walk and probe walked it twice. Returns the probe sum
   (unbudgeted: the verdict walks every string element regardless);
   *reject carries the verdict, mizu_zc_tree_probe's view policy
   preserved: a view passed by a whitelist shape rejects as the probe
   did, and a lazy ALTREP costs 0 (the write copies it through
   *_GET_REGION). The string leaves' body sums are recorded in pre-order
   for the presized size pass — the verdict walk measured every one, a
   lazy ALTREP string leaf's true bytes included. */
static size_t mizu_zc_tree_walk_fx(SEXP x, uint32_t caps, int *reject,
                                   mizu_zc_sums *sums) {
  if (*reject) return 0;
  switch (TYPEOF(x)) {
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
    if (!ANY_ATTRIB(x)) {
      if (mizu_view_check(x)) { *reject = 1; return 0; }
    } else if (!mizu_view_is_int64(x)) {
      if (!(caps & MIZU_CAP_ATTRS)) { *reject = 1; return 0; }
      /* the whitelisted leaf shapes a region-backed reader homes:
         factor, dim, Date, POSIXct, difftime (the 'I' leaf blob
         carries them) */
      const int q = mizu_interop_attrs_qualify(x);
      if (q != MIZU_IXQ_FACTOR && q != MIZU_IXQ_DIM &&
          q != MIZU_IXQ_DATE && q != MIZU_IXQ_POSIXCT &&
          q != MIZU_IXQ_DIFFTIME) { *reject = 1; return 0; }
    }
    if (ALTREP(x)) {
      if (mizu_view_check(x)) { *reject = 1; return 0; }
      if (!mizu_view_altrep_readable(x)) return 0;
    }
    return (size_t) XLENGTH(x) * mizu_view_sizeof_elt(TYPEOF(x));
  case STRSXP: {
    /* a view leaf keeps rejecting on generic trees (remote leaves are the
       frame gate's, F2.5) */
    if (ANY_ATTRIB(x) || mizu_view_check(x) || !(caps & MIZU_CAP_MIZS)) {
      *reject = 1;
      return 0;
    }
    size_t bytes = 0;
    if (!mizu_interop_strings_utf8(x, &bytes)) { *reject = 1; return 0; }
    const size_t body = mizu_view_str_geometry((size_t) XLENGTH(x)).data +
      bytes;
    mizu_zc_sums_record(sums, body);
    if (ALTREP(x) && !mizu_view_altrep_readable(x)) return 0;
    return MIZU_HEADER_SIZE + body;
  }
  case VECSXP: {
    if (Rf_isS4(x) || mizu_view_check(x)) { *reject = 1; return 0; }
    SEXP names = PROTECT(Rf_getAttrib(x, R_NamesSymbol));
    if (names != R_NilValue &&
        (!(caps & MIZU_CAP_ATTRS) || TYPEOF(names) != STRSXP ||
         !mizu_interop_names_ok(names))) {
      UNPROTECT(1);
      *reject = 1;
      return 0;
    }
    /* a lazy ALTREP node costs the probe 0, but the caps walk still
       verdicts its descendants (a latin1 string under one rejects the
       tree); the size pass walks them too, so the recording stands */
    if (ALTREP(x) && !mizu_view_altrep_readable(x)) {
      for (R_xlen_t i = 0; !*reject && i < XLENGTH(x); i++)
        mizu_zc_tree_walk_fx(VECTOR_ELT(x, i), caps, reject, sums);
      UNPROTECT(1);
      return 0;
    }
    size_t total = 0;
    for (R_xlen_t i = 0; !*reject && i < XLENGTH(x); i++)
      total += mizu_zc_tree_walk_fx(VECTOR_ELT(x, i), caps, reject, sums);
    UNPROTECT(1);
    return total;
  }
  default:
    *reject = 1;
    return 0;
  }
}


/* The foreign-handle SHM_VEC filter: a reader is never sent a layout it
   cannot wrap (its identity word's capability mask is the sender's
   contract), so a value past the floor outside the peer's set takes the
   interop writer as a copy instead of failing one hop late at receive.
   The baseline needs no bit: an attribute-free MIZH atomic — ALTREP
   admitted here, unlike the same-language gate, the layout write copying
   through *_GET_REGION — or a class-only integer64. */
int mizu_zc_eligible_foreign(SEXP x, uint32_t inline_max, size_t *out_total,
                             uint32_t caps) {
  const int type = TYPEOF(x);
  const size_t elt = mizu_view_sizeof_elt(type);
  if (elt != 0) {
    if (Rf_isS4(x)) return 0;
    if (!ANY_ATTRIB(x) || mizu_view_is_int64_any(x)) {
      /* the baseline MIZH, any representation — the layout write's
         foreign mode adds the validity reserve, so size it there */
      size_t data = (size_t) XLENGTH(x) * elt;
      if (data <= (size_t) inline_max || data < MIZU_ZC_FLOOR) return 0;
      size_t total = mizu_view_layout_size(x, 1);
      if (total <= (size_t) inline_max) return 0;
      *out_total = total;
      return 1;
    }
    /* an attributed MIZH root: the whitelisted shapes, ATTRS-gated */
    if (!(caps & MIZU_CAP_ATTRS)) return 0;
    const int q = mizu_interop_attrs_qualify(x);
    if (q == MIZU_IXQ_NONE || q == MIZU_IXQ_ENCODABLE) return 0;
    return mizu_zc_eligible(x, inline_max, out_total, 1);
  }
  if (type == STRSXP) {
    /* MIZS, gated on the bit and every element crossing as UTF-8 (a
       latin1 vector takes the 'I' copy, which translates it; a
       bytes-marked one declines there). The fence runs ahead of any
       walk; the gate walk then doubles as the probe's byte sum, and for
       an attribute-free vector that sum is the exact layout size. */
    if (!(caps & MIZU_CAP_MIZS)) return 0;
    if (ALTREP(x) && !mizu_view_check(x) && !mizu_view_altrep_readable(x))
      return 0;
    size_t bytes = 0;
    if (!mizu_interop_strings_utf8(x, &bytes)) return 0;
    const size_t gate = (size_t) inline_max > MIZU_ZC_FLOOR ?
      (size_t) inline_max : MIZU_ZC_FLOOR;
    const size_t total = MIZU_HEADER_SIZE +
      mizu_view_str_geometry((size_t) XLENGTH(x)).data + bytes;
    if (total <= gate) return 0;
    if (!ANY_ATTRIB(x)) {
      *out_total = total;
      return 1;
    }
    const size_t exact = mizu_view_layout_size(x, 1);
    if (exact == 0) return 0;
    *out_total = exact;
    return 1;
  }
  if (type != VECSXP) return 0;
  /* a plain data.frame: ATTRS + MIZL, plus MIZS when any column is a
     string; a generic tree: MIZL, plus MIZS for a string leaf and ATTRS
     for the names blob or a factor leaf — a peer short of any one bit
     gets the whole value as an attr copy */
  const int q = mizu_interop_attrs_qualify(x);
  if (q == MIZU_IXQ_FRAME) {
    uint32_t need = MIZU_CAP_ATTRS | MIZU_CAP_MIZL;
    int has_ref = 0;
    for (R_xlen_t i = 0; i < XLENGTH(x); i++) {
      SEXP col = VECTOR_ELT(x, i);
      if (TYPEOF(col) == STRSXP) need |= MIZU_CAP_MIZS;
      if (!has_ref && mizu_view_refable(col)) has_ref = 1;
    }
    /* a view column crosses as a remote leaf (F2.5): the conditional
       conjunction — a peer short of the bit keeps the rejection (the
       whole frame takes the 'I' copy) */
    if (has_ref) need |= MIZU_CAP_MIZL_REF;
    if ((caps & need) != need) return 0;
    /* the root probe the tail call ran, inlined: a frame that is itself
       a view (a REF-able one's span never clears the gate) or a lazy
       ALTREP declines there */
    if (ALTREP(x) &&
        (mizu_view_check(x) || !mizu_view_altrep_readable(x)))
      return 0;
    /* one pass over the columns, the string-column gate fused with the
       floor-gate probe: a string column gates like a top-level vector
       (a latin1 column takes the 'I' copy, which translates it; a
       bytes-marked one declines there — ungated it would reach the
       reader one hop late), its verdict walk doubling as the probe's
       byte sum. A REF-able view column crosses by reference and is
       never walked; a non-REF-able view rejects through the probe, as
       does one nested in a list column. Verdict before sum per column,
       and every column visited: a late verdict decides as the early
       one did. */
    const size_t gate = (size_t) inline_max > MIZU_ZC_FLOOR ?
      (size_t) inline_max : MIZU_ZC_FLOOR;
    size_t total = 0;
    int reject = 0;
    mizu_zc_sums sums = { .n = 0, .exact = 1 };
    for (R_xlen_t i = 0; i < XLENGTH(x) && !reject; i++) {
      SEXP col = VECTOR_ELT(x, i);
      if (TYPEOF(col) == STRSXP && !mizu_view_refable(col)) {
        if (mizu_view_check(col)) { reject = 1; break; }
        size_t bytes = 0;
        if (!mizu_interop_strings_utf8(col, &bytes)) return 0;
        const size_t body =
          mizu_view_str_geometry((size_t) XLENGTH(col)).data + bytes;
        mizu_zc_sums_record(&sums, body);
        if (!(ALTREP(col) && !mizu_view_altrep_readable(col)))
          total += MIZU_HEADER_SIZE + body;
      } else {
        /* ix_qualify_frame admits atomic and string columns only: the
           probe here never meets a string leaf, so nothing records */
        total += mizu_zc_tree_probe(col, total < gate ? gate - total : 0,
                                    &reject);
      }
    }
    if (reject || total <= gate) return 0;
    total = 0;
    if (sums.exact)
      total = mizu_view_layout_size_sums(x, 1, sums.v, sums.n);
    if (total == 0)
      total = mizu_view_layout_size(x, 1);
    if (total == 0) return 0;
    *out_total = total;
    return 1;
  }
  if (!(caps & MIZU_CAP_MIZL)) return 0;
  const size_t gate = (size_t) inline_max > MIZU_ZC_FLOOR ?
    (size_t) inline_max : MIZU_ZC_FLOOR;
  int reject = 0;
  mizu_zc_sums sums = { .n = 0, .exact = 1 };
  if (mizu_zc_tree_walk_fx(x, caps, &reject, &sums) <= gate || reject)
    return 0;
  size_t total = 0;
  if (sums.exact)
    total = mizu_view_layout_size_sums(x, 1, sums.v, sums.n);
  if (total == 0)
    total = mizu_view_layout_size(x, 1);
  if (total == 0) return 0;
  *out_total = total;
  return 1;
}

/* The REF half of the filter: a view re-sent top-level crosses by
   reference only when the peer wraps its layout (the claim marks the
   region REFHELD, so the filter runs ahead of mizu_zc_ref_stage). */
int mizu_zc_ref_foreign_ok(SEXP x, uint32_t caps) {
  if (!mizu_view_check(x)) return 1;   /* not a view: REF never claims it */
  switch (TYPEOF(x)) {
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
    if (!ANY_ATTRIB(x) || mizu_view_is_int64_any(x)) return 1;
    return (caps & MIZU_CAP_ATTRS) != 0;
  case STRSXP: {
    const uint32_t need = ANY_ATTRIB(x) ?
      MIZU_CAP_MIZS | MIZU_CAP_ATTRS : MIZU_CAP_MIZS;
    return (caps & need) == need;
  }
  default: {
    const uint32_t need = ANY_ATTRIB(x) ?
      MIZU_CAP_MIZL | MIZU_CAP_ATTRS : MIZU_CAP_MIZL;
    return (caps & need) == need;
  }
  }
}

// Stage ----------------------------------------------------------------------------

/* Stage x as SHM_VEC: one layout write into a spill region (free-list pop
   or fresh create — the irreducible producer-writes-once floor), refcount
   store 1 (the producer's loan — both the field's only initialization and
   its own reference; a recycled region carries a stale count), and the
   name as the payload. Fills the retain entry: the region, the pin of x,
   and the consumer key cell (-1) the release point may re-stamp. ctx is
   the stage hook's binding ctx. foreign is the peer-language signal: the
   layout write builds the validity-bitmap section (mizu.h's [40-55]
   header words) and aux carries the write's actual total — the size
   pass's reservation counts clean leaves' unspent bitmap bytes. */
void mizu_zc_stage(mizu_slot_hdr *hdr, unsigned char *payload, SEXP x,
                  size_t total, mizu_handle *h, void *ctx, int foreign) {
  mizu_shm *shm = mizu_spill_get_raise(h, (size_t) total);
  size_t used =
    mizu_view_layout_write((unsigned char *) shm->addr, x, foreign);
  /* the aux code must match the layout's root sexptype: class-only
     integer64 stamped its MIZH root MIZU_VIEW_TYPE_INT64 */
  int type = mizu_view_is_int64_any(x) ? MIZU_TYPE_INT64 : (int) TYPEOF(x);
  hdr->kind = MIZU_KIND_SHM_VEC;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = mizu_aux_shm_vec(type == LISTSXP ? VECSXP : type,
                             (uint64_t) used);
  memcpy(payload, shm->name, shm->name_len);
  mizu_r_pin(h, ctx, x);
  /* the producer-loan refcount store (rc = 1, flags = 0) rides the retain */
  mizu_stage_retain_zc(h, shm);
}

// Receive ---------------------------------------------------------------------------

// The view cache (name -> split mapping, wrap-pinned) -------------------------

/* The name-keyed lookup: the wrap SEXP on a hit (stamp bumped), R_NilValue
   on a miss or a finalized entry (the caller re-opens and re-stores).
   Eviction only ever drops the reference — a live view keeps its mapping
   through its own chain. */
SEXP mizu_zc_lookup_wrap(mizu_zc_cache *oc, const unsigned char *name,
                        uint32_t len) {
  for (int i = 0; i < MIZU_OPEN_CACHE_MAX; i++)
    if (oc->name_len[i] == len &&
        memcmp(oc->names[i], name, len) == 0) {
      SEXP wrap = VECTOR_ELT(oc->wraps, i);
      if (mizu_shm_unwrap(wrap) == NULL) return R_NilValue;  /* finalized */
      oc->stamp[i] = ++oc->tick;
      oc->hits++;
      return wrap;
    }
  return R_NilValue;
}

void mizu_zc_cache_store(mizu_zc_cache *oc, const unsigned char *name,
                        uint32_t len, SEXP wrap) {
  int slot = 0;
  for (int i = 0; i < MIZU_OPEN_CACHE_MAX; i++) {
    if (oc->name_len[i] == 0) {
      slot = i;
      break;
    }
    if (oc->stamp[i] < oc->stamp[slot]) slot = i;
  }
  SET_VECTOR_ELT(oc->wraps, slot, wrap);   /* evicted LRU drops to GC */
  memcpy(oc->names[slot], name, len);
  oc->name_len[slot] = (uint8_t) len;
  oc->stamp[slot] = ++oc->tick;
  oc->misses++;
}

/* Open the named region's split mapping (cache first) and build the view
   chain anchor: rel_xp (the release record, unarmed) pinned through
   name_xp (the vendored chain-terminus tag) pinned on the mapping wrap.
   Returns name_xp UNPROTECTED — the caller PROTECTs before any allocation
   (nothing allocates between); rel_xp and the mapping wrap ride its prot
   chain. R_NilValue with *gone set when the region vanished and the
   caller absorbs that (NULL gone raises instead). */
static SEXP mizu_zc_prep(const char *name, uint32_t name_len, int *gone,
                         mizu_zc_cache *oc, mizu_shm **shm_out) {
  SEXP map_wrap = oc != NULL ?
    mizu_zc_lookup_wrap(oc, (const unsigned char *) name, name_len) :
    R_NilValue;
  mizu_shm *shm = map_wrap == R_NilValue ? NULL : mizu_shm_unwrap(map_wrap);
  if (shm == NULL) {
    char namebuf[MIZU_NAME_MAX];
    memcpy(namebuf, name, name_len);
    namebuf[name_len] = '\0';
    shm = mizu_zc_open(namebuf);
    if (shm == NULL) {
      if (gone != NULL) {
        *gone = 1;
        return R_NilValue;
      }
      mizu_stop_shm(NA_REAL, "mizu: cannot open payload region '%s'", namebuf);
    }
    map_wrap = mizu_shm_wrap_consumer(shm);
    if (oc != NULL)
      mizu_zc_cache_store(oc, (const unsigned char *) name, name_len, map_wrap);
  }
  PROTECT(map_wrap);             /* cached or fresh: one constant pin */
  mizu_zc_rel *rel = malloc(sizeof(mizu_zc_rel));
  if (rel == NULL) {
    UNPROTECT(1);
    Rf_error("mizu: allocation failure");
  }
  rel->shm = shm;
  rel->pid = mizu_self_pid();
  rel->armed = 0;
  SEXP rel_xp = PROTECT(R_MakeExternalPtr(rel, mizu_rel_tag, map_wrap));
  R_RegisterCFinalizerEx(rel_xp, mizu_rel_finalizer, TRUE);
  /* the vendored chain walk (identifier formatting) ends at the first
     shm-tag hop reading it as a mizu_shm — name_xp borrows the mapping's */
  SEXP name_xp = PROTECT(R_MakeExternalPtr(shm, mizu_shm_tag_sym, rel_xp));
  *shm_out = shm;
  UNPROTECT(3);
  return name_xp;
}

/* Validate the layout at the region base and wrap it as a view, the
   release hook riding the view's owned metadata. aux != 0 (the SHM_VEC
   wire form) cross-checks the staged type and exact byte count against
   the header. The refcount add is the caller's — it lands after the wrap,
   before the consumer-done signal. */
static SEXP mizu_zc_wrap0(mizu_shm *shm, SEXP name_xp, SEXP rel_xp,
                         uint64_t aux) {
  unsigned char *base = (unsigned char *) shm->addr;
  int64_t region_size = (int64_t) shm->size;
  if (region_size < (int64_t) MIZU_HEADER_SIZE)
    Rf_error("mizu: corrupt payload slot");
  uint32_t magic;
  memcpy(&magic, base, 4);
  switch (magic) {
  case MIZU_MAGIC_VEC: {
    int32_t type;
    int64_t length, attrs_size;
    memcpy(&type, base + 4, 4);
    memcpy(&length, base + 8, 8);
    memcpy(&attrs_size, base + 16, 8);
    size_t elt = mizu_view_sizeof_elt(type);
    if (elt == 0 || length < 0 || attrs_size < 0 ||
        length > (region_size - (int64_t) MIZU_HEADER_SIZE) / (int64_t) elt ||
        attrs_size >
          region_size - (int64_t) MIZU_HEADER_SIZE - length * (int64_t) elt)
      Rf_error("mizu: corrupt payload slot");
    /* the three-state validity pair (mizu.h): {0, 0} absent, {0, -1}
       known-NA-free, or a 64-aligned offset whose bitmap fits the region
       — the R view never reads the section; a foreign writer's carries
       the exact-extent term below */
    int64_t voff, vcount;
    memcpy(&voff, base + MIZU_HDR_VALID_OFF, 8);
    memcpy(&vcount, base + MIZU_HDR_VALID_COUNT, 8);
    if (voff == 0) {
      if (vcount != 0 && vcount != -1) Rf_error("mizu: corrupt payload slot");
    } else if ((voff & 63) != 0 || vcount < 0 || vcount > length ||
               voff > region_size ||
               (uint64_t) ((length + 7) / 8) > (uint64_t) (region_size - voff))
      Rf_error("mizu: corrupt payload slot");
    if (aux != 0) {
      uint64_t extent = (uint64_t) voff > 0 ?
        (uint64_t) voff + ((uint64_t) length + 7) / 8 :
        (uint64_t) ((size_t) MIZU_HEADER_SIZE + (size_t) length * elt +
                    (size_t) attrs_size);
      if ((uint32_t) mizu_aux_type(aux) != (uint32_t) type ||
          mizu_aux_hi(aux) != extent)
        Rf_error("mizu: corrupt payload slot");
    }
    SEXP view = PROTECT(mizu_view_vec_wrap(base + MIZU_HEADER_SIZE,
                                      (R_xlen_t) length, type, name_xp,
                                      mizu_zc_rel_fire, rel_xp));
    if (attrs_size > 0)
      mizu_view_restore_attrs(view,
                         base + MIZU_HEADER_SIZE + (size_t) length * elt,
                         (size_t) attrs_size);
    view = mizu_view_apply_s4(view, base);
    UNPROTECT(1);
    return view;
  }
  case MIZU_MAGIC_STR: {
    int32_t attrs_size;
    int64_t n, str_size;
    memcpy(&attrs_size, base + 4, 4);
    memcpy(&n, base + 8, 8);
    memcpy(&str_size, base + 16, 8);
    if (n < 0 || str_size < 0 || attrs_size < 0 ||
        str_size > region_size - (int64_t) MIZU_HEADER_SIZE ||
        attrs_size > region_size - (int64_t) MIZU_HEADER_SIZE - str_size)
      Rf_error("mizu: corrupt payload slot");
    if (aux != 0 &&
        ((uint32_t) mizu_aux_type(aux) != (uint32_t) STRSXP ||
         mizu_aux_hi(aux) != (uint64_t) ((size_t) MIZU_HEADER_SIZE +
                                        (size_t) str_size +
                                        (size_t) attrs_size)))
      Rf_error("mizu: corrupt payload slot");
    SEXP view = PROTECT(mizu_view_str_wrap(base + MIZU_HEADER_SIZE, (R_xlen_t) n,
                                      str_size, name_xp, mizu_zc_rel_fire,
                                      rel_xp));
    if (attrs_size > 0)
      mizu_view_restore_attrs(view, base + MIZU_HEADER_SIZE + (size_t) str_size,
                         (size_t) attrs_size);
    view = mizu_view_apply_s4(view, base);
    UNPROTECT(1);
    return view;
  }
  case MIZU_MAGIC_LIST:
    /* the list wrap validates the header and directory internally; the
       layout size isn't header-derivable, so aux cross-checks the type */
    if (aux != 0 && (uint32_t) mizu_aux_type(aux) != (uint32_t) VECSXP)
      Rf_error("mizu: corrupt payload slot");
    return mizu_view_list_wrap(base, region_size, -1, name_xp, mizu_zc_rel_fire,
                          rel_xp);
  }
  Rf_error("mizu: corrupt payload slot");
}

SEXP mizu_zc_read(const mizu_slot_hdr *hdr, const unsigned char *payload,
                 int *gone, mizu_zc_cache *oc) {
  if (hdr->len == 0 || hdr->len >= MIZU_NAME_MAX)
    Rf_error("mizu: corrupt payload slot");
  mizu_shm *shm = NULL;
  SEXP name_xp = mizu_zc_prep((const char *) payload, hdr->len, gone, oc,
                              &shm);
  if (name_xp == R_NilValue) return R_NilValue;
  PROTECT(name_xp);
  SEXP rel_xp = PROTECT(R_ExternalPtrProtected(name_xp));
  SEXP view = PROTECT(mizu_zc_wrap0(shm, name_xp, rel_xp, hdr->aux));
  mizu_zc_ref(shm);
  ((mizu_zc_rel *) R_ExternalPtrAddr(rel_xp))->armed = 1;
  UNPROTECT(3);
  return view;
}

// REF -----------------------------------------------------------------------------

/* The region behind a view: walk data1's protected chain to the shm-tag
   terminus and read its mizu_shm (mizu's name_xp and the vendored wraps
   both terminate there). R_NilValue for anything else. */
static SEXP mizu_view_terminus(SEXP x) {
  SEXP hop = R_ExternalPtrProtected(R_altrep_data1(x));
  while (TYPEOF(hop) == EXTPTRSXP) {
    if (R_ExternalPtrTag(hop) == mizu_shm_tag_sym) return hop;
    hop = R_ExternalPtrProtected(hop);
  }
  return R_NilValue;
}

/* Mark a view's region REFHELD (the holder set widens beyond the direct
   peer, so the producer's death backstop must leak + unlink rather than
   force-reclaim) and record the view on the mark list — the funnel every
   by-reference emission (the REF tier's claim below, the serialize
   fallback's wire hooks, the 'I' ref gate) goes through. Every mizu
   consumer mapping has a writable page 0 — the prep path's split open
   and the vendored cache's hook open alike — so the flag store goes
   straight through the chain terminus. */
void mizu_zc_ref_mark(SEXP x) {
  mizu_zc_marks = zc_list_push(mizu_zc_marks, &mizu_zc_nmarks, x);
  SEXP terminus = mizu_view_terminus(x);
  if (terminus == R_NilValue) return;
  mizu_shm *shm = (mizu_shm *) R_ExternalPtrAddr(terminus);
  if (shm == NULL || shm->addr == NULL) return;
  atomic_fetch_or_explicit(mizu_zc_flags_(shm->addr), MIZU_ZC_FLAG_REFHELD,
                           memory_order_acq_rel);
}

int mizu_zc_ref_stage(mizu_slot_hdr *hdr, unsigned char *payload,
                     uint32_t inline_max, SEXP x) {
  if (!mizu_view_check(x)) return 0;
  /* data2 set on a vector or string view means COW-materialized: the
     release already fired (the region may be recycled) and the private
     copy may hold mutations — such a view must cross by value. A list
     view's data2 is only the element cache: reads never detach it (the
     release fires at the finalizer), so list views REF at any time. */
  if (TYPEOF(x) != VECSXP && R_altrep_data2(x) != R_NilValue) return 0;
  SEXP id = mizu_view_shm_name(x);
  if (id == R_NilValue) return 0;
  const char *s = CHAR(STRING_ELT(id, 0));
  size_t len = strlen(s);
  if (len == 0 || len > (size_t) inline_max) return 0;
  mizu_zc_ref_mark(x);
  hdr->kind = MIZU_KIND_REF;
  hdr->len = (uint32_t) len;
  hdr->aux = 0;
  memcpy(payload, s, len);
  return 1;
}

SEXP mizu_zc_ref_read(const mizu_slot_hdr *hdr, const unsigned char *payload,
                     int *gone, mizu_zc_cache *oc) {
  if (hdr->len == 0 || hdr->len >= MIZU_VIEW_IDENTIFIER_MAX)
    Rf_error("mizu: corrupt payload slot");
  char buf[MIZU_VIEW_IDENTIFIER_MAX];
  memcpy(buf, payload, hdr->len);
  buf[hdr->len] = '\0';
  char name[MIZU_NAME_MAX];
  int32_t path[MIZU_VIEW_MAX_PATH];
  int path_len = 0;
  if (mizu_view_parse_id(buf, name, sizeof(name), path, &path_len) < 0)
    Rf_error("mizu: corrupt payload slot");
  mizu_shm *shm = NULL;
  SEXP name_xp = mizu_zc_prep(name, (uint32_t) strlen(name), gone, oc,
                              &shm);
  if (name_xp == R_NilValue) return R_NilValue;
  PROTECT(name_xp);
  SEXP rel_xp = PROTECT(R_ExternalPtrProtected(name_xp));
  SEXP view;
  if (path_len == 0) {
    view = PROTECT(mizu_zc_wrap0(shm, name_xp, rel_xp, 0));
    mizu_zc_ref(shm);
    ((mizu_zc_rel *) R_ExternalPtrAddr(rel_xp))->armed = 1;
  } else {
    view = PROTECT(mizu_view_walk_path((unsigned char *) shm->addr,
                                  (int64_t) shm->size, path, path_len,
                                  name_xp));
    /* a path leaf that is itself a view gets the release hook armed (a
       serialized leaf references nothing — no count needed) */
    if (mizu_view_check(view)) {
      mizu_view_owned *o = (mizu_view_owned *) R_ExternalPtrAddr(R_altrep_data1(view));
      if (o != NULL && o->release == NULL) {
        o->release = mizu_zc_rel_fire;
        o->release_arg = (void *) rel_xp;
        mizu_zc_ref(shm);
        ((mizu_zc_rel *) R_ExternalPtrAddr(rel_xp))->armed = 1;
      }
    }
  }
  UNPROTECT(3);
  return view;
}

/* The wire-resolve release: the same sub as the prep-path finalizer, the
   record riding the view's mizu_view_owned release slot (the layer's once-only
   discipline fires it at materialize or finalizer — no extptr of our own
   to anchor). The sub goes through the vendored cache's shared mapping, so
   it relies on same-cycle GC order: if a dead view and an evicted cached
   mapping die in one GC cycle, the view's release must run before the
   mapping's munmap. It does by registration order — the mapping wrap is
   registered before every view over it, and same-cycle finalizers run in
   reverse registration order (weak-ref list, prepended at registration, run
   head-first; de-facto but long-stable — the prep path's
   map_wrap -> rel_xp prot chain already relies on it). */
static void mizu_zc_wire_rel(void *arg) {
  mizu_zc_rel *rel = (mizu_zc_rel *) arg;
  if (rel->armed && rel->pid == mizu_self_pid())
    mizu_zc_unref(rel->shm);
  free(rel);
}

/* Mori's resolve hook: a view arriving via the serialize-hook path (nested
   in a larger payload) gets the same counted protocol as the SHM_VEC / REF
   tiers. The add lands inside R_Unserialize — before the read returns,
   hence before the consumer-done signal, while the sender's keeper still
   pins the payload and with it the view's own count. shm is the layer's
   cached mapping — page 0 writable because mizu registered mizu_zc_open as
   the open hook — so the record pins no mapping of its own; the view's
   keeper chain holds it. */
static void mizu_zc_wire_resolve(SEXP view, mizu_shm *shm) {
  if (!mizu_view_check(view)) return;  /* a serialized leaf references nothing */
  mizu_view_owned *o = (mizu_view_owned *) R_ExternalPtrAddr(R_altrep_data1(view));
  if (o == NULL || o->release != NULL) return;
  mizu_zc_rel *rel = malloc(sizeof(mizu_zc_rel));
  if (rel == NULL)
    Rf_error("mizu: allocation failure");
  rel->shm = shm;
  rel->pid = mizu_self_pid();
  rel->armed = 0;
  o->release = mizu_zc_wire_rel;
  o->release_arg = rel;
  mizu_zc_ref(shm);
  rel->armed = 1;
  /* the F1 argument-loan record: a worker's task-stream decode arms this
     so the release pass can fire the loan deterministically at the
     outcome write; submits and collects never arm */
  if (mizu_zc_resolved_armed)
    mizu_zc_resolved =
      zc_list_push(mizu_zc_resolved, &mizu_zc_nresolved, view);
}

// Test / debug surface --------------------------------------------------------------

SEXP mizu_zc_view_check_call(SEXP x) {
  return Rf_ScalarLogical(mizu_view_check(x));
}

/* c(refcount, flags) of the region behind a view; integer(0) for anything
   else. Reads through the view's own chain (the refcount word is page 0,
   mapped at least RO on every holder). */
SEXP mizu_zc_refcount_call(SEXP x) {
  /* the chain walk reads ALTREP slots: gate on view identity first —
     on a plain vector data1 aliases the length field */
  if (!mizu_view_check(x)) return Rf_allocVector(INTSXP, 0);
  SEXP terminus = mizu_view_terminus(x);
  mizu_shm *shm = terminus == R_NilValue ? NULL :
    (mizu_shm *) R_ExternalPtrAddr(terminus);
  if (shm == NULL || shm->addr == NULL) return Rf_allocVector(INTSXP, 0);
  SEXP out = Rf_allocVector(INTSXP, 2);
  INTEGER(out)[0] = (int) mizu_zc_refcount(shm);
  INTEGER(out)[1] = (int) mizu_zc_flags(shm);
  return out;
}

/* The identifier of the region behind a view — the region's own name for a
   root view; NULL for anything else. Lets a test open the region and read
   the layout bytes. */
SEXP mizu_zc_view_name_call(SEXP x) {
  if (!mizu_view_check(x)) return R_NilValue;
  return mizu_view_shm_name(x);
}

/* c(free-list entries, lent-ledger entries) for a handle's spill state. */
SEXP mizu_zc_fl_info(mizu_handle *h) {
  uint32_t fl_n, led_n;
  mizu_handle_spill_info(h, &fl_n, &led_n);
  SEXP out = Rf_allocVector(INTSXP, 2);
  INTEGER(out)[0] = (int) fl_n;
  INTEGER(out)[1] = (int) led_n;
  return out;
}
