/* Zero-copy payload tiers (zc.c): SHM_VEC — a view-layout object (atomic
   vector, string vector, or list tree) in a spill region, wrapped ALTREP
   at receive (no allocVector, no memcpy, no parse) — and REF — the /rei_
   identifier of an object already in shared memory, resolved to a view of
   the same pages. The receive machinery is the view layer (view.c);
   what zc.c adds is the cross-process release protocol: a
   refcount in the region header's reserved bytes (rei.h), a per-handle
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
   hooks (rei_view_set_wire_hooks, set at load) keep that path inside the
   protocol: emit marks the region REFHELD (the holder set widens beyond
   the direct peer), resolve does the counted add and arms the release
   callback before the read returns — which happens-before the
   consumer-done signal, the sender's keeper pinning the payload (and
   with it the view) until then. */

#include <stdlib.h>
#include "rei.h"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

static SEXP rei_rel_tag;      /* the release-record extptr */
static SEXP rei_shm_tag_sym;  /* installed REI_VIEW_TAG_SHM: the chain terminus */

static void rei_zc_ref_mark(SEXP x);
static void rei_zc_wire_resolve(SEXP view, rei_shm *shm);

void rei_zc_init(void) {
  rei_rel_tag = Rf_install("rei_view_release");
  rei_shm_tag_sym = Rf_install(REI_VIEW_TAG_SHM);
  rei_view_set_wire_hooks(rei_zc_ref_mark, rei_zc_wire_resolve);
  /* the vendored resolve cache opens through rei's split open, so every
     consumer mapping — cached or prep-path — has a writable page 0 */
  rei_view_set_open_hook(rei_zc_open);
}

// View release ------------------------------------------------------------------

/* One per wrapped view: the refcount sub, armed only after the add lands
   (a longjmp between wrap and add must not sub a count it never added).
   pid is the fork guard (the view layer's host-finalizer pattern): views are
   ordinary R objects that cross mclapply forks, and a child-side GC must
   not sub a count it never added. base points into the view's mapping —
   the prep path's own, or the vendored resolve cache's shared one (pinned
   by the view's keeper chain there, never by the record). */
typedef struct rei_zc_rel_s {
  unsigned char *base;
  long pid;
  int armed;
} rei_zc_rel;

static void rei_rel_finalizer(SEXP ptr) {
  rei_zc_rel *rel = (rei_zc_rel *) R_ExternalPtrAddr(ptr);
  if (rel != NULL) {
    if (rel->armed && rel->pid == rei_self_pid())
      atomic_fetch_sub_explicit(rei_zc_rc(rel->base), 1, memory_order_acq_rel);
    free(rel);
    R_ClearExternalPtr(ptr);
  }
}

/* The rei_view_owned release hook: run the finalizer early (it clears the
   extptr, so the GC pass is a no-op). Fires at COW materialization — the
   shared pages are dead weight from there. */
static void rei_zc_rel_fire(void *arg) {
  rei_rel_finalizer((SEXP) arg);
}

// Consumer open: split mapping ---------------------------------------------------

/* Page 0 read-write (the refcount word needs it), remaining pages
   read-only (the view layer's RO discipline). Lazy everywhere — a view is touched
   on demand, so eager PTE install would prefault never-read pages on the
   recv hot path (the SHM_RAW cache's populated open exists because a
   stream is unserialized in full immediately; a view is not).
   Two call paths: registered as the view layer's embedder open hook
   (rei_zc_init) — the wire-resolve cache's misses — and called directly by
   the prep path below (the per-handle cache). */
rei_shm *rei_zc_open(const char *name) {
  rei_shm *shm;
  if (rei_shm_open_rw(&shm, name, 0) != REI_OK) return NULL;
  size_t size = shm->size;
#ifdef _WIN32
  /* MapViewOfFile offsets must be 64 KiB allocation-granularity aligned,
     so a second view starting at page 1 fails: one RW view, then protect
     the tail. */
  static DWORD pagesz = 0;
  if (pagesz == 0) {
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    pagesz = si.dwPageSize;
  }
  if (size > pagesz) {
    DWORD old;
    VirtualProtect((unsigned char *) shm->addr + pagesz, size - pagesz,
                   PAGE_READONLY, &old);
  }
#else
  static long pagesz = 0;
  if (pagesz == 0) pagesz = sysconf(_SC_PAGESIZE);
  /* Best-effort: if mprotect fails the mapping stays fully RW — the
     refcount still works, only the defense-in-depth degrades. */
  if (size > (size_t) pagesz)
    (void) mprotect((unsigned char *) shm->addr + pagesz, size - pagesz,
                    PROT_READ);
#endif
  return shm;
}

// Eligibility --------------------------------------------------------------------

/* Lower bound on the REIS layout size (header + offset table + packed
   string bytes; attrs excluded): 64 + align64(16 per entry) + the CHARSXP
   bytes. Walks string lengths only — no allocation, no serialize count. */
static size_t rei_zc_str_probe(SEXP x) {
  R_xlen_t n = XLENGTH(x);
  size_t total = REI_HEADER_SIZE + REI_ALIGN64(16 * (size_t) n);
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP s = STRING_ELT(x, i);
    if (s != NA_STRING) total += (size_t) LENGTH(s);
  }
  return total;
}

/* Lower bound on the REIL layout size: layout-eligible leaf bytes only
   (headers, directory, attrs, and serialized leaves all excluded), so the
   exact rei_view_layout_size always exceeds it. A rei view anywhere in the
   tree rejects it outright: nested views must cross by reference on the
   serialize-hook path (the wire hooks keep them counted) — the layout
   writer would copy their bytes, and a copied view can never be REF'd
   back. Foreign ALTREP leaves failing rei_view_altrep_readable cost 0 here;
   the oracle rejects the tree later. Serialized leaves cost 0 too,
   so a tree whose bulk is
   non-eligible (a big environment, a call) stays on the serialize tiers —
   REIL's win is the eligible leaves. */
static size_t rei_zc_tree_probe(SEXP x, int *reject) {
  if (*reject) return 0;
  if (ALTREP(x)) {
    if (rei_view_check(x)) { *reject = 1; return 0; }
    if (!rei_view_altrep_readable(x)) return 0;  /* lazy: the oracle vetoes */
  }
  int type = TYPEOF(x);
  size_t elt = rei_view_sizeof_elt(type);
  if (elt != 0) return (size_t) XLENGTH(x) * elt;
  if (type == STRSXP) return rei_zc_str_probe(x);
  if (type == VECSXP) {
    R_xlen_t n = XLENGTH(x);
    size_t total = 0;
    for (R_xlen_t i = 0; i < n && !*reject; i++)
      total += rei_zc_tree_probe(VECTOR_ELT(x, i), reject);
    return total;
  }
  if (type == LISTSXP) {
    size_t total = 0;
    for (SEXP s = x; s != R_NilValue && !*reject; s = CDR(s))
      total += rei_zc_tree_probe(CAR(s), reject);
    return total;
  }
  return 0;
}

/* SHM_VEC eligibility: an object the view layouts cover whose layout
   bytes exceed both the inline budget and REI_ZC_FLOOR. Atomic vectors
   gate on the O(1) data size (a lazy ALTREP input must never stage:
   staging grabs DATAPTR and materializes it — 1:1e8 would become an
   800 MB memcpy against the ~100-byte stream its serialized-state hook
   emits. The rei_view_altrep_readable probe admits directly readable,
   keeper-free data without materializing: R's S4 data-part wrappers
   forward to their data part).
   Strings and list trees have no O(1) size, and rei_view_layout_size's walk
   serialize-counts non-eligible leaves — a per-send tax the pool's small
   task payloads must not pay — so a cheap lower-bound probe gates the
   exact walk, which then doubles as region size and oracle (0 rejects
   foreign ALTREP nodes). Top-level LISTSXP stays on the serialize
   tiers: the layout coerces it to VECSXP, a type change a transport must
   not make. */
int rei_zc_eligible(SEXP x, uint32_t inline_max, size_t *out_total) {
  int type = TYPEOF(x);
  size_t elt = rei_view_sizeof_elt(type);
  if (elt != 0) {
    if (ALTREP(x) && !rei_view_altrep_readable(x)) return 0;
    size_t data = (size_t) XLENGTH(x) * elt;
    if (data <= (size_t) inline_max || data < REI_ZC_FLOOR) return 0;
    size_t total = rei_view_layout_size(x);
    if (total == 0 || total <= (size_t) inline_max) return 0;
    *out_total = total;
    return 1;
  }
  size_t gate = (size_t) inline_max > REI_ZC_FLOOR ?
    (size_t) inline_max : REI_ZC_FLOOR;
  if (type == STRSXP) {
    /* a foreign ALTREP string's Elt may materialize it — the probe must
       not touch it (a rei view is safe: its accessors read the shared
       pages or the materialized copy). The rei_view_altrep_readable probe
       admits directly readable, keeper-free data (R's S4 data-part
       wrappers) without touching elements. */
    if (ALTREP(x) && !rei_view_check(x) && !rei_view_altrep_readable(x))
      return 0;
    if (rei_zc_str_probe(x) <= gate) return 0;
  } else if (type == VECSXP) {
    int reject = 0;
    if (rei_zc_tree_probe(x, &reject) <= gate || reject) return 0;
  } else {
    return 0;
  }
  size_t total = rei_view_layout_size(x);
  if (total == 0) return 0;
  *out_total = total;
  return 1;
}

// Stage ----------------------------------------------------------------------------

/* Stage x as SHM_VEC: one layout write into a spill region (free-list pop
   or fresh create — the irreducible producer-writes-once floor), refcount
   store 1 (the producer's loan — both the field's only initialization and
   its own reference; a recycled region carries a stale count), and the
   name as the payload. Fills the retain entry: the region, the pin of x,
   and the consumer key cell (-1) the release point may re-stamp. */
void rei_zc_stage(rei_slot_hdr *hdr, unsigned char *payload, SEXP x,
                  size_t total, rei_handle *h) {
  rei_shm *shm = rei_spill_get_raise(h, (size_t) total);
  rei_view_layout_write((unsigned char *) shm->addr, x);
  /* the aux code must match the layout's root sexptype: class-only
     integer64 stamped its REIH root REI_VIEW_TYPE_INT64 */
  int type = rei_view_is_int64(x) ? REI_TYPE_INT64 : (int) TYPEOF(x);
  hdr->kind = REI_KIND_SHM_VEC;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) (type == LISTSXP ? VECSXP : type) |
    ((uint64_t) total << 8);
  memcpy(payload, shm->name, shm->name_len);
  rei_r_pin(h, x);
  /* the producer-loan refcount store (rc = 1, flags = 0) rides the retain */
  rei_stage_retain_zc(h, shm);
}

// Receive ---------------------------------------------------------------------------

// The view cache (name -> split mapping, wrap-pinned) -------------------------

/* The name-keyed lookup: the wrap SEXP on a hit (stamp bumped), R_NilValue
   on a miss or a finalized entry (the caller re-opens and re-stores).
   Eviction only ever drops the reference — a live view keeps its mapping
   through its own chain. */
SEXP rei_zc_lookup_wrap(rei_zc_cache *oc, const unsigned char *name,
                        uint32_t len) {
  for (int i = 0; i < REI_OPEN_CACHE_MAX; i++)
    if (oc->name_len[i] == len &&
        memcmp(oc->names[i], name, len) == 0) {
      SEXP wrap = VECTOR_ELT(oc->wraps, i);
      if (rei_shm_unwrap(wrap) == NULL) return R_NilValue;  /* finalized */
      oc->stamp[i] = ++oc->tick;
      oc->hits++;
      return wrap;
    }
  return R_NilValue;
}

void rei_zc_cache_store(rei_zc_cache *oc, const unsigned char *name,
                        uint32_t len, SEXP wrap) {
  int slot = 0;
  for (int i = 0; i < REI_OPEN_CACHE_MAX; i++) {
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
static SEXP rei_zc_prep(const char *name, uint32_t name_len, int *gone,
                         rei_zc_cache *oc, rei_shm **shm_out) {
  SEXP map_wrap = oc != NULL ?
    rei_zc_lookup_wrap(oc, (const unsigned char *) name, name_len) :
    R_NilValue;
  rei_shm *shm = map_wrap == R_NilValue ? NULL : rei_shm_unwrap(map_wrap);
  if (shm == NULL) {
    char namebuf[REI_NAME_MAX];
    memcpy(namebuf, name, name_len);
    namebuf[name_len] = '\0';
    shm = rei_zc_open(namebuf);
    if (shm == NULL) {
      if (gone != NULL) {
        *gone = 1;
        return R_NilValue;
      }
      rei_stop_shm(NA_REAL, "rei: cannot open payload region '%s'", namebuf);
    }
    map_wrap = rei_shm_wrap_consumer(shm);
    if (oc != NULL)
      rei_zc_cache_store(oc, (const unsigned char *) name, name_len, map_wrap);
  }
  PROTECT(map_wrap);             /* cached or fresh: one constant pin */
  rei_zc_rel *rel = malloc(sizeof(rei_zc_rel));
  if (rel == NULL) {
    UNPROTECT(1);
    Rf_error("rei: allocation failure");
  }
  rel->base = (unsigned char *) shm->addr;
  rel->pid = rei_self_pid();
  rel->armed = 0;
  SEXP rel_xp = PROTECT(R_MakeExternalPtr(rel, rei_rel_tag, map_wrap));
  R_RegisterCFinalizerEx(rel_xp, rei_rel_finalizer, TRUE);
  /* the vendored chain walk (identifier formatting) ends at the first
     shm-tag hop reading it as a rei_shm — name_xp borrows the mapping's */
  SEXP name_xp = PROTECT(R_MakeExternalPtr(shm, rei_shm_tag_sym, rel_xp));
  *shm_out = shm;
  UNPROTECT(3);
  return name_xp;
}

/* Validate the layout at the region base and wrap it as a view, the
   release hook riding the view's owned metadata. aux != 0 (the SHM_VEC
   wire form) cross-checks the staged type and exact byte count against
   the header. The refcount add is the caller's — it lands after the wrap,
   before the consumer-done signal. */
static SEXP rei_zc_wrap0(rei_shm *shm, SEXP name_xp, SEXP rel_xp,
                         uint64_t aux) {
  unsigned char *base = (unsigned char *) shm->addr;
  int64_t region_size = (int64_t) shm->size;
  if (region_size < (int64_t) REI_HEADER_SIZE)
    Rf_error("rei: corrupt payload slot");
  uint32_t magic;
  memcpy(&magic, base, 4);
  switch (magic) {
  case REI_MAGIC_VEC: {
    int32_t type;
    int64_t length, attrs_size;
    memcpy(&type, base + 4, 4);
    memcpy(&length, base + 8, 8);
    memcpy(&attrs_size, base + 16, 8);
    size_t elt = rei_view_sizeof_elt(type);
    if (elt == 0 || length < 0 || attrs_size < 0 ||
        length > (region_size - (int64_t) REI_HEADER_SIZE) / (int64_t) elt ||
        attrs_size >
          region_size - (int64_t) REI_HEADER_SIZE - length * (int64_t) elt)
      Rf_error("rei: corrupt payload slot");
    if (aux != 0 &&
        ((uint32_t) (aux & 0xff) != (uint32_t) type ||
         (aux >> 8) != (uint64_t) ((size_t) REI_HEADER_SIZE +
                                   (size_t) length * elt +
                                   (size_t) attrs_size)))
      Rf_error("rei: corrupt payload slot");
    SEXP view = PROTECT(rei_view_vec_wrap(base + REI_HEADER_SIZE,
                                      (R_xlen_t) length, type, name_xp,
                                      rei_zc_rel_fire, rel_xp));
    if (attrs_size > 0)
      rei_view_restore_attrs(view,
                         base + REI_HEADER_SIZE + (size_t) length * elt,
                         (size_t) attrs_size);
    view = rei_view_apply_s4(view, base);
    UNPROTECT(1);
    return view;
  }
  case REI_MAGIC_STR: {
    int32_t attrs_size;
    int64_t n, str_size;
    memcpy(&attrs_size, base + 4, 4);
    memcpy(&n, base + 8, 8);
    memcpy(&str_size, base + 16, 8);
    if (n < 0 || str_size < 0 || attrs_size < 0 ||
        str_size > region_size - (int64_t) REI_HEADER_SIZE ||
        attrs_size > region_size - (int64_t) REI_HEADER_SIZE - str_size)
      Rf_error("rei: corrupt payload slot");
    if (aux != 0 &&
        ((uint32_t) (aux & 0xff) != (uint32_t) STRSXP ||
         (aux >> 8) != (uint64_t) ((size_t) REI_HEADER_SIZE +
                                   (size_t) str_size +
                                   (size_t) attrs_size)))
      Rf_error("rei: corrupt payload slot");
    SEXP view = PROTECT(rei_view_str_wrap(base + REI_HEADER_SIZE, (R_xlen_t) n,
                                      str_size, name_xp, rei_zc_rel_fire,
                                      rel_xp));
    if (attrs_size > 0)
      rei_view_restore_attrs(view, base + REI_HEADER_SIZE + (size_t) str_size,
                         (size_t) attrs_size);
    view = rei_view_apply_s4(view, base);
    UNPROTECT(1);
    return view;
  }
  case REI_MAGIC_LIST:
    /* the list wrap validates the header and directory internally; the
       layout size isn't header-derivable, so aux cross-checks the type */
    if (aux != 0 && (uint32_t) (aux & 0xff) != (uint32_t) VECSXP)
      Rf_error("rei: corrupt payload slot");
    return rei_view_list_wrap(base, region_size, -1, name_xp, rei_zc_rel_fire,
                          rel_xp);
  }
  Rf_error("rei: corrupt payload slot");
}

SEXP rei_zc_read(const rei_slot_hdr *hdr, const unsigned char *payload,
                 int *gone, rei_zc_cache *oc) {
  if (hdr->len == 0 || hdr->len >= REI_NAME_MAX)
    Rf_error("rei: corrupt payload slot");
  rei_shm *shm = NULL;
  SEXP name_xp = rei_zc_prep((const char *) payload, hdr->len, gone, oc,
                              &shm);
  if (name_xp == R_NilValue) return R_NilValue;
  PROTECT(name_xp);
  SEXP rel_xp = PROTECT(R_ExternalPtrProtected(name_xp));
  SEXP view = PROTECT(rei_zc_wrap0(shm, name_xp, rel_xp, hdr->aux));
  atomic_fetch_add_explicit(rei_zc_rc(shm->addr), 1, memory_order_acq_rel);
  ((rei_zc_rel *) R_ExternalPtrAddr(rel_xp))->armed = 1;
  UNPROTECT(3);
  return view;
}

// REF -----------------------------------------------------------------------------

/* The region behind a view: walk data1's protected chain to the shm-tag
   terminus and read its rei_shm (rei's name_xp and the vendored wraps
   both terminate there). R_NilValue for anything else. */
static SEXP rei_view_terminus(SEXP x) {
  SEXP hop = R_ExternalPtrProtected(R_altrep_data1(x));
  while (TYPEOF(hop) == EXTPTRSXP) {
    if (R_ExternalPtrTag(hop) == rei_shm_tag_sym) return hop;
    hop = R_ExternalPtrProtected(hop);
  }
  return R_NilValue;
}

/* Mark a view's region REFHELD (the holder set widens beyond the direct
   peer, so the producer's death backstop must leak + unlink rather than
   force-reclaim). Every rei consumer mapping has a writable page 0 — the
   prep path's split open and the vendored cache's hook open alike — so the
   flag store goes straight through the chain terminus. */
static void rei_zc_ref_mark(SEXP x) {
  SEXP terminus = rei_view_terminus(x);
  if (terminus == R_NilValue) return;
  rei_shm *shm = (rei_shm *) R_ExternalPtrAddr(terminus);
  if (shm == NULL || shm->addr == NULL) return;
  atomic_fetch_or_explicit(rei_zc_flags_(shm->addr), REI_ZC_FLAG_REFHELD,
                           memory_order_acq_rel);
}

int rei_zc_ref_stage(rei_slot_hdr *hdr, unsigned char *payload,
                     uint32_t inline_max, SEXP x) {
  if (!rei_view_check(x)) return 0;
  /* data2 set on a vector or string view means COW-materialized: the
     release already fired (the region may be recycled) and the private
     copy may hold mutations — such a view must cross by value. A list
     view's data2 is only the element cache: reads never detach it (the
     release fires at the finalizer), so list views REF at any time. */
  if (TYPEOF(x) != VECSXP && R_altrep_data2(x) != R_NilValue) return 0;
  SEXP id = rei_view_shm_name(x);
  if (id == R_NilValue) return 0;
  const char *s = CHAR(STRING_ELT(id, 0));
  size_t len = strlen(s);
  if (len == 0 || len > (size_t) inline_max) return 0;
  rei_zc_ref_mark(x);
  hdr->kind = REI_KIND_REF;
  hdr->len = (uint32_t) len;
  hdr->aux = 0;
  memcpy(payload, s, len);
  return 1;
}

SEXP rei_zc_ref_read(const rei_slot_hdr *hdr, const unsigned char *payload,
                     int *gone, rei_zc_cache *oc) {
  if (hdr->len == 0 || hdr->len >= REI_VIEW_IDENTIFIER_MAX)
    Rf_error("rei: corrupt payload slot");
  char buf[REI_VIEW_IDENTIFIER_MAX];
  memcpy(buf, payload, hdr->len);
  buf[hdr->len] = '\0';
  char name[REI_NAME_MAX];
  int32_t path[REI_VIEW_MAX_PATH];
  int path_len = 0;
  if (rei_view_parse_id(buf, name, sizeof(name), path, &path_len) < 0)
    Rf_error("rei: corrupt payload slot");
  rei_shm *shm = NULL;
  SEXP name_xp = rei_zc_prep(name, (uint32_t) strlen(name), gone, oc,
                              &shm);
  if (name_xp == R_NilValue) return R_NilValue;
  PROTECT(name_xp);
  SEXP rel_xp = PROTECT(R_ExternalPtrProtected(name_xp));
  SEXP view;
  if (path_len == 0) {
    view = PROTECT(rei_zc_wrap0(shm, name_xp, rel_xp, 0));
    atomic_fetch_add_explicit(rei_zc_rc(shm->addr), 1, memory_order_acq_rel);
    ((rei_zc_rel *) R_ExternalPtrAddr(rel_xp))->armed = 1;
  } else {
    view = PROTECT(rei_view_walk_path((unsigned char *) shm->addr,
                                  (int64_t) shm->size, path, path_len,
                                  name_xp));
    /* a path leaf that is itself a view gets the release hook armed (a
       serialized leaf references nothing — no count needed) */
    if (rei_view_check(view)) {
      rei_view_owned *o = (rei_view_owned *) R_ExternalPtrAddr(R_altrep_data1(view));
      if (o != NULL && o->release == NULL) {
        o->release = rei_zc_rel_fire;
        o->release_arg = (void *) rel_xp;
        atomic_fetch_add_explicit(rei_zc_rc(shm->addr), 1, memory_order_acq_rel);
        ((rei_zc_rel *) R_ExternalPtrAddr(rel_xp))->armed = 1;
      }
    }
  }
  UNPROTECT(3);
  return view;
}

/* The wire-resolve release: the same sub as the prep-path finalizer, the
   record riding the view's rei_view_owned release slot (the layer's once-only
   discipline fires it at materialize or finalizer — no extptr of our own
   to anchor). The sub goes through the vendored cache's shared mapping, so
   it relies on same-cycle GC order: if a dead view and an evicted cached
   mapping die in one GC cycle, the view's release must run before the
   mapping's munmap. It does by registration order — the mapping wrap is
   registered before every view over it, and same-cycle finalizers run in
   reverse registration order (weak-ref list, prepended at registration, run
   head-first; de-facto but long-stable — the prep path's
   map_wrap -> rel_xp prot chain already relies on it). */
static void rei_zc_wire_rel(void *arg) {
  rei_zc_rel *rel = (rei_zc_rel *) arg;
  if (rel->armed && rel->pid == rei_self_pid())
    atomic_fetch_sub_explicit(rei_zc_rc(rel->base), 1, memory_order_acq_rel);
  free(rel);
}

/* Mori's resolve hook: a view arriving via the serialize-hook path (nested
   in a larger payload) gets the same counted protocol as the SHM_VEC / REF
   tiers. The add lands inside R_Unserialize — before the read returns,
   hence before the consumer-done signal, while the sender's keeper still
   pins the payload and with it the view's own count. shm is the layer's
   cached mapping — page 0 writable because rei registered rei_zc_open as
   the open hook — so the record pins no mapping of its own; the view's
   keeper chain holds it. */
static void rei_zc_wire_resolve(SEXP view, rei_shm *shm) {
  if (!rei_view_check(view)) return;  /* a serialized leaf references nothing */
  rei_view_owned *o = (rei_view_owned *) R_ExternalPtrAddr(R_altrep_data1(view));
  if (o == NULL || o->release != NULL) return;
  rei_zc_rel *rel = malloc(sizeof(rei_zc_rel));
  if (rel == NULL)
    Rf_error("rei: allocation failure");
  rel->base = (unsigned char *) shm->addr;
  rel->pid = rei_self_pid();
  rel->armed = 0;
  o->release = rei_zc_wire_rel;
  o->release_arg = rel;
  atomic_fetch_add_explicit(rei_zc_rc(shm->addr), 1, memory_order_acq_rel);
  rel->armed = 1;
}

// Test / debug surface --------------------------------------------------------------

SEXP rei_zc_view_check_call(SEXP x) {
  return Rf_ScalarLogical(rei_view_check(x));
}

/* c(refcount, flags) of the region behind a view; integer(0) for anything
   else. Reads through the view's own chain (the refcount word is page 0,
   mapped at least RO on every holder). */
SEXP rei_zc_refcount_call(SEXP x) {
  /* the chain walk reads ALTREP slots: gate on view identity first —
     on a plain vector data1 aliases the length field */
  if (!rei_view_check(x)) return Rf_allocVector(INTSXP, 0);
  SEXP terminus = rei_view_terminus(x);
  rei_shm *shm = terminus == R_NilValue ? NULL :
    (rei_shm *) R_ExternalPtrAddr(terminus);
  if (shm == NULL || shm->addr == NULL) return Rf_allocVector(INTSXP, 0);
  SEXP out = Rf_allocVector(INTSXP, 2);
  INTEGER(out)[0] =
    (int) atomic_load_explicit(rei_zc_rc(shm->addr), memory_order_acquire);
  INTEGER(out)[1] =
    (int) atomic_load_explicit(rei_zc_flags_(shm->addr), memory_order_acquire);
  return out;
}

/* c(free-list entries, lent-ledger entries) for a handle's spill state. */
SEXP rei_zc_fl_info(rei_handle *h) {
  uint32_t fl_n, led_n;
  rei_handle_spill_info(h, &fl_n, &led_n);
  SEXP out = Rf_allocVector(INTSXP, 2);
  INTEGER(out)[0] = (int) fl_n;
  INTEGER(out)[1] = (int) led_n;
  return out;
}
