/* Zero-copy payload tiers (zc.c): SHM_VEC — a mori-layout object (atomic
   vector, string vector, or list tree) in a spill region, wrapped ALTREP
   at receive (no allocVector, no memcpy, no parse) — and REF — the /kio_
   identifier of an object already in shared memory, resolved to a view of
   the same pages. The receive machinery is the vendored mori ALTREP
   layer; what kioto adds is the cross-process release protocol: a
   refcount in the region header's reserved bytes (kioto.h), a per-handle
   lent-region ledger on the producer, and a once-only release callback
   per view (mori's embedder hook — fired at COW materialization or at the
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
   hooks (mori_set_wire_hooks, set at load) keep that path inside the
   protocol: emit marks the region REFHELD (the holder set widens beyond
   the direct peer), resolve does the counted add and arms the release
   callback before the read returns — which happens-before the
   consumer-done signal, the sender's keeper pinning the payload (and
   with it the view) until then. */

#include <stdlib.h>
#include "kioto.h"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

static SEXP kio_zc_marker;    /* SHM_VEC keeper identity (keeper slot 2) */
static SEXP kio_rel_tag;      /* the release-record extptr */
static SEXP kio_shm_tag_sym;  /* installed MORI_TAG_SHM: the chain terminus */

static void kio_zc_ref_mark(SEXP x);
static void kio_zc_wire_resolve(SEXP view, mori_shm *shm);

void kio_zc_init(void) {
  kio_zc_marker = R_MakeExternalPtr(NULL, R_NilValue, R_NilValue);
  R_PreserveObject(kio_zc_marker);
  kio_rel_tag = Rf_install("kio_view_release");
  kio_shm_tag_sym = Rf_install(MORI_TAG_SHM);
  mori_set_wire_hooks(kio_zc_ref_mark, kio_zc_wire_resolve);
}

// Refcount / flags words --------------------------------------------------------

static inline _Atomic uint32_t *zc_rc(void *base) {
  return (_Atomic uint32_t *) ((unsigned char *) base + KIO_ZC_REFCOUNT_OFF);
}

static inline _Atomic uint32_t *zc_flags(void *base) {
  return (_Atomic uint32_t *) ((unsigned char *) base + KIO_ZC_FLAGS_OFF);
}

// View release ------------------------------------------------------------------

/* One per wrapped view: the refcount sub, armed only after the add lands
   (a longjmp between wrap and add must not sub a count it never added).
   pid is the fork guard (mori's host-finalizer pattern): views are
   ordinary R objects that cross mclapply forks, and a child-side GC must
   not sub a count it never added. owned is the record's own RW mapping
   of the region (wire-resolve records only — the vendored resolve maps
   fully RO, but the refcount word needs a writable page 0); NULL on the
   prep path, where the mapping belongs to the open cache / wrap chain. */
typedef struct kio_zc_rel_s {
  unsigned char *base;
  mori_shm *owned;
  long pid;
  int armed;
} kio_zc_rel;

static void kio_rel_finalizer(SEXP ptr) {
  kio_zc_rel *rel = (kio_zc_rel *) R_ExternalPtrAddr(ptr);
  if (rel != NULL) {
    if (rel->armed && rel->pid == kio_self_pid())
      atomic_fetch_sub_explicit(zc_rc(rel->base), 1, memory_order_acq_rel);
    if (rel->owned != NULL) {
      mori_shm_close(rel->owned, 0);
      free(rel->owned);
    }
    free(rel);
    R_ClearExternalPtr(ptr);
  }
}

/* The mori_owned release hook: run the finalizer early (it clears the
   extptr, so the GC pass is a no-op). Fires at COW materialization — the
   shared pages are dead weight from there. */
static void kio_zc_rel_fire(void *arg) {
  kio_rel_finalizer((SEXP) arg);
}

// Consumer open: split mapping ---------------------------------------------------

/* Page 0 read-write (the refcount word needs it), remaining pages
   read-only (mori's RO discipline). Lazy everywhere — a view is touched
   on demand, so eager PTE install would prefault never-read pages on the
   recv hot path (the SHM_RAW cache's populated open exists because a
   stream is unserialized in full immediately; a view is not). */
static mori_shm *kio_zc_open(const char *name) {
  mori_shm *shm = kio_shm_open_rw_heap(name, 0);
  if (shm == NULL) return NULL;
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

/* Lower bound on the MORS layout size (header + offset table + packed
   string bytes; attrs excluded): 64 + align64(16 per entry) + the CHARSXP
   bytes. Walks string lengths only — no allocation, no serialize count. */
static size_t kio_zc_str_probe(SEXP x) {
  R_xlen_t n = XLENGTH(x);
  size_t total = MORI_HEADER_SIZE + MORI_ALIGN64(16 * (size_t) n);
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP s = STRING_ELT(x, i);
    if (s != NA_STRING) total += (size_t) LENGTH(s);
  }
  return total;
}

/* Lower bound on the MORL layout size: layout-eligible leaf bytes only
   (headers, directory, attrs, and serialized leaves all excluded), so the
   exact mori_layout_size always exceeds it. A kioto view anywhere in the
   tree rejects it outright: nested views must cross by reference on the
   serialize-hook path (the wire hooks keep them counted) — the layout
   writer would copy their bytes, and a copied view can never be REF'd
   back. Foreign ALTREP leaves cost 0 here; the oracle rejects the tree
   later. Serialized leaves cost 0 too, so a tree whose bulk is
   non-eligible (a big environment, a call) stays on the serialize tiers —
   MORL's win is the eligible leaves. */
static size_t kio_zc_tree_probe(SEXP x, int *reject) {
  if (*reject || ALTREP(x)) {
    if (*reject == 0 && ALTREP(x) && mori_view_check(x)) *reject = 1;
    return 0;
  }
  int type = TYPEOF(x);
  size_t elt = mori_sizeof_elt(type);
  if (elt != 0) return (size_t) XLENGTH(x) * elt;
  if (type == STRSXP) return kio_zc_str_probe(x);
  if (type == VECSXP) {
    R_xlen_t n = XLENGTH(x);
    size_t total = 0;
    for (R_xlen_t i = 0; i < n && !*reject; i++)
      total += kio_zc_tree_probe(VECTOR_ELT(x, i), reject);
    return total;
  }
  if (type == LISTSXP) {
    size_t total = 0;
    for (SEXP s = x; s != R_NilValue && !*reject; s = CDR(s))
      total += kio_zc_tree_probe(CAR(s), reject);
    return total;
  }
  return 0;
}

/* SHM_VEC eligibility: an object the mori layouts cover whose layout
   bytes exceed both the inline budget and KIO_ZC_FLOOR. Atomic vectors
   gate on the O(1) data size (an ALTREP input must never stage: staging
   grabs DATAPTR and materializes it — 1:1e8 would become an 800 MB
   memcpy against the ~100-byte stream its serialized-state hook emits).
   Strings and list trees have no O(1) size, and mori_layout_size's walk
   serialize-counts non-eligible leaves — a per-send tax the pool's small
   task payloads must not pay — so a cheap lower-bound probe gates the
   exact walk, which then doubles as region size and oracle (0 rejects
   foreign ALTREP nodes and S4). Top-level LISTSXP stays on the serialize
   tiers: the layout coerces it to VECSXP, a type change a transport must
   not make. */
int kio_zc_eligible(SEXP x, uint32_t inline_max, size_t *out_total) {
  int type = TYPEOF(x);
  size_t elt = mori_sizeof_elt(type);
  if (elt != 0) {
    if (ALTREP(x) || Rf_isS4(x)) return 0;
    size_t data = (size_t) XLENGTH(x) * elt;
    if (data <= (size_t) inline_max || data < KIO_ZC_FLOOR) return 0;
    size_t total = mori_layout_size(x);
    if (total == 0 || total <= (size_t) inline_max) return 0;
    *out_total = total;
    return 1;
  }
  size_t gate = (size_t) inline_max > KIO_ZC_FLOOR ?
    (size_t) inline_max : KIO_ZC_FLOOR;
  if (type == STRSXP) {
    /* a foreign ALTREP string's Elt may materialize it — the probe must
       not touch it (a kioto view is safe: its accessors read the shared
       pages or the materialized copy) */
    if ((ALTREP(x) && !mori_view_check(x)) || Rf_isS4(x)) return 0;
    if (kio_zc_str_probe(x) <= gate) return 0;
  } else if (type == VECSXP) {
    int reject = 0;
    if (kio_zc_tree_probe(x, &reject) <= gate || reject) return 0;
  } else {
    return 0;
  }
  size_t total = mori_layout_size(x);
  if (total == 0) return 0;
  *out_total = total;
  return 1;
}

// Stage ----------------------------------------------------------------------------

/* Stage x as SHM_VEC: one layout write into a spill region (free-list pop
   or fresh create — the irreducible producer-writes-once floor), refcount
   store 1 (the producer's loan — both the field's only initialization and
   its own reference; a recycled region carries a stale count), and the
   name as the payload. Returns the keeper list(x, wrap, marker, key). */
SEXP kio_zc_stage(kio_slot_hdr *hdr, unsigned char *payload, SEXP x,
                  size_t total, kio_spill_fl *fl) {
  mori_shm *shm = NULL;
  SEXP wrap = kio_spill_region_get(fl, total, &shm);   /* PROTECTed */
  mori_layout_write((unsigned char *) shm->addr, x);
  kio_spill_collapse(shm, fl);
  atomic_store_explicit(zc_rc(shm->addr), 1, memory_order_relaxed);
  atomic_store_explicit(zc_flags(shm->addr), 0, memory_order_relaxed);
  int type = TYPEOF(x);
  hdr->kind = KIO_KIND_SHM_VEC;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) (type == LISTSXP ? VECSXP : type) |
    ((uint64_t) total << 8);
  memcpy(payload, shm->name, shm->name_len);
  SEXP keep = PROTECT(Rf_allocVector(VECSXP, 4));
  SET_VECTOR_ELT(keep, 0, x);
  SET_VECTOR_ELT(keep, 1, wrap);
  SET_VECTOR_ELT(keep, 2, kio_zc_marker);
  SEXP key = Rf_allocVector(INTSXP, 1);
  INTEGER(key)[0] = -1;
  SET_VECTOR_ELT(keep, 3, key);
  UNPROTECT(2);
  return keep;
}

// Receive ---------------------------------------------------------------------------

/* Open the named region's split mapping (cache first) and build the view
   chain anchor: rel_xp (the release record, unarmed) pinned through
   name_xp (the vendored chain-terminus tag) pinned on the mapping wrap.
   Returns the PROTECT count pushed, or -1 with *gone set when the region
   vanished and the caller absorbs that (NULL gone raises instead). */
static int kio_zc_prep(const char *name, uint32_t name_len, int *gone,
                       kio_open_cache *oc, mori_shm **shm_out,
                       SEXP *rel_xp_out, SEXP *name_xp_out) {
  SEXP map_wrap = oc != NULL ?
    kio_oc_lookup_wrap(oc, (const unsigned char *) name, name_len) :
    R_NilValue;
  mori_shm *shm = map_wrap == R_NilValue ? NULL : kio_shm_unwrap(map_wrap);
  int nprotect = 0;
  if (shm == NULL) {
    char namebuf[MORI_NAME_MAX];
    memcpy(namebuf, name, name_len);
    namebuf[name_len] = '\0';
    shm = kio_zc_open(namebuf);
    if (shm == NULL) {
      if (gone != NULL) {
        *gone = 1;
        return -1;
      }
      kio_stop_shm(NA_REAL, "kioto: cannot open payload region '%s'", namebuf);
    }
    map_wrap = PROTECT(kio_shm_wrap_consumer(shm));
    nprotect++;
    if (oc != NULL)
      kio_oc_store(oc, (const unsigned char *) name, name_len, map_wrap);
  }
  kio_zc_rel *rel = malloc(sizeof(kio_zc_rel));
  if (rel == NULL) {
    UNPROTECT(nprotect);
    Rf_error("kioto: allocation failure");
  }
  rel->base = (unsigned char *) shm->addr;
  rel->owned = NULL;
  rel->pid = kio_self_pid();
  rel->armed = 0;
  SEXP rel_xp = PROTECT(R_MakeExternalPtr(rel, kio_rel_tag, map_wrap));
  R_RegisterCFinalizerEx(rel_xp, kio_rel_finalizer, TRUE);
  /* the vendored chain walk (identifier formatting) ends at the first
     shm-tag hop reading it as a mori_shm — name_xp borrows the mapping's */
  SEXP name_xp = PROTECT(R_MakeExternalPtr(shm, kio_shm_tag_sym, rel_xp));
  *shm_out = shm;
  *rel_xp_out = rel_xp;
  *name_xp_out = name_xp;
  return nprotect + 2;
}

/* Validate the layout at the region base and wrap it as a view, the
   release hook riding the view's owned metadata. aux != 0 (the SHM_VEC
   wire form) cross-checks the staged type and exact byte count against
   the header. The refcount add is the caller's — it lands after the wrap,
   before the consumer-done signal. */
static SEXP kio_zc_wrap0(mori_shm *shm, SEXP name_xp, SEXP rel_xp,
                         uint64_t aux) {
  unsigned char *base = (unsigned char *) shm->addr;
  int64_t region_size = (int64_t) shm->size;
  if (region_size < (int64_t) MORI_HEADER_SIZE)
    Rf_error("kioto: corrupt payload slot");
  uint32_t magic;
  memcpy(&magic, base, 4);
  switch (magic) {
  case MORI_MAGIC_VEC: {
    int32_t type;
    int64_t length, attrs_size;
    memcpy(&type, base + 4, 4);
    memcpy(&length, base + 8, 8);
    memcpy(&attrs_size, base + 16, 8);
    size_t elt = mori_sizeof_elt(type);
    if (elt == 0 || length < 0 || attrs_size < 0 ||
        length > (region_size - (int64_t) MORI_HEADER_SIZE) / (int64_t) elt ||
        attrs_size >
          region_size - (int64_t) MORI_HEADER_SIZE - length * (int64_t) elt)
      Rf_error("kioto: corrupt payload slot");
    if (aux != 0 &&
        ((uint32_t) (aux & 0xff) != (uint32_t) type ||
         (aux >> 8) != (uint64_t) ((size_t) MORI_HEADER_SIZE +
                                   (size_t) length * elt +
                                   (size_t) attrs_size)))
      Rf_error("kioto: corrupt payload slot");
    SEXP view = PROTECT(mori_vec_wrap(base + MORI_HEADER_SIZE,
                                      (R_xlen_t) length, type, name_xp,
                                      kio_zc_rel_fire, rel_xp));
    if (attrs_size > 0)
      mori_restore_attrs(view,
                         base + MORI_HEADER_SIZE + (size_t) length * elt,
                         (size_t) attrs_size);
    UNPROTECT(1);
    return view;
  }
  case MORI_MAGIC_STR: {
    int32_t attrs_size;
    int64_t n, str_size;
    memcpy(&attrs_size, base + 4, 4);
    memcpy(&n, base + 8, 8);
    memcpy(&str_size, base + 16, 8);
    if (n < 0 || str_size < 0 || attrs_size < 0 ||
        str_size > region_size - (int64_t) MORI_HEADER_SIZE ||
        attrs_size > region_size - (int64_t) MORI_HEADER_SIZE - str_size)
      Rf_error("kioto: corrupt payload slot");
    if (aux != 0 &&
        ((uint32_t) (aux & 0xff) != (uint32_t) STRSXP ||
         (aux >> 8) != (uint64_t) ((size_t) MORI_HEADER_SIZE +
                                   (size_t) str_size +
                                   (size_t) attrs_size)))
      Rf_error("kioto: corrupt payload slot");
    SEXP view = PROTECT(mori_str_wrap(base + MORI_HEADER_SIZE, (R_xlen_t) n,
                                      str_size, name_xp, kio_zc_rel_fire,
                                      rel_xp));
    if (attrs_size > 0)
      mori_restore_attrs(view, base + MORI_HEADER_SIZE + (size_t) str_size,
                         (size_t) attrs_size);
    UNPROTECT(1);
    return view;
  }
  case MORI_MAGIC_LIST:
    /* the list wrap validates the header and directory internally; the
       layout size isn't header-derivable, so aux cross-checks the type */
    if (aux != 0 && (uint32_t) (aux & 0xff) != (uint32_t) VECSXP)
      Rf_error("kioto: corrupt payload slot");
    return mori_list_wrap(base, region_size, -1, name_xp, kio_zc_rel_fire,
                          rel_xp);
  }
  Rf_error("kioto: corrupt payload slot");
}

SEXP kio_zc_read(const kio_slot_hdr *hdr, const unsigned char *payload,
                 int *gone, kio_open_cache *oc) {
  if (hdr->len == 0 || hdr->len >= MORI_NAME_MAX)
    Rf_error("kioto: corrupt payload slot");
  mori_shm *shm;
  SEXP rel_xp, name_xp;
  int np = kio_zc_prep((const char *) payload, hdr->len, gone, oc,
                       &shm, &rel_xp, &name_xp);
  if (np < 0) return R_NilValue;
  SEXP view = PROTECT(kio_zc_wrap0(shm, name_xp, rel_xp, hdr->aux));
  atomic_fetch_add_explicit(zc_rc(shm->addr), 1, memory_order_acq_rel);
  ((kio_zc_rel *) R_ExternalPtrAddr(rel_xp))->armed = 1;
  UNPROTECT(np + 1);
  return view;
}

// REF -----------------------------------------------------------------------------

/* The region behind a view: walk data1's protected chain to the shm-tag
   terminus and read its mori_shm (kioto's name_xp and the vendored wraps
   both terminate there). R_NilValue for anything else. */
static SEXP kio_view_terminus(SEXP x) {
  SEXP hop = R_ExternalPtrProtected(R_altrep_data1(x));
  while (TYPEOF(hop) == EXTPTRSXP) {
    if (R_ExternalPtrTag(hop) == kio_shm_tag_sym) return hop;
    hop = R_ExternalPtrProtected(hop);
  }
  return R_NilValue;
}

/* Mark a view's region REFHELD (the holder set widens beyond the direct
   peer, so the producer's death backstop must leak + unlink rather than
   force-reclaim). A kioto zc mapping is page-0 RW already (the terminus's
   prot is the release extptr); a vendored (hook-path) mapping is fully
   RO, so set the flag through a brief RW open instead. */
static void kio_zc_ref_mark(SEXP x) {
  SEXP terminus = kio_view_terminus(x);
  if (terminus == R_NilValue) return;
  mori_shm *shm = (mori_shm *) R_ExternalPtrAddr(terminus);
  if (shm == NULL || shm->addr == NULL) return;
  SEXP prot = R_ExternalPtrProtected(terminus);
  if (TYPEOF(prot) == EXTPTRSXP && R_ExternalPtrTag(prot) == kio_rel_tag) {
    atomic_fetch_or_explicit(zc_flags(shm->addr), KIO_ZC_FLAG_REFHELD,
                             memory_order_acq_rel);
  } else {
    mori_shm tmp;
    if (kio_shm_open_rw(&tmp, shm->name, 0) == 0) {
      atomic_fetch_or_explicit(zc_flags(tmp.addr), KIO_ZC_FLAG_REFHELD,
                               memory_order_acq_rel);
      mori_shm_close(&tmp, 0);
    }
  }
}

int kio_zc_ref_stage(kio_slot_hdr *hdr, unsigned char *payload,
                     uint32_t inline_max, SEXP x) {
  if (!mori_view_check(x)) return 0;
  /* data2 set on a vector or string view means COW-materialized: the
     release already fired (the region may be recycled) and the private
     copy may hold mutations — such a view must cross by value. A list
     view's data2 is only the element cache: reads never detach it (the
     release fires at the finalizer), so list views REF at any time. */
  if (TYPEOF(x) != VECSXP && R_altrep_data2(x) != R_NilValue) return 0;
  SEXP id = mori_shm_name(x);
  if (id == R_NilValue) return 0;
  const char *s = CHAR(STRING_ELT(id, 0));
  size_t len = strlen(s);
  if (len == 0 || len > (size_t) inline_max) return 0;
  kio_zc_ref_mark(x);
  hdr->kind = KIO_KIND_REF;
  hdr->len = (uint32_t) len;
  hdr->aux = 0;
  memcpy(payload, s, len);
  return 1;
}

SEXP kio_zc_ref_read(const kio_slot_hdr *hdr, const unsigned char *payload,
                     int *gone, kio_open_cache *oc) {
  if (hdr->len == 0 || hdr->len >= MORI_IDENTIFIER_MAX)
    Rf_error("kioto: corrupt payload slot");
  char buf[MORI_IDENTIFIER_MAX];
  memcpy(buf, payload, hdr->len);
  buf[hdr->len] = '\0';
  char name[MORI_NAME_MAX];
  int32_t path[MORI_MAX_PATH];
  int path_len = 0;
  if (mori_parse_id(buf, name, sizeof(name), path, &path_len) < 0)
    Rf_error("kioto: corrupt payload slot");
  mori_shm *shm;
  SEXP rel_xp, name_xp;
  int np = kio_zc_prep(name, (uint32_t) strlen(name), gone, oc,
                       &shm, &rel_xp, &name_xp);
  if (np < 0) return R_NilValue;
  SEXP view;
  if (path_len == 0) {
    view = PROTECT(kio_zc_wrap0(shm, name_xp, rel_xp, 0));
    atomic_fetch_add_explicit(zc_rc(shm->addr), 1, memory_order_acq_rel);
    ((kio_zc_rel *) R_ExternalPtrAddr(rel_xp))->armed = 1;
  } else {
    view = PROTECT(mori_walk_path((unsigned char *) shm->addr,
                                  (int64_t) shm->size, path, path_len,
                                  name_xp));
    /* a path leaf that is itself a view gets the release hook armed (a
       serialized leaf references nothing — no count needed) */
    if (mori_view_check(view)) {
      mori_owned *o = (mori_owned *) R_ExternalPtrAddr(R_altrep_data1(view));
      if (o != NULL && o->release == NULL) {
        o->release = kio_zc_rel_fire;
        o->release_arg = (void *) rel_xp;
        atomic_fetch_add_explicit(zc_rc(shm->addr), 1, memory_order_acq_rel);
        ((kio_zc_rel *) R_ExternalPtrAddr(rel_xp))->armed = 1;
      }
    }
  }
  UNPROTECT(np + 1);
  return view;
}

/* The wire-resolve release: the same sub as the prep-path finalizer, but
   the record rides the view's mori_owned release slot (mori's once-only
   discipline fires it at materialize or finalizer — no extptr of our own
   to anchor) and owns its RW mapping of the region. */
static void kio_zc_wire_rel(void *arg) {
  kio_zc_rel *rel = (kio_zc_rel *) arg;
  if (rel->armed && rel->pid == kio_self_pid())
    atomic_fetch_sub_explicit(zc_rc(rel->base), 1, memory_order_acq_rel);
  mori_shm_close(rel->owned, 0);
  free(rel->owned);
  free(rel);
}

/* Mori's resolve hook: a view arriving via the serialize-hook path (nested
   in a larger payload) gets the same counted protocol as the SHM_VEC / REF
   tiers. The add lands inside R_Unserialize — before the read returns,
   hence before the consumer-done signal, while the sender's keeper still
   pins the payload and with it the view's own count. */
static void kio_zc_wire_resolve(SEXP view, mori_shm *shm) {
  if (!mori_view_check(view)) return;  /* a serialized leaf references nothing */
  mori_owned *o = (mori_owned *) R_ExternalPtrAddr(R_altrep_data1(view));
  if (o == NULL || o->release != NULL) return;
  mori_shm *rw = kio_zc_open(shm->name);
  if (rw == NULL)
    Rf_error("kioto: cannot open payload region '%s'", shm->name);
  kio_zc_rel *rel = malloc(sizeof(kio_zc_rel));
  if (rel == NULL) {
    mori_shm_close(rw, 0);
    free(rw);
    Rf_error("kioto: allocation failure");
  }
  rel->base = (unsigned char *) rw->addr;
  rel->owned = rw;
  rel->pid = kio_self_pid();
  rel->armed = 0;
  o->release = kio_zc_wire_rel;
  o->release_arg = rel;
  atomic_fetch_add_explicit(zc_rc(rw->addr), 1, memory_order_acq_rel);
  rel->armed = 1;
}

// Producer-side release: keeper predicate, ledger --------------------------------

int kio_zc_keeper(SEXP k) {
  return TYPEOF(k) == VECSXP && Rf_xlength(k) == 4 &&
    VECTOR_ELT(k, 2) == kio_zc_marker;
}

void kio_zc_keeper_key(SEXP keeper, int32_t key) {
  if (kio_zc_keeper(keeper))
    INTEGER(VECTOR_ELT(keeper, 3))[0] = key;
}

/* The producer-loan drop, from kio_spill_fl_offer at the consumer-done
   release points: refcount sub, then the free list on 0 (no live views)
   or the lent-region ledger otherwise. A full ledger drops the wrap to GC
   — the name unlinks, live views keep their own mappings, and only
   recycling is forfeited. */
void kio_zc_release(kio_spill_fl *fl, SEXP keeper) {
  SEXP wrap = VECTOR_ELT(keeper, 1);
  mori_shm *shm = kio_shm_unwrap(wrap);
  if (shm == NULL || shm->addr == NULL) return;
  uint32_t prev =
    atomic_fetch_sub_explicit(zc_rc(shm->addr), 1, memory_order_acq_rel);
  if (prev <= 1) {
    kio_spill_fl_insert(fl, wrap, shm);
    return;
  }
  if (fl->led_wraps == NULL || fl->led_n >= KIO_LEDGER_MAX) return;
  SET_VECTOR_ELT(fl->led_wraps, fl->led_n, wrap);
  fl->led_key[fl->led_n] = INTEGER(VECTOR_ELT(keeper, 3))[0];
  fl->led_n++;
}

static void kio_ledger_drop(kio_spill_fl *fl, uint32_t i) {
  fl->led_n--;
  SET_VECTOR_ELT(fl->led_wraps, i, VECTOR_ELT(fl->led_wraps, fl->led_n));
  SET_VECTOR_ELT(fl->led_wraps, fl->led_n, R_NilValue);
  fl->led_key[i] = fl->led_key[fl->led_n];
}

void kio_ledger_sweep(kio_spill_fl *fl, uint32_t quota) {
  if (fl->led_wraps == NULL) return;
  uint32_t i = 0, visited = 0;
  while (i < fl->led_n && visited < quota) {
    SEXP wrap = VECTOR_ELT(fl->led_wraps, i);
    mori_shm *shm = kio_shm_unwrap(wrap);
    visited++;
    uint32_t count = 0;
    if (shm != NULL && shm->addr != NULL)
      count = atomic_load_explicit(zc_rc(shm->addr), memory_order_acquire);
    if (count == 0) {
      if (shm != NULL && shm->addr != NULL)
        kio_spill_fl_insert(fl, wrap, shm);
      kio_ledger_drop(fl, i);
    } else {
      i++;
    }
  }
}

/* The death backstop, after the liveness verdict: a dead consumer's
   finalizers never ran, so its counts leaked. Unflagged entries rejoin
   the free list (the stage-time store of 1 re-initializes) — sound only
   while the dead peer is the sole possible view-holder. REFHELD entries
   have a wider holder set: leak the count and kill the name (live views
   keep their own mappings), never force-reclaim. */
void kio_ledger_force(kio_spill_fl *fl, int32_t key) {
  if (fl->led_wraps == NULL) return;
  uint32_t i = 0;
  while (i < fl->led_n) {
    if (key >= 0 && fl->led_key[i] != key) {
      i++;
      continue;
    }
    SEXP wrap = VECTOR_ELT(fl->led_wraps, i);
    mori_shm *shm = kio_shm_unwrap(wrap);
    if (shm != NULL && shm->addr != NULL) {
      uint32_t flags =
        atomic_load_explicit(zc_flags(shm->addr), memory_order_acquire);
      if (flags & KIO_ZC_FLAG_REFHELD) {
        SEXP host = R_ExternalPtrProtected(wrap);
        if (TYPEOF(host) == EXTPTRSXP) mori_host_finalizer(host);
      } else {
        kio_spill_fl_insert(fl, wrap, shm);
      }
    }
    kio_ledger_drop(fl, i);
  }
}

// Test / debug surface --------------------------------------------------------------

SEXP kio_zc_view_check_call(SEXP x) {
  return Rf_ScalarLogical(mori_view_check(x));
}

/* c(refcount, flags) of the region behind a view; integer(0) for anything
   else. Reads through the view's own chain (the refcount word is page 0,
   mapped at least RO on every holder). */
SEXP kio_zc_refcount_call(SEXP x) {
  SEXP terminus = kio_view_terminus(x);
  mori_shm *shm = terminus == R_NilValue ? NULL :
    (mori_shm *) R_ExternalPtrAddr(terminus);
  if (shm == NULL || shm->addr == NULL) return Rf_allocVector(INTSXP, 0);
  SEXP out = Rf_allocVector(INTSXP, 2);
  INTEGER(out)[0] =
    (int) atomic_load_explicit(zc_rc(shm->addr), memory_order_acquire);
  INTEGER(out)[1] =
    (int) atomic_load_explicit(zc_flags(shm->addr), memory_order_acquire);
  return out;
}

/* c(free-list entries, lent-ledger entries) for a handle's spill state. */
SEXP kio_zc_fl_info(kio_spill_fl *fl) {
  SEXP out = Rf_allocVector(INTSXP, 2);
  INTEGER(out)[0] = (int) fl->n;
  INTEGER(out)[1] = (int) fl->led_n;
  return out;
}
