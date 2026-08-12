/* Shared payload framing over the kio_slot_hdr wire form — the staging and
   materializing code common to Part I channel slots and Part II pool entries
   / result slots. The channel adds its arena tier around these; the pool has
   no arena (its payloads release at collect or slot reuse — unordered — so a
   producer-local FIFO allocator does not apply) and stages through
   kio_payload_stage directly. */

#include "kioto.h"
#ifdef __linux__
#include <sys/mman.h>
#endif

/* ANY_ATTRIB() joined the C API in R 4.5.0; equivalent fallback for earlier
   R, where ATTRIB() was still the sanctioned spelling. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

void *kio_vec_ptr(SEXP x) {
  switch (TYPEOF(x)) {
  case LGLSXP:  return LOGICAL(x);
  case INTSXP:  return INTEGER(x);
  case REALSXP: return REAL(x);
  case CPLXSXP: return COMPLEX(x);
  case RAWSXP:  return RAW(x);
  }
  return NULL;
}

int kio_raw_eligible(SEXP x, uint32_t inline_max, size_t *out_len) {
  switch (TYPEOF(x)) {
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
    break;
  default:
    return 0;
  }
  /* The ALTREP exclusion is the linkage-free gate keeping mori-shared
     vectors on the hook path; the S4 bit is recorded by serialize, so it
     disqualifies alongside attributes. */
  if (ALTREP(x) || ANY_ATTRIB(x) || Rf_isS4(x)) return 0;
  size_t n = (size_t) XLENGTH(x) * mori_sizeof_elt(TYPEOF(x));
  if (n > inline_max) return 0;
  *out_len = n;
  return 1;
}

// Producer spill-region free list ---------------------------------------------

/* Spill keepers are identified by pointer identity of this preserved,
   never-exposed object at slot 2 — exact where a shape check is not: a
   result keeper is the user's value itself, which could be a length-2 list
   ending in a region wrap the user still references. */
static SEXP kio_spill_marker;

void kio_payload_init(void) {
  kio_spill_marker = R_MakeExternalPtr(NULL, R_NilValue, R_NilValue);
  R_PreserveObject(kio_spill_marker);
}

static int spill_keeper(SEXP k) {
  return TYPEOF(k) == VECSXP && Rf_xlength(k) == 3 &&
    VECTOR_ELT(k, 2) == kio_spill_marker;
}

static size_t spill_round(size_t n) {
  size_t c = KIO_SPILL_FL_FLOOR;
  while (c < n) c <<= 1;
  return c;
}

void kio_spill_fl_offer(kio_spill_fl *fl, SEXP keeper) {
  if (fl == NULL || fl->wraps == NULL) return;
  /* SHM_VEC keepers go through the refcount protocol (zc.c): producer
     loan drop, then free list or lent-region ledger */
  if (kio_zc_keeper(keeper)) {
    kio_zc_release(fl, keeper);
    return;
  }
  if (!spill_keeper(keeper)) return;
  SEXP wrap = VECTOR_ELT(keeper, 1);
  mori_shm *shm = kio_shm_unwrap(wrap);
  if (shm == NULL || shm->addr == NULL) return;
  kio_spill_fl_insert(fl, wrap, shm);
}

void kio_spill_fl_surrender(kio_spill_fl *fl, SEXP keepers, R_xlen_t at) {
  kio_spill_fl_offer(fl, VECTOR_ELT(keepers, at));
  SET_VECTOR_ELT(keepers, at, R_NilValue);
}

/* Smallest entry with size >= n, removed from the list. A miss runs a
   full ledger sweep (zero-count lent regions rejoin here) and retries
   once — the free-list-miss full sweep of the release protocol. The
   wrap's only reference is the returned value: the caller must PROTECT
   before any allocation. */
static SEXP spill_fl_pop(kio_spill_fl *fl, size_t n) {
  for (int attempt = 0; attempt < 2; attempt++) {
    int best = -1;
    for (int i = 0; i < KIO_SPILL_FL_MAX; i++) {
      if (fl->size[i] < n || fl->size[i] == 0) continue;
      if (best < 0 || fl->size[i] < fl->size[best]) best = i;
    }
    if (best >= 0) {
      SEXP wrap = VECTOR_ELT(fl->wraps, best);
      SET_VECTOR_ELT(fl->wraps, best, R_NilValue);
      fl->total -= fl->size[best];
      fl->size[best] = 0;
      fl->n--;
      if (kio_shm_unwrap(wrap) != NULL) return wrap;
      return R_NilValue;                  /* finalized */
    }
    if (attempt > 0 || fl->led_n == 0) return R_NilValue;
    kio_ledger_sweep(fl, KIO_LEDGER_MAX);
  }
  return R_NilValue;
}

/* The free-list insert under the size-class and total-byte caps (evicting
   largest-oldest), shared by keeper offers and the ledger sweep. A wrap
   that doesn't fit drops to GC. */
void kio_spill_fl_insert(kio_spill_fl *fl, SEXP wrap, mori_shm *shm) {
  size_t size = shm->size;
  if (size > KIO_SPILL_FL_BYTES) return;
  int cls = 0, slot = -1;
  for (int i = 0; i < KIO_SPILL_FL_MAX; i++) {
    if (fl->size[i] == 0) slot = i;
    else cls += spill_round(fl->size[i]) == spill_round(size);
  }
  if (cls >= KIO_SPILL_FL_CLASS) return;
  /* total-byte cap: evict largest (oldest among equals) until it fits */
  while (fl->n > 0 && fl->total + size > KIO_SPILL_FL_BYTES) {
    int vic = -1;
    for (int i = 0; i < KIO_SPILL_FL_MAX; i++) {
      if (fl->size[i] == 0) continue;
      if (vic < 0 || fl->size[i] > fl->size[vic] ||
          (fl->size[i] == fl->size[vic] && fl->stamp[i] < fl->stamp[vic]))
        vic = i;
    }
    SET_VECTOR_ELT(fl->wraps, vic, R_NilValue);
    fl->total -= fl->size[vic];
    fl->size[vic] = 0;
    fl->n--;
    slot = vic;
  }
  if (slot < 0) return;                      /* every entry occupied */
  SET_VECTOR_ELT(fl->wraps, slot, wrap);
  fl->size[slot] = size;
  fl->stamp[slot] = ++fl->tick;
  fl->total += size;
  fl->n++;
}

/* Pop-or-create a spill region: the pow2 size class when a free list is
   in play so nearby payload sizes hit it later, exact bytes for one-shot
   (no-fl) regions. Returns the PROTECTed producer wrap; *out the region.
   Raises kio_error_shm on create failure. */
SEXP kio_spill_region_get(kio_spill_fl *fl, size_t n, mori_shm **out) {
  SEXP wrap = R_NilValue;
  mori_shm *shm = NULL;
  if (fl != NULL) {
    fl->last_reused = 0;
    wrap = spill_fl_pop(fl, n);
  }
  if (wrap != R_NilValue) {
    PROTECT(wrap);
    shm = kio_shm_unwrap(wrap);
    fl->last_reused = 1;
    fl->hits++;
  } else {
    size_t cap = fl != NULL && n <= KIO_SPILL_FL_BYTES ? spill_round(n) : n;
    int rc = mori_shm_create_heap(&shm, cap);
    if (rc != MORI_OK) {
      const char *summary, *hint;
      mori_err_describe(rc, &summary, &hint);
      kio_stop_shm((double) cap,
                   "kioto: cannot create payload region (%llu bytes): %s%s%s",
                   (unsigned long long) cap, summary,
                   hint[0] != '\0' ? ". " : "", hint);
    }
    wrap = PROTECT(kio_shm_wrap_producer(shm));
  }
  *out = shm;
  return wrap;
}

/* The vendored create's MADV_HUGEPAGE is inert under the stock Linux
   shmem_enabled=[never]; a synchronous collapse (kernel >= 6.1) works on
   shmem regardless. Once per region lifetime — after the stage write so
   the pages exist, on the fresh-create path only, where the pass
   amortizes over every later free-list hit. Failure is benign. */
void kio_spill_collapse(mori_shm *shm, kio_spill_fl *fl) {
#if defined(__linux__) && defined(MADV_COLLAPSE)
  if (fl != NULL && !fl->last_reused && shm->size >= ((size_t) 2 << 20))
    madvise(shm->addr, shm->size, MADV_COLLAPSE);
#else
  (void) shm; (void) fl;
#endif
}

SEXP kio_payload_spill_shm(kio_slot_hdr *hdr, unsigned char *payload, SEXP x,
                           size_t n, kio_spill_fl *fl) {
  mori_shm *shm = NULL;
  SEXP wrap = kio_spill_region_get(fl, n, &shm);       /* PROTECTed */
  mori_serialize_into((unsigned char *) shm->addr, n, x);
  kio_spill_collapse(shm, fl);
  hdr->kind = KIO_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;
  memcpy(payload, shm->name, shm->name_len);
  SEXP keep = Rf_allocVector(VECSXP, 3);
  SET_VECTOR_ELT(keep, 0, x);
  SET_VECTOR_ELT(keep, 1, wrap);
  SET_VECTOR_ELT(keep, 2, kio_spill_marker);
  UNPROTECT(1);
  return keep;
}

SEXP kio_payload_stage(kio_slot_hdr *hdr, unsigned char *payload,
                       uint32_t inline_max, SEXP x, kio_spill_fl *fl) {
  size_t rawlen, total;
  /* a kioto-native view crosses by reference (REF) at any size — required
     once SHM_VEC views exist: the serialize-hook fallback resolves
     uncounted, and the producer could recycle under the far side's view */
  if (kio_zc_ref_stage(hdr, payload, inline_max, x))
    return x;
  if (kio_raw_eligible(x, inline_max, &rawlen)) {
    memcpy(payload, kio_vec_ptr(x), rawlen);
    hdr->kind = KIO_KIND_RAWVEC;
    hdr->len = (uint32_t) rawlen;
    hdr->aux = (uint64_t) TYPEOF(x);
    return x;
  }
  /* SHM_VEC: mori-layout-eligible objects (atomic vectors, strings, list
     trees) past the budget and the zc floor — cheap probes keep the
     layout-size walk off the inline path (zc.c). */
  if (kio_zc_eligible(x, inline_max, &total))
    return kio_zc_stage(hdr, payload, x, total, fl);
  size_t n = kio_serialize_bounded(payload, inline_max, x);
  if (n <= inline_max) {
    hdr->kind = KIO_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = 0;
    return x;
  }
  return kio_payload_spill_shm(hdr, payload, x, n, fl);
}

/* The name-keyed cache lookup: the wrap SEXP on a hit (stamp bumped),
   R_NilValue on a miss or a finalized entry (the caller re-opens and
   re-stores). */
SEXP kio_oc_lookup_wrap(kio_open_cache *oc, const unsigned char *name,
                        uint32_t len) {
  for (int i = 0; i < KIO_OPEN_CACHE_MAX; i++)
    if (oc->name_len[i] == len &&
        memcmp(oc->names[i], name, len) == 0) {
      SEXP wrap = VECTOR_ELT(oc->wraps, i);
      if (kio_shm_unwrap(wrap) == NULL) return R_NilValue;  /* finalized */
      oc->stamp[i] = ++oc->tick;
      oc->hits++;
      return wrap;
    }
  return R_NilValue;
}

void kio_oc_store(kio_open_cache *oc, const unsigned char *name, uint32_t len,
                  SEXP wrap) {
  int slot = 0;
  for (int i = 0; i < KIO_OPEN_CACHE_MAX; i++) {
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

SEXP kio_payload_read(const kio_slot_hdr *hdr, const unsigned char *payload,
                      uint32_t inline_max, int *gone, kio_open_cache *oc,
                      kio_open_cache *zoc) {
  switch (hdr->kind) {
  case KIO_KIND_INLINE:
    if (hdr->len > inline_max)
      Rf_error("kioto: corrupt payload slot");
    return mori_unserialize_from((unsigned char *) payload, hdr->len);
  case KIO_KIND_RAWVEC: {
    int type = (int) hdr->aux;
    size_t elt = mori_sizeof_elt(type);
    if (elt == 0 || hdr->len > inline_max || hdr->len % elt != 0)
      Rf_error("kioto: corrupt payload slot");
    SEXP y = Rf_allocVector((SEXPTYPE) type, (R_xlen_t) (hdr->len / elt));
    memcpy(kio_vec_ptr(y), payload, hdr->len);
    return y;
  }
  case KIO_KIND_SHM_VEC:
    return kio_zc_read(hdr, payload, gone, zoc);
  case KIO_KIND_REF:
    return kio_zc_ref_read(hdr, payload, gone, zoc);
  case KIO_KIND_SHM_RAW: {
    if (hdr->len == 0 || hdr->len >= MORI_NAME_MAX)
      Rf_error("kioto: corrupt payload slot");
    mori_shm *shm = NULL;
    int nprotect = 0;
    if (oc != NULL) {
      SEXP cached = kio_oc_lookup_wrap(oc, payload, hdr->len);
      if (cached != R_NilValue) shm = kio_shm_unwrap(cached);
    }
    if (shm == NULL) {
      char name[MORI_NAME_MAX];
      memcpy(name, payload, hdr->len);
      name[hdr->len] = '\0';
      shm = kio_shm_open_ro_heap(name);
      if (shm == NULL) {
        /* the region died with its creator (Win32 mappings cannot outlive
           theirs): report rather than raise when the caller can absorb it */
        if (gone != NULL) {
          *gone = 1;
          return R_NilValue;
        }
        kio_stop_shm(NA_REAL, "kioto: cannot open payload region '%s'", name);
      }
      SEXP wrap = PROTECT(kio_shm_wrap_consumer(shm));
      nprotect = 1;              /* no cache: mapping drops at the wrap's GC */
      if (oc != NULL)
        kio_oc_store(oc, payload, hdr->len, wrap);
    }
    /* aux is the exact stream length: a recycled region is larger than the
       stream it carries, and the slack bytes are a previous payload's */
    size_t len = hdr->aux != 0 && hdr->aux <= (uint64_t) shm->size ?
      (size_t) hdr->aux : shm->size;
    SEXP y = mori_unserialize_from((unsigned char *) shm->addr, len);
    UNPROTECT(nprotect);
    return y;
  }
  }
  Rf_error("kioto: corrupt payload slot");
}
