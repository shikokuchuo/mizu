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
  if (fl == NULL || fl->wraps == NULL || !spill_keeper(keeper)) return;
  SEXP wrap = VECTOR_ELT(keeper, 1);
  mori_shm *shm = kio_shm_unwrap(wrap);
  if (shm == NULL || shm->addr == NULL) return;
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

void kio_spill_fl_surrender(kio_spill_fl *fl, SEXP keepers, R_xlen_t at) {
  kio_spill_fl_offer(fl, VECTOR_ELT(keepers, at));
  SET_VECTOR_ELT(keepers, at, R_NilValue);
}

/* Smallest entry with size >= n, removed from the list. The wrap's only
   reference is the returned value: the caller must PROTECT before any
   allocation. */
static SEXP spill_fl_pop(kio_spill_fl *fl, size_t n) {
  if (fl->n == 0) return R_NilValue;
  int best = -1;
  for (int i = 0; i < KIO_SPILL_FL_MAX; i++) {
    if (fl->size[i] < n || fl->size[i] == 0) continue;
    if (best < 0 || fl->size[i] < fl->size[best]) best = i;
  }
  if (best < 0) return R_NilValue;
  SEXP wrap = VECTOR_ELT(fl->wraps, best);
  SET_VECTOR_ELT(fl->wraps, best, R_NilValue);
  fl->total -= fl->size[best];
  fl->size[best] = 0;
  fl->n--;
  if (kio_shm_unwrap(wrap) == NULL) return R_NilValue;   /* finalized */
  return wrap;
}

SEXP kio_payload_spill_shm(kio_slot_hdr *hdr, unsigned char *payload, SEXP x,
                           size_t n, kio_spill_fl *fl) {
  SEXP wrap = R_NilValue;
  mori_shm *shm = NULL;
  int fresh = 0;
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
    /* fresh, at the pow2 size class when a free list is in play so nearby
       payload sizes hit it later; one-shot (no-fl) regions stay exact */
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
    fresh = 1;
  }
  mori_serialize_into((unsigned char *) shm->addr, n, x);
#if defined(__linux__) && defined(MADV_COLLAPSE)
  /* The vendored create's MADV_HUGEPAGE is inert under the stock
     shmem_enabled=[never]; a synchronous collapse (kernel >= 6.1) works on
     shmem regardless. Once per region lifetime — after the serialize so
     the pages exist, on the reuse path only, where the pass amortizes over
     every later hit. Failure is benign. */
  if (fresh && fl != NULL && shm->size >= ((size_t) 2 << 20))
    madvise(shm->addr, shm->size, MADV_COLLAPSE);
#else
  (void) fresh;
#endif
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
  size_t rawlen;
  if (kio_raw_eligible(x, inline_max, &rawlen)) {
    memcpy(payload, kio_vec_ptr(x), rawlen);
    hdr->kind = KIO_KIND_RAWVEC;
    hdr->len = (uint32_t) rawlen;
    hdr->aux = (uint64_t) TYPEOF(x);
    return x;
  }
  size_t n = kio_serialize_bounded(payload, inline_max, x);
  if (n <= inline_max) {
    hdr->kind = KIO_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = 0;
    return x;
  }
  return kio_payload_spill_shm(hdr, payload, x, n, fl);
}

SEXP kio_payload_read(const kio_slot_hdr *hdr, const unsigned char *payload,
                      uint32_t inline_max, int *gone, kio_open_cache *oc) {
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
  case KIO_KIND_SHM_RAW: {
    if (hdr->len == 0 || hdr->len >= MORI_NAME_MAX)
      Rf_error("kioto: corrupt payload slot");
    mori_shm *shm = NULL;
    int nprotect = 0;
    if (oc != NULL)
      for (int i = 0; i < KIO_OPEN_CACHE_MAX; i++)
        if (oc->name_len[i] == hdr->len &&
            memcmp(oc->names[i], payload, hdr->len) == 0) {
          shm = kio_shm_unwrap(VECTOR_ELT(oc->wraps, i));
          if (shm != NULL) {
            oc->stamp[i] = ++oc->tick;
            oc->hits++;
          }
          break;
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
      if (oc != NULL) {
        int slot = 0;
        for (int i = 0; i < KIO_OPEN_CACHE_MAX; i++) {
          if (oc->name_len[i] == 0) {
            slot = i;
            break;
          }
          if (oc->stamp[i] < oc->stamp[slot]) slot = i;
        }
        SET_VECTOR_ELT(oc->wraps, slot, wrap);   /* evicted LRU drops to GC */
        memcpy(oc->names[slot], payload, hdr->len);
        oc->name_len[slot] = (uint8_t) hdr->len;
        oc->stamp[slot] = ++oc->tick;
        oc->misses++;
      }
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
