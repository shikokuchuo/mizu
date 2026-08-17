/* Shared payload framing over the sora_slot_hdr wire form — the staging and
   materializing code common to Part I channel slots and Part II pool entries
   / result slots. The channel adds its arena tier around these; the pool has
   no arena (its payloads release at collect or slot reuse — unordered — so a
   producer-local FIFO allocator does not apply) and stages through
   sora_payload_stage directly. */

#include "sora.h"
#ifdef __linux__
#include <sys/mman.h>
#endif

/* ANY_ATTRIB() joined the C API in R 4.5.0; equivalent fallback for earlier
   R, where ATTRIB() was still the sanctioned spelling. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

void *sora_vec_ptr(SEXP x) {
  switch (TYPEOF(x)) {
  case LGLSXP:  return LOGICAL(x);
  case INTSXP:  return INTEGER(x);
  case REALSXP: return REAL(x);
  case CPLXSXP: return COMPLEX(x);
  case RAWSXP:  return RAW(x);
  }
  return NULL;
}

int sora_raw_type(SEXP x, size_t *out_len) {
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
  *out_len = (size_t) XLENGTH(x) * mori_sizeof_elt(TYPEOF(x));
  return 1;
}

int sora_raw_eligible(SEXP x, uint32_t inline_max, size_t *out_len) {
  size_t n;
  if (!sora_raw_type(x, &n) || n > inline_max) return 0;
  *out_len = n;
  return 1;
}

int sora_str1_stage(sora_slot_hdr *hdr, unsigned char *payload,
                   uint32_t inline_max, SEXP x) {
  /* The ALTREP exclusion keeps foreign ALTSTRINGs on the serialize path
     (their Elt may materialize); sora's own string views never reach here
     (the REF check upstream claims them first) */
  if (TYPEOF(x) != STRSXP || XLENGTH(x) != 1 || ALTREP(x) ||
      ANY_ATTRIB(x) || Rf_isS4(x))
    return 0;
  SEXP s = STRING_ELT(x, 0);
  if (s == NA_STRING) {
    hdr->kind = SORA_KIND_STR1;
    hdr->len = 0;
    hdr->aux = SORA_STR1_NA;
    return 1;
  }
  size_t n = (size_t) LENGTH(s);
  if (n > (size_t) inline_max) return 0;
  hdr->kind = SORA_KIND_STR1;
  hdr->len = (uint32_t) n;
  hdr->aux = (uint64_t) Rf_getCharCE(s);
  memcpy(payload, CHAR(s), n);
  return 1;
}

// Producer spill-region free list ---------------------------------------------

/* Spill keepers are identified by pointer identity of this preserved,
   never-exposed object at slot 2 — exact where a shape check is not: a
   result keeper is the user's value itself, which could be a length-2 list
   ending in a region wrap the user still references. */
static SEXP sora_spill_marker;

void sora_payload_init(void) {
  sora_spill_marker = R_MakeExternalPtr(NULL, R_NilValue, R_NilValue);
  R_PreserveObject(sora_spill_marker);
}

static int spill_keeper(SEXP k) {
  return TYPEOF(k) == VECSXP && Rf_xlength(k) == 3 &&
    VECTOR_ELT(k, 2) == sora_spill_marker;
}

static size_t spill_round(size_t n) {
  size_t c = SORA_SPILL_FL_FLOOR;
  while (c < n) c <<= 1;
  return c;
}

void sora_spill_fl_offer(sora_spill_fl *fl, SEXP keeper) {
  if (fl == NULL || fl->wraps == NULL) return;
  /* SHM_VEC keepers go through the refcount protocol (zc.c): producer
     loan drop, then free list or lent-region ledger */
  if (sora_zc_keeper(keeper)) {
    sora_zc_release(fl, keeper);
    return;
  }
  if (!spill_keeper(keeper)) return;
  SEXP wrap = VECTOR_ELT(keeper, 1);
  mori_shm *shm = sora_shm_unwrap(wrap);
  if (shm == NULL || shm->addr == NULL) return;
  sora_spill_fl_insert(fl, wrap, shm);
}

void sora_spill_fl_surrender(sora_spill_fl *fl, SEXP keepers, R_xlen_t at) {
  sora_spill_fl_offer(fl, VECTOR_ELT(keepers, at));
  SET_VECTOR_ELT(keepers, at, R_NilValue);
}

/* Smallest entry with size >= n, removed from the list. A miss runs a
   full ledger sweep (zero-count lent regions rejoin here) and retries
   once — the free-list-miss full sweep of the release protocol. The
   wrap's only reference is the returned value: the caller must PROTECT
   before any allocation. */
static SEXP spill_fl_pop(sora_spill_fl *fl, size_t n) {
  for (int attempt = 0; attempt < 2; attempt++) {
    int best = -1;
    for (int i = 0; i < SORA_SPILL_FL_MAX; i++) {
      if (fl->size[i] < n || fl->size[i] == 0) continue;
      if (best < 0 || fl->size[i] < fl->size[best]) best = i;
    }
    if (best >= 0) {
      SEXP wrap = VECTOR_ELT(fl->wraps, best);
      SET_VECTOR_ELT(fl->wraps, best, R_NilValue);
      fl->total -= fl->size[best];
      fl->size[best] = 0;
      fl->n--;
      if (sora_shm_unwrap(wrap) != NULL) return wrap;
      return R_NilValue;                  /* finalized */
    }
    if (attempt > 0 || fl->led_n == 0) break;
    sora_ledger_sweep(fl, SORA_LEDGER_MAX);
  }
#ifdef __linux__
  /* A miss with lent regions still outstanding: the sweep just proved
     consumer-side views outlive their traffic. The signal is Linux-only
     because only there is a fresh region dear: the vendored create
     pre-faults every page (posix_fallocate + MAP_POPULATE, SIGBUS-
     proofing tmpfs), so a fresh region per SHM_VEC payload pays a full
     extra pass over the bytes and the copy tiers' deterministic reuse
     wins. macOS and Windows creates are lazy — the layout write faults
     the pages it touches anyway — so SHM_VEC (one layout write, no
     receive copy) beats the fallback even under churn. */
  if (fl->led_n > 0) fl->churn = 1;
#endif
  return R_NilValue;
}

/* The free-list insert under the size-class and total-byte caps (evicting
   largest-oldest), shared by keeper offers and the ledger sweep. A wrap
   that doesn't fit drops to GC. */
void sora_spill_fl_insert(sora_spill_fl *fl, SEXP wrap, mori_shm *shm) {
  size_t size = shm->size;
  if (size > SORA_SPILL_FL_BYTES) return;
  int cls = 0, slot = -1;
  for (int i = 0; i < SORA_SPILL_FL_MAX; i++) {
    if (fl->size[i] == 0) slot = i;
    else cls += spill_round(fl->size[i]) == spill_round(size);
  }
  if (cls >= SORA_SPILL_FL_CLASS) return;
  /* total-byte cap: evict largest (oldest among equals) until it fits */
  while (fl->n > 0 && fl->total + size > SORA_SPILL_FL_BYTES) {
    int vic = -1;
    for (int i = 0; i < SORA_SPILL_FL_MAX; i++) {
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
  /* The Linux THP collapse, deferred to the moment a region proves
     reusable by completing a consumer-done cycle: under zc churn lent
     regions never reach insert, so the pass never taxes a churned
     fresh-create-per-payload regime (the vendored create's
     MADV_HUGEPAGE is inert under stock Linux shmem_enabled=[never]).
     Idempotent re-collapse is sub-µs, so once per cycle is fine.
     Failure is benign. */
#if defined(__linux__) && defined(MADV_COLLAPSE)
  if (size >= ((size_t) 2 << 20))
    (void) madvise(shm->addr, size, MADV_COLLAPSE);
#endif
  SET_VECTOR_ELT(fl->wraps, slot, wrap);
  fl->size[slot] = size;
  fl->stamp[slot] = ++fl->tick;
  fl->total += size;
  fl->n++;
}

/* Pop-or-create a spill region: the pow2 size class when a free list is
   in play so nearby payload sizes hit it later, exact bytes for one-shot
   (no-fl) regions. Returns the PROTECTed producer wrap; *out the region.
   Raises sora_error_shm on create failure. */
SEXP sora_spill_region_get(sora_spill_fl *fl, size_t n, mori_shm **out) {
  SEXP wrap = R_NilValue;
  mori_shm *shm = NULL;
  if (fl != NULL) {
    fl->last_reused = 0;
    wrap = spill_fl_pop(fl, n);
  }
  if (wrap != R_NilValue) {
    PROTECT(wrap);
    shm = sora_shm_unwrap(wrap);
    fl->last_reused = 1;
    fl->hits++;
  } else {
    size_t cap = fl != NULL && n <= SORA_SPILL_FL_BYTES ? spill_round(n) : n;
    int rc = mori_shm_create_heap(&shm, cap);
    if (rc != MORI_OK) {
      const char *summary, *hint;
      mori_err_describe(rc, &summary, &hint);
      sora_stop_shm((double) cap,
                   "sora: cannot create payload region (%llu bytes): %s%s%s",
                   (unsigned long long) cap, summary,
                   hint[0] != '\0' ? ". " : "", hint);
    }
    wrap = PROTECT(sora_shm_wrap_producer(shm));
  }
  *out = shm;
  return wrap;
}

SEXP sora_payload_spill_shm(sora_slot_hdr *hdr, unsigned char *payload, SEXP x,
                           size_t n, sora_spill_fl *fl) {
  mori_shm *shm = NULL;
  SEXP wrap = sora_spill_region_get(fl, n, &shm);       /* PROTECTed */
  mori_serialize_into((unsigned char *) shm->addr, x);
  hdr->kind = SORA_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;
  memcpy(payload, shm->name, shm->name_len);
  SEXP keep = Rf_allocVector(VECSXP, 3);
  SET_VECTOR_ELT(keep, 0, x);
  SET_VECTOR_ELT(keep, 1, wrap);
  SET_VECTOR_ELT(keep, 2, sora_spill_marker);
  UNPROTECT(1);
  return keep;
}

SEXP sora_payload_spill_raw(sora_slot_hdr *hdr, unsigned char *payload, SEXP x,
                           size_t n, sora_spill_fl *fl) {
  mori_shm *shm = NULL;
  SEXP wrap = sora_spill_region_get(fl, n, &shm);       /* PROTECTed */
  memcpy(shm->addr, sora_vec_ptr(x), n);
  hdr->kind = SORA_KIND_RAWSPILL;
  hdr->len = (uint32_t) n;
  hdr->aux = (uint64_t) TYPEOF(x) | ((uint64_t) shm->name_len << 8);
  memcpy(payload, shm->name, shm->name_len);
  SEXP keep = Rf_allocVector(VECSXP, 3);
  SET_VECTOR_ELT(keep, 0, R_NilValue);
  SET_VECTOR_ELT(keep, 1, wrap);
  SET_VECTOR_ELT(keep, 2, sora_spill_marker);
  UNPROTECT(1);
  return keep;
}

/* The SHM_RAW spill of a codec stream (n from the counting first pass):
   the same keeper shape minus x — the writer rejected ALTREP, so no
   hook-emitted identifier can ride along and only the region wrap needs
   the pin. */
SEXP sora_payload_spill_codec(sora_slot_hdr *hdr, unsigned char *payload,
                             SEXP x, size_t n, sora_spill_fl *fl) {
  mori_shm *shm = NULL;
  SEXP wrap = sora_spill_region_get(fl, n, &shm);       /* PROTECTed */
  if (sora_codec_write((unsigned char *) shm->addr, shm->size, x) != n)
    Rf_error("sora: codec write mismatch");   /* the walk is deterministic */
  hdr->kind = SORA_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;
  memcpy(payload, shm->name, shm->name_len);
  SEXP keep = Rf_allocVector(VECSXP, 3);
  SET_VECTOR_ELT(keep, 0, R_NilValue);
  SET_VECTOR_ELT(keep, 1, wrap);
  SET_VECTOR_ELT(keep, 2, sora_spill_marker);
  UNPROTECT(1);
  return keep;
}

/* The NIL, RAWVEC, and STR1 kinds return R_NilValue as the keeper — pin
   nothing: their slot bytes are self-contained (RAWVEC and STR1 exclude
   ALTREP, attributes, and S4, so no hook-emitted identifier can ride
   along), unlike the serialize tiers, where a stream may carry mori
   identifiers whose regions the keeper pins until consumer-done. */
SEXP sora_payload_stage(sora_slot_hdr *hdr, unsigned char *payload,
                       uint32_t inline_max, SEXP x, sora_spill_fl *fl) {
  size_t rawlen, total;
  /* NULL stages as the immediate kind: the canonical empty result / ACK
     pays no serialize pass and no receive-side allocation */
  if (x == R_NilValue) {
    hdr->kind = SORA_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
    return R_NilValue;
  }
  /* a sora-native view crosses by reference (REF) at any size — required
     once SHM_VEC views exist: the serialize-hook fallback resolves
     uncounted, and the producer could recycle under the far side's view */
  if (sora_zc_ref_stage(hdr, payload, inline_max, x))
    return x;
  if (sora_raw_eligible(x, inline_max, &rawlen)) {
    memcpy(payload, sora_vec_ptr(x), rawlen);
    hdr->kind = SORA_KIND_RAWVEC;
    hdr->len = (uint32_t) rawlen;
    hdr->aux = (uint64_t) TYPEOF(x);
    return R_NilValue;
  }
  if (sora_str1_stage(hdr, payload, inline_max, x))
    return R_NilValue;
  /* SHM_VEC: mori-layout-eligible objects (atomic vectors, strings, list
     trees) past the budget and the zc floor — cheap probes keep the
     layout-size walk off the inline path (zc.c). Under churn (the last
     spill miss swept the lent ledger and reclaimed nothing — a Linux-
     only signal, see spill_fl_pop) the fresh region per SHM_VEC payload
     is dearer than the serialize copy: fall to SHM_RAW, whose region
     surrenders deterministically at consumer-done. */
  if ((fl == NULL || !fl->churn) && sora_zc_eligible(x, inline_max, &total))
    return sora_zc_stage(hdr, payload, x, total, fl);
  /* Raw-bytes spill: the vectors RAWVEC takes inline, past the inline
     budget — the layout tier above already passed (under the zc floor, or
     churn-gated), and bare bytes skip both the serialize pass here and
     the parse at the far end. */
  size_t n;
  if (sora_raw_type(x, &n) && n <= UINT32_MAX)
    return sora_payload_spill_raw(hdr, payload, x, n, fl);
  /* the compact codec ahead of R_Serialize: no per-call ref-table
     allocation on either side, and a self-contained stream (the writer
     rejects ALTREP, so no mori identifier can ride along) that pins no
     keeper — the NIL/RAWVEC/STR1 discipline */
  n = sora_codec_write(payload, inline_max, x);
  if (n != 0) {
    if (n <= inline_max) {
      hdr->kind = SORA_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = 0;
      return R_NilValue;
    }
    return sora_payload_spill_codec(hdr, payload, x, n, fl);
  }
  n = sora_serialize_bounded(payload, inline_max, x);
  if (n <= inline_max) {
    hdr->kind = SORA_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = 0;
    return x;
  }
  return sora_payload_spill_shm(hdr, payload, x, n, fl);
}

/* The name-keyed cache lookup: the wrap SEXP on a hit (stamp bumped),
   R_NilValue on a miss or a finalized entry (the caller re-opens and
   re-stores). */
SEXP sora_oc_lookup_wrap(sora_open_cache *oc, const unsigned char *name,
                        uint32_t len) {
  for (int i = 0; i < SORA_OPEN_CACHE_MAX; i++)
    if (oc->name_len[i] == len &&
        memcmp(oc->names[i], name, len) == 0) {
      SEXP wrap = VECTOR_ELT(oc->wraps, i);
      if (sora_shm_unwrap(wrap) == NULL) return R_NilValue;  /* finalized */
      oc->stamp[i] = ++oc->tick;
      oc->hits++;
      return wrap;
    }
  return R_NilValue;
}

void sora_oc_store(sora_open_cache *oc, const unsigned char *name, uint32_t len,
                  SEXP wrap) {
  int slot = 0;
  for (int i = 0; i < SORA_OPEN_CACHE_MAX; i++) {
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

/* Open the named payload region read-only, through the cache when given.
   A vanished region reports through *gone when the caller can absorb it,
   else raises. A freshly opened wrap is PROTECTed and *nprotect receives
   the count (the caller unprotects); a cached wrap needs none. */
static mori_shm *payload_region_open(const unsigned char *name_bytes,
                                     uint32_t name_len, int *gone,
                                     sora_open_cache *oc, int *nprotect) {
  mori_shm *shm = NULL;
  if (oc != NULL) {
    SEXP cached = sora_oc_lookup_wrap(oc, name_bytes, name_len);
    if (cached != R_NilValue) shm = sora_shm_unwrap(cached);
  }
  if (shm == NULL) {
    char name[MORI_NAME_MAX];
    memcpy(name, name_bytes, name_len);
    name[name_len] = '\0';
    shm = sora_shm_open_ro_heap(name);
    if (shm == NULL) {
      /* the region died with its creator (Win32 mappings cannot outlive
         theirs): report rather than raise when the caller can absorb it */
      if (gone != NULL) {
        *gone = 1;
        return NULL;
      }
      sora_stop_shm(NA_REAL, "sora: cannot open payload region '%s'", name);
    }
    SEXP wrap = PROTECT(sora_shm_wrap_consumer(shm));
    *nprotect = 1;               /* no cache: mapping drops at the wrap's GC */
    if (oc != NULL)
      sora_oc_store(oc, name_bytes, name_len, wrap);
  }
  return shm;
}

SEXP sora_payload_read(const sora_slot_hdr *hdr, const unsigned char *payload,
                      uint32_t inline_max, int *gone, sora_open_cache *oc,
                      sora_open_cache *zoc) {
  switch (hdr->kind) {
  case SORA_KIND_NIL:
    return R_NilValue;
  case SORA_KIND_STR1: {
    if (hdr->aux == SORA_STR1_NA) {
      if (hdr->len != 0) Rf_error("sora: corrupt payload slot");
      SEXP y = Rf_allocVector(STRSXP, 1);
      SET_STRING_ELT(y, 0, NA_STRING);
      return y;
    }
    if (hdr->len > inline_max || hdr->aux > CE_BYTES)
      Rf_error("sora: corrupt payload slot");
    SEXP y = Rf_allocVector(STRSXP, 1);
    SET_STRING_ELT(y, 0, Rf_mkCharLenCE((const char *) payload,
                                        (int) hdr->len,
                                        (cetype_t) hdr->aux));
    return y;
  }
  case SORA_KIND_INLINE:
    if (hdr->len > inline_max || hdr->len == 0)
      Rf_error("sora: corrupt payload slot");
    return payload[0] == SORA_CODEC_MAGIC ?
      sora_codec_read(payload, hdr->len) :
      mori_unserialize_from((unsigned char *) payload, hdr->len);
  case SORA_KIND_RAWVEC: {
    int type = (int) hdr->aux;
    size_t elt = mori_sizeof_elt(type);
    if (elt == 0 || hdr->len > inline_max || hdr->len % elt != 0)
      Rf_error("sora: corrupt payload slot");
    SEXP y = Rf_allocVector((SEXPTYPE) type, (R_xlen_t) (hdr->len / elt));
    memcpy(sora_vec_ptr(y), payload, hdr->len);
    return y;
  }
  case SORA_KIND_SHM_VEC:
    return sora_zc_read(hdr, payload, gone, zoc);
  case SORA_KIND_REF:
    return sora_zc_ref_read(hdr, payload, gone, zoc);
  case SORA_KIND_RAWSPILL: {
    /* pool framing: the region name in the payload, its length and the
       SEXPTYPE packed in aux (the channel's arena framing of the same
       kind is resolved in chan_materialize, never reaching here) */
    int type = (int) (hdr->aux & 0xff);
    uint32_t name_len = (uint32_t) (hdr->aux >> 8);
    size_t elt = mori_sizeof_elt(type);
    if (elt == 0 || hdr->len % elt != 0 ||
        name_len == 0 || name_len >= MORI_NAME_MAX)
      Rf_error("sora: corrupt payload slot");
    int nprotect = 0;
    mori_shm *shm = payload_region_open(payload, name_len, gone, oc,
                                        &nprotect);
    if (shm == NULL) return R_NilValue;                /* *gone set */
    if (hdr->len > shm->size) Rf_error("sora: corrupt payload slot");
    SEXP y = Rf_allocVector((SEXPTYPE) type, (R_xlen_t) (hdr->len / elt));
    memcpy(sora_vec_ptr(y), shm->addr, hdr->len);
    UNPROTECT(nprotect);
    return y;
  }
  case SORA_KIND_SHM_RAW: {
    if (hdr->len == 0 || hdr->len >= MORI_NAME_MAX)
      Rf_error("sora: corrupt payload slot");
    int nprotect = 0;
    mori_shm *shm = payload_region_open(payload, hdr->len, gone, oc,
                                        &nprotect);
    if (shm == NULL) return R_NilValue;                /* *gone set */
    /* aux is the exact stream length: a recycled region is larger than the
       stream it carries, and the slack bytes are a previous payload's */
    size_t len = hdr->aux != 0 && hdr->aux <= (uint64_t) shm->size ?
      (size_t) hdr->aux : shm->size;
    unsigned char *stream = (unsigned char *) shm->addr;
    SEXP y = stream[0] == SORA_CODEC_MAGIC ?
      sora_codec_read(stream, len) : mori_unserialize_from(stream, len);
    UNPROTECT(nprotect);
    return y;
  }
  }
  Rf_error("sora: corrupt payload slot");
}
