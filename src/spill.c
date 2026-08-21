/* Producer-side region lifetime: the spill free list, the lent-region
   ledger, the zc producer-loan release, the SHM_RAW consumer mapping
   cache, and the per-slot retain table — the explicit, handle-owned
   successors of what were VECSXP keepers and wrap vectors pinned in prot
   slots. Regions are mori_shm pointers owned by the handle: created or
   popped at stage, surrendered at the consumer-done release points,
   closed + unlinked at eviction and teardown. A staged object that must
   outlive consumer-done (the serialize tiers, whose streams may carry
   hook-emitted mori identifiers) rides the entry's pin — preserved at
   stage time, released at release. */

#include <stdlib.h>
#include "sora.h"
#include <R_ext/Utils.h>
#ifdef __linux__
#include <sys/mman.h>
#endif

// Binding hooks ----------------------------------------------------------------

/* The R binding's interrupt poll: R_CheckUserInterrupt longjmps on a
   pending interrupt, so the abandon return is never taken — the nonzero
   contract exists for bindings without a longjmp behind them. */
int sora_r_check(void *ctx) {
  (void) ctx;
  R_CheckUserInterrupt();
  return 0;
}

// Region teardown ------------------------------------------------------------

/* Producer-side destroy: release the name (POSIX: unlink + the macOS
   registry balance) and the mapping. Windows folds both into
   mori_shm_close (the creator handle rides the struct; closing it is the
   unlink). */
static void sora_region_destroy(mori_shm *shm) {
  if (shm == NULL) return;
#ifndef _WIN32
  mori_shm_host_release(shm);
#endif
  mori_shm_close(shm, 0);
  free(shm);
}

/* The survivor-unlink half of a control region's teardown: release the
   name / creator handle, keep the mapping (the death watch and parkers
   still reference it until the handle's release). */
void sora_region_unlink(mori_shm *shm) {
  mori_shm_host_release(shm);
#ifdef _WIN32
  shm->handle = NULL;   /* released; mori_shm_close must not re-close it */
#endif
}

// Free list --------------------------------------------------------------------

static size_t spill_round(size_t n) {
  size_t c = SORA_SPILL_FL_FLOOR;
  while (c < n) c <<= 1;
  return c;
}

/* The free-list insert under the size-class and total-byte caps (evicting
   largest-oldest), shared by keeper releases and the ledger sweep. A
   region that doesn't fit is closed + unlinked in place. */
void sora_spill_fl_insert(sora_spill_fl *fl, mori_shm *shm) {
  size_t size = shm->size;
  if (size > SORA_SPILL_FL_BYTES) {
    sora_region_destroy(shm);
    return;
  }
  int cls = 0, slot = -1;
  for (int i = 0; i < SORA_SPILL_FL_MAX; i++) {
    if (fl->size[i] == 0) slot = i;
    else cls += spill_round(fl->size[i]) == spill_round(size);
  }
  if (cls >= SORA_SPILL_FL_CLASS) {
    sora_region_destroy(shm);
    return;
  }
  /* total-byte cap: evict largest (oldest among equals) until it fits */
  while (fl->n > 0 && fl->total + size > SORA_SPILL_FL_BYTES) {
    int vic = -1;
    for (int i = 0; i < SORA_SPILL_FL_MAX; i++) {
      if (fl->size[i] == 0) continue;
      if (vic < 0 || fl->size[i] > fl->size[vic] ||
          (fl->size[i] == fl->size[vic] && fl->stamp[i] < fl->stamp[vic]))
        vic = i;
    }
    sora_region_destroy(fl->regions[vic]);
    fl->regions[vic] = NULL;
    fl->total -= fl->size[vic];
    fl->size[vic] = 0;
    fl->n--;
    slot = vic;
  }
  if (slot < 0) {                        /* every entry occupied */
    sora_region_destroy(shm);
    return;
  }
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
  fl->regions[slot] = shm;
  fl->size[slot] = size;
  fl->stamp[slot] = ++fl->tick;
  fl->total += size;
  fl->n++;
}

/* Smallest entry with size >= n, removed from the list. A miss runs a
   full ledger sweep (zero-count lent regions rejoin here) and retries
   once — the free-list-miss full sweep of the release protocol. */
static mori_shm *spill_fl_pop(sora_spill_fl *fl, size_t n) {
  for (int attempt = 0; attempt < 2; attempt++) {
    int best = -1;
    for (int i = 0; i < SORA_SPILL_FL_MAX; i++) {
      if (fl->size[i] < n || fl->size[i] == 0) continue;
      if (best < 0 || fl->size[i] < fl->size[best]) best = i;
    }
    if (best >= 0) {
      mori_shm *shm = fl->regions[best];
      fl->regions[best] = NULL;
      fl->total -= fl->size[best];
      fl->size[best] = 0;
      fl->n--;
      return shm;
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
  return NULL;
}

/* Pop-or-create a spill region: the pow2 size class when a free list is
   in play so nearby payload sizes hit it later, exact bytes for one-shot
   (no-fl) regions. The checkout is recorded in fl->staging — the caller
   commits it to the retain table (sora_keeper_commit), discards it
   (sora_keeper_discard), or abandons it to sora_stage_rollback. */
mori_shm *sora_spill_region_get(sora_spill_fl *fl, size_t n) {
  mori_shm *shm = NULL;
  if (fl != NULL) {
    fl->last_reused = 0;
    shm = spill_fl_pop(fl, n);
  }
  if (shm != NULL) {
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
  }
  if (fl != NULL) fl->staging = shm;
  return shm;
}

// Retain table -----------------------------------------------------------------

/* Roll back an abandoned checkout (a stage that raised before commit):
   the region rejoins the free list intact. Runs at the top of every
   staging verb and at handle teardown, so a handle that stages-then-raises
   never leaks. (No pin is touched: a pin is stored only at commit.) */
void sora_stage_rollback(sora_spill_fl *fl) {
  mori_shm *shm = fl->staging;
  fl->staging = NULL;
  if (shm != NULL) sora_spill_fl_insert(fl, shm);
}

void sora_keeper_commit(sora_spill_fl *fl, sora_keeper *tab, SEXP pins,
                        R_xlen_t at, const sora_keeper *k) {
  tab[at] = *k;
  SET_VECTOR_ELT(pins, at, k->pin);
  fl->staging = NULL;
}

/* The consumer-done release: surrender the region per its kind (SPILL to
   the free list, ZC through the refcount protocol), drop the pin. */
void sora_keeper_release(sora_spill_fl *fl, sora_keeper *tab, SEXP pins,
                         R_xlen_t at) {
  sora_keeper *k = &tab[at];
  uint8_t kind = k->kind;
  if (kind == SORA_KEEP_FREE) return;
  mori_shm *shm = k->region;
  int32_t key = k->key;
  k->kind = SORA_KEEP_FREE;
  k->region = NULL;
  k->key = -1;
  SET_VECTOR_ELT(pins, at, R_NilValue);
  if (shm == NULL) return;
  if (kind == SORA_KEEP_ZC) sora_zc_release(fl, shm, key);
  else sora_spill_fl_insert(fl, shm);
}

/* The never-published discard (a cancelled pool publish): surrender the
   staged region and clear the checkout. The pin was never stored, so no
   pin store is touched. */
void sora_keeper_discard(sora_spill_fl *fl, sora_keeper *k) {
  uint8_t kind = k->kind;
  mori_shm *shm = k->region;
  int32_t key = k->key;
  k->kind = SORA_KEEP_FREE;
  k->region = NULL;
  k->key = -1;
  k->pin = R_NilValue;
  fl->staging = NULL;
  if (shm == NULL) return;
  if (kind == SORA_KEEP_ZC) sora_zc_release(fl, shm, key);
  else sora_spill_fl_insert(fl, shm);
}

/* Handle teardown: close + unlink every retained region, drop every pin.
   No free-list surrender and no refcount sub — the handle is dying,
   exactly as the keeper wraps' GC finalizers closed + unlinked without
   one. */
void sora_keepers_teardown(sora_keeper *tab, SEXP pins, uint32_t n) {
  if (tab == NULL) return;
  for (uint32_t i = 0; i < n; i++) {
    if (tab[i].kind == SORA_KEEP_FREE) continue;
    sora_region_destroy(tab[i].region);
    tab[i].kind = SORA_KEEP_FREE;
    tab[i].region = NULL;
    tab[i].key = -1;
    if (pins != R_NilValue) SET_VECTOR_ELT(pins, (R_xlen_t) i, R_NilValue);
  }
}

void sora_spill_fl_teardown(sora_spill_fl *fl) {
  sora_region_destroy(fl->staging);
  fl->staging = NULL;
  for (int i = 0; i < SORA_SPILL_FL_MAX; i++) {
    sora_region_destroy(fl->regions[i]);
    fl->regions[i] = NULL;
    fl->size[i] = 0;
  }
  fl->n = 0;
  fl->total = 0;
  for (int i = 0; i < SORA_LEDGER_MAX; i++) {
    sora_region_destroy(fl->led_regions[i]);
    fl->led_regions[i] = NULL;
  }
  fl->led_n = 0;
  for (uint32_t i = 0; i < fl->dropped_n; i++) {
    sora_region_destroy(fl->dropped[i]);
  }
  free(fl->dropped);
  fl->dropped = NULL;
  fl->dropped_n = 0;
  fl->dropped_cap = 0;
}

// Consumer mapping cache ---------------------------------------------------------

/* The name-keyed lookup: the mapping on a hit (stamp bumped), NULL on a
   miss (the caller re-opens and re-stores). Owned mappings never finalize,
   so a hit is always live. */
mori_shm *sora_oc_lookup(sora_open_cache *oc, const unsigned char *name,
                        uint32_t len) {
  for (int i = 0; i < SORA_OPEN_CACHE_MAX; i++)
    if (oc->name_len[i] == len &&
        memcmp(oc->names[i], name, len) == 0) {
      oc->stamp[i] = ++oc->tick;
      oc->hits++;
      return oc->maps[i];
    }
  return NULL;
}

/* The LRU store: takes ownership of shm; an evicted entry is closed. */
void sora_oc_store(sora_open_cache *oc, mori_shm *shm) {
  int slot = 0;
  for (int i = 0; i < SORA_OPEN_CACHE_MAX; i++) {
    if (oc->name_len[i] == 0) {
      slot = i;
      break;
    }
    if (oc->stamp[i] < oc->stamp[slot]) slot = i;
  }
  if (oc->name_len[slot] != 0) {         /* evicted LRU */
    mori_shm_close(oc->maps[slot], 0);
    free(oc->maps[slot]);
  }
  oc->maps[slot] = shm;
  memcpy(oc->names[slot], shm->name, shm->name_len);
  oc->name_len[slot] = shm->name_len;
  oc->stamp[slot] = ++oc->tick;
  oc->misses++;
}

void sora_oc_teardown(sora_open_cache *oc) {
  for (int i = 0; i < SORA_OPEN_CACHE_MAX; i++) {
    if (oc->maps[i] != NULL) {           /* consumer mappings: no unlink */
      mori_shm_close(oc->maps[i], 0);
      free(oc->maps[i]);
      oc->maps[i] = NULL;
    }
    oc->name_len[i] = 0;
  }
}

// ZC producer-loan release and the lent-region ledger -----------------------------

/* A ledger-overflow drop: the region's lent views may still be re-sent by
   reference (a REF in flight resolves by name), so the name must outlive
   any possible resolve. The mapping closes here; the unlink is deferred
   to handle teardown — the same point a tracked ledger region's name
   dies. Live views keep their own mappings; only recycling is forfeited.
   An alloc failure forfeits the REF window, never the region. */
static void sora_overflow_drop(sora_spill_fl *fl, mori_shm *shm) {
  if (fl->dropped_n == fl->dropped_cap) {
    uint32_t cap = fl->dropped_cap == 0 ? 8 : 2 * fl->dropped_cap;
    mori_shm **next = realloc(fl->dropped, cap * sizeof(*next));
    if (next == NULL) {
      sora_region_destroy(shm);
      return;
    }
    fl->dropped = next;
    fl->dropped_cap = cap;
  }
  mori_shm_close(shm, 0);   /* keep name + pid; drop only the mapping */
  fl->dropped[fl->dropped_n++] = shm;
}

/* The producer-loan drop at a consumer-done release point: refcount sub,
   then the free list on 0 (no live views) or the lent-region ledger
   otherwise. A full ledger drops the region to sora_overflow_drop. */
void sora_zc_release(sora_spill_fl *fl, mori_shm *shm, int32_t key) {
  uint32_t prev =
    atomic_fetch_sub_explicit(sora_zc_rc(shm->addr), 1, memory_order_acq_rel);
  if (prev <= 1) {
    sora_spill_fl_insert(fl, shm);
    return;
  }
  if (fl->led_n >= SORA_LEDGER_MAX) {
    sora_overflow_drop(fl, shm);
    return;
  }
  fl->led_regions[fl->led_n] = shm;
  fl->led_key[fl->led_n] = key;
  fl->led_n++;
}

static void sora_ledger_drop(sora_spill_fl *fl, uint32_t i) {
  fl->led_n--;
  fl->led_regions[i] = fl->led_regions[fl->led_n];
  fl->led_regions[fl->led_n] = NULL;
  fl->led_key[i] = fl->led_key[fl->led_n];
}

void sora_ledger_sweep(sora_spill_fl *fl, uint32_t quota) {
  uint32_t i = 0, visited = 0;
  while (i < fl->led_n && visited < quota) {
    mori_shm *shm = fl->led_regions[i];
    visited++;
    uint32_t count =
      atomic_load_explicit(sora_zc_rc(shm->addr), memory_order_acquire);
    if (count == 0) {
      sora_spill_fl_insert(fl, shm);
      sora_ledger_drop(fl, i);
      fl->churn = 0;   /* releases are landing: zero-copy reuse is viable */
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
void sora_ledger_force(sora_spill_fl *fl, int32_t key) {
  uint32_t i = 0;
  while (i < fl->led_n) {
    if (key >= 0 && fl->led_key[i] != key) {
      i++;
      continue;
    }
    mori_shm *shm = fl->led_regions[i];
    uint32_t flags =
      atomic_load_explicit(sora_zc_flags(shm->addr), memory_order_acquire);
    if (flags & SORA_ZC_FLAG_REFHELD) {
      sora_region_destroy(shm);
    } else {
      sora_spill_fl_insert(fl, shm);
      fl->churn = 0;
    }
    sora_ledger_drop(fl, i);
  }
}
