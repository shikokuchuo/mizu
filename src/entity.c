/* .Call surface over the per-entity parker and the death listener, keyed by
   the fixed channel layout's entity blocks (host 128, peer 192; the park
   epoch is the block's first word). Part I's channel binds these same
   primitives; this surface exists so Phase 0 can exercise them from R. */

#include <stdlib.h>
#include "sora.h"

mori_shm *sora_region(SEXP xp);   /* wrap.c */

sora_death_watch *sora_death_watch_start(long pid, _Atomic int *flag,
                                       const sora_parker *pk) {
  return sora_death_watch_start2(pid, flag, pk, NULL, NULL);
}

static _Atomic uint32_t *sora_entity_epoch(mori_shm *shm, int entity) {
  if (entity != SORA_ENTITY_HOST && entity != SORA_ENTITY_PEER)
    Rf_error("sora: invalid entity");
  if (shm->size < SORA_ENTITY_OFFSET(entity) + 64)
    Rf_error("sora: region too small for an entity block");
  return (_Atomic uint32_t *) ((char *) shm->addr + SORA_ENTITY_OFFSET(entity));
}

// Parker ------------------------------------------------------------------------

SEXP sora_park_call(SEXP xp, SEXP entity, SEXP timeout_ms, SEXP create) {
  mori_shm *shm = sora_region(xp);
  int ent = Rf_asInteger(entity);

  sora_parker pk;
  if (sora_parker_attach(&pk, sora_entity_epoch(shm, ent), shm->name, ent,
                        Rf_asLogical(create) == TRUE) != 0)
    Rf_error("sora: cannot attach parker");
  int rc = sora_park(&pk, sora_parker_snapshot(&pk),
                    (long) Rf_asInteger(timeout_ms));
  sora_parker_detach(&pk);
  return Rf_ScalarInteger(rc);
}

SEXP sora_unpark_call(SEXP xp, SEXP entity, SEXP create) {
  mori_shm *shm = sora_region(xp);
  int ent = Rf_asInteger(entity);

  sora_parker pk;
  if (sora_parker_attach(&pk, sora_entity_epoch(shm, ent), shm->name, ent,
                        Rf_asLogical(create) == TRUE) != 0)
    Rf_error("sora: cannot attach parker");
  sora_unpark(&pk);
  sora_parker_detach(&pk);
  return R_NilValue;
}

SEXP sora_epoch_call(SEXP xp, SEXP entity) {
  mori_shm *shm = sora_region(xp);
  _Atomic uint32_t *epoch = sora_entity_epoch(shm, Rf_asInteger(entity));
  return Rf_ScalarReal((double)
    atomic_load_explicit(epoch, memory_order_acquire));
}

// Death listener ------------------------------------------------------------------

typedef struct sora_death_handle_s {
  _Atomic int fired;
  sora_parker pk;
  int has_pk;
  sora_death_watch *watch;
} sora_death_handle;

static SEXP sora_death_tag;

void sora_entity_init(void) {
  sora_death_tag = Rf_install("sora_death");
}

static void sora_death_finalizer(SEXP xp) {
  sora_death_handle *h = (sora_death_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) return;
  sora_death_watch_stop(h->watch);   /* synchronizes with in-flight callbacks */
  if (h->has_pk) sora_parker_detach(&h->pk);
  free(h);
  R_ClearExternalPtr(xp);
}

/* Watch pid; on its exit the handle's fired flag is set and, when a region +
   entity is supplied, that entity's parker is unparked. The region extptr
   rides in the handle's protected slot so the epoch word outlives the watch. */
SEXP sora_death_watch_call(SEXP pid, SEXP xp, SEXP entity, SEXP create) {
  sora_death_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) Rf_error("sora: allocation failure");

  if (xp != R_NilValue) {
    mori_shm *shm = sora_region(xp);
    int ent = Rf_asInteger(entity);
    if (sora_parker_attach(&h->pk, sora_entity_epoch(shm, ent), shm->name, ent,
                          Rf_asLogical(create) == TRUE) != 0) {
      free(h);
      Rf_error("sora: cannot attach parker");
    }
    h->has_pk = 1;
  }

  h->watch = sora_death_watch_start((long) Rf_asReal(pid), &h->fired,
                                   h->has_pk ? &h->pk : NULL);
  if (h->watch == NULL) {
    long p = (long) Rf_asReal(pid);
    if (h->has_pk) sora_parker_detach(&h->pk);
    free(h);
    Rf_error("sora: cannot watch pid %ld", p);
  }

  SEXP out = R_MakeExternalPtr(h, sora_death_tag, xp);
  R_RegisterCFinalizerEx(out, sora_death_finalizer, TRUE);
  return out;
}

static sora_death_handle *sora_death_handle_get(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != sora_death_tag)
    Rf_error("sora: not a death-watch handle");
  sora_death_handle *h = (sora_death_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) Rf_error("sora: death-watch handle is stopped");
  return h;
}

SEXP sora_death_fired_call(SEXP xp) {
  sora_death_handle *h = sora_death_handle_get(xp);
  return Rf_ScalarLogical(
    atomic_load_explicit(&h->fired, memory_order_acquire));
}

SEXP sora_death_stop_call(SEXP xp) {
  sora_death_handle_get(xp);   /* validates type, tag, and live address */
  sora_death_finalizer(xp);
  return R_NilValue;
}
