/* .Call surface over the per-entity parker and the death listener, keyed by
   the fixed channel layout's entity blocks (host 128, peer 192; the park
   epoch is the block's first word). Part I's channel binds these same
   primitives; this surface exists so Phase 0 can exercise them from R. */

#include <stdlib.h>
#include "mizu.h"

static _Atomic uint32_t *mizu_entity_epoch(mizu_shm *shm, int entity) {
  if (entity != MIZU_ENTITY_HOST && entity != MIZU_ENTITY_PEER)
    Rf_error("mizu: invalid entity");
  if (shm->size < MIZU_ENTITY_OFFSET(entity) + 64)
    Rf_error("mizu: region too small for an entity block");
  return (_Atomic uint32_t *) ((char *) shm->addr + MIZU_ENTITY_OFFSET(entity));
}

// Parker ------------------------------------------------------------------------

SEXP mizu_park_call(SEXP xp, SEXP entity, SEXP timeout_ms, SEXP create) {
  mizu_shm *shm = mizu_region(xp);
  int ent = Rf_asInteger(entity);

  mizu_parker pk;
  if (mizu_parker_attach(&pk, mizu_entity_epoch(shm, ent), shm->name, ent,
                        Rf_asLogical(create) == TRUE) != 0)
    Rf_error("mizu: cannot attach parker");
  int rc = mizu_park(&pk, mizu_parker_snapshot(&pk),
                    (long) Rf_asInteger(timeout_ms));
  mizu_parker_detach(&pk);
  return Rf_ScalarInteger(rc);
}

SEXP mizu_unpark_call(SEXP xp, SEXP entity, SEXP create) {
  mizu_shm *shm = mizu_region(xp);
  int ent = Rf_asInteger(entity);

  mizu_parker pk;
  if (mizu_parker_attach(&pk, mizu_entity_epoch(shm, ent), shm->name, ent,
                        Rf_asLogical(create) == TRUE) != 0)
    Rf_error("mizu: cannot attach parker");
  mizu_unpark(&pk);
  mizu_parker_detach(&pk);
  return R_NilValue;
}

SEXP mizu_epoch_call(SEXP xp, SEXP entity) {
  mizu_shm *shm = mizu_region(xp);
  _Atomic uint32_t *epoch = mizu_entity_epoch(shm, Rf_asInteger(entity));
  return Rf_ScalarReal((double)
    atomic_load_explicit(epoch, memory_order_acquire));
}

// Death listener ------------------------------------------------------------------

typedef struct mizu_death_handle_s {
  _Atomic int fired;
  mizu_parker pk;
  int has_pk;
  mizu_death_watch *watch;
} mizu_death_handle;

static SEXP mizu_death_tag;

void mizu_entity_init(void) {
  mizu_death_tag = Rf_install("mizu_death");
}

static void mizu_death_finalizer(SEXP xp) {
  mizu_death_handle *h = (mizu_death_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) return;
  mizu_death_watch_stop(h->watch);   /* synchronizes with in-flight callbacks */
  if (h->has_pk) mizu_parker_detach(&h->pk);
  free(h);
  R_ClearExternalPtr(xp);
}

/* Watch pid; on its exit the handle's fired flag is set and, when a region +
   entity is supplied, that entity's parker is unparked. The region extptr
   rides in the handle's protected slot so the epoch word outlives the watch. */
SEXP mizu_death_watch_call(SEXP pid, SEXP xp, SEXP entity, SEXP create) {
  mizu_death_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) Rf_error("mizu: allocation failure");

  if (xp != R_NilValue) {
    mizu_shm *shm = mizu_region(xp);
    int ent = Rf_asInteger(entity);
    if (mizu_parker_attach(&h->pk, mizu_entity_epoch(shm, ent), shm->name, ent,
                          Rf_asLogical(create) == TRUE) != 0) {
      free(h);
      Rf_error("mizu: cannot attach parker");
    }
    h->has_pk = 1;
  }

  h->watch = mizu_death_watch_start((long) Rf_asReal(pid), &h->fired,
                                   h->has_pk ? &h->pk : NULL);
  if (h->watch == NULL) {
    long p = (long) Rf_asReal(pid);
    if (h->has_pk) mizu_parker_detach(&h->pk);
    free(h);
    Rf_error("mizu: cannot watch pid %ld", p);
  }

  SEXP out = R_MakeExternalPtr(h, mizu_death_tag, xp);
  R_RegisterCFinalizerEx(out, mizu_death_finalizer, TRUE);
  return out;
}

static mizu_death_handle *mizu_death_handle_get(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_death_tag)
    Rf_error("mizu: not a death-watch handle");
  mizu_death_handle *h = (mizu_death_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) Rf_error("mizu: death-watch handle is stopped");
  return h;
}

SEXP mizu_death_fired_call(SEXP xp) {
  mizu_death_handle *h = mizu_death_handle_get(xp);
  return Rf_ScalarLogical(
    atomic_load_explicit(&h->fired, memory_order_acquire));
}

SEXP mizu_death_stop_call(SEXP xp) {
  mizu_death_handle_get(xp);   /* validates type, tag, and live address */
  mizu_death_finalizer(xp);
  return R_NilValue;
}
