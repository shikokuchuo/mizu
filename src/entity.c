/* .Call surface over the per-entity parker and the death listener, keyed by
   the fixed channel layout's entity blocks (host 128, peer 192; the park
   epoch is the block's first word). Part I's channel binds these same
   primitives; this surface exists so Phase 0 can exercise them from R. */

#include <stdlib.h>
#include "mov.h"

mori_shm *mov_region(SEXP xp);   /* wrap.c */

mov_death_watch *mov_death_watch_start(long pid, _Atomic int *flag,
                                       const mov_parker *pk) {
  return mov_death_watch_start2(pid, flag, pk, NULL, NULL);
}

static _Atomic uint32_t *mov_entity_epoch(mori_shm *shm, int entity) {
  if (entity != MOV_ENTITY_HOST && entity != MOV_ENTITY_PEER)
    Rf_error("mov: invalid entity");
  if (shm->size < MOV_ENTITY_OFFSET(entity) + 64)
    Rf_error("mov: region too small for an entity block");
  return (_Atomic uint32_t *) ((char *) shm->addr + MOV_ENTITY_OFFSET(entity));
}

// Parker ------------------------------------------------------------------------

SEXP mov_park_call(SEXP xp, SEXP entity, SEXP timeout_ms, SEXP create) {
  mori_shm *shm = mov_region(xp);
  int ent = Rf_asInteger(entity);

  mov_parker pk;
  if (mov_parker_attach(&pk, mov_entity_epoch(shm, ent), shm->name, ent,
                        Rf_asLogical(create) == TRUE) != 0)
    Rf_error("mov: cannot attach parker");
  int rc = mov_park(&pk, mov_parker_snapshot(&pk),
                    (long) Rf_asInteger(timeout_ms));
  mov_parker_detach(&pk);
  return Rf_ScalarInteger(rc);
}

SEXP mov_unpark_call(SEXP xp, SEXP entity, SEXP create) {
  mori_shm *shm = mov_region(xp);
  int ent = Rf_asInteger(entity);

  mov_parker pk;
  if (mov_parker_attach(&pk, mov_entity_epoch(shm, ent), shm->name, ent,
                        Rf_asLogical(create) == TRUE) != 0)
    Rf_error("mov: cannot attach parker");
  mov_unpark(&pk);
  mov_parker_detach(&pk);
  return R_NilValue;
}

SEXP mov_epoch_call(SEXP xp, SEXP entity) {
  mori_shm *shm = mov_region(xp);
  _Atomic uint32_t *epoch = mov_entity_epoch(shm, Rf_asInteger(entity));
  return Rf_ScalarReal((double)
    atomic_load_explicit(epoch, memory_order_acquire));
}

// Death listener ------------------------------------------------------------------

typedef struct mov_death_handle_s {
  _Atomic int fired;
  mov_parker pk;
  int has_pk;
  mov_death_watch *watch;
} mov_death_handle;

static SEXP mov_death_tag;

void mov_entity_init(void) {
  mov_death_tag = Rf_install("mov_death");
}

static void mov_death_finalizer(SEXP xp) {
  mov_death_handle *h = (mov_death_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) return;
  mov_death_watch_stop(h->watch);   /* synchronizes with in-flight callbacks */
  if (h->has_pk) mov_parker_detach(&h->pk);
  free(h);
  R_ClearExternalPtr(xp);
}

/* Watch pid; on its exit the handle's fired flag is set and, when a region +
   entity is supplied, that entity's parker is unparked. The region extptr
   rides in the handle's protected slot so the epoch word outlives the watch. */
SEXP mov_death_watch_call(SEXP pid, SEXP xp, SEXP entity, SEXP create) {
  mov_death_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) Rf_error("mov: allocation failure");

  if (xp != R_NilValue) {
    mori_shm *shm = mov_region(xp);
    int ent = Rf_asInteger(entity);
    if (mov_parker_attach(&h->pk, mov_entity_epoch(shm, ent), shm->name, ent,
                          Rf_asLogical(create) == TRUE) != 0) {
      free(h);
      Rf_error("mov: cannot attach parker");
    }
    h->has_pk = 1;
  }

  h->watch = mov_death_watch_start((long) Rf_asReal(pid), &h->fired,
                                   h->has_pk ? &h->pk : NULL);
  if (h->watch == NULL) {
    long p = (long) Rf_asReal(pid);
    if (h->has_pk) mov_parker_detach(&h->pk);
    free(h);
    Rf_error("mov: cannot watch pid %ld", p);
  }

  SEXP out = R_MakeExternalPtr(h, mov_death_tag, xp);
  R_RegisterCFinalizerEx(out, mov_death_finalizer, TRUE);
  return out;
}

static mov_death_handle *mov_death_handle_get(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mov_death_tag)
    Rf_error("mov: not a death-watch handle");
  mov_death_handle *h = (mov_death_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) Rf_error("mov: death-watch handle is stopped");
  return h;
}

SEXP mov_death_fired_call(SEXP xp) {
  mov_death_handle *h = mov_death_handle_get(xp);
  return Rf_ScalarLogical(
    atomic_load_explicit(&h->fired, memory_order_acquire));
}

SEXP mov_death_stop_call(SEXP xp) {
  mov_death_finalizer(xp);
  return R_NilValue;
}
