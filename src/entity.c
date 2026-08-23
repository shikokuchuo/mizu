/* .Call surface over the per-entity parker and the death listener, keyed by
   the fixed channel layout's entity blocks (host 128, peer 192; the park
   epoch is the block's first word). Part I's channel binds these same
   primitives; this surface exists so Phase 0 can exercise them from R. */

#include <stdlib.h>
#include "rei.h"

rei_shm *rei_region(SEXP xp);   /* wrap.c */

static _Atomic uint32_t *rei_entity_epoch(rei_shm *shm, int entity) {
  if (entity != REI_ENTITY_HOST && entity != REI_ENTITY_PEER)
    Rf_error("rei: invalid entity");
  if (shm->size < REI_ENTITY_OFFSET(entity) + 64)
    Rf_error("rei: region too small for an entity block");
  return (_Atomic uint32_t *) ((char *) shm->addr + REI_ENTITY_OFFSET(entity));
}

// Parker ------------------------------------------------------------------------

SEXP rei_park_call(SEXP xp, SEXP entity, SEXP timeout_ms, SEXP create) {
  rei_shm *shm = rei_region(xp);
  int ent = Rf_asInteger(entity);

  rei_parker pk;
  if (rei_parker_attach(&pk, rei_entity_epoch(shm, ent), shm->name, ent,
                        Rf_asLogical(create) == TRUE) != 0)
    Rf_error("rei: cannot attach parker");
  int rc = rei_park(&pk, rei_parker_snapshot(&pk),
                    (long) Rf_asInteger(timeout_ms));
  rei_parker_detach(&pk);
  return Rf_ScalarInteger(rc);
}

SEXP rei_unpark_call(SEXP xp, SEXP entity, SEXP create) {
  rei_shm *shm = rei_region(xp);
  int ent = Rf_asInteger(entity);

  rei_parker pk;
  if (rei_parker_attach(&pk, rei_entity_epoch(shm, ent), shm->name, ent,
                        Rf_asLogical(create) == TRUE) != 0)
    Rf_error("rei: cannot attach parker");
  rei_unpark(&pk);
  rei_parker_detach(&pk);
  return R_NilValue;
}

SEXP rei_epoch_call(SEXP xp, SEXP entity) {
  rei_shm *shm = rei_region(xp);
  _Atomic uint32_t *epoch = rei_entity_epoch(shm, Rf_asInteger(entity));
  return Rf_ScalarReal((double)
    atomic_load_explicit(epoch, memory_order_acquire));
}

// Death listener ------------------------------------------------------------------

typedef struct rei_death_handle_s {
  _Atomic int fired;
  rei_parker pk;
  int has_pk;
  rei_death_watch *watch;
} rei_death_handle;

static SEXP rei_death_tag;

void rei_entity_init(void) {
  rei_death_tag = Rf_install("rei_death");
}

static void rei_death_finalizer(SEXP xp) {
  rei_death_handle *h = (rei_death_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) return;
  rei_death_watch_stop(h->watch);   /* synchronizes with in-flight callbacks */
  if (h->has_pk) rei_parker_detach(&h->pk);
  free(h);
  R_ClearExternalPtr(xp);
}

/* Watch pid; on its exit the handle's fired flag is set and, when a region +
   entity is supplied, that entity's parker is unparked. The region extptr
   rides in the handle's protected slot so the epoch word outlives the watch. */
SEXP rei_death_watch_call(SEXP pid, SEXP xp, SEXP entity, SEXP create) {
  rei_death_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) Rf_error("rei: allocation failure");

  if (xp != R_NilValue) {
    rei_shm *shm = rei_region(xp);
    int ent = Rf_asInteger(entity);
    if (rei_parker_attach(&h->pk, rei_entity_epoch(shm, ent), shm->name, ent,
                          Rf_asLogical(create) == TRUE) != 0) {
      free(h);
      Rf_error("rei: cannot attach parker");
    }
    h->has_pk = 1;
  }

  h->watch = rei_death_watch_start((long) Rf_asReal(pid), &h->fired,
                                   h->has_pk ? &h->pk : NULL);
  if (h->watch == NULL) {
    long p = (long) Rf_asReal(pid);
    if (h->has_pk) rei_parker_detach(&h->pk);
    free(h);
    Rf_error("rei: cannot watch pid %ld", p);
  }

  SEXP out = R_MakeExternalPtr(h, rei_death_tag, xp);
  R_RegisterCFinalizerEx(out, rei_death_finalizer, TRUE);
  return out;
}

static rei_death_handle *rei_death_handle_get(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != rei_death_tag)
    Rf_error("rei: not a death-watch handle");
  rei_death_handle *h = (rei_death_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) Rf_error("rei: death-watch handle is stopped");
  return h;
}

SEXP rei_death_fired_call(SEXP xp) {
  rei_death_handle *h = rei_death_handle_get(xp);
  return Rf_ScalarLogical(
    atomic_load_explicit(&h->fired, memory_order_acquire));
}

SEXP rei_death_stop_call(SEXP xp) {
  rei_death_handle_get(xp);   /* validates type, tag, and live address */
  rei_death_finalizer(xp);
  return R_NilValue;
}
