/* .Call surface over the per-entity parker and the death listener, keyed by
   the fixed channel layout's entity blocks (host 128, peer 192; the park
   epoch is the block's first word). Part I's channel binds these same
   primitives; this surface exists so Phase 0 can exercise them from R. */

#include <stdlib.h>
#include "kioto.h"

mori_shm *kio_region(SEXP xp);   /* wrap.c */

kio_death_watch *kio_death_watch_start(long pid, _Atomic int *flag,
                                       const kio_parker *pk) {
  return kio_death_watch_start2(pid, flag, pk, NULL, NULL);
}

static _Atomic uint32_t *kio_entity_epoch(mori_shm *shm, int entity) {
  if (entity != KIO_ENTITY_HOST && entity != KIO_ENTITY_PEER)
    Rf_error("kioto: invalid entity");
  if (shm->size < KIO_ENTITY_OFFSET(entity) + 64)
    Rf_error("kioto: region too small for an entity block");
  return (_Atomic uint32_t *) ((char *) shm->addr + KIO_ENTITY_OFFSET(entity));
}

// Parker ------------------------------------------------------------------------

SEXP kio_park_call(SEXP xp, SEXP entity, SEXP timeout_ms, SEXP create) {
  mori_shm *shm = kio_region(xp);
  int ent = Rf_asInteger(entity);

  kio_parker pk;
  if (kio_parker_attach(&pk, kio_entity_epoch(shm, ent), shm->name, ent,
                        Rf_asLogical(create) == TRUE) != 0)
    Rf_error("kioto: cannot attach parker");
  int rc = kio_park(&pk, kio_parker_snapshot(&pk),
                    (long) Rf_asInteger(timeout_ms));
  kio_parker_detach(&pk);
  return Rf_ScalarInteger(rc);
}

SEXP kio_unpark_call(SEXP xp, SEXP entity, SEXP create) {
  mori_shm *shm = kio_region(xp);
  int ent = Rf_asInteger(entity);

  kio_parker pk;
  if (kio_parker_attach(&pk, kio_entity_epoch(shm, ent), shm->name, ent,
                        Rf_asLogical(create) == TRUE) != 0)
    Rf_error("kioto: cannot attach parker");
  kio_unpark(&pk);
  kio_parker_detach(&pk);
  return R_NilValue;
}

SEXP kio_epoch_call(SEXP xp, SEXP entity) {
  mori_shm *shm = kio_region(xp);
  _Atomic uint32_t *epoch = kio_entity_epoch(shm, Rf_asInteger(entity));
  return Rf_ScalarReal((double)
    atomic_load_explicit(epoch, memory_order_acquire));
}

// Death listener ------------------------------------------------------------------

typedef struct kio_death_handle_s {
  _Atomic int fired;
  kio_parker pk;
  int has_pk;
  kio_death_watch *watch;
} kio_death_handle;

static SEXP kio_death_tag;

void kio_entity_init(void) {
  kio_death_tag = Rf_install("kio_death");
}

static void kio_death_finalizer(SEXP xp) {
  kio_death_handle *h = (kio_death_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) return;
  kio_death_watch_stop(h->watch);   /* synchronizes with in-flight callbacks */
  if (h->has_pk) kio_parker_detach(&h->pk);
  free(h);
  R_ClearExternalPtr(xp);
}

/* Watch pid; on its exit the handle's fired flag is set and, when a region +
   entity is supplied, that entity's parker is unparked. The region extptr
   rides in the handle's protected slot so the epoch word outlives the watch. */
SEXP kio_death_watch_call(SEXP pid, SEXP xp, SEXP entity, SEXP create) {
  kio_death_handle *h = calloc(1, sizeof(*h));
  if (h == NULL) Rf_error("kioto: allocation failure");

  if (xp != R_NilValue) {
    mori_shm *shm = kio_region(xp);
    int ent = Rf_asInteger(entity);
    if (kio_parker_attach(&h->pk, kio_entity_epoch(shm, ent), shm->name, ent,
                          Rf_asLogical(create) == TRUE) != 0) {
      free(h);
      Rf_error("kioto: cannot attach parker");
    }
    h->has_pk = 1;
  }

  h->watch = kio_death_watch_start((long) Rf_asReal(pid), &h->fired,
                                   h->has_pk ? &h->pk : NULL);
  if (h->watch == NULL) {
    long p = (long) Rf_asReal(pid);
    if (h->has_pk) kio_parker_detach(&h->pk);
    free(h);
    Rf_error("kioto: cannot watch pid %ld", p);
  }

  SEXP out = R_MakeExternalPtr(h, kio_death_tag, xp);
  R_RegisterCFinalizerEx(out, kio_death_finalizer, TRUE);
  return out;
}

static kio_death_handle *kio_death_handle_get(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != kio_death_tag)
    Rf_error("kioto: not a death-watch handle");
  kio_death_handle *h = (kio_death_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL) Rf_error("kioto: death-watch handle is stopped");
  return h;
}

SEXP kio_death_fired_call(SEXP xp) {
  kio_death_handle *h = kio_death_handle_get(xp);
  return Rf_ScalarLogical(
    atomic_load_explicit(&h->fired, memory_order_acquire));
}

SEXP kio_death_stop_call(SEXP xp) {
  kio_death_finalizer(xp);
  return R_NilValue;
}
