/* GC extptr wrappers over the vendored finalizers — SHM lifetime is fully
   automatic, the same chained-finalizer discipline mori uses internally.
   Producer side: a kio_shm-tagged mapping extptr (munmap finalizer) chained
   to a kio_host-tagged extptr (unlink / CloseHandle finalizer, which also
   balances the macOS registry log) via its protected slot; the host copy
   gets the name / Windows creator handle, the mapping extptr keeps only the
   mapping, so the two fire independently and a consumer keeps reading after
   the host wrapper is GC'd. Consumer side: a single mapping extptr — munmap
   only, never unlink. All finalizers are session-exit registered. */

#include <stdlib.h>
#include <stdio.h>
#include "kioto.h"

static SEXP kio_shm_tag;
static SEXP kio_host_tag;

void kio_wrap_init(void) {
  kio_shm_tag = Rf_install("kio_shm");
  kio_host_tag = Rf_install("kio_host");
}

SEXP kio_shm_wrap_consumer(mori_shm *shm) {
  SEXP ptr = R_MakeExternalPtr(shm, kio_shm_tag, R_NilValue);
  R_RegisterCFinalizerEx(ptr, mori_shm_finalizer, TRUE);
  return ptr;
}

/* The host half alone: a heap copy carrying the name (POSIX) / creator handle
   (Windows), finalizer -> unlink / CloseHandle. The channel uses this without
   the mapping extptr — its mapping must outlive GC ordering (the death
   listener's parker points into it), so the channel unmaps in its own
   release path and drives this one-shot at protocol time (close rendezvous /
   survivor cleanup), with GC as the fallback. Running the finalizer manually
   is safe: it clears the extptr, so the GC pass is a no-op. */
SEXP kio_shm_wrap_host(mori_shm *shm) {

  mori_shm *host = malloc(sizeof(mori_shm));
  if (host == NULL) Rf_error("kioto: allocation failure");
  memcpy(host, shm, sizeof(mori_shm));
  host->addr = NULL;
  host->size = 0;
#ifdef _WIN32
  shm->handle = NULL;
#endif

  SEXP host_ptr = R_MakeExternalPtr(host, kio_host_tag, R_NilValue);
  R_RegisterCFinalizerEx(host_ptr, mori_host_finalizer, TRUE);
  return host_ptr;
}

SEXP kio_shm_wrap_producer(mori_shm *shm) {
  SEXP host_ptr = PROTECT(kio_shm_wrap_host(shm));
  SEXP shm_ptr = R_MakeExternalPtr(shm, kio_shm_tag, host_ptr);
  R_RegisterCFinalizerEx(shm_ptr, mori_shm_finalizer, TRUE);
  UNPROTECT(1);
  return shm_ptr;
}

/* Non-erroring unwrap for the spill free list: the region behind a
   kio_shm-tagged wrap, NULL for anything else (a finalized wrap's cleared
   pointer included). */
mori_shm *kio_shm_unwrap(SEXP x) {
  if (TYPEOF(x) != EXTPTRSXP || R_ExternalPtrTag(x) != kio_shm_tag)
    return NULL;
  return (mori_shm *) R_ExternalPtrAddr(x);
}

/* Shared by the .Call test surface across compilation units. */
mori_shm *kio_region(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != kio_shm_tag)
    Rf_error("kioto: not a kioto region handle");
  mori_shm *shm = (mori_shm *) R_ExternalPtrAddr(xp);
  if (shm == NULL || shm->addr == NULL)
    Rf_error("kioto: region handle is closed");
  return shm;
}

// .Call surface -----------------------------------------------------------------

SEXP kio_region_create(SEXP size) {
  double sz = Rf_asReal(size);
  if (!(sz >= 1) || sz > 9.007199254740992e15)
    Rf_error("kioto: invalid region size");

  mori_shm *shm;
  int rc = mori_shm_create_heap(&shm, (size_t) sz);
  if (rc) {
    const char *summary, *hint;
    mori_err_describe(rc, &summary, &hint);
    kio_stop_shm(sz,
                 "kioto: cannot create region (requested %.0f bytes): %s%s%s",
                 sz, summary, hint[0] != '\0' ? ". " : "", hint);
  }
  return kio_shm_wrap_producer(shm);
}

SEXP kio_region_open(SEXP name, SEXP rw) {
  if (TYPEOF(name) != STRSXP || XLENGTH(name) != 1)
    Rf_error("kioto: expected a region name");
  const char *nm = CHAR(STRING_ELT(name, 0));

  mori_shm *shm = Rf_asLogical(rw) == TRUE ?
    kio_shm_open_rw_heap(nm, 1) : mori_shm_open_heap(nm);
  if (shm == NULL)
    kio_stop_shm(NA_REAL, "kioto: cannot open region '%s'", nm);
  return kio_shm_wrap_consumer(shm);
}

SEXP kio_region_name(SEXP xp) {
  mori_shm *shm = kio_region(xp);
  return Rf_ScalarString(Rf_mkCharLenCE(shm->name, shm->name_len, CE_NATIVE));
}

SEXP kio_region_size(SEXP xp) {
  return Rf_ScalarReal((double) kio_region(xp)->size);
}

/* Raw byte access for tests (e.g. corrupting a preamble). Writing through a
   read-only consumer mapping faults — poke only host / rw handles. */
SEXP kio_peek(SEXP xp, SEXP offset, SEXP n) {
  mori_shm *shm = kio_region(xp);
  double off = Rf_asReal(offset), len = Rf_asReal(n);
  if (!(off >= 0) || !(len >= 0) || off + len > (double) shm->size)
    Rf_error("kioto: peek out of bounds");
  SEXP out = Rf_allocVector(RAWSXP, (R_xlen_t) len);
  memcpy(RAW(out), (unsigned char *) shm->addr + (size_t) off, (size_t) len);
  return out;
}

SEXP kio_poke(SEXP xp, SEXP offset, SEXP bytes) {
  mori_shm *shm = kio_region(xp);
  if (TYPEOF(bytes) != RAWSXP) Rf_error("kioto: expected a raw vector");
  double off = Rf_asReal(offset);
  size_t len = (size_t) XLENGTH(bytes);
  if (!(off >= 0) || off + (double) len > (double) shm->size)
    Rf_error("kioto: poke out of bounds");
  memcpy((unsigned char *) shm->addr + (size_t) off, RAW(bytes), len);
  return R_NilValue;
}

/* Reap /kio_ orphans of dead creators via the vendored reaper. Returns the
   region names actually removed, or R_NilValue if none — including on
   platforms that cannot enumerate the SHM namespace (Windows, where a
   mapping cannot outlive its creator anyway). */
SEXP kio_prune_call(void) {
  int n = 0;
  char **list = mori_shm_reap(&n);
  if (n == 0) return R_NilValue;
  SEXP out = PROTECT(Rf_allocVector(STRSXP, n));
  for (int i = 0; i < n; i++) {
    SET_STRING_ELT(out, i, Rf_mkChar(list[i]));
    free(list[i]);
  }
  free(list);
  UNPROTECT(1);
  return out;
}
