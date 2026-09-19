/* GC extptr wrappers over the vendored finalizers — SHM lifetime is fully
   automatic, the same chained-finalizer discipline the view layer uses.
   Producer side: a mizu_shm-tagged mapping extptr (munmap finalizer) chained
   to a mizu_host-tagged extptr (unlink / CloseHandle finalizer, which also
   balances the macOS registry log) via its protected slot; the host copy
   gets the name / Windows creator handle, the mapping extptr keeps only the
   mapping, so the two fire independently and a consumer keeps reading after
   the host wrapper is GC'd. Consumer side: a single mapping extptr — munmap
   only, never unlink. All finalizers are session-exit registered. */

#include <stdlib.h>
#include <stdio.h>
#include "mizu.h"

static SEXP mizu_shm_tag;
static SEXP mizu_host_tag;

void mizu_wrap_init(void) {
  mizu_shm_tag = Rf_install("mizu_shm");
  mizu_host_tag = Rf_install("mizu_host");
}

SEXP mizu_shm_wrap_consumer(mizu_shm *shm) {
  SEXP ptr = R_MakeExternalPtr(shm, mizu_shm_tag, R_NilValue);
  R_RegisterCFinalizerEx(ptr, mizu_view_shm_finalizer, TRUE);
  return ptr;
}

/* The host half alone: a heap copy carrying the name (POSIX) / creator handle
   (Windows), finalizer -> unlink / CloseHandle. The channel uses this without
   the mapping extptr — its mapping must outlive GC ordering (the death
   listener's parker points into it), so the channel unmaps in its own
   release path and drives this one-shot at protocol time (close rendezvous /
   survivor cleanup), with GC as the fallback. Running the finalizer manually
   is safe: it clears the extptr, so the GC pass is a no-op. */
SEXP mizu_shm_wrap_host(mizu_shm *shm) {

  mizu_shm *host = malloc(sizeof(mizu_shm));
  if (host == NULL) Rf_error("mizu: allocation failure");
  memcpy(host, shm, sizeof(mizu_shm));
  host->addr = NULL;
  host->size = 0;
#ifdef _WIN32
  shm->handle = NULL;
#endif

  SEXP host_ptr = R_MakeExternalPtr(host, mizu_host_tag, R_NilValue);
  R_RegisterCFinalizerEx(host_ptr, mizu_view_host_finalizer, TRUE);
  return host_ptr;
}

SEXP mizu_shm_wrap_producer(mizu_shm *shm) {
  SEXP host_ptr = PROTECT(mizu_shm_wrap_host(shm));
  SEXP shm_ptr = R_MakeExternalPtr(shm, mizu_shm_tag, host_ptr);
  R_RegisterCFinalizerEx(shm_ptr, mizu_view_shm_finalizer, TRUE);
  UNPROTECT(1);
  return shm_ptr;
}

/* Non-erroring unwrap for the spill free list: the region behind a
   mizu_shm-tagged wrap, NULL for anything else (a finalized wrap's cleared
   pointer included). */
mizu_shm *mizu_shm_unwrap(SEXP x) {
  if (TYPEOF(x) != EXTPTRSXP || R_ExternalPtrTag(x) != mizu_shm_tag)
    return NULL;
  return (mizu_shm *) R_ExternalPtrAddr(x);
}

/* Shared by the .Call test surface across compilation units. */
mizu_shm *mizu_region(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != mizu_shm_tag)
    Rf_error("mizu: not a mizu region handle");
  mizu_shm *shm = (mizu_shm *) R_ExternalPtrAddr(xp);
  if (shm == NULL || shm->addr == NULL)
    Rf_error("mizu: region handle is closed");
  return shm;
}

// .Call surface -----------------------------------------------------------------

SEXP mizu_region_create(SEXP size) {
  double sz = Rf_asReal(size);
  if (!(sz >= 1) || sz > 9.007199254740992e15)
    Rf_error("mizu: invalid region size");

  mizu_shm *shm;
  if (mizu_shm_create(&shm, (size_t) sz) != MIZU_OK) {
    const char *summary, *hint;
    mizu_err_describe(mizu_last_error_category(), &summary, &hint);
    mizu_stop_shm(sz,
                 "mizu: cannot create region (requested %.0f bytes): %s%s%s",
                 sz, summary, hint[0] != '\0' ? ". " : "", hint);
  }
  return mizu_shm_wrap_producer(shm);
}

SEXP mizu_region_open(SEXP name, SEXP rw) {
  if (TYPEOF(name) != STRSXP || XLENGTH(name) != 1)
    Rf_error("mizu: expected a region name");
  const char *nm = CHAR(STRING_ELT(name, 0));

  mizu_shm *shm;
  mizu_status st = Rf_asLogical(rw) == TRUE ?
    mizu_shm_open_rw(&shm, nm, 1) : mizu_shm_open(&shm, nm);
  if (st != MIZU_OK)
    mizu_stop_shm(NA_REAL, "mizu: cannot open region '%s'", nm);
  return mizu_shm_wrap_consumer(shm);
}

SEXP mizu_region_name(SEXP xp) {
  mizu_shm *shm = mizu_region(xp);
  return Rf_ScalarString(Rf_mkCharLenCE(shm->name, shm->name_len, CE_NATIVE));
}

SEXP mizu_region_size(SEXP xp) {
  return Rf_ScalarReal((double) mizu_region(xp)->size);
}

/* Raw byte access for tests (e.g. corrupting a preamble). Writing through a
   read-only consumer mapping faults — poke only host / rw handles. */
SEXP mizu_peek(SEXP xp, SEXP offset, SEXP n) {
  mizu_shm *shm = mizu_region(xp);
  double off = Rf_asReal(offset), len = Rf_asReal(n);
  if (!(off >= 0) || !(len >= 0) || off + len > (double) shm->size)
    Rf_error("mizu: peek out of bounds");
  SEXP out = Rf_allocVector(RAWSXP, (R_xlen_t) len);
  memcpy(RAW(out), (unsigned char *) shm->addr + (size_t) off, (size_t) len);
  return out;
}

SEXP mizu_poke(SEXP xp, SEXP offset, SEXP bytes) {
  mizu_shm *shm = mizu_region(xp);
  if (TYPEOF(bytes) != RAWSXP) Rf_error("mizu: expected a raw vector");
  double off = Rf_asReal(offset);
  size_t len = (size_t) XLENGTH(bytes);
  if (!(off >= 0) || off + (double) len > (double) shm->size)
    Rf_error("mizu: poke out of bounds");
  memcpy((unsigned char *) shm->addr + (size_t) off, RAW(bytes), len);
  return R_NilValue;
}

/* Reap /mizu_ orphans of dead creators via the vendored reaper. Returns the
   region names actually removed, or R_NilValue if none — including on
   platforms that cannot enumerate the SHM namespace (Windows, where a
   mapping cannot outlive its creator anyway). */
SEXP mizu_prune_call(void) {
  int n = 0;
  char **list = mizu_shm_reap(&n);
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
