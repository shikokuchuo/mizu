/* GC extptr wrappers over the vendored finalizers — SHM lifetime is fully
   automatic, the same chained-finalizer discipline the view layer uses.
   Producer side: a rei_shm-tagged mapping extptr (munmap finalizer) chained
   to a rei_host-tagged extptr (unlink / CloseHandle finalizer, which also
   balances the macOS registry log) via its protected slot; the host copy
   gets the name / Windows creator handle, the mapping extptr keeps only the
   mapping, so the two fire independently and a consumer keeps reading after
   the host wrapper is GC'd. Consumer side: a single mapping extptr — munmap
   only, never unlink. All finalizers are session-exit registered. */

#include <stdlib.h>
#include <stdio.h>
#include "rei.h"

static SEXP rei_shm_tag;
static SEXP rei_host_tag;

void rei_wrap_init(void) {
  rei_shm_tag = Rf_install("rei_shm");
  rei_host_tag = Rf_install("rei_host");
}

SEXP rei_shm_wrap_consumer(rei_shm *shm) {
  SEXP ptr = R_MakeExternalPtr(shm, rei_shm_tag, R_NilValue);
  R_RegisterCFinalizerEx(ptr, rei_view_shm_finalizer, TRUE);
  return ptr;
}

/* The host half alone: a heap copy carrying the name (POSIX) / creator handle
   (Windows), finalizer -> unlink / CloseHandle. The channel uses this without
   the mapping extptr — its mapping must outlive GC ordering (the death
   listener's parker points into it), so the channel unmaps in its own
   release path and drives this one-shot at protocol time (close rendezvous /
   survivor cleanup), with GC as the fallback. Running the finalizer manually
   is safe: it clears the extptr, so the GC pass is a no-op. */
SEXP rei_shm_wrap_host(rei_shm *shm) {

  rei_shm *host = malloc(sizeof(rei_shm));
  if (host == NULL) Rf_error("rei: allocation failure");
  memcpy(host, shm, sizeof(rei_shm));
  host->addr = NULL;
  host->size = 0;
#ifdef _WIN32
  shm->handle = NULL;
#endif

  SEXP host_ptr = R_MakeExternalPtr(host, rei_host_tag, R_NilValue);
  R_RegisterCFinalizerEx(host_ptr, rei_view_host_finalizer, TRUE);
  return host_ptr;
}

SEXP rei_shm_wrap_producer(rei_shm *shm) {
  SEXP host_ptr = PROTECT(rei_shm_wrap_host(shm));
  SEXP shm_ptr = R_MakeExternalPtr(shm, rei_shm_tag, host_ptr);
  R_RegisterCFinalizerEx(shm_ptr, rei_view_shm_finalizer, TRUE);
  UNPROTECT(1);
  return shm_ptr;
}

/* Non-erroring unwrap for the spill free list: the region behind a
   rei_shm-tagged wrap, NULL for anything else (a finalized wrap's cleared
   pointer included). */
rei_shm *rei_shm_unwrap(SEXP x) {
  if (TYPEOF(x) != EXTPTRSXP || R_ExternalPtrTag(x) != rei_shm_tag)
    return NULL;
  return (rei_shm *) R_ExternalPtrAddr(x);
}

/* Shared by the .Call test surface across compilation units. */
rei_shm *rei_region(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != rei_shm_tag)
    Rf_error("rei: not a rei region handle");
  rei_shm *shm = (rei_shm *) R_ExternalPtrAddr(xp);
  if (shm == NULL || shm->addr == NULL)
    Rf_error("rei: region handle is closed");
  return shm;
}

// .Call surface -----------------------------------------------------------------

SEXP rei_region_create(SEXP size) {
  double sz = Rf_asReal(size);
  if (!(sz >= 1) || sz > 9.007199254740992e15)
    Rf_error("rei: invalid region size");

  rei_shm *shm;
  if (rei_shm_create(&shm, (size_t) sz) != REI_OK) {
    const char *summary, *hint;
    rei_err_describe(rei_last_error_category(), &summary, &hint);
    rei_stop_shm(sz,
                 "rei: cannot create region (requested %.0f bytes): %s%s%s",
                 sz, summary, hint[0] != '\0' ? ". " : "", hint);
  }
  return rei_shm_wrap_producer(shm);
}

SEXP rei_region_open(SEXP name, SEXP rw) {
  if (TYPEOF(name) != STRSXP || XLENGTH(name) != 1)
    Rf_error("rei: expected a region name");
  const char *nm = CHAR(STRING_ELT(name, 0));

  rei_shm *shm;
  rei_status st = Rf_asLogical(rw) == TRUE ?
    rei_shm_open_rw(&shm, nm, 1) : rei_shm_open(&shm, nm);
  if (st != REI_OK)
    rei_stop_shm(NA_REAL, "rei: cannot open region '%s'", nm);
  return rei_shm_wrap_consumer(shm);
}

SEXP rei_region_name(SEXP xp) {
  rei_shm *shm = rei_region(xp);
  return Rf_ScalarString(Rf_mkCharLenCE(shm->name, shm->name_len, CE_NATIVE));
}

SEXP rei_region_size(SEXP xp) {
  return Rf_ScalarReal((double) rei_region(xp)->size);
}

/* Raw byte access for tests (e.g. corrupting a preamble). Writing through a
   read-only consumer mapping faults — poke only host / rw handles. */
SEXP rei_peek(SEXP xp, SEXP offset, SEXP n) {
  rei_shm *shm = rei_region(xp);
  double off = Rf_asReal(offset), len = Rf_asReal(n);
  if (!(off >= 0) || !(len >= 0) || off + len > (double) shm->size)
    Rf_error("rei: peek out of bounds");
  SEXP out = Rf_allocVector(RAWSXP, (R_xlen_t) len);
  memcpy(RAW(out), (unsigned char *) shm->addr + (size_t) off, (size_t) len);
  return out;
}

SEXP rei_poke(SEXP xp, SEXP offset, SEXP bytes) {
  rei_shm *shm = rei_region(xp);
  if (TYPEOF(bytes) != RAWSXP) Rf_error("rei: expected a raw vector");
  double off = Rf_asReal(offset);
  size_t len = (size_t) XLENGTH(bytes);
  if (!(off >= 0) || off + (double) len > (double) shm->size)
    Rf_error("rei: poke out of bounds");
  memcpy((unsigned char *) shm->addr + (size_t) off, RAW(bytes), len);
  return R_NilValue;
}

/* Reap /rei_ orphans of dead creators via the vendored reaper. Returns the
   region names actually removed, or R_NilValue if none — including on
   platforms that cannot enumerate the SHM namespace (Windows, where a
   mapping cannot outlive its creator anyway). */
SEXP rei_prune_call(void) {
  int n = 0;
  char **list = rei_shm_reap(&n);
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
