/* The .Call test surface over the core's liveness lock
   (vendor/librei/liveness.c): the verdict machinery is librei's. */

#include <stdlib.h>
#include "sora.h"

// .Call test surface -----------------------------------------------------------

static void sora_live_finalizer(SEXP xp) {
  void *addr = R_ExternalPtrAddr(xp);
  if (addr != NULL) {
    rei_live_close((intptr_t) addr);
    R_ClearExternalPtr(xp);
  }
}

static intptr_t sora_live_handle(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP) Rf_error("sora: not a liveness handle");
  void *addr = R_ExternalPtrAddr(xp);
  if (addr == NULL) Rf_error("sora: liveness handle is closed");
  return (intptr_t) addr;
}

SEXP sora_live_open_call(SEXP path) {
  if (TYPEOF(path) != STRSXP || XLENGTH(path) != 1)
    Rf_error("sora: expected a file path");
  intptr_t h;
  if (rei_live_open(CHAR(STRING_ELT(path, 0)), &h) != 0)
    Rf_error("sora: cannot open liveness file '%s'", CHAR(STRING_ELT(path, 0)));
  /* fd 0 / NULL handle cannot occur (R holds stdin), so NULL marks closed */
  SEXP xp = R_MakeExternalPtr((void *) h, R_NilValue, R_NilValue);
  R_RegisterCFinalizerEx(xp, sora_live_finalizer, TRUE);
  return xp;
}

SEXP sora_live_try_call(SEXP xp) {
  int rc = rei_live_try(sora_live_handle(xp));
  if (rc < 0) Rf_error("sora: liveness probe failed");
  return Rf_ScalarInteger(rc);
}

SEXP sora_live_close_call(SEXP xp) {
  sora_live_finalizer(xp);
  return R_NilValue;
}

SEXP sora_live_dir_call(void) {
  const char *dir = rei_live_dir();
  if (dir == NULL) Rf_error("sora: cannot resolve liveness lock directory");
  return Rf_mkString(dir);
}
