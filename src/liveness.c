/* The .Call test surface over the core's liveness lock
   (vendor/libmizu/liveness.c): the verdict machinery is libmizu's. */

#include <stdlib.h>
#include "mizu.h"

// .Call test surface -----------------------------------------------------------

static void mizu_live_finalizer(SEXP xp) {
  void *addr = R_ExternalPtrAddr(xp);
  if (addr != NULL) {
    mizu_live_close((intptr_t) addr);
    R_ClearExternalPtr(xp);
  }
}

static intptr_t mizu_live_handle(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP) Rf_error("mizu: not a liveness handle");
  void *addr = R_ExternalPtrAddr(xp);
  if (addr == NULL) Rf_error("mizu: liveness handle is closed");
  return (intptr_t) addr;
}

SEXP mizu_live_open_call(SEXP path) {
  if (TYPEOF(path) != STRSXP || XLENGTH(path) != 1)
    Rf_error("mizu: expected a file path");
  intptr_t h;
  if (mizu_live_open(CHAR(STRING_ELT(path, 0)), &h) != 0)
    Rf_error("mizu: cannot open liveness file '%s'", CHAR(STRING_ELT(path, 0)));
  /* fd 0 / NULL handle cannot occur (R holds stdin), so NULL marks closed */
  SEXP xp = R_MakeExternalPtr((void *) h, R_NilValue, R_NilValue);
  R_RegisterCFinalizerEx(xp, mizu_live_finalizer, TRUE);
  return xp;
}

SEXP mizu_live_try_call(SEXP xp) {
  int rc = mizu_live_try(mizu_live_handle(xp));
  if (rc < 0) Rf_error("mizu: liveness probe failed");
  return Rf_ScalarInteger(rc);
}

SEXP mizu_live_close_call(SEXP xp) {
  mizu_live_finalizer(xp);
  return R_NilValue;
}

SEXP mizu_live_dir_call(void) {
  const char *dir = mizu_live_dir();
  if (dir == NULL) Rf_error("mizu: cannot resolve liveness lock directory");
  return Rf_mkString(dir);
}
