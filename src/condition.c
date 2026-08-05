/* Classed error conditions. Terminal failures a dependent can act on
   programmatically are signalled as structured R conditions — class
   c(<subclass>, "kio_error", "error", "condition"), NULL call, fields over
   message parsing — via base::stop(cond), which longjmps out of the
   Rf_eval. R-level errors join the hierarchy through stop_kio() in
   R/conditions.R; the class vectors are API. */

#include <stdarg.h>
#include <stdio.h>
#include "kioto.h"

/* Returns the PROTECTed condition with nf field slots after message / call
   left NULL for the caller to fill — the condition itself is their
   protection. */
static SEXP kio_cond(const char *subclass, const char **fnames, int nf,
                     const char *fmt, va_list ap) {
  char msg[1024];
  vsnprintf(msg, sizeof(msg), fmt, ap);
  SEXP cond = PROTECT(Rf_allocVector(VECSXP, 2 + nf));
  SEXP names = PROTECT(Rf_allocVector(STRSXP, 2 + nf));
  SET_VECTOR_ELT(cond, 0, Rf_mkString(msg));
  SET_STRING_ELT(names, 0, Rf_mkChar("message"));
  SET_STRING_ELT(names, 1, Rf_mkChar("call"));
  for (int i = 0; i < nf; i++)
    SET_STRING_ELT(names, 2 + i, Rf_mkChar(fnames[i]));
  Rf_setAttrib(cond, R_NamesSymbol, names);
  SEXP klass = PROTECT(Rf_allocVector(STRSXP, 4));
  SET_STRING_ELT(klass, 0, Rf_mkChar(subclass));
  SET_STRING_ELT(klass, 1, Rf_mkChar("kio_error"));
  SET_STRING_ELT(klass, 2, Rf_mkChar("error"));
  SET_STRING_ELT(klass, 3, Rf_mkChar("condition"));
  Rf_setAttrib(cond, R_ClassSymbol, klass);
  UNPROTECT(2);                    /* names, klass: reachable from cond */
  return cond;
}

NORET static void kio_cond_signal(SEXP cond) {
  SEXP call = PROTECT(Rf_lang2(Rf_install("stop"), cond));
  Rf_eval(call, R_BaseEnv);        /* no return */
  Rf_error("kioto: condition not signalled");
}

NORET void kio_stop(const char *subclass, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  SEXP cond = kio_cond(subclass, NULL, 0, fmt, ap);
  va_end(ap);
  kio_cond_signal(cond);
}

/* Sentinel-mode wrap: the condition boxed in a length-1 list of class
   "kio_caught", so a collect loop branches on class instead of arming a
   tryCatch handler. Only C wraps — a task value that is itself a
   condition comes back bare and is never mistaken for one. */
SEXP kio_caught(SEXP cond) {
  PROTECT(cond);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 1));
  SET_VECTOR_ELT(out, 0, cond);
  Rf_setAttrib(out, R_ClassSymbol, Rf_mkString("kio_caught"));
  UNPROTECT(2);
  return out;
}

SEXP kio_caught_cond(const char *subclass, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  SEXP cond = kio_cond(subclass, NULL, 0, fmt, ap);
  va_end(ap);
  SEXP out = kio_caught(cond);
  UNPROTECT(1);
  return out;
}

NORET void kio_stop_shm(double bytes, const char *fmt, ...) {
  static const char *fnames[] = { "bytes" };
  va_list ap;
  va_start(ap, fmt);
  SEXP cond = kio_cond("kio_error_shm", fnames, 1, fmt, ap);
  va_end(ap);
  SET_VECTOR_ELT(cond, 2, Rf_ScalarReal(bytes));
  kio_cond_signal(cond);
}

static SEXP kio_cond_died(int slot, double pid, const char *fmt,
                          va_list ap) {
  static const char *fnames[] = { "slot", "pid" };
  SEXP cond = kio_cond("kio_error_worker_died", fnames, 2, fmt, ap);
  SET_VECTOR_ELT(cond, 2, Rf_ScalarInteger(slot < 0 ? NA_INTEGER : slot));
  SET_VECTOR_ELT(cond, 3, Rf_ScalarReal(pid <= 0 ? NA_REAL : pid));
  return cond;
}

NORET void kio_stop_died(int slot, double pid, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  SEXP cond = kio_cond_died(slot, pid, fmt, ap);
  va_end(ap);
  kio_cond_signal(cond);
}

SEXP kio_caught_died(int slot, double pid, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  SEXP cond = kio_cond_died(slot, pid, fmt, ap);
  va_end(ap);
  SEXP out = kio_caught(cond);
  UNPROTECT(1);
  return out;
}
