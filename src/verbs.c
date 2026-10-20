/* The channel/pool veneer's shared helpers: the argument validations and
   the handle unwrap both veneer files need, written once. Raise-only
   helpers carry MIZU_COLD so their call sites stay off the hot verbs'
   code layout. */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "mizu.h"

/* A create-failure record carries the requested region size as "(N bytes)"
   (the core's shm/channel/pool create messages); mizu_error_shm's bytes
   field is that number structured. Open failures and validation records
   carry no size and stay NA. */
static double tls_record_bytes(const char *msg) {
  const char *p = strchr(msg, '(');
  if (p == NULL) return NA_REAL;
  errno = 0;
  char *end;
  unsigned long long v = strtoull(p + 1, &end, 10);
  if (end == p + 1 || errno == ERANGE || strncmp(end, " bytes)", 7) != 0)
    return NA_REAL;
  return (double) v;
}

/* Raise a create/attach/join failure off the thread-local slot, where the
   core composes the full message (size + hint included). Space/existence
   failures carry the shm class; everything else is a plain error. */
NORET void mizu_r_raise_tls(void) {
  mizu_errcat cat = mizu_last_error_category();
  const char *msg = mizu_last_error_message();
  switch (cat) {
  case MIZU_ERRCAT_NOSPACE:
  case MIZU_ERRCAT_NOMEMORY:
  case MIZU_ERRCAT_EXISTS:
    mizu_stop_shm(tls_record_bytes(msg), "mizu: %s", msg);
  default:
    Rf_error("mizu: %s", msg);
  }
}

int mizu_r_pow2(uint64_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

/* A create-geometry argument, gated before any range check: Rf_asInteger
   on a fractional double truncates (4.9 -> 4) and NA/Inf wrap, so
   integrality is rejected first and the per-argument range message stays
   the failure the caller raises next. */
double mizu_r_size_arg(SEXP x, const char *what) {
  const double d = Rf_asReal(x);
  if (ISNAN(d) || d < 0 || d >= 18446744073709551616.0 /* 2^64 */ ||
      d != (double) (uint64_t) d)
    Rf_error("mizu: %s must be a whole number", what);
  return d;
}

/* The region-name-suffix argument of the attach/join entries. NA is
   rejected here: CHAR(NA_STRING) is the literal "NA", which otherwise
   fails the core's name grammar with a misleading "malformed" error. */
const char *mizu_r_suffix_arg(SEXP suffix_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("mizu: expected a region-name suffix");
  SEXP s = STRING_ELT(suffix_sexp, 0);
  if (s == NA_STRING) Rf_error("mizu: suffix must not be NA");
  return CHAR(s);
}

/* The test-only identity override (helper.R's harnesses): an integer
   c(lang, caps) pair replaces this build's word — NULL keeps `dflt`. */
uint64_t mizu_r_ident_override(SEXP ident_sexp, uint64_t dflt) {
  if (ident_sexp == R_NilValue) return dflt;
  if (TYPEOF(ident_sexp) != INTSXP || XLENGTH(ident_sexp) != 2)
    Rf_error("mizu: expected an identity pair c(lang, caps)");
  return MIZU_IDENT((uint32_t) INTEGER(ident_sexp)[0],
                    (uint32_t) INTEGER(ident_sexp)[1]);
}

/* The seconds value of a verb's timeout: Rf_asReal coerces non-numeric
   input (a non-numeric string, a logical NA) to NA, and the core maps
   every non-finite value to an indefinite wait — an NA would park forever
   where a 0 or a negative returns at once. Non-coercible input fails in
   Rf_asReal itself. */
double mizu_r_timeout_arg(SEXP timeout_sexp) {
  const double t = Rf_asReal(timeout_sexp);
  if (ISNAN(t)) Rf_error("mizu: timeout must be a number of seconds, not NA");
  return t;
}

/* NULL when the handle has already been released (closed / destroyed).
   `what` is "channel" or "pool" and composes the exact messages the
   per-type wrappers always raised. */
mizu_r_handle *mizu_r_handle_peek(SEXP xp, SEXP tag, const char *what) {
  if (TYPEOF(xp) != EXTPTRSXP || R_ExternalPtrTag(xp) != tag)
    Rf_error("mizu: not a %s handle", what);
  mizu_r_handle *h = (mizu_r_handle *) R_ExternalPtrAddr(xp);
  if (h == NULL || h->core == NULL) return NULL;
  if (h->self_pid != mizu_self_pid())
    Rf_error("mizu: %s handles do not survive fork()", what);
  return h;
}

mizu_r_handle *mizu_r_handle_get(SEXP xp, SEXP tag, const char *what) {
  mizu_r_handle *h = mizu_r_handle_peek(xp, tag, what);
  if (h == NULL) Rf_error("mizu: %s handle is closed", what);
  return h;
}
