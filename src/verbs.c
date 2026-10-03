/* The channel/pool veneer's shared helpers: the argument validations and
   the handle unwrap both veneer files need, written once. Raise-only
   helpers carry MIZU_COLD so their call sites stay off the hot verbs'
   code layout. */

#include "mizu.h"

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
    mizu_stop_shm(NA_REAL, "mizu: %s", msg);
  default:
    Rf_error("mizu: %s", msg);
  }
}

int mizu_r_pow2(uint64_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

/* The region-name-suffix argument of the attach/join entries. */
const char *mizu_r_suffix_arg(SEXP suffix_sexp) {
  if (TYPEOF(suffix_sexp) != STRSXP || XLENGTH(suffix_sexp) != 1)
    Rf_error("mizu: expected a region-name suffix");
  return CHAR(STRING_ELT(suffix_sexp, 0));
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
