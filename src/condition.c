/* Classed error conditions. Terminal failures a dependent can act on
   programmatically are signalled as structured R conditions — class
   c(<subclass>, "rei_error", "error", "condition"), NULL call, fields over
   message parsing — via base::stop(cond), which longjmps out of the
   Rf_eval. R-level errors join the hierarchy through stop_rei() in
   R/conditions.R; the class vectors are API. */

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include "rei.h"

/* Returns the condition with nf field slots after message / call left
   NULL for the caller to fill, UNPROTECTED — the caller PROTECTs at the
   call site (nothing allocates between). */
static SEXP rei_cond(const char *subclass, const char **fnames, int nf,
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
  SET_STRING_ELT(klass, 1, Rf_mkChar("rei_error"));
  SET_STRING_ELT(klass, 2, Rf_mkChar("error"));
  SET_STRING_ELT(klass, 3, Rf_mkChar("condition"));
  Rf_setAttrib(cond, R_ClassSymbol, klass);
  UNPROTECT(3);                    /* cond, names, klass */
  return cond;
}

NORET void rei_cond_signal(SEXP cond) {
  SEXP call = PROTECT(Rf_lang2(Rf_install("stop"), cond));
  Rf_eval(call, R_BaseEnv);        /* no return */
  Rf_error("rei: condition not signalled");
}

/* Set the "index" field on a condition list with `$<-` semantics: replace
   in place when the name is present, else append one element (attributes,
   class included, carried over). Returns the condition UNPROTECTED — the
   caller PROTECTs at the call site (nothing allocates between). */
SEXP rei_cond_set_index(SEXP cond, int index) {
  PROTECT(cond);
  SEXP names = Rf_getAttrib(cond, R_NamesSymbol);
  R_xlen_t n = XLENGTH(cond);
  R_xlen_t nn = TYPEOF(names) == STRSXP ? XLENGTH(names) : 0;
  R_xlen_t m = n < nn ? n : nn;
  for (R_xlen_t i = 0; i < m; i++) {
    SEXP nm = STRING_ELT(names, i);
    if (nm != NA_STRING && strcmp(CHAR(nm), "index") == 0) {
      SET_VECTOR_ELT(cond, i, Rf_ScalarInteger(index));
      UNPROTECT(1);
      return cond;
    }
  }
  PROTECT(names);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, n + 1));
  SEXP onames = PROTECT(Rf_allocVector(STRSXP, n + 1));
  for (R_xlen_t i = 0; i < n; i++) {
    SET_VECTOR_ELT(out, i, VECTOR_ELT(cond, i));
    SET_STRING_ELT(onames, i, i < nn ? STRING_ELT(names, i) : R_BlankString);
  }
  SET_VECTOR_ELT(out, n, Rf_ScalarInteger(index));
  SET_STRING_ELT(onames, n, Rf_mkChar("index"));
  Rf_setAttrib(out, R_NamesSymbol, onames);
  Rf_copyMostAttrib(cond, out);
  UNPROTECT(4);                  /* cond, names, out, onames */
  return out;
}

NORET void rei_stop(const char *subclass, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  SEXP cond = PROTECT(rei_cond(subclass, NULL, 0, fmt, ap));
  va_end(ap);
  rei_cond_signal(cond);
  UNPROTECT(1);                  /* unreachable: rei_cond_signal is NORET */
}

/* Sentinel-mode wrap: the condition boxed in a length-1 list of class
   "rei_caught", so a collect loop branches on class instead of arming a
   tryCatch handler. Only C wraps — a task value that is itself a
   condition comes back bare and is never mistaken for one. */
SEXP rei_caught(SEXP cond) {
  PROTECT(cond);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 1));
  SET_VECTOR_ELT(out, 0, cond);
  Rf_setAttrib(out, R_ClassSymbol, Rf_mkString("rei_caught"));
  UNPROTECT(2);
  return out;
}

SEXP rei_caught_cond(const char *subclass, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  SEXP cond = PROTECT(rei_cond(subclass, NULL, 0, fmt, ap));
  va_end(ap);
  SEXP out = rei_caught(cond);
  UNPROTECT(1);
  return out;
}

NORET void rei_stop_shm(double bytes, const char *fmt, ...) {
  static const char *fnames[] = { "bytes" };
  va_list ap;
  va_start(ap, fmt);
  SEXP cond = PROTECT(rei_cond("rei_error_shm", fnames, 1, fmt, ap));
  va_end(ap);
  SET_VECTOR_ELT(cond, 2, Rf_ScalarReal(bytes));
  rei_cond_signal(cond);
  UNPROTECT(1);                  /* unreachable: rei_cond_signal is NORET */
}

static SEXP rei_cond_died(int slot, double pid, const char *fmt,
                          va_list ap) {
  static const char *fnames[] = { "slot", "pid" };
  SEXP cond = PROTECT(rei_cond("rei_error_worker_died", fnames, 2, fmt,
                                ap));
  SET_VECTOR_ELT(cond, 2, Rf_ScalarInteger(slot < 0 ? NA_INTEGER : slot));
  SET_VECTOR_ELT(cond, 3, Rf_ScalarReal(pid <= 0 ? NA_REAL : pid));
  UNPROTECT(1);
  return cond;   /* UNPROTECTED: the caller PROTECTs at the call site */
}

NORET void rei_stop_died(int slot, double pid, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  SEXP cond = PROTECT(rei_cond_died(slot, pid, fmt, ap));
  va_end(ap);
  rei_cond_signal(cond);
  UNPROTECT(1);                  /* unreachable: rei_cond_signal is NORET */
}

SEXP rei_caught_died(int slot, double pid, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  SEXP cond = PROTECT(rei_cond_died(slot, pid, fmt, ap));
  va_end(ap);
  SEXP out = rei_caught(cond);
  UNPROTECT(1);
  return out;
}

/* Task-error transport conditions (the pool's ERR publish path). The caught
   condition never crosses itself: a hostile field — an environment whose
   graph holds a connection, an external pointer, a pathologically deep
   tree — would raise or zombie inside staging, and a raise at publish is
   infrastructure failure (the worker exits; the collector misattributes
   the cause as rei_error_worker_died). Flatten instead, into a condition
   that is safe by construction:

   - Fields are read directly off the condition: message, call, the named
     elements. No conditionMessage / conditionCall — user methods can
     raise, and no user code runs on this path (what makes R_UnwindProtect
     unnecessary).
   - The message crosses when it is a length-1 string — translated to
     UTF-8, truncated at a character boundary past its budget share (half
     the whole-condition budget, so call and fields keep room) — else a
     fixed fallback. The class attribute crosses verbatim. A caught
     condition that is not a VECSXP (stop can signal any classed object)
     flattens to the fallback message with its class carried and no fields.
   - call and every named field cross iff the codec can carry them within
     the budget remaining: the codec writer is total — it emits a stream
     or fails cleanly — so the codec attempt is the safety check, with no
     separate walker. Retention priority: message, call, fields in order,
     dropped_fields. No per-field cap: one field may use the whole
     remaining budget; later fields drop-and-name on overflow. Elements
     without a name cannot be named there and drop silently.
   - Everything spends from the one budget — the target result slot's
     inline budget — so the transport condition stages INLINE and the
     publish cannot fail. One verification pass closes assembly; on
     overflow the terminal fallback (fallback message, no call, no fields,
     dropped_fields retained) is a handful of bytes and fits wherever a
     classed condition fits at all. */

#define REI_COND_FALLBACK "rei: task error (untransportable condition)"

/* The message element: the condition's own message field when it is a
   length-1 string (translated to UTF-8 and truncated at a character
   boundary past share bytes), else the fixed fallback. Returns a
   CHARSXP. */
static SEXP rei_cond_message(SEXP cond, size_t share) {
  SEXP msg = R_NilValue;
  if (TYPEOF(cond) == VECSXP) {
    SEXP names = Rf_getAttrib(cond, R_NamesSymbol);
    if (TYPEOF(names) == STRSXP) {
      R_xlen_t n = XLENGTH(cond);
      if (n > XLENGTH(names)) n = XLENGTH(names);
      for (R_xlen_t i = 0; i < n; i++) {
        SEXP nm = STRING_ELT(names, i);
        if (nm != NA_STRING && strcmp(CHAR(nm), "message") == 0) {
          SEXP m = VECTOR_ELT(cond, i);
          if (TYPEOF(m) == STRSXP && XLENGTH(m) == 1 &&
              STRING_ELT(m, 0) != NA_STRING)
            msg = STRING_ELT(m, 0);
          break;
        }
      }
    }
  }
  if (msg == R_NilValue) return Rf_mkChar(REI_COND_FALLBACK);
  cetype_t ce = Rf_getCharCE(msg);
  if (ce != CE_BYTES) {
    /* translate to UTF-8 so the boundary truncation below is valid; the
       translated bytes are re-interned before any allocation */
    msg = Rf_mkCharCE(Rf_translateCharUTF8(msg), CE_UTF8);
    ce = CE_UTF8;
  }
  if ((size_t) LENGTH(msg) <= share) return msg;
  PROTECT(msg);                           /* pins CHAR(msg) over the copy */
  int len = (int) share;
  const char *p = CHAR(msg);
  if (ce != CE_BYTES)
    while (len > 0 && (p[len] & 0xC0) == 0x80) len--;   /* UTF-8 boundary */
  SEXP out = Rf_mkCharLenCE(p, len, ce);
  UNPROTECT(1);
  return out;
}

/* Assemble a transport condition: list(message, call?, kept fields...,
   dropped_fields?) with class carried verbatim. mark[i] == 2 selects a
   kept field. Returns the condition UNPROTECTED — the caller PROTECTs at
   the call site (nothing allocates between). */
static SEXP rei_cond_build(SEXP msg, SEXP cond, SEXP names,
                            const unsigned char *mark, R_xlen_t n,
                            R_xlen_t call_idx, SEXP dropped, R_xlen_t ndrop,
                            SEXP klass) {
  R_xlen_t nkeep = 0;
  for (R_xlen_t i = 0; i < n; i++)
    if (mark[i] == 2) nkeep++;
  R_xlen_t outn = 1 + (call_idx >= 0 ? 1 : 0) + nkeep + (ndrop > 0 ? 1 : 0);
  SEXP flat = PROTECT(Rf_allocVector(VECSXP, outn));
  SEXP fnames = PROTECT(Rf_allocVector(STRSXP, outn));
  R_xlen_t k = 0;
  SEXP msgs = Rf_allocVector(STRSXP, 1);
  SET_STRING_ELT(msgs, 0, msg);
  SET_VECTOR_ELT(flat, k, msgs);
  SET_STRING_ELT(fnames, k, Rf_mkChar("message"));
  k++;
  if (call_idx >= 0) {
    SET_VECTOR_ELT(flat, k, VECTOR_ELT(cond, call_idx));
    SET_STRING_ELT(fnames, k, Rf_mkChar("call"));
    k++;
  }
  for (R_xlen_t i = 0; i < n; i++) {
    if (mark[i] != 2) continue;
    SET_VECTOR_ELT(flat, k, VECTOR_ELT(cond, i));
    SET_STRING_ELT(fnames, k, STRING_ELT(names, i));
    k++;
  }
  if (ndrop > 0) {
    SEXP df = Rf_allocVector(STRSXP, ndrop);
    for (R_xlen_t i = 0; i < ndrop; i++)
      SET_STRING_ELT(df, i, STRING_ELT(dropped, i));
    SET_VECTOR_ELT(flat, k, df);
    SET_STRING_ELT(fnames, k, Rf_mkChar("dropped_fields"));
    k++;
  }
  Rf_setAttrib(flat, R_NamesSymbol, fnames);
  Rf_classgets(flat, klass);
  UNPROTECT(2);                    /* flat, fnames */
  return flat;
}

/* Flatten a caught task condition for the ERR publish; budget is the
   target result slot's inline budget. Returns the transport condition
   UNPROTECTED — nothing allocates between the last internal allocation
   and return, so the caller's immediate PROTECT is safe. */
SEXP rei_condition_flatten(SEXP cond, size_t budget) {
  SEXP msg = PROTECT(rei_cond_message(cond, budget / 2));
  SEXP klass = Rf_getAttrib(cond, R_ClassSymbol);
  if (TYPEOF(klass) != STRSXP || XLENGTH(klass) == 0) {
    klass = PROTECT(Rf_allocVector(STRSXP, 2));
    SET_STRING_ELT(klass, 0, Rf_mkChar("error"));
    SET_STRING_ELT(klass, 1, Rf_mkChar("condition"));
  } else {
    PROTECT(klass);
  }

  /* the named elements of a VECSXP condition; anything else carries none */
  SEXP names = R_NilValue;
  R_xlen_t n = 0;
  if (TYPEOF(cond) == VECSXP) {
    SEXP nm = Rf_getAttrib(cond, R_NamesSymbol);
    if (TYPEOF(nm) == STRSXP) {
      names = nm;
      n = XLENGTH(cond);
      if (n > XLENGTH(names)) n = XLENGTH(names);
    }
  }

  SEXP mark = PROTECT(Rf_allocVector(RAWSXP, n > 0 ? n : 1));
  unsigned char *mk = RAW(mark);
  memset(mk, 0, (size_t) (n > 0 ? n : 1));
  SEXP dropped = PROTECT(Rf_allocVector(STRSXP, n + 1));
  R_xlen_t ndrop = 0;

  /* classify: the first "message" feeds the message element, the first
     "call" is vetted ahead of the fields, the rest are fields in order */
  R_xlen_t call_idx = -1;
  int msg_seen = 0;
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP nm = STRING_ELT(names, i);
    if (nm == NA_STRING || LENGTH(nm) == 0) continue;
    const char *s = CHAR(nm);
    if (!msg_seen && strcmp(s, "message") == 0) {
      msg_seen = 1;
    } else if (call_idx < 0 && strcmp(s, "call") == 0) {
      call_idx = i;
    } else {
      mk[i] = 1;                 /* candidate field */
    }
  }

  /* exact codec-framing accounting: magic(1) + tag(1) + length(8) +
     attribute count(4); each attribute its name string (13) + STRSXP
     scaffold (9) + per element (8 + bytes); each condition element its
     name (8 + bytes) + node (its stream size less the magic byte) */
  size_t spent = 14 + 22 + 22;
  for (R_xlen_t i = 0; i < XLENGTH(klass); i++)
    spent += 8 + (size_t) LENGTH(STRING_ELT(klass, i));
  spent += (8 + 7) + (9 + 8 + (size_t) LENGTH(msg));   /* message */

  int have_call = 0;
  if (call_idx >= 0) {
    size_t ns = rei_codec_write(NULL, 0, VECTOR_ELT(cond, call_idx));
    size_t cost = (8 + 4) + (ns != 0 ? ns - 1 : 0);
    if (ns != 0 && spent + cost <= budget) {
      spent += cost;
      have_call = 1;
    } else {
      SET_STRING_ELT(dropped, ndrop++, Rf_mkChar("call"));
    }
  }
  for (R_xlen_t i = 0; i < n; i++) {
    if (mk[i] != 1) continue;
    SEXP nm = STRING_ELT(names, i);
    size_t ns = rei_codec_write(NULL, 0, VECTOR_ELT(cond, i));
    size_t cost = (8 + (size_t) LENGTH(nm)) + (ns != 0 ? ns - 1 : 0);
    if (ns != 0 && spent + cost <= budget) {
      spent += cost;
      mk[i] = 2;                 /* kept */
    } else {
      SET_STRING_ELT(dropped, ndrop++, nm);
    }
  }

  /* dropped_fields, last priority: as many names in order as fit */
  size_t dcost = (8 + 14) + 9;
  R_xlen_t dfit = 0;
  for (R_xlen_t i = 0; i < ndrop; i++) {
    size_t c = 8 + (size_t) LENGTH(STRING_ELT(dropped, i));
    if (spent + dcost + c > budget) break;
    dcost += c;
    dfit++;
  }

  SEXP flat = PROTECT(rei_cond_build(msg, cond, names, mk, n,
                                      have_call ? call_idx : (R_xlen_t) -1,
                                      dropped, dfit, klass));

  /* the verification pass: the exact whole-condition size against the
     budget. On overflow the terminal fallback — fallback message, no
     call, no fields, dropped_fields retained — a handful of bytes that
     fits wherever a classed condition fits at all. */
  size_t total = rei_codec_write(NULL, 0, flat);
  if (total == 0 || total > budget) {
    UNPROTECT(1);                /* flat */
    SEXP fmsg = PROTECT(Rf_mkChar(REI_COND_FALLBACK));
    size_t fspent = 14 + 22 + 22;
    for (R_xlen_t i = 0; i < XLENGTH(klass); i++)
      fspent += 8 + (size_t) LENGTH(STRING_ELT(klass, i));
    fspent += (8 + 7) + (9 + 8 + (size_t) LENGTH(fmsg));
    size_t fdcost = (8 + 14) + 9;
    R_xlen_t fdfit = 0;
    for (R_xlen_t i = 0; i < ndrop; i++) {
      size_t c = 8 + (size_t) LENGTH(STRING_ELT(dropped, i));
      if (fspent + fdcost + c > budget) break;
      fdcost += c;
      fdfit++;
    }
    flat = PROTECT(rei_cond_build(fmsg, cond, names, mk, 0, -1, dropped,
                                   fdfit, klass));
    UNPROTECT(6);                /* msg, klass, mark, dropped, fmsg, flat */
    return flat;
  }
  UNPROTECT(5);                  /* msg, klass, mark, dropped, flat */
  return flat;
}
