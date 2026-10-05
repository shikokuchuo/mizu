/* The 'I' interchange codec, R half: the value walk (what qualifies, in
   what order) and the builder (native allocation per item) over the
   core's byte-level half — the validating pull cursor (mizu_ix_*) and
   the dual-form emit helpers (mizu_ix_put_*), so the wire grammar itself
   has exactly one implementation. DESIGN.md's Interchange codec section
   is the byte authority.

   The writer mirrors codec.c's two-pass contract:
   mizu_interop_write(NULL, 0, x) returns the size, 0 = decline (the
   decline record carries the path and reason for the send-time raise —
   the writer runs on foreign handles only, where a decline is an
   mizu_error_not_portable, never a fallback). The size pass validates
   everything, so the write pass cannot decline; ALTREP values cross by
   value (compact sequences are never expanded on the sender —
   *_GET_REGION computes them in place), and strings cross as UTF-8
   (ASCII and CE_UTF8 as-is, unmarked native after a validity check,
   latin1 translated, CE_BYTES declined).

   The reader is a builder over the cursor: bounds, depth, UTF-8
   validity, and the unknown-tag text are the core's; the builder only
   allocates SEXPs (PROTECT discipline: fresh SEXPs leave builders as
   return values, the caller PROTECTs at the call site) and owns the two
   allocation-level checks the cursor is specified not to — duplicate
   dict keys and the attr shape whitelist. integer64 construction is
   mizu_wire_alloc's (one int64 site). */

#include "mizu.h"
#include <string.h>
#include <math.h>
#include <stdlib.h>
#include <R_ext/Parse.h>

#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

/* R_mkClosure() joined the C API in R 4.5.0; the definition is map.c's
   backport on earlier R. */
#if R_VERSION < R_Version(4, 5, 0)
SEXP R_mkClosure(SEXP formals, SEXP body, SEXP env);
#endif

static SEXP ix_tzone_sym, ix_units_sym;
static SEXP ix_date_class, ix_posixct_class, ix_factor_class,
  ix_frame_class, ix_ordered_class, ix_ref_marker_class, ix_difftime_class;

/* The attr vocabulary single-sources through the core's MIZU_IX_ATTR_* /
   MIZU_IX_CLASS_* / MIZU_IX_UNIT_* macros (the byte-shape helper
   registry); a key crosses with its length as sizeof - 1. */
#define IX_KEYLEN(s) ((uint32_t) (sizeof(s) - 1))

static int ix_int_is_seq1n(SEXP rn, R_xlen_t n);

/* The UTF-8 rule single-sources through the core's mizu_ix_utf8_valid
   (the byte-shape helper registry; the vendored mizu_ext.h inline). */

/* The UTF-8 byte form of one CHARSXP, or a decline (CE_BYTES always;
   unmarked native when not valid UTF-8). latin1 translates through
   Rf_translateCharUTF8 (total for latin1; the translation cache lives on
   the CHARSXP, so the returned pointer needs no protection). ASCII is
   valid UTF-8 by construction. */
static const char *ix_char_utf8(SEXP cs, int *ok, int32_t *out_len) {
  *ok = 1;
  switch (Rf_getCharCE(cs)) {
  case CE_UTF8:
    *out_len = (int32_t) LENGTH(cs);
    return CHAR(cs);
  case CE_LATIN1: {
    const char *tr = Rf_translateCharUTF8(cs);
    *out_len = (int32_t) strlen(tr);
    return tr;
  }
  case CE_BYTES:
    *ok = 0;
    return NULL;
  default:
    break;
  }
  /* unmarked native: as-is on the UTF-8-native supported platforms,
     after a validity check */
  if (!mizu_ix_utf8_valid(CHAR(cs), (size_t) LENGTH(cs))) {
    *ok = 0;
    return NULL;
  }
  *out_len = (int32_t) LENGTH(cs);
  return CHAR(cs);
}

/* class(x) is exactly cls (built with mkChar, so CHARSXP identity
   holds). */
static int ix_class_is(SEXP x, SEXP cls) {
  SEXP klass = Rf_getAttrib(x, R_ClassSymbol);
  if (TYPEOF(klass) != STRSXP || XLENGTH(klass) != XLENGTH(cls))
    return 0;
  for (R_xlen_t i = 0; i < XLENGTH(cls); i++)
    if (STRING_ELT(klass, i) != STRING_ELT(cls, i)) return 0;
  return 1;
}

/* x carries the integer64 class (a REALSXP classed exactly "integer64",
   however many attributes) — the dim shape's value, whose class the 0x0e
   tag consumes. mizu_view_is_int64 is the class-only probe (exactly one
   attribute), a different question. */
static int ix_is_int64(SEXP x) {
  return TYPEOF(x) == REALSXP && ix_class_is(x, mizu_view_int64_class);
}

/* The attribute count, both API forms. */
static R_xlen_t ix_attr_count(SEXP x) {
#if R_VERSION >= R_Version(4, 6, 0)
  SEXP attrs = PROTECT(R_getAttributes(x));
  R_xlen_t n = XLENGTH(attrs);
  UNPROTECT(1);
  return n;
#else
  R_xlen_t n = 0;
  for (SEXP a = ATTRIB(x); a != R_NilValue; a = CDR(a)) n++;
  return n;
#endif
}

/* A dict value by key (the dict is a named VECSXP; getAttrib's
   special-cased symbols never reach the pair values). */
static SEXP ix_dict_get(SEXP dict, const char *key) {
  SEXP keys = Rf_getAttrib(dict, R_NamesSymbol);
  if (TYPEOF(keys) != STRSXP) return R_NilValue;
  for (R_xlen_t i = 0; i < XLENGTH(dict); i++)
    if (strcmp(CHAR(STRING_ELT(keys, i)), key) == 0)
      return VECTOR_ELT(dict, i);
  return R_NilValue;
}

/* A names vector qualifies as dict keys: non-NA, UTF-8-writable, unique
   after translation. */
int mizu_interop_names_ok(SEXP names) {
  const R_xlen_t n = XLENGTH(names);
  for (R_xlen_t i = 0; i < n; i++) {
    int ok;
    int32_t len;
    const char *u = ix_char_utf8(STRING_ELT(names, i), &ok, &len);
    if (!ok) return 0;
    for (R_xlen_t j = 0; j < i; j++) {
      int ok2;
      int32_t len2;
      const char *u2 = ix_char_utf8(STRING_ELT(names, j), &ok2, &len2);
      if (ok2 && len == len2 && memcmp(u, u2, (size_t) len) == 0)
        return 0;
    }
  }
  return 1;
}

/* The foreign MIZS gate's string walk: every element ASCII, CE_UTF8, or
   native that validates as UTF-8 — a latin1 vector takes the 'I' copy
   (which translates it), a bytes-marked one declines there. R >= 4.5
   reads the ASCII bit first: R clears every mark on pure-ASCII CHARSXPs
   at creation, so the bit means unmarked native, and pure ASCII is
   valid UTF-8 by construction. bytes, when non-NULL, takes the packed
   byte total (NAs span nothing) so the caller's size gate rides the
   same walk. */
int mizu_interop_strings_utf8(SEXP x, size_t *bytes) {
  const R_xlen_t n = XLENGTH(x);
  const SEXP *base = mizu_view_str_base(x);
  size_t total = 0;
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP cs = mizu_view_str_elt(x, i, base);
    if (cs == NA_STRING) continue;
#if R_VERSION >= R_Version(4, 5, 0)
    if (Rf_charIsASCII(cs)) {
      total += (size_t) LENGTH(cs);
      continue;
    }
#endif
    switch (Rf_getCharCE(cs)) {
    case CE_UTF8:
      break;
    case CE_LATIN1:
    case CE_BYTES:
      return 0;
    default:
      if (!mizu_ix_utf8_valid(CHAR(cs), (size_t) LENGTH(cs)))
        return 0;
    }
    total += (size_t) LENGTH(cs);
  }
  if (bytes != NULL) *bytes = total;
  return 1;
}

// The attr qualification ------------------------------------------------------------

/* A plain factor: class exactly "factor", attributes exactly levels and
   class, levels unique, non-NA, UTF-8-writable. */
static int ix_qualify_factor(SEXP x) {
  SEXP levels = PROTECT(Rf_getAttrib(x, R_LevelsSymbol));
  int ok = TYPEOF(x) == INTSXP && ix_class_is(x, ix_factor_class) &&
    ix_attr_count(x) == 2 && TYPEOF(levels) == STRSXP &&
    mizu_interop_names_ok(levels);
  UNPROTECT(1);
  return ok;
}

/* A plain Date: class exactly "Date", attributes exactly class, every
   value integral (a fractional Date has no datetime64[D] home — the
   check fuses the size pass's scan). */
static int ix_date_integral(const double *d, R_xlen_t n) {
  for (R_xlen_t i = 0; i < n; i++)
    if (!ISNA(d[i]) && (d[i] != floor(d[i]) || !R_FINITE(d[i])))
      return 0;
  return 1;
}

static int ix_qualify_date(SEXP x) {
  if (TYPEOF(x) != REALSXP || !ix_class_is(x, ix_date_class) ||
      ix_attr_count(x) != 1)
    return 0;
  const R_xlen_t n = XLENGTH(x);
  if (ALTREP(x)) {
    double buf[512];
    for (R_xlen_t i = 0; i < n; ) {
      const R_xlen_t m = n - i < 512 ? n - i : 512;
      REAL_GET_REGION(x, i, m, buf);
      if (!ix_date_integral(buf, m)) return 0;
      i += m;
    }
    return 1;
  }
  return ix_date_integral(REAL(x), n);
}

/* A plain POSIXct: class exactly c("POSIXct", "POSIXt"), attributes
   class plus an optional length-1 non-NA tzone. */
static int ix_qualify_posixct(SEXP x) {
  if (TYPEOF(x) != REALSXP || !ix_class_is(x, ix_posixct_class))
    return 0;
  const R_xlen_t na = ix_attr_count(x);
  if (na == 1) return 1;
  if (na != 2) return 0;
  SEXP tz = PROTECT(Rf_getAttrib(x, ix_tzone_sym));
  int ok = TYPEOF(tz) == STRSXP && XLENGTH(tz) == 1 &&
    STRING_ELT(tz, 0) != NA_STRING;
  UNPROTECT(1);
  return ok;
}

/* A units string the wire admits (R's five difftime units). */
static int ix_units_known(SEXP units) {
  if (TYPEOF(units) != STRSXP || XLENGTH(units) != 1 ||
      STRING_ELT(units, 0) == NA_STRING)
    return 0;
  const char *u = CHAR(STRING_ELT(units, 0));
  return !strcmp(u, MIZU_IX_UNIT_SECS) || !strcmp(u, MIZU_IX_UNIT_MINS) ||
    !strcmp(u, MIZU_IX_UNIT_HOURS) || !strcmp(u, MIZU_IX_UNIT_DAYS) ||
    !strcmp(u, MIZU_IX_UNIT_WEEKS);
}

/* A plain difftime: class exactly "difftime", attributes exactly class
   and a known length-1 units. The value crosses in its declared units
   (the reader homes it), so no conversion pass runs here. */
static int ix_qualify_difftime(SEXP x) {
  if (TYPEOF(x) != REALSXP || !ix_class_is(x, ix_difftime_class) ||
      ix_attr_count(x) != 2)
    return 0;
  SEXP units = PROTECT(Rf_getAttrib(x, ix_units_sym));
  const int ok = ix_units_known(units);
  UNPROTECT(1);
  return ok;
}

/* A plain dim array: no class (or the integer64 class the 0x0e value tag
   consumes), attributes exactly dim (+ class), dim an integer vector of
   length >= 1 with no NA, every element >= 0, product == length(x). The
   value is any atomic vector but character. */
static int ix_qualify_dim(SEXP x) {
  switch (TYPEOF(x)) {
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
    break;
  default:
    return 0;
  }
  if (ix_is_int64(x)) {
    if (ix_attr_count(x) != 2) return 0;
  } else if (Rf_getAttrib(x, R_ClassSymbol) != R_NilValue ||
             ix_attr_count(x) != 1) {
    return 0;
  }
  SEXP dim = PROTECT(Rf_getAttrib(x, R_DimSymbol));
  if (TYPEOF(dim) != INTSXP || XLENGTH(dim) < 1 || ALTREP(dim)) {
    UNPROTECT(1);
    return 0;
  }
  const R_xlen_t nd = XLENGTH(dim);
  const int *d = INTEGER(dim);
  R_xlen_t prod = 1;
  for (R_xlen_t i = 0; i < nd; i++) {
    if (d[i] == NA_INTEGER || d[i] < 0 || 
        (d[i] != 0 && prod > XLENGTH(x) / (R_xlen_t) d[i])) {
      UNPROTECT(1);
      return 0;
    }
    prod *= d[i];
  }
  UNPROTECT(1);
  return prod == XLENGTH(x);
}

/* A plain data.frame: class exactly "data.frame", attributes exactly
   names, class and row.names; names unique, non-NA, UTF-8-writable;
   row.names an integer or character vector of length n with no NA (the
   compact automatic c(NA, ±n) included); one or more columns, each an
   attribute-free atomic vector, a class-only integer64, a plain factor,
   a Date, or a POSIXct, all of one length. */
static int ix_qualify_frame(SEXP x, int *rn_seq1n) {
  if (TYPEOF(x) != VECSXP || !ix_class_is(x, ix_frame_class) ||
      ix_attr_count(x) != 3)
    return 0;
  SEXP names = PROTECT(Rf_getAttrib(x, R_NamesSymbol));
  if (TYPEOF(names) != STRSXP || XLENGTH(names) != XLENGTH(x) ||
      !mizu_interop_names_ok(names)) {
    UNPROTECT(1);
    return 0;
  }
  const R_xlen_t ncols = XLENGTH(x);
  if (ncols < 1) {
    UNPROTECT(1);
    return 0;
  }
  R_xlen_t n = -1;
  int ok_cols = 1;
  for (R_xlen_t j = 0; ok_cols && j < ncols; j++) {
    SEXP col = VECTOR_ELT(x, j);
    switch (TYPEOF(col)) {
    case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
    case STRSXP:
      break;
    default:
      ok_cols = 0;
      break;
    }
    if (ok_cols && ANY_ATTRIB(col)) {
      if (mizu_view_is_int64_any(col)) {
        if (ix_attr_count(col) != 1) ok_cols = 0;
      } else {
        const int q = mizu_interop_attrs_qualify(col);
        if (q != MIZU_IXQ_FACTOR && q != MIZU_IXQ_DATE &&
            q != MIZU_IXQ_POSIXCT && q != MIZU_IXQ_DIFFTIME)
          ok_cols = 0;
      }
    }
    if (n < 0) {
      n = XLENGTH(col);
    } else if (XLENGTH(col) != n) {
      ok_cols = 0;
    }
  }
  if (!ok_cols) {
    UNPROTECT(1);
    return 0;
  }
  SEXP rn = PROTECT(Rf_getAttrib(x, R_RowNamesSymbol));
  int ok = 0;
  if (rn_seq1n != NULL) *rn_seq1n = 0;
  switch (TYPEOF(rn)) {
  case INTSXP:
    if (XLENGTH(rn) == 2 && !ALTREP(rn) && INTEGER(rn)[0] == NA_INTEGER) {
      ok = INTEGER(rn)[1] == n || INTEGER(rn)[1] == -n;  /* compact */
    } else if (XLENGTH(rn) != n) {
      ok = 0;
    } else {
      /* the seq test doubles as the NA check (1:n carries no NA): one
         pass settles both, and the blob's row.names normalization reads
         the verdict back instead of re-scanning */
      int seq = ix_int_is_seq1n(rn, n);
      if (seq) {
        if (rn_seq1n != NULL) *rn_seq1n = 1;
        ok = 1;
      } else if (ALTREP(rn)) {
        ok = 1;
        int buf[512];
        for (R_xlen_t i = 0; ok && i < n; ) {
          const R_xlen_t m = n - i < 512 ? n - i : 512;
          INTEGER_GET_REGION(rn, i, m, buf);
          for (R_xlen_t j = 0; j < m; j++)
            if (buf[j] == NA_INTEGER) ok = 0;
          i += m;
        }
      } else {
        ok = 1;
        for (R_xlen_t i = 0; i < n; i++)
          if (INTEGER(rn)[i] == NA_INTEGER) ok = 0;
      }
    }
    break;
  case STRSXP:
    ok = XLENGTH(rn) == n;
    for (R_xlen_t i = 0; ok && i < n; i++)
      if (STRING_ELT(rn, i) == NA_STRING) ok = 0;
    break;
  default:
    ok = 0;
  }
  UNPROTECT(2);
  return ok;
}

/* One value is encodable: an attribute-free atomic or string vector, a
   class-only integer64, a plain list tree with dict-key-ok names, or a
   whitelisted shape (recursively — strictly smaller values, so the walk
   terminates). The §3.5 layout blob's R-peer gate: an attribute set is
   encodable when its names pass the dict-key rules and every attribute
   value is encodable. */
static int ix_encodable(SEXP x, unsigned depth) {
  if (depth > MIZU_IX_DEPTH_MAX || Rf_isS4(x)) return 0;
  if (x == R_NilValue) return 1;
  switch (TYPEOF(x)) {
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
  case STRSXP:
    break;
  case VECSXP:
    for (R_xlen_t i = 0; i < XLENGTH(x); i++)
      if (!ix_encodable(VECTOR_ELT(x, i), depth + 1)) return 0;
    break;
  default:
    return 0;
  }
  if (!ANY_ATTRIB(x)) return 1;
  if (mizu_view_is_int64_any(x)) return ix_attr_count(x) == 1;
  /* the attribute set itself: names under the dict-key rules, every
     value encodable (a whitelisted shape's values always pass, so this
     alone answers both questions without consulting the whitelist) */
#if R_VERSION >= R_Version(4, 6, 0)
  SEXP attrs = PROTECT(R_getAttributes(x));
  SEXP keys = PROTECT(Rf_getAttrib(attrs, R_NamesSymbol));
  int ok = mizu_interop_names_ok(keys);
  for (R_xlen_t i = 0; ok && i < XLENGTH(attrs); i++)
    ok = ix_encodable(VECTOR_ELT(attrs, i), depth + 1);
  UNPROTECT(2);
  return ok;
#else
  SEXP keys = PROTECT(Rf_allocVector(STRSXP, ix_attr_count(x)));
  R_xlen_t i = 0;
  for (SEXP a = ATTRIB(x); a != R_NilValue; a = CDR(a), i++)
    SET_STRING_ELT(keys, i, TAG(a) == R_NilValue ? R_BlankString :
                   PRINTNAME(TAG(a)));
  int ok = mizu_interop_names_ok(keys);
  UNPROTECT(1);
  for (SEXP a = ATTRIB(x); ok && a != R_NilValue; a = CDR(a))
    ok = ix_encodable(CAR(a), depth + 1);
  return ok;
#endif
}

/* The one qualification function (the inline writer's gate on foreign
   handles — whitelisted required; the §3.5 layout writer's — encodable
   suffices for an R peer): a (class, attribute set) pair is a shape the
   foreign reader homes, or the attribute set is at least encodable. */
int mizu_interop_attrs_qualify(SEXP x) {
  if (Rf_isS4(x) || !ANY_ATTRIB(x)) return MIZU_IXQ_NONE;
  if (ix_qualify_factor(x)) return MIZU_IXQ_FACTOR;
  if (ix_qualify_frame(x, NULL)) return MIZU_IXQ_FRAME;
  if (ix_qualify_dim(x)) return MIZU_IXQ_DIM;
  if (ix_qualify_date(x)) return MIZU_IXQ_DATE;
  if (ix_qualify_posixct(x)) return MIZU_IXQ_POSIXCT;
  if (ix_qualify_difftime(x)) return MIZU_IXQ_DIFFTIME;
  return ix_encodable(x, 0) ? MIZU_IXQ_ENCODABLE : MIZU_IXQ_NONE;
}

/* The blob hooks' half: the qualification, with the frame row.names
   seq verdict handed back (its scan doubles as the qualification's NA
   check, so the blob's normalization never re-scans). */
static int ix_qualify_blob(SEXP x, int *rn_seq1n) {
  if (Rf_isS4(x) || !ANY_ATTRIB(x)) return MIZU_IXQ_NONE;
  if (ix_qualify_factor(x)) return MIZU_IXQ_FACTOR;
  if (ix_qualify_frame(x, rn_seq1n)) return MIZU_IXQ_FRAME;
  if (ix_qualify_dim(x)) return MIZU_IXQ_DIM;
  if (ix_qualify_date(x)) return MIZU_IXQ_DATE;
  if (ix_qualify_posixct(x)) return MIZU_IXQ_POSIXCT;
  return ix_encodable(x, 0) ? MIZU_IXQ_ENCODABLE : MIZU_IXQ_NONE;
}

/* The size→write verdict memo: the blob write hook re-runs the
   qualification by contract (the two passes must agree), but the common
   layout has exactly one attributed node — a frame's root, a factor, a
   named vector — whose verdict the size hook computed moments ago.
   Reuse it, one pointer-compared entry consumed on hit; a miss runs the
   full qualification and restocks. The naked pointer is compare-only
   and never aliases a different object: the stage keeps x anchored
   through both passes, and every layout write's own size pass restocks
   the entry before any write hook compares it. */
static SEXP ix_blob_memo_x;
static int ix_blob_memo_q = MIZU_IXQ_NONE;
static int ix_blob_memo_seq = -1;

/* The size hook's half: restock the verdict (it always computes fresh —
   the object may have been mutated between stages). */
static void ix_blob_memo_store(SEXP x, int q, int rn_seq1n) {
  ix_blob_memo_x = x;
  ix_blob_memo_q = q;
  ix_blob_memo_seq = rn_seq1n;
}

/* The write hook's half: consume the size hook's verdict when it is for
   this object, else run the full qualification and restock (a tree's
   attributed leaves do this; the root's hit is then a miss — correct,
   just unsaved). */
static int ix_qualify_blob_mixed(SEXP x, int *rn_seq1n) {
  if (x == ix_blob_memo_x) {
    ix_blob_memo_x = NULL;   /* one-shot: a tree's later sites re-qualify */
    *rn_seq1n = ix_blob_memo_seq;
    return ix_blob_memo_q;
  }
  *rn_seq1n = -1;
  const int q = ix_qualify_blob(x, rn_seq1n);
  ix_blob_memo_store(x, q, *rn_seq1n);
  return q;
}

// Writer ------------------------------------------------------------------------

typedef struct mizu_ixw_s {
  unsigned char *dst;
  size_t limit;
  size_t total;
  int overflow;          /* the write exceeded limit; the count runs on */
  int depth;
  mizu_ix_decline *rec;  /* the send-time decline record, or NULL */
  char path[128];        /* the walk's current path (for the record) */
  size_t path_len;
  /* the F1 refs state: with refs, a REF-qualifying view emits a 0x13
     leaf (the caps filter read off h's pool word, skipped when h is
     NULL); zc_node records the plan's SHM_VEC candidate (first encounter
     stages the stream's one checkout; a mid-write checkout failure sets
     abandon and unwinds — the caller re-runs with no_zc = 1) */
  mizu_handle *h;
  uint32_t caps;
  uint32_t inline_max;
  int refs;
  int no_zc;
  SEXP zc_node;
  int zc_spent;
  int abandon;
  /* the W2 plan record: non-NULL runs the walk in record mode — the
     optimistic write flips to count-only at the first SHM_VEC candidate,
     the first 16 candidates recording with their by-value subtree sizes
     (detection inside a recorded candidate's subtree suppressed, the
     never-nest rule) and ref leaves setting has_ref as they emit */
  mizu_ix_plan *plan;
  int plan_suppress;
  int plan_nocand;         /* churn or no handle: no candidates */
} mizu_ixw;

/* One emit: the put helpers count with a NULL dst; past the limit the
   count runs on but the writes stop (the codec.c overflow discipline). */
#define IXW_PUT(w, fn, ...)                              \
  do {                                                   \
    size_t n_ = fn(NULL, ##__VA_ARGS__);                 \
    if ((w)->dst != NULL) {                              \
      if ((w)->total + n_ > (w)->limit) {                \
        (w)->overflow = 1;                               \
        (w)->dst = NULL;                                 \
      } else {                                           \
        (void) fn((w)->dst + (w)->total, ##__VA_ARGS__); \
      }                                                  \
    }                                                    \
    (w)->total += n_;                                    \
  } while (0)

static void ixw_decline(mizu_ixw *w, const char *reason,
                         const char *remedy) {
  if (w->rec != NULL && !w->rec->decline) {
    w->rec->decline = 1;
    snprintf(w->rec->path, sizeof w->rec->path, "%s", w->path);
    snprintf(w->rec->reason, sizeof w->rec->reason, "%s", reason);
    snprintf(w->rec->remedy, sizeof w->rec->remedy, "%s",
             remedy != NULL ? remedy : "");
  }
  w->depth = -1;             /* unwind */
}

static void ixw_path_push(mizu_ixw *w, R_xlen_t i) {
  size_t room = sizeof w->path - w->path_len;
  if (room < 8) return;
  /* clamp like ixw_path_key: snprintf reports the would-be length, so an
     unclamped add can run path_len past the buffer */
  int n = snprintf(w->path + w->path_len, room, "[[%ld]]", (long) i);
  w->path_len += (size_t) n < room ? (size_t) n : room - 1;
}

static void ixw_path_key(mizu_ixw *w, const char *key) {
  size_t room = sizeof w->path - w->path_len;
  if (room < 2) return;
  int n = snprintf(w->path + w->path_len, room, "$%s", key);
  w->path_len += (size_t) n < room ? (size_t) n : room - 1;
}

static void ixw_path_pop(mizu_ixw *w, size_t saved) {
  w->path[saved] = '\0';
  w->path_len = saved;
}

static void ixw_node(mizu_ixw *w, SEXP x);
static void ixw_value(mizu_ixw *w, SEXP x);

/* One CHARSXP's wire form: the byte length through out_len (-1 = NA), 0
   on a decline. */
static int ixw_str_size(SEXP cs, int32_t *out_len) {
  if (cs == NA_STRING) {
    *out_len = -1;
    return 1;
  }
  int ok;
  ix_char_utf8(cs, &ok, out_len);
  return ok;
}

/* The emit half of ixw_str_size: bare (strv element / dict key) or the
   0x04 scalar. latin1 re-reads the cached translation. */
static void ixw_str_put(mizu_ixw *w, SEXP cs, int32_t len, int bare) {
  if (len < 0) {
    if (bare) {
      IXW_PUT(w, mizu_ix_put_strelt, NULL, -1);
    } else {
      IXW_PUT(w, mizu_ix_put_str, NULL, -1);
    }
    return;
  }
  int ok;
  int32_t ulen;
  const char *u = ix_char_utf8(cs, &ok, &ulen);
  if (bare) {
    IXW_PUT(w, mizu_ix_put_strelt, u, len);
  } else {
    IXW_PUT(w, mizu_ix_put_str, u, len);
  }
}

/* An atomic vector tag: resident data rides mizu_ix_put_vec's one call;
   ALTREP crosses by value — a manual header, then *_GET_REGION in
   bounded chunks (a compact sequence is never expanded on the sender).
   The tag byte is the core's mizu_ix_tag_of table (only the six atomic
   wire types reach here). */
static void ixw_atomic_put(mizu_ixw *w, int wire_type, SEXP x) {
  const R_xlen_t n = XLENGTH(x);
  if (!ALTREP(x)) {
    IXW_PUT(w, mizu_ix_put_vec, wire_type, mizu_vec_ptr(x), (uint64_t) n);
    return;
  }
  const uint32_t tag = (uint32_t) mizu_ix_tag_of(wire_type);
  if (w->dst != NULL) {
    if (w->total + 9 > w->limit) {
      w->overflow = 1;
      w->dst = NULL;
    } else {
      uint64_t n64 = (uint64_t) n;
      w->dst[w->total] = (unsigned char) tag;
      memcpy(w->dst + w->total + 1, &n64, 8);
    }
  }
  w->total += 9;
  const size_t elt = mizu_view_sizeof_elt(TYPEOF(x));
  const R_xlen_t chunk = (R_xlen_t) (4096 / elt);
  for (R_xlen_t i = 0; i < n; i += chunk) {
    const R_xlen_t m = n - i < chunk ? n - i : chunk;
    if (w->dst != NULL && w->total + (size_t) m * elt <= w->limit) {
      switch (TYPEOF(x)) {
      case LGLSXP:
      case INTSXP:
        INTEGER_GET_REGION(x, i, m, (int *) (w->dst + w->total));
        break;
      case REALSXP:
        REAL_GET_REGION(x, i, m, (double *) (w->dst + w->total));
        break;
      case CPLXSXP:
        COMPLEX_GET_REGION(x, i, m, (Rcomplex *) (w->dst + w->total));
        break;
      default:
        RAW_GET_REGION(x, i, m, (Rbyte *) (w->dst + w->total));
        break;
      }
    } else if (w->dst != NULL) {
      w->overflow = 1;
      w->dst = NULL;
    }
    w->total += (size_t) m * elt;
  }
}

/* One string vector's elements (the strv begin was emitted by the
   caller): ALTREP admitted — STRING_ELT walks it in place. */
static void ixw_strv_elts(mizu_ixw *w, SEXP x) {
  const R_xlen_t n = XLENGTH(x);
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP cs = STRING_ELT(x, i);
    int32_t len;
    if (!ixw_str_size(cs, &len)) {
      ixw_decline(w, "a string is not writable as UTF-8 (CE_BYTES or "
                     "invalid native bytes)", NULL);
      return;
    }
    ixw_str_put(w, cs, len, 1);
    if (w->depth < 0) return;
  }
}

/* A dict pair list (names + values), keys under the dict-key rules. */
static void ixw_dict_pairs(mizu_ixw *w, SEXP names, SEXP values) {
  const R_xlen_t n = XLENGTH(values);
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP cs = STRING_ELT(names, i);
    int32_t len;
    if (!ixw_str_size(cs, &len) || len < 0) {
      ixw_decline(w, "a name is NA or not writable as UTF-8", NULL);
      return;
    }
    ixw_str_put(w, cs, len, 1);
    size_t saved = w->path_len;
    ixw_path_key(w, CHAR(STRING_ELT(names, i)));
    ixw_node(w, VECTOR_ELT(values, i));
    ixw_path_pop(w, saved);
    if (w->depth < 0) return;
  }
}

/* row.names on the wire: the automatic form (the compact c(NA, ±n) the
   raw ATTRIB pair carries before R 4.6, or an integer vector equal to
   1:n by value on later R) writes intv[na, ∓n]; a zero-row frame's
   integer(0) writes intv[na, 0]; any other integer or character vector
   writes as-is. */
static int ix_int_is_seq1n_chunked(SEXP rn, R_xlen_t n) {
  int buf[8192];
  for (R_xlen_t i = 0; i < n; ) {
    const R_xlen_t m = n - i < 8192 ? n - i : 8192;
    INTEGER_GET_REGION(rn, i, m, buf);
    for (R_xlen_t j = 0; j < m; j++)
      if (buf[j] != (int) (i + j + 1)) return 0;
    i += m;
  }
  return 1;
}

/* A warm scratch for the big-vector scan: the per-call malloc's
   first-touch page faults are the cost at 1e6 rows, so the buffer grows
   on demand and stays. Single-threaded staging (the core's contract —
   the stage hook runs on the verb's thread). */
static int *ix_seq_buf;
static size_t ix_seq_buf_n;

static int ix_int_is_seq1n(SEXP rn, R_xlen_t n) {
  if (XLENGTH(rn) != n || n <= 0) return 0;
  if (ALTREP(rn)) {
    /* R's compact integer sequence: the info is REALSXP c(length, first,
       incr) (R >= 4.4; R >= 4.6's automatic row.names ride it), so the
       1:n verdict is three reads. Shape-gated — a mismatch is some other
       ALTREP class and falls to the scan, never a wrong verdict. */
    SEXP info = R_altrep_data1(rn);
    if (TYPEOF(info) == REALSXP && !ALTREP(info) && XLENGTH(info) == 3) {
      const double *d = REAL(info);
      if (d[0] == (double) n && d[1] == 1 && d[2] == 1) return 1;
    }
  }
  if (n <= 8192 || n > (R_xlen_t) 1 << 28)
    return ix_int_is_seq1n_chunked(rn, n);
  /* one flat pass: the GET_REGION fill and the compare are both
     memory-bandwidth work (a compact sequence computes its fill; the
     compare vectorizes) */
  if (ix_seq_buf_n < (size_t) n) {
    int *nb = (int *) realloc(ix_seq_buf, (size_t) n * sizeof(int));
    if (nb == NULL) {
      free(ix_seq_buf);
      ix_seq_buf = NULL;
      ix_seq_buf_n = 0;
      return ix_int_is_seq1n_chunked(rn, n);
    }
    ix_seq_buf = nb;
    ix_seq_buf_n = (size_t) n;
  }
  INTEGER_GET_REGION(rn, 0, n, ix_seq_buf);
  for (R_xlen_t j = 0; j < n; j++)
    if (ix_seq_buf[j] != (int) (j + 1)) return 0;
  return 1;
}

static void ixw_rownames(mizu_ixw *w, SEXP rn, R_xlen_t n) {
  const int compact = TYPEOF(rn) == INTSXP && XLENGTH(rn) == 2 &&
    !ALTREP(rn) && INTEGER(rn)[0] == NA_INTEGER;
  if (compact || (TYPEOF(rn) == INTSXP && XLENGTH(rn) == 0) ||
      (TYPEOF(rn) == INTSXP && ix_int_is_seq1n(rn, n))) {
    /* the compact automatic form, rebuilt at the one wire size */
    int32_t v[2];
    v[0] = NA_INTEGER;
    v[1] = compact ? INTEGER(rn)[1] : (int32_t) -n;
    IXW_PUT(w, mizu_ix_put_vec, MIZU_TYPE_INT, v, 2);
    return;
  }
  ixw_node(w, rn);
}

/* The attribute dict for a whitelisted shape: pairs in the canonical
   stored order (the corpus's order), each value an ordinary value. The
   integer64 dim array's class rides the 0x0e value tag, so its dict
   skips the class key. */
static void ixw_attr_dict(mizu_ixw *w, SEXP x, int q) {
  SEXP names_attr = PROTECT(Rf_getAttrib(x, R_NamesSymbol));
  SEXP klass = PROTECT(Rf_getAttrib(x, R_ClassSymbol));
  SEXP levels = PROTECT(Rf_getAttrib(x, R_LevelsSymbol));
  SEXP dim = PROTECT(Rf_getAttrib(x, R_DimSymbol));
  SEXP rn = PROTECT(Rf_getAttrib(x, R_RowNamesSymbol));
  SEXP tz = PROTECT(Rf_getAttrib(x, ix_tzone_sym));
  SEXP units = PROTECT(q == MIZU_IXQ_DIFFTIME ?
    Rf_getAttrib(x, ix_units_sym) : R_NilValue);
  const int skip_class = q == MIZU_IXQ_DIM && ix_is_int64(x);

  uint64_t count = 0;
  if (names_attr != R_NilValue) count++;
  if (levels != R_NilValue) count++;
  if (dim != R_NilValue) count++;
  if (klass != R_NilValue && !skip_class) count++;
  if (units != R_NilValue) count++;
  if (rn != R_NilValue) count++;
  if (tz != R_NilValue) count++;
  IXW_PUT(w, mizu_ix_put_dict_begin, count);

  if (names_attr != R_NilValue) {
    IXW_PUT(w, mizu_ix_put_key, MIZU_IX_ATTR_NAMES,
            IX_KEYLEN(MIZU_IX_ATTR_NAMES));
    ixw_node(w, names_attr);
    if (w->depth < 0) { UNPROTECT(7); return; }
  }
  if (levels != R_NilValue) {
    IXW_PUT(w, mizu_ix_put_key, MIZU_IX_ATTR_LEVELS,
            IX_KEYLEN(MIZU_IX_ATTR_LEVELS));
    ixw_node(w, levels);
    if (w->depth < 0) { UNPROTECT(7); return; }
  }
  if (dim != R_NilValue) {
    IXW_PUT(w, mizu_ix_put_key, MIZU_IX_ATTR_DIM, IX_KEYLEN(MIZU_IX_ATTR_DIM));
    ixw_node(w, dim);
    if (w->depth < 0) { UNPROTECT(7); return; }
  }
  if (klass != R_NilValue && !skip_class) {
    IXW_PUT(w, mizu_ix_put_key, MIZU_IX_ATTR_CLASS,
            IX_KEYLEN(MIZU_IX_ATTR_CLASS));
    ixw_node(w, klass);
    if (w->depth < 0) { UNPROTECT(7); return; }
  }
  if (units != R_NilValue) {
    IXW_PUT(w, mizu_ix_put_key, MIZU_IX_ATTR_UNITS,
            IX_KEYLEN(MIZU_IX_ATTR_UNITS));
    ixw_node(w, units);
    if (w->depth < 0) { UNPROTECT(7); return; }
  }
  if (rn != R_NilValue) {
    IXW_PUT(w, mizu_ix_put_key, MIZU_IX_ATTR_ROWNAMES,
            IX_KEYLEN(MIZU_IX_ATTR_ROWNAMES));
    ixw_rownames(w, rn,
                 TYPEOF(x) == VECSXP && XLENGTH(x) > 0 ?
                   XLENGTH(VECTOR_ELT(x, 0)) : 0);
    if (w->depth < 0) { UNPROTECT(7); return; }
  }
  if (tz != R_NilValue) {
    IXW_PUT(w, mizu_ix_put_key, MIZU_IX_ATTR_TZONE,
            IX_KEYLEN(MIZU_IX_ATTR_TZONE));
    ixw_node(w, tz);
  }
  UNPROTECT(7);
}

static void ixw_attr(mizu_ixw *w, SEXP x, int q) {
  /* the ref gate never fires inside an attr shape: the whitelisted
     shapes are value-exact byte contracts (a frame's columns, a factor's
     levels), and the F1 readers home no views there — a view in an
     attribute crosses by value (GET_REGION) as before */
  const int saved_refs = w->refs;
  w->refs = 0;
  if (q == MIZU_IXQ_NONE || q == MIZU_IXQ_ENCODABLE) {
    SEXP names = Rf_getAttrib(x, R_NamesSymbol);
    if (names != R_NilValue && TYPEOF(names) == STRSXP &&
        ix_attr_count(x) == 1) {
      /* the case data work hits most names its one-line rewrite */
      ixw_decline(w, "a named atomic vector has no portable home",
                  "as.list(x)");
    } else if (ix_class_is(x, ix_ordered_class)) {
      ixw_decline(w, "an ordered factor has no portable home", NULL);
    } else {
      ixw_decline(w, "an attribute set outside the portable whitelist",
                  NULL);
    }
    w->refs = saved_refs;
    return;
  }
  IXW_PUT(w, mizu_ix_put_attr);
  switch (q) {
  case MIZU_IXQ_FACTOR:
    ixw_atomic_put(w, MIZU_TYPE_INT, x);
    ixw_attr_dict(w, x, q);
    break;
  case MIZU_IXQ_FRAME: {
    const R_xlen_t nc = XLENGTH(x);
    IXW_PUT(w, mizu_ix_put_list_begin, (uint64_t) nc);
    w->depth++;
    for (R_xlen_t j = 0; j < nc; j++) {
      size_t saved = w->path_len;
      ixw_path_push(w, j + 1);
      ixw_node(w, VECTOR_ELT(x, j));
      ixw_path_pop(w, saved);
      if (w->depth < 0) break;
    }
    if (w->depth > 0) w->depth--;
    if (w->depth >= 0) ixw_attr_dict(w, x, q);
    break;
  }
  case MIZU_IXQ_DIM: {
    const int code = ix_is_int64(x) ? MIZU_TYPE_INT64 :
      TYPEOF(x) == LGLSXP ? MIZU_TYPE_LGL :
      TYPEOF(x) == INTSXP ? MIZU_TYPE_INT :
      TYPEOF(x) == REALSXP ? MIZU_TYPE_REAL :
      TYPEOF(x) == CPLXSXP ? MIZU_TYPE_CPLX : MIZU_TYPE_RAW;
    ixw_atomic_put(w, code, x);
    ixw_attr_dict(w, x, q);
    break;
  }
  default:
    /* Date and POSIXct: the realv days / epoch seconds, then the dict */
    ixw_atomic_put(w, MIZU_TYPE_REAL, x);
    ixw_attr_dict(w, x, q);
    break;
  }
  w->refs = saved_refs;
}

// The value walk ---------------------------------------------------------------------

/* The plan's SHM_VEC candidate: one layout write into the stream's single
   spill checkout, the producer-loan retain, and the ref leaf carrying the
   fresh region's name (F1's D1: the one tag serves both by-reference
   cases). The size pass counts the conservative reservation — the fresh
   region's name length is known only at the checkout, so the plan's fit
   decision uses MIZU_IX_ZC_RESERVE and the actual write never exceeds it.
   A checkout failure (a churn race) abandons: nothing is retained yet,
   the caller re-runs with no_zc = 1. */
static void ixw_zc_leaf(mizu_ixw *w, SEXP x) {
  if (w->dst == NULL) {
    w->total += MIZU_IX_ZC_RESERVE;
    return;
  }
  size_t total;
  if (!mizu_zc_eligible_foreign(x, w->inline_max, &total, w->caps)) {
    /* the plan and the write cannot disagree (eligibility is a pure
       function and no verbs run between the passes) — treat as a caller
       bug, never a wrong stream */
    ixw_decline(w, "the recorded zero-copy node is no longer eligible",
                NULL);
    return;
  }
  mizu_shm *shm = NULL;
  if (mizu_stage_spill_get(w->h, total, &shm) != MIZU_OK) {
    w->abandon = 1;
    w->depth = -1;
    return;
  }
  mizu_view_layout_write((unsigned char *) shm->addr, x, 1);
  mizu_stage_retain_zc(w->h, shm);
  IXW_PUT(w, mizu_ix_put_ref, shm->name, (uint32_t) shm->name_len);
}

static void ixw_node(mizu_ixw *w, SEXP x) {
  if (w->depth < 0) return;
  if (w->depth > MIZU_IX_DEPTH_MAX) {
    ixw_decline(w, "the value nests past the depth cap (64)", NULL);
    return;
  }
  R_CheckStack();

  if (w->refs && x != R_NilValue) {
    /* the F1 ref gate: the plan's zc node first (first encounter only —
       one checkout per stream), then a re-referenceable view. A
       materialized vector view (data2 set — the copy may hold mutations)
       or a view whose layout the peer cannot wrap falls through to the
       value write, the §4.2 filter behavior. */
    if (x == w->zc_node && !w->zc_spent && w->h != NULL && !w->no_zc) {
      w->zc_spent = 1;
      ixw_zc_leaf(w, x);
      return;
    }
    /* the corpus's marker (the hook writer, never a live stage): a
       classed string stands in for a view so synthetic identifiers
       round-trip through the conformance loop */
    if (w->h == NULL && TYPEOF(x) == STRSXP && XLENGTH(x) == 1 &&
        ix_class_is(x, ix_ref_marker_class)) {
      SEXP cs = STRING_ELT(x, 0);
      IXW_PUT(w, mizu_ix_put_ref, CHAR(cs), (uint32_t) LENGTH(cs));
      return;
    }
    if (mizu_view_check(x) &&
        (TYPEOF(x) == VECSXP || R_altrep_data2(x) == R_NilValue)) {
      SEXP id = PROTECT(mizu_view_shm_name(x));
      if (id != R_NilValue &&
          (w->h == NULL || mizu_zc_ref_foreign_ok(x, w->caps))) {
        const char *s = CHAR(STRING_ELT(id, 0));
        const uint32_t len = (uint32_t) strlen(s);
        /* the leaf's u8 length: a path identifier past 255 bytes takes
           the value write like any unreferenceable view */
        if (len > 0 && len <= 255) {
          IXW_PUT(w, mizu_ix_put_ref, s, len);
          mizu_zc_ref_mark(x);
          if (w->plan != NULL) w->plan->has_ref = 1;
          UNPROTECT(1);
          return;
        }
      }
      UNPROTECT(1);
    }
    /* the W2 fold: the plan's candidate record lives in the walk itself
       (the pre-scan's predicates are the gate's own, a REF-able view
       never a candidate — the branch above returned). A layout-eligible
       node records and walks by value for its subtree size, nested
       detection suppressed; the optimistic write's tail past the first
       candidate is abandoned (selection follows the walk). */
    if (w->plan != NULL && !w->plan_suppress && !w->plan_nocand &&
        w->plan->ncand < 16) {
      size_t zc_total;
      if (mizu_zc_eligible_foreign(x, w->inline_max, &zc_total, w->caps)) {
        w->dst = NULL;
        const size_t start = w->total;
        w->plan_suppress = 1;
        ixw_value(w, x);
        w->plan_suppress = 0;
        if (w->depth >= 0) {
          w->plan->cand[w->plan->ncand] = x;
          w->plan->size[w->plan->ncand] = w->total - start;
          w->plan->ncand++;
        }
        return;
      }
    }
  }

  ixw_value(w, x);
}

/* The value dispatch (the ref gate's fall-through): every position the
   gate covers reaches here — the gate itself, or a recorded candidate's
   by-value subtree walk. */
static void ixw_value(mizu_ixw *w, SEXP x) {
  if (x == R_NilValue) {
    IXW_PUT(w, mizu_ix_put_nil);
    return;
  }

  const SEXPTYPE ty = TYPEOF(x);
  switch (ty) {
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP: {
    if (mizu_view_is_int64_any(x) && ix_attr_count(x) == 1 && !Rf_isS4(x)) {
      /* a class-only integer64 writes 0x0e at every length */
      ixw_atomic_put(w, MIZU_TYPE_INT64, x);
      return;
    }
    if (!ANY_ATTRIB(x) && !Rf_isS4(x)) {
      /* attribute-free (resident or ALTREP, which crosses by value) */
      const R_xlen_t n = XLENGTH(x);
      if (n == 1 && ty != RAWSXP) {
        if (!ALTREP(x)) {
          switch (ty) {
          case LGLSXP: {
            const int v = LOGICAL(x)[0];
            IXW_PUT(w, mizu_ix_put_lgl, v == NA_LOGICAL ? 2 : v);
            break;
          }
          case INTSXP: {
            const int v = INTEGER(x)[0];
            IXW_PUT(w, mizu_ix_put_int,
                    v == NA_INTEGER ? INT64_MIN : (int64_t) v);
            break;
          }
          case REALSXP:
            IXW_PUT(w, mizu_ix_put_real, REAL(x)[0]);
            break;
          default: {
            const Rcomplex v = COMPLEX(x)[0];
            IXW_PUT(w, mizu_ix_put_cplx, v.r, v.i);
            break;
          }
          }
        } else {
          switch (ty) {
          case LGLSXP: {
            int v;
            INTEGER_GET_REGION(x, 0, 1, &v);
            IXW_PUT(w, mizu_ix_put_lgl, v == NA_LOGICAL ? 2 : v);
            break;
          }
          case INTSXP: {
            int v;
            INTEGER_GET_REGION(x, 0, 1, &v);
            IXW_PUT(w, mizu_ix_put_int,
                    v == NA_INTEGER ? INT64_MIN : (int64_t) v);
            break;
          }
          case REALSXP: {
            double v;
            REAL_GET_REGION(x, 0, 1, &v);
            IXW_PUT(w, mizu_ix_put_real, v);
            break;
          }
          default: {
            Rcomplex v;
            COMPLEX_GET_REGION(x, 0, 1, &v);
            IXW_PUT(w, mizu_ix_put_cplx, v.r, v.i);
            break;
          }
          }
        }
        return;
      }
      const int code = ty == LGLSXP ? MIZU_TYPE_LGL :
        ty == INTSXP ? MIZU_TYPE_INT : ty == REALSXP ? MIZU_TYPE_REAL :
        ty == CPLXSXP ? MIZU_TYPE_CPLX : MIZU_TYPE_RAW;
      ixw_atomic_put(w, code, x);
      return;
    }
    if (Rf_isS4(x)) {
      ixw_decline(w, "an S4 object has no portable home", NULL);
      return;
    }
    w->depth++;
    ixw_attr(w, x, mizu_interop_attrs_qualify(x));
    if (w->depth > 0) w->depth--;
    return;
  }
  case STRSXP: {
    if (Rf_isS4(x)) {
      ixw_decline(w, "an S4 object has no portable home", NULL);
      return;
    }
    if (ANY_ATTRIB(x)) {
      w->depth++;
      ixw_attr(w, x, mizu_interop_attrs_qualify(x));
      if (w->depth > 0) w->depth--;
      return;
    }
    const R_xlen_t n = XLENGTH(x);
    if (n == 1) {
      SEXP cs = STRING_ELT(x, 0);
      int32_t len;
      if (!ixw_str_size(cs, &len)) {
        ixw_decline(w, "a string is not writable as UTF-8 (CE_BYTES or "
                       "invalid native bytes)", NULL);
        return;
      }
      ixw_str_put(w, cs, len, 0);
      return;
    }
    IXW_PUT(w, mizu_ix_put_strv_begin, (uint64_t) n);
    ixw_strv_elts(w, x);
    return;
  }
  case VECSXP: {
    if (Rf_isS4(x)) {
      ixw_decline(w, "an S4 object has no portable home", NULL);
      return;
    }
    SEXP names = PROTECT(Rf_getAttrib(x, R_NamesSymbol));
    const R_xlen_t n = XLENGTH(x);
    if (names != R_NilValue && TYPEOF(names) == STRSXP &&
        ix_attr_count(x) == 1) {
      /* a named list: the dict, names under the dict-key rules */
      if (!mizu_interop_names_ok(names)) {
        UNPROTECT(1);
        ixw_decline(w, "a named list has duplicate, NA, or non-UTF-8 "
                       "names", NULL);
        return;
      }
      IXW_PUT(w, mizu_ix_put_dict_begin, (uint64_t) n);
      w->depth++;
      ixw_dict_pairs(w, names, x);
      if (w->depth > 0) w->depth--;
      UNPROTECT(1);
      return;
    }
    UNPROTECT(1);
    if (ANY_ATTRIB(x)) {
      w->depth++;
      ixw_attr(w, x, mizu_interop_attrs_qualify(x));
      if (w->depth > 0) w->depth--;
      return;
    }
    IXW_PUT(w, mizu_ix_put_list_begin, (uint64_t) n);
    w->depth++;
    for (R_xlen_t i = 0; i < n; i++) {
      size_t saved = w->path_len;
      ixw_path_push(w, i + 1);
      ixw_node(w, VECTOR_ELT(x, i));
      ixw_path_pop(w, saved);
      if (w->depth < 0) return;
    }
    w->depth--;
    return;
  }
  default:
    ixw_decline(w, ty == LISTSXP ? "a pairlist has no portable home" :
                ty == SYMSXP ? "a symbol has no portable home" :
                ty == CLOSXP ? "a closure has no portable home" :
                ty == ENVSXP ? "an environment has no portable home" :
                "a value of this type has no portable home", NULL);
    return;
  }
}

/* The two-pass entry: NULL dst sizes and qualifies (0 = decline, the
   record filled); a real dst writes behind the size pass's total. */
size_t mizu_interop_write(unsigned char *dst, size_t limit, SEXP x,
                          mizu_ix_decline *rec) {
  /* the value entry is the channel mode: refs off — a view nested in a
     channel payload keeps the materialize behavior (the channel readers
     decline 0x13), and nested attribute blobs inherit this bit */
  mizu_ixw w = { .dst = dst, .limit = limit, .rec = rec,
                 .path = "x", .path_len = 1 };
  if (rec != NULL) {
    rec->decline = 0;
    rec->path[0] = '\0';
    rec->reason[0] = '\0';
    rec->remedy[0] = '\0';
  }
  IXW_PUT(&w, mizu_ix_put_header);
  ixw_node(&w, x);
  if (w.depth < 0) return 0;
  return w.total;
}

// Task streams (Phase 4) ------------------------------------------------------

/* The spec shape: the constructor's and the submit veneer's contract —
   (code, kind, positional, named). */
static int ixw_spec_check(SEXP spec) {
  if (TYPEOF(spec) != VECSXP || XLENGTH(spec) != 4 ||
      TYPEOF(VECTOR_ELT(spec, 0)) != STRSXP ||
      XLENGTH(VECTOR_ELT(spec, 0)) != 1 ||
      TYPEOF(VECTOR_ELT(spec, 1)) != INTSXP ||
      XLENGTH(VECTOR_ELT(spec, 1)) != 1 ||
      TYPEOF(VECTOR_ELT(spec, 2)) != VECSXP ||
      TYPEOF(VECTOR_ELT(spec, 3)) != VECSXP)
    return 0;
  const int kind = INTEGER(VECTOR_ELT(spec, 1))[0];
  return kind >= 0 && kind <= 255;
}

/* The task fields' emission, shared by the task writer and the map
   descriptor writer: the code string, the positional list and the named
   dict directly off the spec's components — the elements through the
   generic writer, never an intermediate object. A decline (a non-portable
   argument, bad names) fills the record and flips the depth marker. */
static void ixw_task_fields(mizu_ixw *w, SEXP spec) {
  SEXP cs = STRING_ELT(VECTOR_ELT(spec, 0), 0);
  int32_t clen;
  if (!ixw_str_size(cs, &clen)) {
    ixw_decline(w, "the task code is not writable as UTF-8 (CE_BYTES or "
                   "invalid native bytes)", NULL);
    return;
  }
  ixw_str_put(w, cs, clen, 0);
  if (w->depth < 0) return;

  SEXP positional = VECTOR_ELT(spec, 2);
  const R_xlen_t np = XLENGTH(positional);
  IXW_PUT(w, mizu_ix_put_list_begin, (uint64_t) np);
  for (R_xlen_t i = 0; i < np; i++) {
    size_t saved = w->path_len;
    ixw_path_push(w, i + 1);
    ixw_node(w, VECTOR_ELT(positional, i));
    ixw_path_pop(w, saved);
    if (w->depth < 0) return;
  }

  SEXP named = VECTOR_ELT(spec, 3);
  const R_xlen_t nn = XLENGTH(named);
  SEXP names = PROTECT(Rf_getAttrib(named, R_NamesSymbol));
  if (nn > 0 &&
      (TYPEOF(names) != STRSXP || XLENGTH(names) != nn ||
       !mizu_interop_names_ok(names))) {
    UNPROTECT(1);
    ixw_decline(w, "named arguments have duplicate, NA, or non-UTF-8 "
                   "names", NULL);
    return;
  }
  IXW_PUT(w, mizu_ix_put_dict_begin, (uint64_t) nn);
  if (nn > 0) ixw_dict_pairs(w, names, named);
  UNPROTECT(1);
}

/* The task writer (0x12): the header fields through the core emitters,
   then the spec's fields. Refs on (F1): the ref gate emits 0x13 leaves
   and the selected zc node stages the single checkout. A decline fills
   the record and returns 0 — a foreign task with non-portable args can
   never run, so there is no fallback. A mid-write checkout failure
   returns SIZE_MAX — the caller re-runs with no_zc = 1, never a partial
   stream (the core rolls an uncommitted checkout back at the next verb
   entry). plan non-NULL runs the walk in record mode (W2): the
   optimistic write counts past the first candidate, the candidates and
   has_ref recorded for the caller's arithmetic selection. */
size_t mizu_interop_write_task(unsigned char *dst, size_t limit, SEXP spec,
                               uint32_t target, uint64_t ident,
                               mizu_ix_decline *rec, mizu_handle *h,
                               uint32_t caps, uint32_t inline_max,
                               SEXP zc_node, int no_zc,
                               mizu_ix_plan *plan) {
  if (rec != NULL) {
    rec->decline = 0;
    rec->path[0] = '\0';
    rec->reason[0] = '\0';
    rec->remedy[0] = '\0';
  }
  if (!ixw_spec_check(spec))
    Rf_error("mizu: a malformed mizu_call spec");
  const int kind = INTEGER(VECTOR_ELT(spec, 1))[0];
  if (plan != NULL) {
    plan->ncand = 0;
    plan->has_ref = 0;
  }
  const int nocand = plan == NULL || h == NULL || mizu_handle_churn(h);

  mizu_ixw w = { .dst = dst, .limit = limit, .rec = rec,
                 .path = "args", .path_len = 4, .h = h, .caps = caps,
                 .inline_max = inline_max, .refs = 1, .no_zc = no_zc,
                 .zc_node = zc_node, .plan = plan, .plan_nocand = nocand };
  IXW_PUT(&w, mizu_ix_put_header);
  IXW_PUT(&w, mizu_ix_put_task, (int) target, kind, ident);
  ixw_task_fields(&w, spec);
  if (w.abandon) return SIZE_MAX;
  if (w.depth < 0) return 0;
  return w.total;
}

// Map descriptors and runner tasks (Phase 5) ---------------------------------------

/* The map descriptor's 'I' form: one complete stream, list[task, x | nil]
   — the f spec nested as a kind 0/1 task tag (target byte and submitter
   identity included, as the exec tasks'), then the list-x values as a
   bare 0x0c list (names stay submitter-side for assembly), or nil when x
   rides the raw section. Two-pass like the task writer; a decline means a
   non-portable constant or element, the record filled. */
size_t mizu_interop_write_map_desc(unsigned char *dst, size_t limit,
                                   SEXP spec, SEXP x, uint32_t target,
                                   uint64_t ident, mizu_ix_decline *rec) {
  if (rec != NULL) {
    rec->decline = 0;
    rec->path[0] = '\0';
    rec->reason[0] = '\0';
    rec->remedy[0] = '\0';
  }
  if (!ixw_spec_check(spec))
    Rf_error("mizu: a malformed mizu_call spec");
  const int kind = INTEGER(VECTOR_ELT(spec, 1))[0];
  if (x != R_NilValue && TYPEOF(x) != VECSXP && !mizu_view_check(x))
    Rf_error("mizu: a malformed map descriptor x");

  /* refs on (F1's D6): the descriptor shares the value grammar, so a
     view x crosses to foreign workers as a ref — the worker's map
     context holds the resolved view between morsels and the idle sweep
     releases it. No handle: no checkout (x rides its identifier) and no
     caps filter — a layout the workers cannot wrap declines at the
     descriptor read */
  mizu_ixw w = { .dst = dst, .limit = limit, .rec = rec,
                 .path = "args", .path_len = 4, .refs = 1 };
  IXW_PUT(&w, mizu_ix_put_header);
  IXW_PUT(&w, mizu_ix_put_list_begin, 2);
  IXW_PUT(&w, mizu_ix_put_task, (int) target, kind, ident);
  ixw_task_fields(&w, spec);
  if (w.depth < 0) return 0;
  if (x == R_NilValue) {
    IXW_PUT(&w, mizu_ix_put_nil);
  } else if (mizu_view_check(x)) {
    /* a view x: one ref leaf, never an element walk — the worker's batch
       loop reads the resolved view off the shared pages */
    memcpy(w.path, "x", 2);
    w.path_len = 1;
    ixw_node(&w, x);
    if (w.depth < 0) return 0;
  } else {
    const R_xlen_t n = XLENGTH(x);
    IXW_PUT(&w, mizu_ix_put_list_begin, (uint64_t) n);
    w.depth++;
    memcpy(w.path, "x", 2);
    w.path_len = 1;
    for (R_xlen_t i = 0; i < n; i++) {
      size_t saved = w.path_len;
      ixw_path_push(&w, i + 1);
      ixw_node(&w, VECTOR_ELT(x, i));
      ixw_path_pop(&w, saved);
      if (w.depth < 0) return 0;
    }
    w.depth--;
  }
  return w.total;
}

/* The kind-2 (runner) task stream: the header fields, then the region
   name, the packed ordinal+generation i64 (ordinal the high 32 bits, the
   morsel generation the low 32 — DESIGN.md's task kind registry), and the
   seed as nil or the (seed, offset) i64v[2]. Bounded by the region name's
   MIZU_NAME_MAX, so it always fits an entry's inline budget; the inputs
   are the submit veneer's, already validated — the writer cannot
   decline. */
size_t mizu_interop_write_runner(unsigned char *dst, size_t limit,
                                 SEXP name, double gen_field, SEXP seed,
                                 uint32_t target, uint64_t ident) {
  mizu_ixw w = { .dst = dst, .limit = limit, .path = "x", .path_len = 1 };
  IXW_PUT(&w, mizu_ix_put_header);
  IXW_PUT(&w, mizu_ix_put_task, (int) target, 2, ident);
  SEXP cs = STRING_ELT(name, 0);
  IXW_PUT(&w, mizu_ix_put_str, CHAR(cs), (int32_t) LENGTH(cs));
  IXW_PUT(&w, mizu_ix_put_int, (int64_t) gen_field);
  if (seed == R_NilValue) {
    IXW_PUT(&w, mizu_ix_put_nil);
  } else {
    int64_t pair[2];
    pair[0] = (int64_t) REAL(seed)[0];
    pair[1] = (int64_t) REAL(seed)[1];
    IXW_PUT(&w, mizu_ix_put_vec, MIZU_TYPE_INT64, pair, 2);
  }
  return w.total;
}

// Err streams ----------------------------------------------------------------------

/* The err tag (0x11) framer: a condition as the bounded top-level error
   value (DESIGN.md's Interchange codec section). Bounded by truncation —
   type past 128 bytes, message past half the inline budget (the task
   flatten's share), detail past what remains, each cut at a UTF-8
   boundary — so the frame fits the slot by construction and stamps INLINE
   with the keeperless claim: the writer cannot fail. Two modes: a
   mizu_error_remote keeps its origin fields (remote_type, message,
   detail, index — a relay preserves the original type); any other
   condition writes its most-specific class, its raw message field, and
   its call as the detail text. Serves the peer shim's uncaught-error send
   (the stage hook's pointer match) and Phase 4's ERR publish. */

/* One named element of a VECSXP condition, or R_NilValue. */
static SEXP ix_cond_field(SEXP cond, const char *name) {
  if (TYPEOF(cond) != VECSXP) return R_NilValue;
  SEXP names = Rf_getAttrib(cond, R_NamesSymbol);
  if (TYPEOF(names) != STRSXP) return R_NilValue;
  R_xlen_t n = XLENGTH(cond);
  if (n > XLENGTH(names)) n = XLENGTH(names);
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP nm = STRING_ELT(names, i);
    if (nm != NA_STRING && strcmp(CHAR(nm), name) == 0)
      return VECTOR_ELT(cond, i);
  }
  return R_NilValue;
}

/* A length-1 string field's UTF-8 bytes, or NULL (absent, NA, or not
   writable as UTF-8). */
static const char *ix_cond_str(SEXP cond, const char *name, int32_t *n) {
  SEXP f = ix_cond_field(cond, name);
  if (TYPEOF(f) != STRSXP || XLENGTH(f) != 1 || STRING_ELT(f, 0) == NA_STRING)
    return NULL;
  int ok;
  const char *s = ix_char_utf8(STRING_ELT(f, 0), &ok, n);
  return ok ? s : NULL;
}

/* The call element as the detail text: deparse1 (a language or symbol
   call only — anything else is no detail). Returns a CHARSXP. */
static SEXP ix_err_call_text(SEXP call) {
  if (TYPEOF(call) != LANGSXP && TYPEOF(call) != SYMSXP)
    return Rf_mkChar("");
  SEXP q = PROTECT(Rf_lang2(Rf_install("quote"), call));
  SEXP d = PROTECT(Rf_lang2(Rf_install("deparse1"), q));
  SEXP s = PROTECT(Rf_eval(d, R_BaseEnv));
  SEXP out = Rf_mkChar("");
  if (TYPEOF(s) == STRSXP && XLENGTH(s) == 1 && STRING_ELT(s, 0) != NA_STRING)
    out = STRING_ELT(s, 0);
  UNPROTECT(3);
  return out;
}

/* Frame cond as an 'I' err stream: the mode logic resolves the spans
   here; the bounded framer is the core's mizu_ix_write_err (the
   byte-shape helper registry) — dst has inline_max bytes; the return is
   the stream size, always within inline_max (the budget guarantees it),
   so the caller stamps INLINE with the keeperless claim. NULL dst sizes
   only (the test hook's two-pass). */
size_t mizu_interop_write_err(unsigned char *dst, uint32_t inline_max,
                              SEXP cond) {
  const char *type = "", *msg = "", *detail = "";
  int32_t type_n = 0, msg_n = 0, detail_n = 0;
  int has_index = 0;
  uint64_t index = 0;

  /* remote mode: a mizu_error_remote keeps its origin fields (a relay
     preserves the original error's type) */
  SEXP klass = PROTECT(Rf_getAttrib(cond, R_ClassSymbol));
  int remote = 0;
  if (TYPEOF(klass) == STRSXP) {
    for (R_xlen_t i = 0; i < XLENGTH(klass); i++) {
      SEXP cs = STRING_ELT(klass, i);
      if (cs != NA_STRING && strcmp(CHAR(cs), "mizu_error_remote") == 0) {
        remote = 1;
        break;
      }
    }
  }
  if (remote) {
    const char *t = ix_cond_str(cond, "remote_type", &type_n);
    const char *m = ix_cond_str(cond, "message", &msg_n);
    const char *d = ix_cond_str(cond, "detail", &detail_n);
    if (t != NULL && m != NULL && d != NULL) {
      type = t;
      msg = m;
      detail = d;
      SEXP ix = ix_cond_field(cond, "index");
      if ((TYPEOF(ix) == INTSXP || TYPEOF(ix) == REALSXP) &&
          XLENGTH(ix) == 1) {
        const double v = Rf_asReal(ix);
        if (!ISNA(v) && v >= 1 && v <= 9007199254740992.0) {
          has_index = 1;
          index = (uint64_t) v - 1;             /* wire is 0-based */
        }
      }
    } else {
      remote = 0;    /* a malformed hand-built one falls to local mode */
    }
  }
  if (!remote) {
    if (TYPEOF(klass) == STRSXP && XLENGTH(klass) > 0 &&
        STRING_ELT(klass, 0) != NA_STRING) {
      int ok;
      const char *t = ix_char_utf8(STRING_ELT(klass, 0), &ok, &type_n);
      if (ok) {
        type = t;
      } else {
        type = "error";
        type_n = 5;
      }
    } else {
      type = "error";
      type_n = 5;
    }
    const char *m = ix_cond_str(cond, "message", &msg_n);
    if (m != NULL) msg = m;
    SEXP call = ix_cond_field(cond, "call");
    if (call != R_NilValue) {
      int ok;
      const char *d = ix_char_utf8(ix_err_call_text(call), &ok, &detail_n);
      if (ok) detail = d;
    }
    /* a map runner's per-element failure crosses with the in-flight
       element index (the runner's 1-based annotation; the wire is
       0-based) — Phase 5's flags bit 0 */
    SEXP ix = ix_cond_field(cond, "mizu_map_index");
    if ((TYPEOF(ix) == INTSXP || TYPEOF(ix) == REALSXP) &&
        XLENGTH(ix) == 1) {
      const double v = Rf_asReal(ix);
      if (!ISNA(v) && v >= 1 && v <= 9007199254740992.0) {
        has_index = 1;
        index = (uint64_t) v - 1;
      }
    }
  }

  const size_t n = mizu_ix_write_err(dst, inline_max,
                                     type, (size_t) type_n,
                                     msg, (size_t) msg_n,
                                     detail, (size_t) detail_n,
                                     has_index, index);
  UNPROTECT(1);                    /* klass */
  return n;
}

// Reader ---------------------------------------------------------------------------

NORET static void mizu_stop_interop(const char *fmt, ...)
  R_PRINTF_FORMAT(1, 2);
NORET static void mizu_stop_interop(const char *fmt, ...) {
  char buf[512];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof buf, fmt, ap);
  va_end(ap);
  Rf_error("mizu: %s", buf);
}

/* The cursor's error as the R error: the informative text rides the
   core's TLS record. */
NORET static void ixr_stop_tls(void) {
  Rf_error("mizu: %s", mizu_last_error_message());
}

static SEXP ixr_value(mizu_ix *cur, int refs);

static void ixr_expect_str(mizu_ix *cur, mizu_ix_item *it) {
  if (mizu_ix_next(cur, it) != MIZU_OK) ixr_stop_tls();
  if (it->kind != MIZU_IX_STR)
    mizu_stop_interop("malformed interop stream: a bare string was "
                      "expected");
}

static SEXP ixr_charsxp(const mizu_ix_item *it) {
  return it->na ? NA_STRING :
    Rf_mkCharLenCE((const char *) it->ptr, (int) it->len, CE_UTF8);
}

/* Duplicate dict keys are the builder's check (the cursor holds no key
   set): interned CHARSXPs are pointer-unique, so an open-addressed
   pointer set settles it in O(n). */
typedef struct {
  SEXP *slots;
  uint32_t cap;
} ix_keyset;

static int ix_keyset_add(ix_keyset *s, SEXP key) {
  uint32_t h = (uint32_t) ((uintptr_t) key >> 4) & (s->cap - 1);
  while (s->slots[h] != NULL) {
    if (s->slots[h] == key) return 0;
    h = (h + 1) & (s->cap - 1);
  }
  s->slots[h] = key;
  return 1;
}

static void ixr_keyset_init(ix_keyset *ks, uint64_t count) {
  if (count > (uint64_t) 1 << 30)
    mizu_stop_interop("malformed interop stream: an implausible dict count");
  uint32_t cap = 16;
  while (cap < count * 2) cap *= 2;
  ks->slots = (SEXP *) R_alloc((size_t) cap, sizeof(SEXP));
  memset(ks->slots, 0, (size_t) cap * sizeof(SEXP));
  ks->cap = cap;
}

static void ixr_dict_into(mizu_ix *cur, SEXP out, SEXP names, uint64_t n,
                          ix_keyset *ks, int refs) {
  mizu_ix_item it;
  for (uint64_t i = 0; i < n; i++) {
    ixr_expect_str(cur, &it);
    SEXP key = PROTECT(ixr_charsxp(&it));
    if (ks != NULL && !ix_keyset_add(ks, key)) {
      UNPROTECT(1);
      mizu_stop_interop("malformed interop stream: a duplicate dict key");
    }
    SET_STRING_ELT(names, (R_xlen_t) i, key);
    UNPROTECT(1);
    SET_VECTOR_ELT(out, (R_xlen_t) i, ixr_value(cur, refs));
  }
}

// The attr shape builder ------------------------------------------------------------

/* The class and attribute names for the no-home error. off tracks the
   would-be length clamped to the buffer: snprintf reports what it wanted
   to write, so a long key must not carry off past the end (the next
   call's size argument would wrap). */
NORET static void ixr_stop_no_home(SEXP attrs) {
  SEXP keys = Rf_getAttrib(attrs, R_NamesSymbol);
  char msg[512];
  size_t off = (size_t) snprintf(msg, sizeof msg,
    "no portable home for an attributed value (attributes: ");
  for (R_xlen_t i = 0; i < XLENGTH(attrs); i++) {
    off += (size_t) snprintf(msg + off, sizeof msg - off, "%s\"%s\"",
                             i == 0 ? "" : ", ",
                             CHAR(STRING_ELT(keys, i)));
    if (off >= sizeof msg) off = sizeof msg - 1;
  }
  SEXP klass = ix_dict_get(attrs, MIZU_IX_ATTR_CLASS);
  if (TYPEOF(klass) == STRSXP && XLENGTH(klass) > 0) {
    off += (size_t) snprintf(msg + off, sizeof msg - off, "; class: ");
    if (off >= sizeof msg) off = sizeof msg - 1;
    for (R_xlen_t i = 0; i < XLENGTH(klass); i++) {
      off += (size_t) snprintf(msg + off, sizeof msg - off, "%s\"%s\"",
                               i == 0 ? "" : ", ",
                               CHAR(STRING_ELT(klass, i)));
      if (off >= sizeof msg) off = sizeof msg - 1;
    }
  }
  snprintf(msg + off, sizeof msg - off, ")");
  mizu_stop_interop("%s", msg);
}

/* Apply the dict as attributes, class last (the
   mizu_view_set_attrs_from discipline). */
static void ixr_attrs_apply(SEXP value, SEXP attrs) {
  SEXP keys = PROTECT(Rf_getAttrib(attrs, R_NamesSymbol));
  SEXP klass = R_NilValue;
  for (R_xlen_t i = 0; i < XLENGTH(attrs); i++) {
    SEXP nm = Rf_installChar(STRING_ELT(keys, i));
    if (nm == R_ClassSymbol) {
      klass = VECTOR_ELT(attrs, i);
      continue;
    }
    Rf_setAttrib(value, nm, VECTOR_ELT(attrs, i));
  }
  if (klass != R_NilValue) Rf_classgets(value, klass);
  UNPROTECT(1);
}

static int ix_strv_is(SEXP v, const char *s) {
  return TYPEOF(v) == STRSXP && XLENGTH(v) == 1 &&
    strcmp(CHAR(STRING_ELT(v, 0)), s) == 0;
}

static int ix_strv_is2(SEXP v, const char *a, const char *b) {
  return TYPEOF(v) == STRSXP && XLENGTH(v) == 2 &&
    strcmp(CHAR(STRING_ELT(v, 0)), a) == 0 &&
    strcmp(CHAR(STRING_ELT(v, 1)), b) == 0;
}

/* The whitelisted shape check, then the application (validating mode:
   only foreign writers emit the inline attr tag, so the shape is
   checked before anything applies; a dict outside the whitelist is the
   informative no-home error). */
static SEXP ixr_attrs_validated(SEXP value, SEXP attrs) {
  SEXP klass = ix_dict_get(attrs, MIZU_IX_ATTR_CLASS);
  SEXP levels = ix_dict_get(attrs, MIZU_IX_ATTR_LEVELS);
  SEXP dim = ix_dict_get(attrs, MIZU_IX_ATTR_DIM);
  SEXP rn = ix_dict_get(attrs, MIZU_IX_ATTR_ROWNAMES);
  SEXP tz = ix_dict_get(attrs, MIZU_IX_ATTR_TZONE);
  SEXP units = ix_dict_get(attrs, MIZU_IX_ATTR_UNITS);
  const R_xlen_t na = XLENGTH(attrs);

  /* factor: value intv; attrs exactly levels, class */
  if (TYPEOF(value) == INTSXP && na == 2 && TYPEOF(levels) == STRSXP &&
      ix_strv_is(klass, MIZU_IX_CLASS_FACTOR)) {
    ixr_attrs_apply(value, attrs);
    return value;
  }
  /* data.frame: value a plain list of one-length columns; attrs exactly
     names, class, row.names */
  if (TYPEOF(value) == VECSXP && !ANY_ATTRIB(value) && na == 3 &&
      ix_strv_is(klass, MIZU_IX_CLASS_DATAFRAME) && rn != R_NilValue) {
    SEXP cnames = ix_dict_get(attrs, MIZU_IX_ATTR_NAMES);
    const R_xlen_t nc = XLENGTH(value);
    if (nc < 1 || TYPEOF(cnames) != STRSXP || XLENGTH(cnames) != nc ||
        !mizu_interop_names_ok(cnames))
      ixr_stop_no_home(attrs);
    R_xlen_t n = -1;
    for (R_xlen_t j = 0; j < nc; j++) {
      SEXP col = VECTOR_ELT(value, j);
      switch (TYPEOF(col)) {
      case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
      case STRSXP:
        break;
      default:
        ixr_stop_no_home(attrs);
      }
      if (ANY_ATTRIB(col)) {
        if (mizu_view_is_int64(col)) {
          if (ix_attr_count(col) != 1) ixr_stop_no_home(attrs);
        } else {
          const int q = mizu_interop_attrs_qualify(col);
          if (q != MIZU_IXQ_FACTOR && q != MIZU_IXQ_DATE &&
              q != MIZU_IXQ_POSIXCT && q != MIZU_IXQ_DIFFTIME)
            ixr_stop_no_home(attrs);
        }
      }
      if (n < 0) {
        n = XLENGTH(col);
      } else if (XLENGTH(col) != n) {
        ixr_stop_no_home(attrs);
      }
    }
    switch (TYPEOF(rn)) {
    case INTSXP:
      if (XLENGTH(rn) == 2 && INTEGER(rn)[0] == NA_INTEGER) {
        if (INTEGER(rn)[1] != n && INTEGER(rn)[1] != -n)
          ixr_stop_no_home(attrs);
      } else {
        if (XLENGTH(rn) != n) ixr_stop_no_home(attrs);
        for (R_xlen_t i = 0; i < n; i++)
          if (INTEGER(rn)[i] == NA_INTEGER) ixr_stop_no_home(attrs);
      }
      break;
    case STRSXP:
      if (XLENGTH(rn) != n) ixr_stop_no_home(attrs);
      for (R_xlen_t i = 0; i < n; i++)
        if (STRING_ELT(rn, i) == NA_STRING) ixr_stop_no_home(attrs);
      break;
    default:
      ixr_stop_no_home(attrs);
    }
    ixr_attrs_apply(value, attrs);
    return value;
  }
  /* dim array: attrs exactly dim, product == length, value atomic but
     character (an integer64 array's class rides the 0x0e value tag, so
     its dict is {dim} alone here too) */
  if (dim != R_NilValue && na == 1 && TYPEOF(dim) == INTSXP &&
      XLENGTH(dim) >= 1) {
    switch (TYPEOF(value)) {
    case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP:
      break;
    default:
      ixr_stop_no_home(attrs);
    }
    const R_xlen_t nd = XLENGTH(dim);
    const int *d = INTEGER(dim);
    R_xlen_t prod = 1;
    for (R_xlen_t i = 0; i < nd; i++) {
      if (d[i] == NA_INTEGER || d[i] < 0) ixr_stop_no_home(attrs);
      if (d[i] != 0 && prod > XLENGTH(value) / (R_xlen_t) d[i])
        ixr_stop_no_home(attrs);
      prod *= d[i];
    }
    if (prod != XLENGTH(value)) ixr_stop_no_home(attrs);
    ixr_attrs_apply(value, attrs);
    return value;
  }
  /* Date: value realv integral days; attrs exactly class */
  if (TYPEOF(value) == REALSXP && na == 1 &&
      ix_strv_is(klass, MIZU_IX_CLASS_DATE)) {
    if (!ix_date_integral(REAL(value), XLENGTH(value)))
      ixr_stop_no_home(attrs);
    ixr_attrs_apply(value, attrs);
    return value;
  }
  /* POSIXct: value realv epoch seconds; attrs class plus optional tzone */
  if (TYPEOF(value) == REALSXP &&
      (na == 1 || (na == 2 && tz != R_NilValue)) &&
      ix_strv_is2(klass, MIZU_IX_CLASS_POSIXCT, MIZU_IX_CLASS_POSIXT)) {
    if (tz != R_NilValue &&
        (TYPEOF(tz) != STRSXP || XLENGTH(tz) != 1 ||
         STRING_ELT(tz, 0) == NA_STRING))
      ixr_stop_no_home(attrs);
    ixr_attrs_apply(value, attrs);
    return value;
  }
  /* difftime: value realv in the declared units; attrs exactly class and
     a known length-1 units */
  if (TYPEOF(value) == REALSXP && na == 2 && units != R_NilValue &&
      ix_strv_is(klass, MIZU_IX_CLASS_DIFFTIME) && ix_units_known(units)) {
    ixr_attrs_apply(value, attrs);
    return value;
  }
  ixr_stop_no_home(attrs);
}

/* The one (value, attrs) -> SEXP builder: validating mode for the inline
   attr tag (the whitelisted shapes only — only foreign writers emit it);
   apply-as-is mode for the §3.5 layout blob (an R reader homes any
   attribute set). */
SEXP mizu_interop_attrs_build(SEXP value, SEXP attrs, int validate) {
  if (!validate) {
    PROTECT(value);
    PROTECT(attrs);
    ixr_attrs_apply(value, attrs);
    UNPROTECT(2);
    return value;
  }
  PROTECT(value);
  PROTECT(attrs);
  value = ixr_attrs_validated(value, attrs);
  UNPROTECT(2);
  return value;
}

// The layout attribute blob (§3.5: the view layer's embedder hooks) ------------------

/* The blob's row.names value for a frame: automatic forms write the
   compact c(NA, ∓n) (the ixw_rownames discipline — a 1:n ALTREP would
   otherwise serialize a full element per row into the blob). seq1n is
   the qualification's verdict (it scanned already — the seq test
   doubles as its NA check), 0 for character or explicit row names. */
static SEXP ix_blob_rownames(SEXP rn, R_xlen_t n, int seq1n) {
  if (TYPEOF(rn) != INTSXP) return rn;
  if (XLENGTH(rn) == 2 && !ALTREP(rn) && INTEGER(rn)[0] == NA_INTEGER)
    return rn;                         /* already the compact form */
  if (XLENGTH(rn) == 0) seq1n = 1;
  if (!seq1n) return rn;
  SEXP compact = PROTECT(Rf_allocVector(INTSXP, 2));
  INTEGER(compact)[0] = NA_INTEGER;
  INTEGER(compact)[1] = XLENGTH(rn) == 0 ? 0 : (int) -n;
  UNPROTECT(1);
  return compact;
}

/* The attribute set as a plain named list — the blob's dict value:
   R_getAttributes' named list on R >= 4.6, the pairlist walked below.
   seq1n (the qualification's row.names verdict) drives the row.names
   normalization; -1 means "not a frame" (no normalization). Caller
   PROTECTs. */
static SEXP ix_attrs_list(SEXP x, int seq1n) {
  if (!ANY_ATTRIB(x)) return R_NilValue;
  const int frame = seq1n >= 0;
  const R_xlen_t na = ix_attr_count(x);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, na));
  SEXP names = PROTECT(Rf_allocVector(STRSXP, na));
  const R_xlen_t nrows = frame && TYPEOF(x) == VECSXP && XLENGTH(x) > 0 ?
    XLENGTH(VECTOR_ELT(x, 0)) : 0;
#if R_VERSION >= R_Version(4, 6, 0)
  SEXP attrs = PROTECT(R_getAttributes(x));
  SEXP keys = PROTECT(Rf_getAttrib(attrs, R_NamesSymbol));
  for (R_xlen_t i = 0; i < na; i++) {
    SEXP nm = STRING_ELT(keys, i);
    SEXP value = VECTOR_ELT(attrs, i);
    if (frame && Rf_installChar(nm) == R_RowNamesSymbol)
      value = ix_blob_rownames(value, nrows, seq1n);
    SET_VECTOR_ELT(out, i, value);
    SET_STRING_ELT(names, i, nm);
  }
  UNPROTECT(2);
#else
  R_xlen_t i = 0;
  for (SEXP a = ATTRIB(x); a != R_NilValue; a = CDR(a), i++) {
    SEXP value = CAR(a);
    if (frame && TAG(a) == R_RowNamesSymbol)
      value = ix_blob_rownames(value, nrows, seq1n);
    SET_VECTOR_ELT(out, i, value);
    SET_STRING_ELT(names, i, PRINTNAME(TAG(a)));
  }
#endif
  Rf_setAttrib(out, R_NamesSymbol, names);
  UNPROTECT(2);
  return out;
}

/* The blob size hook: a complete 'I' stream whose value is the attribute
   dict, when the set is encodable — else a decline (0) and the view
   layer's R_Serialize fallback. No peer knowledge: the §1.1 zero-copy
   filter keeps a peer without MIZU_CAP_ATTRS off the layouts, and an R
   reader homes any dict. */
static size_t mizu_interop_attrs_blob_size(SEXP x) {
  int rn_seq1n = -1;
  const int q = ix_qualify_blob(x, &rn_seq1n);
  ix_blob_memo_store(x, q, rn_seq1n);
  if (q == MIZU_IXQ_NONE) return 0;
  SEXP attrs = PROTECT(ix_attrs_list(x, q == MIZU_IXQ_FRAME ? rn_seq1n : -1));
  size_t total = attrs == R_NilValue ? 0 :
    mizu_interop_write(NULL, 0, attrs, NULL);
  UNPROTECT(1);
  return total;
}

/* The blob write hook: emits behind the size pass's total — the two
   passes run the same qualification over the same object, so they agree
   (a zero return after a nonzero size is the view layer's bug guard,
   never a fallback). */
static size_t mizu_interop_attrs_blob_write(unsigned char *dst, SEXP x) {
  int rn_seq1n = -1;
  const int q = ix_qualify_blob_mixed(x, &rn_seq1n);
  if (q == MIZU_IXQ_NONE) return 0;
  SEXP attrs = PROTECT(ix_attrs_list(x, q == MIZU_IXQ_FRAME ? rn_seq1n : -1));
  size_t total = attrs == R_NilValue ? 0 :
    mizu_interop_write(dst, SIZE_MAX, attrs, NULL);
  UNPROTECT(1);
  return total;
}

/* The blob read hook: the cursor opens the 'I' stream (its own errors
   reject a malformed blob in the corrupt-region house style), the dict
   applies apply-as-is — no frame-shape validation on a layout read: an
   R reader homes any attribute set, whoever wrote the region, and a
   legitimate frame's columns legitimately escape the whitelisted shapes
   (list, matrix, Date and ordered-factor columns ride leaf blobs). */
static void mizu_interop_attrs_blob_read(SEXP result, const unsigned char *buf,
                                         size_t size) {
  SEXP attrs = PROTECT(mizu_interop_read(buf, size));
  mizu_interop_attrs_build(result, attrs, 0);
  UNPROTECT(1);
}

/* The err tag's R home: a mizu_error_remote condition as a *value* — the
   channel's remote-error discipline is that user code decides to raise
   (mizu_raise). Fields: message, remote_type, detail, and index (1-based)
   when the flags carry one. The cursor has validated the three bare
   strings as UTF-8; they are never NA. */
static SEXP ixr_err(const mizu_ix_item *it) {
  static const char *const fnames[4] =
    { "message", "remote_type", "detail", "index" };
  static const int order[3] = { 1, 0, 2 };   /* wire: type, message, detail */
  const int has_index = (it->err_flags & 1u) != 0;
  const R_xlen_t n = has_index ? 4 : 3;
  SEXP cond = PROTECT(Rf_allocVector(VECSXP, n));
  SEXP names = PROTECT(Rf_allocVector(STRSXP, n));
  SEXP klass = PROTECT(Rf_allocVector(STRSXP, 4));
  for (int i = 0; i < 3; i++) {
    SEXP s = PROTECT(Rf_allocVector(STRSXP, 1));
    SET_STRING_ELT(s, 0,
                   Rf_mkCharLenCE((const char *) it->err_str[order[i]].ptr,
                                  (int) it->err_str[order[i]].len, CE_UTF8));
    SET_VECTOR_ELT(cond, i, s);
    SET_STRING_ELT(names, i, Rf_mkChar(fnames[i]));
    UNPROTECT(1);
  }
  if (has_index) {
    const uint64_t w = it->err_index;
    SET_VECTOR_ELT(cond, 3,
                   w < (uint64_t) INT32_MAX ?
                     Rf_ScalarInteger((int) w + 1) :
                     Rf_ScalarReal((double) w + 1.0));
    SET_STRING_ELT(names, 3, Rf_mkChar(fnames[3]));
  }
  SET_STRING_ELT(klass, 0, Rf_mkChar("mizu_error_remote"));
  SET_STRING_ELT(klass, 1, Rf_mkChar("mizu_error"));
  SET_STRING_ELT(klass, 2, Rf_mkChar("error"));
  SET_STRING_ELT(klass, 3, Rf_mkChar("condition"));
  Rf_setAttrib(cond, R_NamesSymbol, names);
  Rf_classgets(cond, klass);
  UNPROTECT(3);
  return cond;
}

/* The value builder over the cursor. refs is the 0x13 mode: 0 declines
   (the channel value reader — a ref never crosses as a channel value),
   1 resolves to a view (the task decode, the map descriptor reader, and
   the pool's collect-side result reader — the counted add lands in the
   resolve, the consumer-mapping cache dedupes), 2 represents the
   identifier as a classed string for the corpus's conformance loop (the
   hook decode — never resolves, so synthetic identifiers round-trip). */
static SEXP ixr_value(mizu_ix *cur, int refs) {
  mizu_ix_item it;
  if (mizu_ix_next(cur, &it) != MIZU_OK) ixr_stop_tls();
  R_CheckStack();

  switch (it.kind) {
  case MIZU_IX_NIL:
    return R_NilValue;
  case MIZU_IX_LGL:
    return Rf_ScalarLogical(it.u64[0] == 2 ? NA_LOGICAL : (int) it.u64[0]);
  case MIZU_IX_INT: {
    int64_t v;
    memcpy(&v, &it.u64[0], 8);
    if (v == INT64_MIN) return Rf_ScalarInteger(NA_INTEGER);
    if (v > INT32_MIN && v <= INT32_MAX)
      return Rf_ScalarInteger((int) v);
    SEXP out = PROTECT(mizu_wire_alloc(MIZU_TYPE_INT64, 1));
    memcpy(REAL(out), &v, 8);
    UNPROTECT(1);
    return out;
  }
  case MIZU_IX_REAL: {
    double v;
    memcpy(&v, &it.u64[0], 8);
    return Rf_ScalarReal(v);
  }
  case MIZU_IX_CPLX: {
    Rcomplex v;
    memcpy(&v.r, &it.u64[0], 8);
    memcpy(&v.i, &it.u64[1], 8);
    return Rf_ScalarComplex(v);
  }
  case MIZU_IX_STR1:
    return Rf_ScalarString(ixr_charsxp(&it));
  case MIZU_IX_BYTES: {
    SEXP out = PROTECT(Rf_allocVector(RAWSXP, (R_xlen_t) it.count));
    memcpy(RAW(out), it.ptr, (size_t) it.count);
    UNPROTECT(1);
    return out;
  }
  case MIZU_IX_VEC: {
    SEXP out = PROTECT(mizu_wire_alloc((int) it.type, (R_xlen_t) it.count));
    memcpy(mizu_vec_ptr(out), it.ptr,
           (size_t) it.count * mizu_view_sizeof_elt(
             it.type == MIZU_TYPE_INT64 ? REALSXP : (int) it.type));
    UNPROTECT(1);
    return out;
  }
  case MIZU_IX_STRV: {
    SEXP out = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t) it.count));
    for (uint64_t i = 0; i < it.count; i++) {
      mizu_ix_item elt;
      ixr_expect_str(cur, &elt);
      SET_STRING_ELT(out, (R_xlen_t) i, ixr_charsxp(&elt));
    }
    UNPROTECT(1);
    return out;
  }
  case MIZU_IX_LIST: {
    SEXP out = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) it.count));
    for (uint64_t i = 0; i < it.count; i++)
      SET_VECTOR_ELT(out, (R_xlen_t) i, ixr_value(cur, refs));
    UNPROTECT(1);
    return out;
  }
  case MIZU_IX_DICT: {
    SEXP names = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t) it.count));
    SEXP out = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) it.count));
    ix_keyset ks = { NULL, 0 };
    if (it.count != 0) ixr_keyset_init(&ks, it.count);
    ixr_dict_into(cur, out, names, it.count, it.count != 0 ? &ks : NULL,
                  refs);
    Rf_setAttrib(out, R_NamesSymbol, names);
    UNPROTECT(2);
    return out;
  }
  case MIZU_IX_ATTR: {
    SEXP value = PROTECT(ixr_value(cur, refs));
    SEXP attrs = PROTECT(ixr_value(cur, refs));
    value = mizu_interop_attrs_build(value, attrs, 1);
    UNPROTECT(2);
    return value;
  }
  case MIZU_IX_ERR:
    return ixr_err(&it);
  case MIZU_IX_REF: {
    if (refs == 0)
      mizu_stop_interop("an interop ref cannot cross as a channel value");
    char idbuf[MIZU_VIEW_IDENTIFIER_MAX];
    /* the cursor bounded the span to 1..255 */
    memcpy(idbuf, it.ptr, it.len);
    idbuf[it.len] = '\0';
    if (refs == 2) {
      SEXP out = PROTECT(Rf_ScalarString(
        Rf_mkCharLenCE(idbuf, (int) it.len, CE_UTF8)));
      SEXP klass = PROTECT(Rf_allocVector(STRSXP, 1));
      SET_STRING_ELT(klass, 0, Rf_mkChar("mizu_ix_ref"));
      Rf_setAttrib(out, R_ClassSymbol, klass);
      UNPROTECT(2);
      return out;
    }
    SEXP out = mizu_view_resolve_id(idbuf, R_NilValue);
    if (out == R_NilValue)
      mizu_stop_interop("a malformed interop ref identifier");
    return out;
  }
  case MIZU_IX_TASK:
    mizu_stop_interop("an interop task is not a value");
  default:
    mizu_stop_interop("an unknown interop item");
  }
}

// The task stream decode (Phase 4) ----------------------------------------------

/* The per-field shape checks are the core's mizu_ixt_* decode shim (the
   byte-shape helper registry — the texts record through the TLS slot, so
   they reach here through ixr_stop_tls like any cursor failure); what
   stays local is the raise idiom for the plain item pulls. */
static void ixt_next(mizu_ix *cur, mizu_ix_item *it) {
  if (mizu_ix_next(cur, it) != MIZU_OK) ixr_stop_tls();
}

/* Resolve the qualified name through the worker's own namespace machinery
   (there is no name registry): pkg::fn / pkg:::fn, the namespace loaded
   on demand; findFun searches the namespace, its imports and base — R's
   own resolution order — and raises on a miss (the exec hook contains
   it). The span is the validated UTF-8 code string. */
static SEXP ixt_resolve_name(const unsigned char *code, uint64_t len) {
  uint64_t sep = 0;
  while (sep + 1 < len && !(code[sep] == ':' && code[sep + 1] == ':')) sep++;
  uint64_t fn_off = sep + 2;
  if (fn_off < len && code[fn_off] == ':') fn_off++;
  if (sep + 1 >= len || sep == 0 || fn_off >= len)
    mizu_stop_interop("malformed task stream: the task name is not "
                      "qualified");
  SEXP pkg = PROTECT(Rf_mkCharLenCE((const char *) code, (int) sep,
                                    CE_UTF8));
  SEXP call = PROTECT(Rf_lang2(Rf_install("loadNamespace"),
                               Rf_ScalarString(pkg)));
  SEXP ns = PROTECT(Rf_eval(call, R_BaseEnv));
  SEXP fn = PROTECT(Rf_mkCharLenCE((const char *) code + fn_off,
                                   (int) (len - fn_off), CE_UTF8));
  SEXP value = Rf_findFun(Rf_installChar(fn), ns);
  UNPROTECT(4);
  return value;    /* a function, anchored by the namespace */
}

/* The per-kind call construction, shared by the exec decode and the map
   descriptor decode: the cursor stands past the TASK item; builds only
   what the call needs — the name kind conses its LANGSXP directly as the
   cursor yields the arguments (the do.call shape); the source kind builds
   a closure over the named-argument frame — the parsed forms its body
   (the trailing form's value is the result, the §4.0 convention), the
   positional arguments riding its "..." so the source's ..N pronouns
   resolve (a plain "..1" binding never does — the dots mechanism owns
   those names) — and the element the first formal on the map path.
   Every value is stored the moment it is allocated (the mizu_codec_read
   discipline). No finishing check: the caller owns the stream boundary.
   Raises the informative shape errors; the exec hook contains them. */
static SEXP ixt_call(mizu_ix *cur, int kind, SEXP base, int map) {
  mizu_ix_item code, pos, named;
  if (mizu_ixt_want_code(cur, &code) != MIZU_OK) ixr_stop_tls();
  if (mizu_ixt_want_list(cur, &pos) != MIZU_OK) ixr_stop_tls();
  SEXP out;
  if (kind == 0) {
    /* fn stays protected to the end — it is the call's CAR anyway, and
       the LIFO pop order is the one rule rchk reads strictly */
    SEXP fn = PROTECT(ixt_resolve_name(code.ptr, code.len));
    out = PROTECT(Rf_lcons(fn, R_NilValue));
    SEXP tail = out;
    for (uint64_t i = 0; i < pos.count; i++) {
      SEXP cell = PROTECT(Rf_cons(R_NilValue, R_NilValue));
      SETCAR(cell, ixr_value(cur, 1));
      SETCDR(tail, cell);
      tail = cell;
      UNPROTECT(1);
    }
    if (mizu_ixt_want_dict(cur, &named) != MIZU_OK) ixr_stop_tls();
    ix_keyset ks = { NULL, 0 };
    if (named.count != 0) ixr_keyset_init(&ks, named.count);
    for (uint64_t i = 0; i < named.count; i++) {
      mizu_ix_item key;
      ixr_expect_str(cur, &key);
      SEXP cs = PROTECT(ixr_charsxp(&key));
      if (!ix_keyset_add(&ks, cs)) {
        UNPROTECT(1);
        mizu_stop_interop("malformed task stream: a duplicate dict key");
      }
      SEXP cell = PROTECT(Rf_cons(R_NilValue, R_NilValue));
      SET_TAG(cell, Rf_installChar(cs));
      SETCAR(cell, ixr_value(cur, 1));
      SETCDR(tail, cell);
      tail = cell;
      UNPROTECT(2);
    }
    UNPROTECT(2);                  /* fn, out */
    return out;
  }
  SEXP cs = PROTECT(Rf_ScalarString(ixr_charsxp(&code)));
  ParseStatus status;
  SEXP exprs = PROTECT(R_ParseVector(cs, -1, &status, R_NilValue));
  if (status != PARSE_OK)
    mizu_stop_interop("malformed task stream: the source does not parse");
  SEXP env = PROTECT(R_NewEnv(base, 0, 0));
  /* the positional arguments as a plain list (they ride the call's "..."),
     the named ones bound in the closure's frame */
  SEXP pos_args = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) pos.count));
  for (uint64_t i = 0; i < pos.count; i++)
    SET_VECTOR_ELT(pos_args, (R_xlen_t) i, ixr_value(cur, 1));
  if (mizu_ixt_want_dict(cur, &named) != MIZU_OK) ixr_stop_tls();
  ix_keyset ks = { NULL, 0 };
  if (named.count != 0) ixr_keyset_init(&ks, named.count);
  for (uint64_t i = 0; i < named.count; i++) {
    mizu_ix_item key;
    ixr_expect_str(cur, &key);
    SEXP kc = PROTECT(ixr_charsxp(&key));
    if (!ix_keyset_add(&ks, kc)) {
      UNPROTECT(1);
      mizu_stop_interop("malformed task stream: a duplicate dict key");
    }
    SEXP v = PROTECT(ixr_value(cur, 1));
    Rf_defineVar(Rf_installChar(kc), v, env);
    UNPROTECT(2);
  }
  SEXP body = PROTECT(Rf_lcons(Rf_install("{"), R_NilValue));
  SEXP tail = body;
  for (R_xlen_t i = 0; i < Rf_xlength(exprs); i++) {
    SEXP cell = PROTECT(Rf_cons(VECTOR_ELT(exprs, i), R_NilValue));
    SETCDR(tail, cell);
    tail = cell;
    UNPROTECT(1);
  }
  SEXP dots = PROTECT(Rf_cons(R_MissingArg, R_NilValue));
  SET_TAG(dots, R_DotsSymbol);
  SEXP formals = PROTECT(map ? Rf_cons(R_MissingArg, dots) : dots);
  if (map) SET_TAG(formals, Rf_install("x"));
  SEXP fn = PROTECT(R_mkClosure(formals, body, env));
  out = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(out, 0, fn);
  SET_VECTOR_ELT(out, 1, pos_args);
  UNPROTECT(9);
  return out;
}

/* The exec-hook decode: the task stream's one value, then the cursor's
   finishing check. */
SEXP mizu_interop_exec_task(const unsigned char *buf, size_t len, SEXP base,
                            int *kind_out) {
  mizu_ix cur;
  mizu_ix_item it;
  if (mizu_ixt_open(&cur, buf, len, &it, 1) != MIZU_OK) ixr_stop_tls();
  /* the submitter identity stashes ahead of every field read, so even a
     torn stream fails the task in the submitter's own format */
  mizu_curpool_ident = it.u64[0];
  const int kind = (int) it.task_kind;
  *kind_out = kind;
  SEXP out = PROTECT(ixt_call(&cur, kind, base, 0));
  if (mizu_ix_end(&cur) != MIZU_OK) {
    UNPROTECT(1);
    ixr_stop_tls();
  }
  UNPROTECT(1);
  return out;
}

// The runner stream and map descriptor decodes (Phase 5) --------------------------

/* The kind-2 (runner) decode for the exec hook: region name, the packed
   ordinal+generation i64, and the seed (nil, or the (seed, offset) i64
   pair, returned as doubles). Returns list(name, gen_field, seed|NULL);
   the R side unpacks and evals. The submitter identity stashes as the
   call kinds' — the runner's result policy keys on it. */
SEXP mizu_interop_exec_runner(const unsigned char *buf, size_t len) {
  mizu_ix cur;
  mizu_ix_item it, name, gen, seed;
  if (mizu_ixt_open(&cur, buf, len, &it, 2) != MIZU_OK) ixr_stop_tls();
  mizu_curpool_ident = it.u64[0];   /* stashed as the call kinds' */
  if (it.task_kind != 2)
    mizu_stop_interop("malformed runner stream: not a runner task");
  ixt_next(&cur, &name);
  if (name.kind != MIZU_IX_STR1 || name.na)
    mizu_stop_interop("malformed runner stream: the region name is not a "
                      "string");
  ixt_next(&cur, &gen);
  if (gen.kind != MIZU_IX_INT)
    mizu_stop_interop("malformed runner stream: the generation is not an "
                      "integer");
  ixt_next(&cur, &seed);
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 3));
  int64_t gv;
  memcpy(&gv, &gen.u64[0], 8);
  SET_VECTOR_ELT(out, 0, Rf_ScalarString(ixr_charsxp(&name)));
  SET_VECTOR_ELT(out, 1, Rf_ScalarReal((double) gv));
  if (seed.kind == MIZU_IX_NIL) {
    SET_VECTOR_ELT(out, 2, R_NilValue);
  } else if (seed.kind == MIZU_IX_VEC && seed.type == MIZU_TYPE_INT64 &&
             seed.count == 2) {
    int64_t pair[2];
    memcpy(pair, seed.ptr, 16);
    SEXP sd = PROTECT(Rf_allocVector(REALSXP, 2));
    REAL(sd)[0] = (double) pair[0];
    REAL(sd)[1] = (double) pair[1];
    SET_VECTOR_ELT(out, 2, sd);
    UNPROTECT(1);
  } else {
    UNPROTECT(1);
    mizu_stop_interop("malformed runner stream: the seed is not nil or an "
                      "i64 pair");
  }
  if (mizu_ix_end(&cur) != MIZU_OK) {
    UNPROTECT(1);
    ixr_stop_tls();
  }
  UNPROTECT(1);
  return out;
}

/* The map descriptor's 'I' form: list[task, x | nil] — the descriptor
   reader is the one other builder with a task case (DESIGN.md): the spec
   decodes through the exec decode's per-kind construction (no stash —
   the runner task's own identity rules the result policy), the list-x
   through the generic builder. Returns list(kind, product, x|NULL):
   name kind: the consed LANGSXP; source kind: list(exprs, env). base is
   the frame parent for the source kind (the global environment — the
   mizu_call() contract). */
SEXP mizu_interop_read_map_desc(const unsigned char *buf, size_t len,
                                SEXP base) {
  mizu_ix cur;
  mizu_ix_item it;
  if (mizu_ix_open(&cur, buf, len) != MIZU_OK) ixr_stop_tls();
  ixt_next(&cur, &it);
  if (it.kind != MIZU_IX_LIST || it.count != 2)
    mizu_stop_interop("malformed map descriptor: not a two-element list");
  ixt_next(&cur, &it);
  if (it.kind != MIZU_IX_TASK)
    mizu_stop_interop("malformed map descriptor: no task tag");
  if (it.task_kind > 1)
    mizu_stop_interop("malformed map descriptor: kind 0x%02X is not a "
                      "call spec", it.task_kind);
  SEXP task = PROTECT(ixt_call(&cur, (int) it.task_kind, base, 1));
  SEXP x = PROTECT(ixr_value(&cur, 1));
  if (mizu_ix_end(&cur) != MIZU_OK) {
    UNPROTECT(2);
    ixr_stop_tls();
  }
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 3));
  SET_VECTOR_ELT(out, 0, Rf_ScalarInteger((int) it.task_kind));
  SET_VECTOR_ELT(out, 1, task);
  SET_VECTOR_ELT(out, 2, x);
  /* the worker's map context dispatches on the descriptor form — a class
     beats shape-sniffing beside the private (f, dots[, x]) triple */
  SEXP klass = PROTECT(Rf_allocVector(STRSXP, 1));
  SET_STRING_ELT(klass, 0, Rf_mkChar("mizu_map_ix"));
  Rf_setAttrib(out, R_ClassSymbol, klass);
  UNPROTECT(4);
  return out;
}

/* The builder entry: one value, then the cursor's finishing check. */
SEXP mizu_interop_read_mode(const unsigned char *buf, size_t len, int refs) {
  mizu_ix cur;
  if (mizu_ix_open(&cur, buf, len) != MIZU_OK) ixr_stop_tls();
  SEXP out = PROTECT(ixr_value(&cur, refs));
  if (mizu_ix_end(&cur) != MIZU_OK) {
    UNPROTECT(1);
    ixr_stop_tls();
  }
  UNPROTECT(1);
  return out;
}

/* The channel mode entry: refs off — a 0x13 leaf raises the builder
   decline. */
SEXP mizu_interop_read(const unsigned char *buf, size_t len) {
  return mizu_interop_read_mode(buf, len, 0);
}

// Test hooks (init.c) -----------------------------------------------------------------

SEXP mizu_interop_write_call(SEXP object) {
  mizu_ix_decline rec;
  size_t n = mizu_interop_write(NULL, 0, object, &rec);
  if (n == 0)
    mizu_stop_interop("not portable at %s (%s)", rec.path, rec.reason);
  SEXP bytes = PROTECT(Rf_allocVector(RAWSXP, (R_xlen_t) n));
  size_t wrote = mizu_interop_write(RAW(bytes), n, object, NULL);
  if (wrote != n) {
    UNPROTECT(1);
    mizu_stop_interop("interop write mismatch");
  }
  UNPROTECT(1);
  return bytes;
}

SEXP mizu_interop_read_call(SEXP bytes) {
  if (TYPEOF(bytes) != RAWSXP) Rf_error("mizu: expected a raw vector");
  return mizu_interop_read(RAW(bytes), (size_t) XLENGTH(bytes));
}

/* The err framer, two-pass behind a budget argument (240 is the default
   slot's inline budget). */
SEXP mizu_interop_write_err_call(SEXP cond, SEXP budget) {
  const int cap = Rf_asInteger(budget);
  if (cap < 0) Rf_error("mizu: budget must be non-negative");
  size_t n = mizu_interop_write_err(NULL, (uint32_t) cap, cond);
  SEXP out = PROTECT(Rf_allocVector(RAWSXP, (R_xlen_t) n));
  if (mizu_interop_write_err(RAW(out), (uint32_t) cap, cond) != n) {
    UNPROTECT(1);
    mizu_stop_interop("interop err write mismatch");
  }
  UNPROTECT(1);
  return out;
}

/* The whole-dispatch read over a synthetic channel context (the decline
   stashes on a scratch prot chain): the 'I' branch, the private-codec
   branches, and the unlisted-byte consumed decline without a live
   channel. Returns the value, or the stashed condition on a consume. */
SEXP mizu_stream_read_call(SEXP bytes) {
  if (TYPEOF(bytes) != RAWSXP) Rf_error("mizu: expected a raw vector");
  mizu_r_handle rh = { 0 };
  SEXP prot = PROTECT(Rf_allocVector(VECSXP, 3));
  SET_VECTOR_ELT(prot, 0, Rf_allocVector(VECSXP, MIZU_OPEN_CACHE_MAX));
  rh.prot = prot;
  rh.decline_slot = 2;
  rh.zoc.wraps = VECTOR_ELT(prot, 0);
  mizu_read_ctx ctx = { 0 };
  ctx.size = sizeof(ctx);
  ctx.binding_ctx = &rh;
  SEXP out = mizu_stream_read(RAW(bytes), (size_t) XLENGTH(bytes), &ctx, 1);
  if (out == NULL) {
    SEXP rec = VECTOR_ELT(rh.prot, rh.decline_slot);
    if (rec != R_NilValue) {
      UNPROTECT(1);
      return rec;
    }
    UNPROTECT(1);
    Rf_error("mizu: read failed");
  }
  UNPROTECT(1);
  return out;
}

/* The task writer as a stream (target a language byte, ident the whole
   submitter word as a double — the corpus's words are < 2^53). h is
   NULL (refs on, no checkout): a REF-qualifying view emits its ref leaf
   and the corpus's marker strings round-trip; no_zc exercises the
   degrade path. */
SEXP mizu_interop_write_task_call(SEXP spec, SEXP target, SEXP ident,
                                  SEXP no_zc) {
  const int t = Rf_asInteger(target);
  if (t < 0 || t > 255) Rf_error("mizu: expected a language byte");
  const uint64_t id = (uint64_t) Rf_asReal(ident);
  const int nz = Rf_asLogical(no_zc) == 1;
  mizu_ix_decline rec;
  size_t n = mizu_interop_write_task(NULL, 0, spec, (uint32_t) t, id, &rec,
                                     NULL, 0, 240, R_NilValue, nz, NULL);
  if (n == 0)
    mizu_stop_interop("not portable at %s (%s)", rec.path, rec.reason);
  SEXP bytes = PROTECT(Rf_allocVector(RAWSXP, (R_xlen_t) n));
  if (mizu_interop_write_task(RAW(bytes), n, spec, (uint32_t) t, id, NULL,
                              NULL, 0, 240, R_NilValue, nz, NULL) != n) {
    UNPROTECT(1);
    mizu_stop_interop("task write mismatch");
  }
  UNPROTECT(1);
  return bytes;
}

/* The map descriptor writer as a stream: the map path's spec staging
   (x the list-x, or NULL when the raw section carries it). */
SEXP mizu_interop_map_desc_call(SEXP spec, SEXP x, SEXP target) {
  const int t = Rf_asInteger(target);
  if (t < 0 || t > 255) Rf_error("mizu: expected a language byte");
  mizu_ix_decline rec;
  size_t n = mizu_interop_write_map_desc(NULL, 0, spec, x, (uint32_t) t,
                                         MIZU_R_IDENT, &rec);
  if (n == 0)
    mizu_stop_not_portable(rec.path, rec.reason, rec.remedy);
  SEXP bytes = PROTECT(Rf_allocVector(RAWSXP, (R_xlen_t) n));
  if (mizu_interop_write_map_desc(RAW(bytes), n, spec, x, (uint32_t) t,
                                  MIZU_R_IDENT, NULL) != n) {
    UNPROTECT(1);
    mizu_stop_interop("map descriptor write mismatch");
  }
  UNPROTECT(1);
  return bytes;
}

/* The runner writer as a stream (the map submit path's staging and the
   test suite's stream construction). */
SEXP mizu_interop_runner_call(SEXP name, SEXP gen_field, SEXP seed,
                              SEXP target, SEXP ident) {
  const int t = Rf_asInteger(target);
  if (t < 0 || t > 255) Rf_error("mizu: expected a language byte");
  const uint64_t id = (uint64_t) Rf_asReal(ident);
  size_t n = mizu_interop_write_runner(NULL, 0, name, Rf_asReal(gen_field),
                                       seed, (uint32_t) t, id);
  SEXP bytes = PROTECT(Rf_allocVector(RAWSXP, (R_xlen_t) n));
  if (mizu_interop_write_runner(RAW(bytes), n, name, Rf_asReal(gen_field),
                                seed, (uint32_t) t, id) != n) {
    UNPROTECT(1);
    mizu_stop_interop("runner write mismatch");
  }
  UNPROTECT(1);
  return bytes;
}

/* The hook decode: the same cursor walk and shape checks as the exec
   decode, building the components for inspection (no name resolution, no
   eval). The stash is not touched — this is not a worker. */
SEXP mizu_interop_read_task_call(SEXP bytes) {
  if (TYPEOF(bytes) != RAWSXP) Rf_error("mizu: expected a raw vector");
  mizu_ix cur;
  mizu_ix_item it, code, pos, named;
  if (mizu_ixt_open(&cur, RAW(bytes), (size_t) XLENGTH(bytes), &it, 1) !=
      MIZU_OK)
    ixr_stop_tls();
  if (mizu_ixt_want_code(&cur, &code) != MIZU_OK) ixr_stop_tls();
  if (mizu_ixt_want_list(&cur, &pos) != MIZU_OK) ixr_stop_tls();
  SEXP positional = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) pos.count));
  for (uint64_t i = 0; i < pos.count; i++)
    SET_VECTOR_ELT(positional, (R_xlen_t) i, ixr_value(&cur, 2));
  if (mizu_ixt_want_dict(&cur, &named) != MIZU_OK) ixr_stop_tls();
  SEXP nn = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t) named.count));
  SEXP nv = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) named.count));
  ix_keyset ks = { NULL, 0 };
  if (named.count != 0) ixr_keyset_init(&ks, named.count);
  for (uint64_t i = 0; i < named.count; i++) {
    mizu_ix_item key;
    ixr_expect_str(&cur, &key);
    SEXP kc = PROTECT(ixr_charsxp(&key));
    if (!ix_keyset_add(&ks, kc)) {
      UNPROTECT(1);
      mizu_stop_interop("malformed task stream: a duplicate dict key");
    }
    SET_STRING_ELT(nn, (R_xlen_t) i, kc);
    SET_VECTOR_ELT(nv, (R_xlen_t) i, ixr_value(&cur, 2));
    UNPROTECT(1);
  }
  Rf_setAttrib(nv, R_NamesSymbol, nn);
  if (mizu_ix_end(&cur) != MIZU_OK) {
    UNPROTECT(3);
    ixr_stop_tls();
  }
  const char *names[] = { "target", "kind", "ident", "code", "positional",
                          "named", "" };
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_ScalarInteger((int) it.target));
  SET_VECTOR_ELT(out, 1, Rf_ScalarInteger((int) it.task_kind));
  SET_VECTOR_ELT(out, 2, Rf_ScalarReal((double) it.u64[0]));
  SET_VECTOR_ELT(out, 3, Rf_ScalarString(ixr_charsxp(&code)));
  SET_VECTOR_ELT(out, 4, positional);
  SET_VECTOR_ELT(out, 5, nv);
  UNPROTECT(4);
  return out;
}

/* The foreign STR1: a top-level length-1 string crosses as today, but
   normalized to UTF-8 first — ASCII and CE_UTF8 as-is, native after the
   validity check, latin1 translated; a bytes-marked string declines
   (aux always CE_UTF8: an R peer reading it back is identical()-clean).
   Returns 1 when staged, 0 when not a length-1 string, and -1 on a
   decline (the record filled). */
int mizu_interop_str1_foreign(mizu_slot_hdr *hdr, unsigned char *payload,
                              uint32_t inline_max, SEXP x,
                              mizu_ix_decline *rec) {
  if (TYPEOF(x) != STRSXP || XLENGTH(x) != 1 || ANY_ATTRIB(x) ||
      Rf_isS4(x))
    return 0;
  SEXP cs = STRING_ELT(x, 0);
  if (cs == NA_STRING) {
    hdr->kind = MIZU_KIND_STR1;
    hdr->len = 0;
    hdr->aux = MIZU_STR1_NA;
    return 1;
  }
  int ok;
  int32_t len;
  const char *u = ix_char_utf8(cs, &ok, &len);
  if (!ok) {
    if (rec != NULL) {
      rec->decline = 1;
      snprintf(rec->path, sizeof rec->path, "x");
      snprintf(rec->reason, sizeof rec->reason, "%s",
               "a string is not writable as UTF-8 (CE_BYTES or invalid "
               "native bytes)");
      rec->remedy[0] = '\0';
    }
    return -1;
  }
  if ((uint32_t) len > inline_max) return 0;
  hdr->kind = MIZU_KIND_STR1;
  hdr->len = (uint32_t) len;
  hdr->aux = MIZU_CE_UTF8;
  memcpy(payload, u, (size_t) len);
  return 1;
}

// Init --------------------------------------------------------------------------------

/* Fill-then-preserve, protected across the allocating mkChar calls. */
static SEXP ix_class_vec(const char *a, const char *b) {
  SEXP v = PROTECT(Rf_allocVector(STRSXP, b != NULL ? 2 : 1));
  SET_STRING_ELT(v, 0, Rf_mkChar(a));
  if (b != NULL) SET_STRING_ELT(v, 1, Rf_mkChar(b));
  R_PreserveObject(v);
  UNPROTECT(1);
  return v;
}

void mizu_interop_init(void) {
  ix_tzone_sym = Rf_install(MIZU_IX_ATTR_TZONE);
  ix_units_sym = Rf_install(MIZU_IX_ATTR_UNITS);
  ix_date_class = ix_class_vec(MIZU_IX_CLASS_DATE, NULL);
  ix_difftime_class = ix_class_vec(MIZU_IX_CLASS_DIFFTIME, NULL);
  ix_factor_class = ix_class_vec(MIZU_IX_CLASS_FACTOR, NULL);
  ix_frame_class = ix_class_vec(MIZU_IX_CLASS_DATAFRAME, NULL);
  ix_posixct_class = ix_class_vec(MIZU_IX_CLASS_POSIXCT, MIZU_IX_CLASS_POSIXT);
  ix_ordered_class = ix_class_vec("ordered", "factor");
  ix_ref_marker_class = ix_class_vec("mizu_ix_ref", NULL);
  /* the view layer's attribute blobs are 'I' streams from here (mori
     leaves the triple unset and keeps R_Serialize both ways) */
  mizu_view_set_attrs_hooks(mizu_interop_attrs_blob_size,
                            mizu_interop_attrs_blob_write,
                            mizu_interop_attrs_blob_read);
}
