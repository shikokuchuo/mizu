/* The sora compact codec: a purpose-built binary framing for the payload
   subset that dominates the hot paths — NULL, symbols, atomic vectors
   (attributes included), strings, list/vector trees, and calls — written
   and read straight off the slot bytes with no R serializer involvement.

   Why it exists: R_Serialize pays a VECSXP(1099) ref-tracking hash table
   (and R_Unserialize a VECSXP(128) read table) on EVERY call — several
   hundred nanoseconds plus ~10 KB of immediate garbage per payload per
   side, the largest remaining cost of a small pool task or channel
   message after the immediate kinds. R's own writer never references
   vectors or pairlist nodes through that table (only symbols,
   environments, and pointers are HashAdded), so a writer that simply
   never emits references loses nothing for this subset — interned
   symbols round-trip through installTrChar exactly as R's reader
   produces them.

   Streams are self-describing: the first byte is SORA_CODEC_MAGIC where
   an R binary stream carries 'B', so the read paths dispatch on the
   payload's first byte and the sora_slot_hdr wire form is untouched.
   Both ends are the same sora build on one machine (the ABI version gate
   rejects mixed builds), so the format needs no R compatibility and no
   byte-order discipline.

   Self-containment is the second dividend: the writer rejects ALTREP
   anywhere in the graph (mori-shared objects are ALTREP views; the
   identifier wire hooks fire only from ALTREP serialization), so a codec
   stream can carry no mori identifier and the sender pins NO keeper —
   the same discipline as the RAWVEC/STR1 immediates.

   Fallback, not failure: anything outside the subset (closures,
   environments, S4, ALTREP, external pointers, promises, bytecode,
   primitives as values, attributed pairlist/language nodes, over-deep
   nesting) declines to write and the caller re-frames through
   R_Serialize, exactly as before.

   Faithfulness: values, types, attributes (order included), and the
   object bit (via the class attribute, as R's own setAttrib maintains
   it) round-trip identically. The gp/LEVELS word is NOT carried — it is
   metadata identical() never compares (string sortedness hints and the
   like), re-derived on demand. Attributes travel as (name, value) pairs
   read out through R_getAttributes and applied with Rf_setAttrib (class
   last, through Rf_classgets) — the post-4.5 C API discipline the
   vendored core already follows. */

#include "sora.h"
#include <string.h>
#include <stdint.h>

/* ANY_ATTRIB() joined the C API in R 4.5.0; equivalent fallback for
   earlier R, where ATTRIB() was still the sanctioned spelling. */
#if R_VERSION < R_Version(4, 5, 0) && !defined(ANY_ATTRIB)
#define ANY_ATTRIB(x) (ATTRIB(x) != R_NilValue)
#endif

/* R_getAttributes() joined the C API in R 4.6.0, returning the
   named-list form; on earlier R the same attributes are read off the
   pairlist directly (WRE "Some backports" discipline — defined
   conditionally). scw_attrs() handles both forms. */
#if R_VERSION < R_Version(4, 6, 0)
#define R_getAttributes(x) ATTRIB(x)
#endif

/* Node types (low nibble of the tag byte). */
enum {
  SC_NIL = 0, SC_SYM, SC_LGL, SC_INT, SC_REAL, SC_CPLX, SC_RAW, SC_STR,
  SC_VEC, SC_EXPR, SC_LIST, SC_LANG, SC_MISSING, SC_UNBOUND
};
/* Tag flags (high nibble). */
#define SC_HASATTR 0x10
#define SC_HASTAG  0x20

/* Recursion bound on both writer and reader: CAR / attribute / element
   nesting only (CDR chains iterate). Past it the writer falls back (a
   cyclic graph walks straight into the bound) and the reader errors (a
   valid stream never approaches it). */
#define SORA_CODEC_MAXDEPTH 500

// Writer ------------------------------------------------------------------------

typedef struct sora_scw_s {
  unsigned char *buf;        /* NULL once overflowed: count-only mode */
  size_t limit;
  size_t total;
  int fail;
} sora_scw;

static void scw_bytes(sora_scw *w, const void *src, size_t n) {
  if (w->buf != NULL) {
    if (w->total + n <= w->limit)
      memcpy(w->buf + w->total, src, n);
    else
      w->buf = NULL;
  }
  w->total += n;
}

static void scw_u8(sora_scw *w, uint32_t v) {
  unsigned char c = (unsigned char) v;
  scw_bytes(w, &c, 1);
}

static void scw_u32(sora_scw *w, uint32_t v) {
  scw_bytes(w, &v, 4);
}

static void scw_u64(sora_scw *w, uint64_t v) {
  scw_bytes(w, &v, 8);
}

/* A CHARSXP: i32 byte length, u32 cetype, bytes. */
static void scw_string(sora_scw *w, SEXP cs) {
  int n = LENGTH(cs);
  scw_u32(w, (uint32_t) n);
  scw_u32(w, (uint32_t) Rf_getCharCE(cs));
  scw_bytes(w, CHAR(cs), (size_t) n);
}

static void scw_node(sora_scw *w, SEXP x, unsigned depth);

/* Attributes as (name, value) pairs, read out through R_getAttributes —
   the named-list form on R >= 4.6, the pairlist before. The object bit is
   not carried: it is the class attribute's shadow (Rf_setAttrib maintains
   it), so an object bit without a class (or a class without the bit) is
   C-crafted and falls back. */
static void scw_attrs(sora_scw *w, SEXP x, unsigned depth) {
  if (Rf_isObject(x) != (Rf_getAttrib(x, R_ClassSymbol) != R_NilValue)) {
    w->fail = 1;
    return;
  }
  SEXP attrs = R_getAttributes(x);
  PROTECT(attrs);
  if (TYPEOF(attrs) == VECSXP) {
    SEXP names = Rf_getAttrib(attrs, R_NamesSymbol);
    R_xlen_t n = XLENGTH(attrs);
    scw_u32(w, (uint32_t) n);
    for (R_xlen_t i = 0; i < n; i++) {
      scw_string(w, STRING_ELT(names, i));
      scw_node(w, VECTOR_ELT(attrs, i), depth + 1);
    }
  } else {
    R_xlen_t n = 0;
    for (SEXP a = attrs; a != R_NilValue; a = CDR(a)) n++;
    scw_u32(w, (uint32_t) n);
    for (SEXP a = attrs; a != R_NilValue; a = CDR(a)) {
      SEXP tag = TAG(a);
      if (tag == R_NilValue) {
        scw_u32(w, 0);                 /* empty attribute name */
        scw_u32(w, (uint32_t) CE_NATIVE);
      } else {
        scw_string(w, PRINTNAME(tag));
      }
      scw_node(w, CAR(a), depth + 1);
    }
  }
  UNPROTECT(1);
}

/* Vector node header: tag byte, element count, then attributes — the
   count first so the reader allocates before attributes apply. */
static void scw_header(sora_scw *w, uint32_t type, SEXP x, unsigned depth) {
  int hasattr = ANY_ATTRIB(x) || Rf_isObject(x);
  scw_u8(w, type | (hasattr ? SC_HASATTR : 0u));
  scw_u64(w, (uint64_t) XLENGTH(x));
  if (hasattr) scw_attrs(w, x, depth);
}

static void scw_node(sora_scw *w, SEXP x, unsigned depth) {
  if (w->fail) return;
  if (depth > SORA_CODEC_MAXDEPTH) { w->fail = 1; return; }
  R_CheckStack();

  if (x == R_NilValue)      { scw_u8(w, SC_NIL); return; }
  if (x == R_MissingArg)    { scw_u8(w, SC_MISSING); return; }
  if (x == R_UnboundValue)  { scw_u8(w, SC_UNBOUND); return; }

  SEXPTYPE ty = TYPEOF(x);
  switch (ty) {
  case SYMSXP:
    scw_u8(w, SC_SYM);
    scw_string(w, PRINTNAME(x));
    return;
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP: {
    if (ALTREP(x) || Rf_isS4(x)) { w->fail = 1; return; }
    int type = ty == LGLSXP ? SC_LGL : ty == INTSXP ? SC_INT :
      ty == REALSXP ? SC_REAL : ty == CPLXSXP ? SC_CPLX : SC_RAW;
    scw_header(w, (uint32_t) type, x, depth);
    if (w->fail) return;
    scw_bytes(w, sora_vec_ptr(x),
              (size_t) XLENGTH(x) * mori_sizeof_elt((int) ty));
    return;
  }
  case STRSXP: {
    if (ALTREP(x) || Rf_isS4(x)) { w->fail = 1; return; }
    scw_header(w, SC_STR, x, depth);
    if (w->fail) return;
    R_xlen_t len = XLENGTH(x);
    for (R_xlen_t i = 0; i < len; i++) {
      SEXP cs = STRING_ELT(x, i);
      if (cs == NA_STRING) {
        scw_u32(w, UINT32_MAX);
      } else {
        scw_string(w, cs);
      }
    }
    return;
  }
  case VECSXP: case EXPRSXP: {
    if (ALTREP(x) || Rf_isS4(x)) { w->fail = 1; return; }
    scw_header(w, ty == VECSXP ? SC_VEC : SC_EXPR, x, depth);
    if (w->fail) return;
    R_xlen_t len = XLENGTH(x);
    for (R_xlen_t i = 0; i < len; i++) {
      scw_node(w, VECTOR_ELT(x, i), depth + 1);
      if (w->fail) return;
    }
    return;
  }
  case LISTSXP: case LANGSXP: {
    if (Rf_isS4(x)) { w->fail = 1; return; }
    /* iterate the CDR chain (R's WriteItem tailcall discipline): each
       node carries its own type nibble, so mixed LISTSXP/LANGSXP chains
       round-trip; recursion stays on CAR / TAG. Attributed pairlist
       nodes fall back — R_getAttributes synthesizes a spurious "names"
       from the tags of the tail on these (attrib.c), so the faithful
       pairlist form is the R_Serialize path's job */
    SEXP s = x;
    for (;;) {
      if (ANY_ATTRIB(s) || Rf_isObject(s)) { w->fail = 1; return; }
      int hastag = TAG(s) != R_NilValue;
      scw_u8(w, (TYPEOF(s) == LISTSXP ? SC_LIST : SC_LANG) |
             (hastag ? SC_HASTAG : 0u));
      if (hastag) scw_node(w, TAG(s), depth + 1);
      scw_node(w, CAR(s), depth + 1);
      if (w->fail) return;
      s = CDR(s);
      if (TYPEOF(s) != LISTSXP && TYPEOF(s) != LANGSXP) {
        scw_node(w, s, depth + 1);   /* the dotted tail, usually NIL */
        return;
      }
    }
  }
  default:
    /* closures, environments, S4, ALTREP, external pointers, promises,
       bytecode, primitives as values, ...: the R_Serialize path */
    w->fail = 1;
    return;
  }
}

/* Bounded single-pass write with count-only flip on overflow (the
   sora_serialize_bounded discipline). Returns the exact total stream size
   (complete in dst iff <= limit), or 0 when x is outside the codec subset
   and must fall back to R_Serialize. A stream is at least 2 bytes, so 0
   is unambiguous. */
size_t sora_codec_write(unsigned char *dst, size_t limit, SEXP x) {
  sora_scw w = { dst, limit, 0, 0 };
  unsigned char magic = SORA_CODEC_MAGIC;
  scw_bytes(&w, &magic, 1);
  scw_node(&w, x, 0);
  return w.fail ? 0 : w.total;
}

// Reader ------------------------------------------------------------------------

typedef struct sora_scr_s {
  const unsigned char *p;
  const unsigned char *end;
} sora_scr;

static void scr_need(sora_scr *r, size_t n) {
  if ((size_t) (r->end - r->p) < n)
    Rf_error("sora: corrupt payload stream");
}

static uint32_t scr_u8(sora_scr *r) {
  scr_need(r, 1);
  return *r->p++;
}

static uint32_t scr_u32(sora_scr *r) {
  scr_need(r, 4);
  uint32_t v;
  memcpy(&v, r->p, 4);
  r->p += 4;
  return v;
}

static uint64_t scr_u64(sora_scr *r) {
  scr_need(r, 8);
  uint64_t v;
  memcpy(&v, r->p, 8);
  r->p += 8;
  return v;
}

static SEXP scr_node(sora_scr *r, unsigned depth);

/* A CHARSXP element: i32 length (UINT32_MAX = NA), u32 cetype, bytes. */
static SEXP scr_string(sora_scr *r) {
  uint32_t n = scr_u32(r);
  if (n == UINT32_MAX) return NA_STRING;
  uint32_t ce = scr_u32(r);
  if (n > (uint32_t) INT32_MAX || ce > CE_BYTES)
    Rf_error("sora: corrupt payload stream");
  scr_need(r, n);
  SEXP cs = Rf_mkCharLenCE((const char *) r->p, (int) n, (cetype_t) ce);
  r->p += n;
  return cs;
}

/* The (name, value) attribute pairs of a HASATTR node, applied with
   Rf_setAttrib — class last through Rf_classgets, so validation sees the
   complete object and the object bit follows the class as R's own
   setAttrib maintains it. */
static void scr_attrs(sora_scr *r, SEXP x, unsigned depth) {
  uint32_t n = scr_u32(r);
  if (n > (uint32_t) (r->end - r->p) / 9)
    Rf_error("sora: corrupt payload stream");
  SEXP classv = R_NilValue;
  for (uint32_t i = 0; i < n; i++) {
    SEXP nm = PROTECT(scr_string(r));
    if (nm == NA_STRING) {
      UNPROTECT(1);
      Rf_error("sora: corrupt payload stream");
    }
    SEXP val = PROTECT(scr_node(r, depth + 1));
    SEXP sym = Rf_installTrChar(nm);
    if (sym == R_ClassSymbol) {
      classv = val;
    } else {
      Rf_setAttrib(x, sym, val);
    }
    UNPROTECT(2);
  }
  if (classv != R_NilValue) Rf_classgets(x, classv);
}

static SEXP scr_body(sora_scr *r, uint32_t tag, unsigned depth) {
  if (depth > SORA_CODEC_MAXDEPTH)
    Rf_error("sora: corrupt payload stream");
  R_CheckStack();
  uint32_t ty = tag & 0x0f;
  /* flags the writer never produces on these types are corruption */
  if ((tag & SC_HASTAG) && ty != SC_LIST && ty != SC_LANG)
    Rf_error("sora: corrupt payload stream");
  if ((tag & SC_HASATTR) &&
      (ty == SC_NIL || ty == SC_MISSING || ty == SC_UNBOUND ||
       ty == SC_SYM || ty == SC_LIST || ty == SC_LANG))
    Rf_error("sora: corrupt payload stream");
  switch (ty) {
  case SC_NIL:     return R_NilValue;
  case SC_MISSING: return R_MissingArg;
  case SC_UNBOUND: return R_UnboundValue;
  case SC_SYM: {
    SEXP pn = PROTECT(scr_string(r));
    SEXP s = Rf_installTrChar(pn);
    UNPROTECT(1);
    return s;
  }
  case SC_LGL: case SC_INT: case SC_REAL: case SC_CPLX: case SC_RAW: {
    SEXPTYPE rt = ty == SC_LGL ? LGLSXP : ty == SC_INT ? INTSXP :
      ty == SC_REAL ? REALSXP : ty == SC_CPLX ? CPLXSXP : RAWSXP;
    uint64_t len = scr_u64(r);
    size_t elt = mori_sizeof_elt((int) rt);
    /* len is stream-bounded: each element costs elt bytes still ahead */
    if (elt == 0 || len > (uint64_t) (r->end - r->p) / elt)
      Rf_error("sora: corrupt payload stream");
    SEXP s = PROTECT(Rf_allocVector(rt, (R_xlen_t) len));
    if (tag & SC_HASATTR) scr_attrs(r, s, depth);
    scr_need(r, (size_t) len * elt);
    memcpy(sora_vec_ptr(s), r->p, (size_t) len * elt);
    r->p += (size_t) len * elt;
    UNPROTECT(1);
    return s;
  }
  case SC_STR: {
    uint64_t len = scr_u64(r);
    if (len > (uint64_t) (r->end - r->p) / 4)
      Rf_error("sora: corrupt payload stream");
    SEXP s = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t) len));
    if (tag & SC_HASATTR) scr_attrs(r, s, depth);
    for (R_xlen_t i = 0; i < (R_xlen_t) len; i++)
      SET_STRING_ELT(s, i, scr_string(r));
    UNPROTECT(1);
    return s;
  }
  case SC_VEC: case SC_EXPR: {
    SEXPTYPE rt = ty == SC_VEC ? VECSXP : EXPRSXP;
    uint64_t len = scr_u64(r);
    if (len > (uint64_t) (r->end - r->p))
      Rf_error("sora: corrupt payload stream");
    SEXP s = PROTECT(Rf_allocVector(rt, (R_xlen_t) len));
    if (tag & SC_HASATTR) scr_attrs(r, s, depth);
    for (R_xlen_t i = 0; i < (R_xlen_t) len; i++)
      SET_VECTOR_ELT(s, i, scr_node(r, depth + 1));
    UNPROTECT(1);
    return s;
  }
  case SC_LIST: case SC_LANG: {
    /* R's ReadItem_Iterative discipline: one PROTECT on the head, each
       fresh node linked into the list before any allocating read */
    SEXP first = R_NilValue, last = R_NilValue;
    int nprot = 0;
    for (;;) {
      SEXP node = ty == SC_LIST ? Rf_cons(R_NilValue, R_NilValue)
                                : Rf_lcons(R_NilValue, R_NilValue);
      if (last == R_NilValue) {
        PROTECT(node);
        nprot++;
        first = node;
      } else {
        SETCDR(last, node);
      }
      last = node;
      if (tag & SC_HASTAG) SET_TAG(node, scr_node(r, depth + 1));
      SETCAR(node, scr_node(r, depth + 1));
      tag = scr_u8(r);
      uint32_t ty = tag & 0x0f;
      if (ty != SC_LIST && ty != SC_LANG) {
        SETCDR(last, scr_body(r, tag, depth + 1));
        break;
      }
    }
    UNPROTECT(nprot);
    return first;
  }
  }
  Rf_error("sora: corrupt payload stream");
}

static SEXP scr_node(sora_scr *r, unsigned depth) {
  return scr_body(r, scr_u8(r), depth);
}

/* Read a codec stream of len bytes at buf (magic byte included). Raises
   on any malformation; the transport never produces one, the checks are
   the corrupt-payload discipline of the other tiers. */
SEXP sora_codec_read(const unsigned char *buf, size_t len) {
  sora_scr r = { buf, buf + len };
  if (len == 0 || scr_u8(&r) != SORA_CODEC_MAGIC)
    Rf_error("sora: corrupt payload stream");
  SEXP out = scr_node(&r, 0);
  if (r.p != r.end)
    Rf_error("sora: corrupt payload stream");
  return out;
}

/* Task-frame read: a pool task payload is list(expr, args) — on this tier
   a SC_VEC of length 2. Reading the frame directly skips materializing
   the outer list, and an empty args reads as the shared empty vector (the
   worker binds from it read-only), so a constant task allocates nothing
   here at all. On success *expr and *args are left PROTECTed and
   *nprotect is 2. A non-task-frame is a 0 return, never an error: the
   caller falls back to the generic read, whose shape validation stays the
   single authority. A truncated or trailing stream raises, exactly as the
   generic read raises. */
int sora_codec_read_task(const unsigned char *buf, size_t len, SEXP *expr,
                         SEXP *args, int *nprotect) {
  if (len < 2 || buf[0] != SORA_CODEC_MAGIC) return 0;
  sora_scr r = { buf + 1, buf + len };
  uint32_t tag = scr_u8(&r);
  if ((tag & 0x0f) != SC_VEC || (tag & (SC_HASATTR | SC_HASTAG))) return 0;
  if (scr_u64(&r) != 2) return 0;
  *expr = PROTECT(scr_node(&r, 1));
  const unsigned char *save = r.p;
  uint32_t atag = scr_u8(&r);
  if ((atag & 0x0f) == SC_VEC && !(atag & (SC_HASATTR | SC_HASTAG)) &&
      scr_u64(&r) == 0) {
    *args = sora_empty_args();
  } else {
    r.p = save;
    *args = scr_node(&r, 1);
  }
  PROTECT(*args);
  if (r.p != r.end) {
    UNPROTECT(2);
    return 0;
  }
  *nprotect = 2;
  return 1;
}

// .Call test surface -------------------------------------------------------------

/* Codec-write object into a fresh raw vector (NULL when outside the
   subset); sora_codec_read_call reads one back. */
SEXP sora_codec_write_call(SEXP object) {
  size_t n = sora_codec_write(NULL, 0, object);
  if (n == 0) return R_NilValue;
  SEXP bytes = PROTECT(Rf_allocVector(RAWSXP, (R_xlen_t) n));
  size_t wrote = sora_codec_write(RAW(bytes), (size_t) n, object);
  if (wrote != n) {
    UNPROTECT(1);
    Rf_error("sora: codec write mismatch");
  }
  UNPROTECT(1);
  return bytes;
}

SEXP sora_codec_read_call(SEXP bytes) {
  if (TYPEOF(bytes) != RAWSXP) Rf_error("sora: expected a raw vector");
  return sora_codec_read(RAW(bytes), (size_t) XLENGTH(bytes));
}
