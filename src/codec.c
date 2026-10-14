/* The mizu compact codec: a purpose-built binary framing for the payload
   subset that dominates the hot paths — NULL, symbols, atomic vectors
   (attributes included), strings, list/vector trees, calls, closures,
   S4 objects, and primitives as values — written and read straight off
   the slot bytes with no R serializer involvement.

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

   Streams are self-describing: the first byte is MIZU_CODEC_MAGIC where
   an R binary stream carries 'B', so the read paths dispatch on the
   payload's first byte and the mizu_slot_hdr wire form is untouched.
   Both ends are the same mizu build on one machine (the ABI version gate
   rejects mixed builds), so the format needs no R compatibility and no
   byte-order discipline.

   Self-containment is the second dividend: the writer rejects ALTREP
   anywhere in the graph (shared objects are ALTREP views; the
   identifier wire hooks fire only from ALTREP serialization), so a codec
   stream can carry no view identifier and the sender pins NO keeper —
   the same discipline as the RAWVEC/STR1 immediates.

   Closures cross with the environment by reference — global / base /
   empty by kind byte, a package namespace by name (base's ..getNamespace
   on read — R_Unserialize's own discipline, so a namespace that will not
   load substitutes .GlobalEnv, it does not fail) — since formals and
   bodies are
   the pairlist / call trees already in the subset. A keep.source parse
   hangs srcref attributes on the closure and its body language nodes;
   the srcfile they name cannot cross processes, so the write crosses a
   stripped copy (the map staging discipline, identical()'s
   ignore.srcref). A closure over a local environment (the
   function-factory case), a bytecode body, or a non-srcref attribute on
   a language node decline like any other non-subset node.

   S4 objects cross as their data part plus the S4 object bit: a flag on
   the vector and closure node tags, a dedicated node for the data-less
   S4SXP form. Slots are attributes and ride the (name, value) pairs;
   the bit is applied with Rf_asS4 only after the attributes land, so a
   read never consults a class definition — R_Unserialize's discipline.
   An S4 object over an ALTREP data part, or with an out-of-subset slot,
   declines like any other non-subset node.

   Primitives as values cross by name — the symbol framing with a
   resolve flag, resolved against the base environment on read, the
   interned-singleton discipline of R's own serializer. The name has no
   public accessor from the object, so the writer reverse-looks it up
   against base; anything unlisted declines like any other non-subset
   node.

   Fallback, not failure: anything outside the subset (environments,
   ALTREP, external pointers, promises, bytecode, language nodes
   carrying non-srcref attributes, over-deep nesting)
   declines to write and the caller re-frames through R_Serialize,
   exactly as before.

   The one addition to the subset above, riding the mizu_frame_tier entry
   rather than the plain pass: an ArrowTabular nested in the payload
   crosses as a frame leaf — the SC_FRAME tag (0x28 = SC_HASTAG | SC_VEC,
   a byte today's plain writer can never emit, since SC_HASTAG is emitted
   only on SC_LIST/SC_LANG chain nodes, and one a stale reader rejects
   loudly through its flag validation), a u8 name length, and the staged
   frame region's name. The leaf is sized at the 'I' writer's
   MIZU_IX_ZC_RESERVE bound in the plan walks, the actual write never
   exceeding it. A stream carrying the leaf is not self-contained: its
   header stamps aux = 0 in place of the keeperless claim, the frame
   checkout's retain riding the handle's retain table like a SHM_VEC
   stage's. At most one leaf per payload (the core's single-checkout
   rule); every other table converts to its value and rides the ordinary
   nodes. The resolve is mizu_view_resolve_id — the one path behind the
   serialize consumer and the 'I' ref leaf — so the reader needs no new
   view export and every consumer of a codec stream (the pool collect,
   the channel recv, the worker task decode) covers the leaf through
   scr_body.

   Faithfulness: values, types, attributes (order included), and the
   object bit (via the class attribute, as R's own setAttrib maintains
   it) round-trip identically. The gp/LEVELS word is NOT carried — it is
   metadata identical() never compares (string sortedness hints and the
   like), re-derived on demand. Source references are the one attribute
   exception: dropped with the strip below (closures and language trees
   alike), never carried. Attributes travel as (name, value) pairs
   read out through R_getAttributes and applied with Rf_setAttrib (class
   last, through Rf_classgets) — the post-4.5 C API discipline the
   vendored core already follows. */

#include "mizu.h"
#include "arrow.h"
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

/* R_ClosureFormals() and siblings plus R_mkClosure() joined the C API in
   R 4.5.0 (the closure slot setters left the public headers then); the
   slot macros for earlier R, and the constructor from map.c's backport. */
#if R_VERSION < R_Version(4, 5, 0)
#define R_ClosureFormals(x) FORMALS(x)
#define R_ClosureBody(x)    BODY(x)
#define R_ClosureEnv(x)     CLOENV(x)
SEXP R_mkClosure(SEXP formals, SEXP body, SEXP env);
#endif

/* Node types (low nibble of the tag byte). */
enum {
  SC_NIL = 0, SC_SYM, SC_LGL, SC_INT, SC_REAL, SC_CPLX, SC_RAW, SC_STR,
  SC_VEC, SC_EXPR, SC_LIST, SC_LANG, SC_MISSING, SC_UNBOUND, SC_CLOS,
  SC_S4
};
/* Tag flags (high nibble). */
#define SC_HASATTR 0x10
#define SC_HASTAG  0x20
#define SC_HASS4   0x40
#define SC_PRIM    0x80
/* The frame leaf (mizu_frame_tier): SC_HASTAG | SC_VEC, a byte the plain
   writer never produces (SC_HASTAG is emitted only on SC_LIST/SC_LANG
   chain nodes) and a stale reader loud-rejects through its flag
   validation. The reader dispatches on one full-byte compare ahead of the
   nibble mask. */
#define SC_FRAME (SC_HASTAG | SC_VEC)
/* Closure environment reference kinds (the byte after an SC_CLOS tag):
   global / base / empty by kind, a package namespace by name — the
   by-reference discipline of R's own serializer, the only environments
   a closure faithfully crosses with. */
enum { SC_ENV_GLOBAL = 0, SC_ENV_BASE, SC_ENV_EMPTY, SC_ENV_NS };

/* Recursion bound on both writer and reader: CAR / attribute / element
   nesting only (CDR chains iterate). Past it the writer falls back (a
   cyclic graph walks straight into the bound) and the reader errors (a
   valid stream never approaches it). */
#define MIZU_CODEC_MAXDEPTH 500

// Writer ------------------------------------------------------------------------

/* The frame tier's plan, threaded through the walk (mizu_frame_tier;
   NULL on the plain walk — zero per-node cost on every existing caller).
   Record mode detects the ArrowTabular nodes and counts each occurrence
   at the leaf reserve; write mode identity-matches against the recorded
   candidates — the frame node emits the leaf, every other walks its
   anchored by-value form. cand borrows into the staged tree; anchor is
   the one PROTECTed VECSXP carrying the per-candidate values (R_NilValue
   for the frame) and, in its last slot, the frame's open handle extptr.
   Duplicate occurrences of one table dedupe to one candidate (one pull —
   a RecordBatchReader is consumed by pulling) with occ counting them, so
   the sizing arithmetic reconciles occurrence-for-occurrence. */
#define MIZU_FRAME_MAX_CAND 16            /* the fold's candidate cap */
typedef struct {
  int mode;                          /* 0 record (count-only), 1 write */
  SEXP anchor;
  SEXP cand[MIZU_FRAME_MAX_CAND];
  int occ[MIZU_FRAME_MAX_CAND];
  int ncand, frame;                  /* frame = -1: by-value */
  unsigned char name[MIZU_NAME_MAX]; /* the staged region (write mode) */
  uint32_t name_len;
} mizu_frameplan;

typedef struct mizu_scw_s {
  unsigned char *buf;        /* NULL once overflowed: count-only mode */
  size_t limit;
  size_t total;
  int fail;
  mizu_frameplan *fp;        /* NULL = the plain walk */
} mizu_scw;

static void scw_bytes(mizu_scw *w, const void *src, size_t n) {
  if (w->buf != NULL) {
    if (w->total + n <= w->limit)
      memcpy(w->buf + w->total, src, n);
    else
      w->buf = NULL;
  }
  w->total += n;
}

static void scw_u8(mizu_scw *w, uint32_t v) {
  unsigned char c = (unsigned char) v;
  scw_bytes(w, &c, 1);
}

static void scw_u32(mizu_scw *w, uint32_t v) {
  scw_bytes(w, &v, 4);
}

static void scw_u64(mizu_scw *w, uint64_t v) {
  scw_bytes(w, &v, 8);
}

/* A CHARSXP: i32 byte length, u32 cetype, bytes. */
static void scw_string(mizu_scw *w, SEXP cs) {
  int n = LENGTH(cs);
  scw_u32(w, (uint32_t) n);
  scw_u32(w, (uint32_t) Rf_getCharCE(cs));
  scw_bytes(w, CHAR(cs), (size_t) n);
}

static void scw_node(mizu_scw *w, SEXP x, unsigned depth);

/* A 1:n verdict for an integer vector: R's compact sequence carries the
   answer in its info (REALSXP c(length, first, incr) — three reads);
   anything else is one chunked scan. The interop writer's
   ix_int_is_seq1n discipline, minus its warm buffer (a row.names attr
   pays this at most once per payload). */
static int scw_int_is_seq1n(SEXP rn, R_xlen_t n) {
  if (ALTREP(rn)) {
    SEXP info = R_altrep_data1(rn);
    if (TYPEOF(info) == REALSXP && !ALTREP(info) && XLENGTH(info) == 3) {
      const double *d = REAL(info);
      if (d[0] == (double) n && d[1] == 1 && d[2] == 1) return 1;
    }
  }
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

/* The row.names attribute: on R >= 4.6 R_getAttributes presents the
   compact form expanded — an ALTREP 1:n the plain node write declines
   (every ordinary data.frame took the R_Serialize fallback for it).
   An ALTREP INTSXP equal to 1:n writes the two-element compact form
   c(NA, -n) instead, which the reader's Rf_setAttrib restores; any other
   value keeps the plain write. */
static int scw_rownames_compact(mizu_scw *w, SEXP value) {
  if (TYPEOF(value) != INTSXP) return 0;
  const R_xlen_t n = XLENGTH(value);
  if (n <= 0 || n > INT32_MAX || !scw_int_is_seq1n(value, n)) return 0;
  int32_t v[2];
  v[0] = NA_INTEGER;
  v[1] = (int32_t) -n;
  scw_u8(w, SC_INT);
  scw_u64(w, 2);
  scw_bytes(w, v, sizeof v);
  return 1;
}

/* Attributes as (name, value) pairs, read out through R_getAttributes —
   the named-list form on R >= 4.6, the pairlist before. The object bit is
   not carried: it is the class attribute's shadow (Rf_setAttrib maintains
   it), so an object bit without a class (or a class without the bit) is
   C-crafted and falls back. */
static void scw_attrs(mizu_scw *w, SEXP x, unsigned depth) {
  if (Rf_isObject(x) != (Rf_getAttrib(x, R_ClassSymbol) != R_NilValue)) {
    w->fail = 1;
    return;
  }
  SEXP attrs = R_getAttributes(x);
  PROTECT(attrs);
  if (TYPEOF(attrs) == VECSXP) {
    SEXP names = PROTECT(Rf_getAttrib(attrs, R_NamesSymbol));
    R_xlen_t n = XLENGTH(attrs);
    scw_u32(w, (uint32_t) n);
    for (R_xlen_t i = 0; i < n; i++) {
      SEXP name = STRING_ELT(names, i);
      SEXP value = VECTOR_ELT(attrs, i);
      scw_string(w, name);
      if (name == PRINTNAME(R_RowNamesSymbol) && ALTREP(value) &&
          scw_rownames_compact(w, value))
        continue;
      scw_node(w, value, depth + 1);
    }
    UNPROTECT(1);                /* names */
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
      if (tag == R_RowNamesSymbol && ALTREP(CAR(a)) &&
          scw_rownames_compact(w, CAR(a)))
        continue;
      scw_node(w, CAR(a), depth + 1);
    }
  }
  UNPROTECT(1);
}

/* Vector node header: tag byte, element count, then attributes — the
   count first so the reader allocates before attributes apply. */
static void scw_header(mizu_scw *w, uint32_t type, SEXP x, unsigned depth) {
  int hasattr = ANY_ATTRIB(x) || Rf_isObject(x);
  scw_u8(w, type | (hasattr ? SC_HASATTR : 0u) |
            (Rf_isS4(x) ? SC_HASS4 : 0u));
  scw_u64(w, (uint64_t) XLENGTH(x));
  if (hasattr) scw_attrs(w, x, depth);
}

/* A closure node: tag byte, environment reference, formals, body, then
   attributes — last, so the reader constructs with R_mkClosure before
   they apply (the closure slots have no public setters). Formals are a
   tagged pairlist and the body a call tree, both already in the subset;
   the environment is the one piece that cannot cross by value, so it
   crosses by reference: global / base / empty by kind byte, a namespace
   by name. A local environment declines here; a bytecode body or an
   attributed language node declines in the recursive writes. */
static void scw_closure(mizu_scw *w, SEXP x, unsigned depth) {
  SEXP env = R_ClosureEnv(x);
  uint32_t envkind;
  SEXP spec = R_NilValue;
  if (env == R_GlobalEnv) {
    envkind = SC_ENV_GLOBAL;
  } else if (env == R_BaseEnv) {
    envkind = SC_ENV_BASE;
  } else if (env == R_EmptyEnv) {
    envkind = SC_ENV_EMPTY;
  } else if (R_IsNamespaceEnv(env)) {
    envkind = SC_ENV_NS;
    spec = R_NamespaceEnvSpec(env);
  } else {
    w->fail = 1;
    return;
  }
  int hasattr = ANY_ATTRIB(x) || Rf_isObject(x);
  scw_u8(w, SC_CLOS | (hasattr ? SC_HASATTR : 0u) |
            (Rf_isS4(x) ? SC_HASS4 : 0u));
  scw_u8(w, envkind);
  if (envkind == SC_ENV_NS) scw_string(w, STRING_ELT(spec, 0));
  scw_node(w, R_ClosureFormals(x), depth + 1);
  if (w->fail) return;
  scw_node(w, R_ClosureBody(x), depth + 1);
  if (w->fail) return;
  if (hasattr) scw_attrs(w, x, depth);
}

static void scw_node(mizu_scw *w, SEXP x, unsigned depth) {
  if (w->fail) return;
  if (depth > MIZU_CODEC_MAXDEPTH) { w->fail = 1; return; }
  R_CheckStack();

  if (x == R_NilValue)      { scw_u8(w, SC_NIL); return; }
  if (x == R_MissingArg)    { scw_u8(w, SC_MISSING); return; }
  if (x == R_UnboundValue)  { scw_u8(w, SC_UNBOUND); return; }

  SEXPTYPE ty = TYPEOF(x);
  if (w->fp != NULL && (ty == ENVSXP || ty == EXTPTRSXP)) {
    /* the frame tier's gate, ahead of the switch and off the vector hot
       path: record mode appends a detected ArrowTabular (deduped — one
       pull per distinct table) and counts the reserve; write mode emits
       the frame leaf or walks the anchored value. An unmatched table
       cannot occur (the record walk found them all) and declines below
       as today. */
    mizu_frameplan *fp = w->fp;
    if (fp->mode == 0) {
      if (mizu_arrow_check(x)) {
        for (int i = 0; i < fp->ncand; i++)
          if (fp->cand[i] == x) {
            fp->occ[i]++;
            w->total += MIZU_IX_ZC_RESERVE;
            return;
          }
        if (fp->ncand == MIZU_FRAME_MAX_CAND) { w->fail = 1; return; }
        fp->cand[fp->ncand] = x;
        fp->occ[fp->ncand] = 1;
        fp->ncand++;
        w->total += MIZU_IX_ZC_RESERVE;
        return;
      }
    } else {
      for (int i = 0; i < fp->ncand; i++)
        if (fp->cand[i] == x) {
          if (i == fp->frame) {
            scw_u8(w, SC_FRAME);
            scw_u8(w, fp->name_len);
            scw_bytes(w, fp->name, fp->name_len);
          } else {
            scw_node(w, VECTOR_ELT(fp->anchor, i), depth);
          }
          return;
        }
    }
  }
  switch (ty) {
  case SYMSXP:
    scw_u8(w, SC_SYM);
    scw_string(w, PRINTNAME(x));
    return;
  case LGLSXP: case INTSXP: case REALSXP: case CPLXSXP: case RAWSXP: {
    int type = ty == LGLSXP ? SC_LGL : ty == INTSXP ? SC_INT :
      ty == REALSXP ? SC_REAL : ty == CPLXSXP ? SC_CPLX : SC_RAW;
    if (ALTREP(x)) {
      /* ALTREP rejects — except inside the frame tier's converted values
         (write mode): the record walk proved the original tree
         ALTREP-free, so an ALTREP node here belongs to a converted value
         (a list column's lazy elements — the bail materializes the
         top level) and emits its materialized form through the region
         accessors — never expanded on the sender, the 'I' writer's
         discipline */
      if (w->fp == NULL || w->fp->mode != 1) { w->fail = 1; return; }
      scw_header(w, (uint32_t) type, x, depth);
      if (w->fail) return;
      const R_xlen_t len = XLENGTH(x);
      const size_t elt = mizu_view_sizeof_elt((int) ty);
      unsigned char buf[8192];
      for (R_xlen_t i = 0; i < len;) {
        R_xlen_t m = (R_xlen_t) (sizeof buf / elt);
        if (m > len - i) m = len - i;
        switch (ty) {
        case LGLSXP: LOGICAL_GET_REGION(x, i, m, (int *) buf); break;
        case INTSXP: INTEGER_GET_REGION(x, i, m, (int *) buf); break;
        case REALSXP: REAL_GET_REGION(x, i, m, (double *) buf); break;
        case CPLXSXP: COMPLEX_GET_REGION(x, i, m, (Rcomplex *) buf); break;
        default:       RAW_GET_REGION(x, i, m, (Rbyte *) buf); break;
        }
        scw_bytes(w, buf, (size_t) m * elt);
        i += m;
      }
      return;
    }
    scw_header(w, (uint32_t) type, x, depth);
    if (w->fail) return;
    scw_bytes(w, mizu_vec_ptr(x),
              (size_t) XLENGTH(x) * mizu_view_sizeof_elt((int) ty));
    return;
  }
  case STRSXP: {
    /* ALTREP strings reject — except inside the frame tier's converted
       values (write mode), where STRING_ELT's lazy materialize serves */
    if (ALTREP(x) && (w->fp == NULL || w->fp->mode != 1)) {
      w->fail = 1;
      return;
    }
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
    if (ALTREP(x)) { w->fail = 1; return; }
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
    /* A keep.source parse hangs srcref attributes on the chain heads of
       a task expression (`{` blocks and the like); the srcfile they name
       cannot cross processes, so the write crosses a stripped copy — the
       closure discipline above. The probe is the head node: the parser
       sets its srcref whenever it sets the subtree's, and nested blocks
       re-enter this case on CAR recursion. Other attributes still
       decline below. */
    if (ANY_ATTRIB(x) && Rf_getAttrib(x, mizu_srcref_sym) != R_NilValue) {
      SEXP stripped = PROTECT(mizu_strip_lang(x));
      scw_node(w, stripped, depth);
      UNPROTECT(1);
      return;
    }
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
  case CLOSXP: {
    /* The pool task / map descriptor payload carries a function more
       often than not. A keep.source parse hangs srcref attributes on
       the closure and its body language nodes; the srcfile they name
       cannot cross processes, so the write crosses a stripped copy (one
       deep duplicate of formals and body). The closure-level attribute
       is the probe: the parser sets it whenever it sets the body's. A
       hand-built srcref-bearing body without it still declines. */
    if (Rf_getAttrib(x, mizu_srcref_sym) != R_NilValue) {
      SEXP stripped = PROTECT(mizu_strip_srcref(x));
      scw_closure(w, stripped, depth);
      UNPROTECT(1);
      return;
    }
    scw_closure(w, x, depth);
    return;
  }
  case BUILTINSXP: case SPECIALSXP: {
    /* A primitive crosses as its name: the symbol framing with the
       resolve flag. The name has no public accessor from the object
       (PRIMNAME stays internal), so it is reverse-looked up against
       base — every reachable primitive is a base binding, and the scan
       runs only when a primitive actually crosses. Anything unlisted
       declines. Primitives can carry no attributes, so the framing is
       complete. */
    SEXP names = PROTECT(R_lsInternal3(R_BaseEnv, TRUE, FALSE));
    w->fail = 1;
    for (R_xlen_t i = 0; i < XLENGTH(names); i++) {
      SEXP cs = STRING_ELT(names, i);
      if (Rf_eval(Rf_installTrChar(cs), R_BaseEnv) == x) {
        scw_u8(w, SC_SYM | SC_PRIM);
        scw_string(w, cs);
        w->fail = 0;
        break;
      }
    }
    UNPROTECT(1);
    return;
  }
  case S4SXP: {
    /* the data-less S4 object: attributes are the whole payload */
    int hasattr = ANY_ATTRIB(x) || Rf_isObject(x);
    scw_u8(w, SC_S4 | (hasattr ? SC_HASATTR : 0u));
    if (hasattr) scw_attrs(w, x, depth);
    return;
  }
  default:
    /* environments, ALTREP, external pointers, promises, bytecode,
       ...: the R_Serialize path */
    w->fail = 1;
    return;
  }
}

/* Bounded single-pass write with count-only flip on overflow (the
   mizu_serialize_bounded discipline). Returns the exact total stream size
   (complete in dst iff <= limit), or 0 when x is outside the codec subset
   and must fall back to R_Serialize. A stream is at least 2 bytes, so 0
   is unambiguous. */
size_t mizu_codec_write(unsigned char *dst, size_t limit, SEXP x) {
  mizu_scw w = { dst, limit, 0, 0, NULL };
  unsigned char magic = MIZU_CODEC_MAGIC;
  scw_bytes(&w, &magic, 1);
  scw_node(&w, x, 0);
  return w.fail ? 0 : w.total;
}

// The frame tier ----------------------------------------------------------------

/* The plan-threaded walk: mizu_codec_write with the frame gate live.
   Record mode is always count-only; write mode emits. */
static size_t frame_walk(unsigned char *dst, size_t limit, SEXP x,
                         mizu_frameplan *plan) {
  mizu_scw w = { dst, limit, 0, 0, plan };
  unsigned char magic = MIZU_CODEC_MAGIC;
  scw_bytes(&w, &magic, 1);
  scw_node(&w, x, 0);
  return w.fail ? 0 : w.total;
}

/* The frame-aware codec tier, owning the codec-tier decision for its call
   sites once the plain pass declined (the hot path never pays for it):
   an ArrowTabular nested in the payload stages as one MIZL frame region
   and crosses in the stream as a frame leaf. Proof before any R call,
   plan before the write (the fold's plan.cand discipline): the record
   walk proves the whole tree in-subset ahead of any stream pull —
   arrow_batches consumes a RecordBatchReader, and the serialize fallback
   must not ship a drained one. First-past-the-gate selection: the first
   table whose frame span passes the gate is the frame (its by-value size
   would cost a conversion to learn, and a past-gate table's stream
   exceeds the budget on its own in all but a string-heavy corner — the
   fold's remainder-fit rule and the gate coincide); every other table
   converts. Stage FIRST, ahead of any stream byte — a decline converts
   the frame too, never a partial stream, never an orphaned checkout (the
   core rolls back an uncommitted checkout at the next verb). */
int mizu_frame_tier(mizu_slot_hdr *hdr, unsigned char *payload,
                    uint32_t inline_max, SEXP x, mizu_handle *h) {
  mizu_frameplan plan;
  plan.mode = 0;
  plan.anchor = R_NilValue;
  plan.ncand = 0;
  plan.frame = -1;
  plan.name_len = 0;
  /* 1. the record walk: count-only, every table counted as the reserve.
     A decline is the plain pass's non-table node (or past the candidate
     cap): the serialize fallback runs with nothing consumed. */
  size_t total = frame_walk(NULL, 0, x, &plan);
  if (total == 0 || plan.ncand == 0) return 0;

  /* 2. pull / open / convert in pre-order between the walks; every fresh
     SEXP anchors in the one PROTECTed vector (the frame's handle extptr
     in the last slot, its prot the triple it reads). */
  plan.anchor = PROTECT(Rf_allocVector(VECSXP, plan.ncand + 1));
  const size_t gate =
    inline_max > MIZU_ZC_FLOOR ? (size_t) inline_max : MIZU_ZC_FLOOR;
  for (int i = 0; i < plan.ncand; i++) {
    SEXP triple = PROTECT(mizu_arrow_pull(plan.cand[i]));
    if (VECTOR_ELT(triple, 1) == R_NilValue) {
      /* a non-struct stream converts on the unconsumed stream */
      SET_VECTOR_ELT(plan.anchor, i, mizu_arrow_value(triple));
      UNPROTECT(1);
      continue;
    }
    if (plan.frame < 0) {
      size_t fsize;
      SEXP xp = PROTECT(mizu_arrow_open_r(triple, MIZU_R_CAPS, &fsize));
      if (fsize > gate) {
        plan.frame = i;
        SET_VECTOR_ELT(plan.anchor, plan.ncand, xp);
        UNPROTECT(2);    /* the extptr anchors the triple through its prot */
        continue;
      }
      mizu_arrow_close_held(xp);
      UNPROTECT(1);      /* xp — the finalizer frees the struct */
    }
    SET_VECTOR_ELT(plan.anchor, i, mizu_arrow_value(triple));
    UNPROTECT(1);
  }

  /* 3. size each converted value individually (the encoding is
     context-free — no ref table — so one count-only write per value
     replaces a second tree walk), the slot's reserve coming out of the
     record walk's total occurrence-for-occurrence. The write-mode walk
     carries any lazy ALTREP left inside the converted values (the bail
     materializes the top level; a list column's elements can still
     arrive lazy). */
  plan.mode = 1;
  for (int i = 0; i < plan.ncand; i++) {
    if (i == plan.frame) continue;
    size_t vn = frame_walk(NULL, 0, VECTOR_ELT(plan.anchor, i), &plan);
    if (vn == 0) {
      /* cannot occur for a nanoarrow-converted value — the caller's
         fallback ships the original tree */
      UNPROTECT(1);
      return 0;
    }
    total -= (size_t) plan.occ[i] * MIZU_IX_ZC_RESERVE;
    total += (size_t) plan.occ[i] * (vn - 1);
  }

  /* 4. the frame branch: the stream carrying the leaf must fit INLINE —
     a SHM_RAW spill would need a second checkout */
  if (plan.frame >= 0 && total <= (size_t) inline_max) {
    mizu_arrow_warn warn = { 0, 0, 0 };
    SEXP xp = VECTOR_ELT(plan.anchor, plan.ncand);
    if (mizu_arrow_stage_held(xp, h, plan.name, &plan.name_len,
                              &warn) == 0) {
      total -= (size_t) plan.occ[plan.frame] * MIZU_IX_ZC_RESERVE;
      total += (size_t) plan.occ[plan.frame] * (2 + plan.name_len);
      size_t n = frame_walk(payload, inline_max, x, &plan);
      if (n != total) Rf_error("mizu: codec write mismatch");
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = 0;      /* the checkout retains — no keeperless claim */
      UNPROTECT(1);
      /* the conversion counts raise after the write half completed */
      mizu_arrow_warn_raise(&warn);
      return 1;
    }
    /* a stage decline (a churn race) converts below, like the skip */
  }
  if (plan.frame >= 0) {
    /* the frame is unstaged — the remainder over budget above, or a stage
       decline: the held batches convert and the whole payload crosses by
       value — never a partial stream, never an orphaned checkout (the
       core rolls back an uncommitted checkout at the next verb) */
    SEXP xp = VECTOR_ELT(plan.anchor, plan.ncand);
    SEXP triple = R_ExternalPtrProtected(xp);
    SET_VECTOR_ELT(plan.anchor, plan.frame, mizu_arrow_value(triple));
    size_t vn = frame_walk(NULL, 0, VECTOR_ELT(plan.anchor, plan.frame),
                           &plan);
    if (vn == 0) {
      UNPROTECT(1);
      return 0;
    }
    total -= (size_t) plan.occ[plan.frame] * MIZU_IX_ZC_RESERVE;
    total += (size_t) plan.occ[plan.frame] * (vn - 1);
    plan.frame = -1;
  }

  /* 5. by value: inline when the remainder fits, else a SHM_RAW spill
     (the channel's arena is skipped here — a performance tier, not a
     correctness one). The write walk is deterministic — a byte-count
     mismatch is the other spills' raise */
  if (total <= (size_t) inline_max) {
    size_t n = frame_walk(payload, inline_max, x, &plan);
    if (n != total) Rf_error("mizu: codec write mismatch");
    hdr->kind = MIZU_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = MIZU_AUX_F_KEEPERLESS;
    UNPROTECT(1);
    return 1;
  }
  mizu_stage_reap(h);
  mizu_shm *shm = mizu_spill_get_raise(h, total);
  size_t n = frame_walk((unsigned char *) shm->addr, shm->size, x, &plan);
  if (n != total) Rf_error("mizu: codec write mismatch");
  hdr->kind = MIZU_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) total;
  memcpy(payload, shm->name, shm->name_len);
  mizu_stage_retain(h, shm);
  UNPROTECT(1);
  return 1;
}

// Reader ------------------------------------------------------------------------

typedef struct mizu_scr_s {
  const unsigned char *p;
  const unsigned char *end;
} mizu_scr;

static void scr_need(mizu_scr *r, size_t n) {
  if ((size_t) (r->end - r->p) < n)
    Rf_error("mizu: corrupt payload stream");
}

static uint32_t scr_u8(mizu_scr *r) {
  scr_need(r, 1);
  return *r->p++;
}

static uint32_t scr_u32(mizu_scr *r) {
  scr_need(r, 4);
  uint32_t v;
  memcpy(&v, r->p, 4);
  r->p += 4;
  return v;
}

static uint64_t scr_u64(mizu_scr *r) {
  scr_need(r, 8);
  uint64_t v;
  memcpy(&v, r->p, 8);
  r->p += 8;
  return v;
}

static SEXP scr_node(mizu_scr *r, unsigned depth);

/* A CHARSXP element: i32 length (UINT32_MAX = NA), u32 cetype, bytes. */
static SEXP scr_string(mizu_scr *r) {
  uint32_t n = scr_u32(r);
  if (n == UINT32_MAX) return NA_STRING;
  uint32_t ce = scr_u32(r);
  if (n > (uint32_t) INT32_MAX || ce > CE_BYTES)
    Rf_error("mizu: corrupt payload stream");
  scr_need(r, n);
  SEXP cs = Rf_mkCharLenCE((const char *) r->p, (int) n, (cetype_t) ce);
  r->p += n;
  return cs;
}

/* The (name, value) attribute pairs of a HASATTR node, applied with
   Rf_setAttrib — class last through Rf_classgets, so validation sees the
   complete object and the object bit follows the class as R's own
   setAttrib maintains it. */
static void scr_attrs(mizu_scr *r, SEXP x, unsigned depth) {
  uint32_t n = scr_u32(r);
  if (n > (uint32_t) (r->end - r->p) / 9)
    Rf_error("mizu: corrupt payload stream");
  PROTECT_INDEX ipx;
  SEXP classv = R_NilValue;
  PROTECT_WITH_INDEX(classv, &ipx);   /* anchors the deferred class value */
  for (uint32_t i = 0; i < n; i++) {
    SEXP nm = PROTECT(scr_string(r));
    if (nm == NA_STRING) {
      UNPROTECT(2);
      Rf_error("mizu: corrupt payload stream");
    }
    SEXP val = PROTECT(scr_node(r, depth + 1));
    SEXP sym = Rf_installTrChar(nm);
    if (sym == R_ClassSymbol) {
      classv = val;
      REPROTECT(classv, ipx);
    } else {
      Rf_setAttrib(x, sym, val);
    }
    UNPROTECT(2);
  }
  if (classv != R_NilValue) Rf_classgets(x, classv);
  UNPROTECT(1);
}

static SEXP scr_body(mizu_scr *r, uint32_t tag, unsigned depth) {
  if (tag == SC_FRAME) {
    /* the frame leaf: u8 name length, the region name — resolved to the
       list view through the one resolve path (the serialize consumer's
       and the 'I' ref leaf's); a VECSXP view is the whole cross-check.
       A gone region raises in the open — the exec containment's task ERR,
       the 'I' ref leaf's discipline */
    uint32_t nl = scr_u8(r);
    if (nl == 0 || nl >= MIZU_NAME_MAX)
      Rf_error("mizu: corrupt payload stream");
    scr_need(r, nl);
    char id[MIZU_NAME_MAX];
    memcpy(id, r->p, nl);
    id[nl] = 0;
    r->p += nl;
    SEXP v = mizu_view_resolve_id(id, R_NilValue);
    if (v == R_NilValue || TYPEOF(v) != VECSXP || !mizu_view_check(v))
      Rf_error("mizu: corrupt payload stream");
    return v;
  }
  if (depth > MIZU_CODEC_MAXDEPTH)
    Rf_error("mizu: corrupt payload stream");
  R_CheckStack();
  uint32_t ty = tag & 0x0f;
  /* flags the writer never produces on these types are corruption */
  if ((tag & SC_HASTAG) && ty != SC_LIST && ty != SC_LANG)
    Rf_error("mizu: corrupt payload stream");
  if ((tag & SC_HASS4) && ty != SC_LGL && ty != SC_INT && ty != SC_REAL &&
      ty != SC_CPLX && ty != SC_RAW && ty != SC_STR && ty != SC_VEC &&
      ty != SC_EXPR && ty != SC_CLOS)
    Rf_error("mizu: corrupt payload stream");
  if ((tag & SC_PRIM) && ty != SC_SYM)
    Rf_error("mizu: corrupt payload stream");
  if ((tag & SC_HASATTR) &&
      (ty == SC_NIL || ty == SC_MISSING || ty == SC_UNBOUND ||
       ty == SC_SYM || ty == SC_LIST || ty == SC_LANG))
    Rf_error("mizu: corrupt payload stream");
  switch (ty) {
  case SC_NIL:     return R_NilValue;
  case SC_MISSING: return R_MissingArg;
  case SC_UNBOUND: return R_UnboundValue;
  case SC_SYM: {
    SEXP pn = PROTECT(scr_string(r));
    SEXP s = Rf_installTrChar(pn);
    if (tag & SC_PRIM) {
      if (!R_existsVarInFrame(R_BaseEnv, s)) {
        UNPROTECT(1);
        Rf_error("mizu: corrupt payload stream");
      }
      s = Rf_eval(s, R_BaseEnv);
      if (TYPEOF(s) != BUILTINSXP && TYPEOF(s) != SPECIALSXP) {
        UNPROTECT(1);
        Rf_error("mizu: corrupt payload stream");
      }
    }
    UNPROTECT(1);
    return s;
  }
  case SC_LGL: case SC_INT: case SC_REAL: case SC_CPLX: case SC_RAW: {
    SEXPTYPE rt = ty == SC_LGL ? LGLSXP : ty == SC_INT ? INTSXP :
      ty == SC_REAL ? REALSXP : ty == SC_CPLX ? CPLXSXP : RAWSXP;
    uint64_t len = scr_u64(r);
    size_t elt = mizu_view_sizeof_elt((int) rt);
    /* len is stream-bounded: each element costs elt bytes still ahead */
    if (elt == 0 || len > (uint64_t) (r->end - r->p) / elt)
      Rf_error("mizu: corrupt payload stream");
    SEXP s = PROTECT(Rf_allocVector(rt, (R_xlen_t) len));
    if (tag & SC_HASATTR) scr_attrs(r, s, depth);
    scr_need(r, (size_t) len * elt);
    memcpy(mizu_vec_ptr(s), r->p, (size_t) len * elt);
    r->p += (size_t) len * elt;
    if (tag & SC_HASS4) s = Rf_asS4(s, TRUE, 0);
    UNPROTECT(1);
    return s;
  }
  case SC_STR: {
    uint64_t len = scr_u64(r);
    if (len > (uint64_t) (r->end - r->p) / 4)
      Rf_error("mizu: corrupt payload stream");
    SEXP s = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t) len));
    if (tag & SC_HASATTR) scr_attrs(r, s, depth);
    for (R_xlen_t i = 0; i < (R_xlen_t) len; i++)
      SET_STRING_ELT(s, i, scr_string(r));
    if (tag & SC_HASS4) s = Rf_asS4(s, TRUE, 0);
    UNPROTECT(1);
    return s;
  }
  case SC_VEC: case SC_EXPR: {
    SEXPTYPE rt = ty == SC_VEC ? VECSXP : EXPRSXP;
    uint64_t len = scr_u64(r);
    if (len > (uint64_t) (r->end - r->p))
      Rf_error("mizu: corrupt payload stream");
    SEXP s = PROTECT(Rf_allocVector(rt, (R_xlen_t) len));
    if (tag & SC_HASATTR) scr_attrs(r, s, depth);
    for (R_xlen_t i = 0; i < (R_xlen_t) len; i++)
      SET_VECTOR_ELT(s, i, scr_node(r, depth + 1));
    if (tag & SC_HASS4) s = Rf_asS4(s, TRUE, 0);
    UNPROTECT(1);
    return s;
  }
  case SC_LIST: case SC_LANG: {
    /* R's ReadItem_Iterative discipline: one PROTECT on the head, each
       fresh node linked into the list before any allocating read */
    SEXP first = PROTECT(ty == SC_LIST ? Rf_cons(R_NilValue, R_NilValue)
                                       : Rf_lcons(R_NilValue, R_NilValue));
    SEXP last = first;
    for (;;) {
      if (tag & SC_HASTAG) SET_TAG(last, scr_node(r, depth + 1));
      SETCAR(last, scr_node(r, depth + 1));
      tag = scr_u8(r);
      uint32_t ty = tag & 0x0f;
      if (ty != SC_LIST && ty != SC_LANG) {
        SETCDR(last, scr_body(r, tag, depth + 1));
        break;
      }
      SEXP node = ty == SC_LIST ? Rf_cons(R_NilValue, R_NilValue)
                                : Rf_lcons(R_NilValue, R_NilValue);
      SETCDR(last, node);
      last = node;
    }
    UNPROTECT(1);
    return first;
  }
  case SC_CLOS: {
    SEXP env = R_NilValue;
    switch (scr_u8(r)) {
    case SC_ENV_GLOBAL: env = R_GlobalEnv; break;
    case SC_ENV_BASE:   env = R_BaseEnv; break;
    case SC_ENV_EMPTY:  env = R_EmptyEnv; break;
    case SC_ENV_NS: {
      SEXP nm = PROTECT(scr_string(r));
      if (nm == NA_STRING) {
        UNPROTECT(1);
        Rf_error("mizu: corrupt payload stream");
      }
      /* R_Unserialize's discipline exactly: base's ..getNamespace — the
         registry short-circuit for a registered namespace, the
         loadNamespace attempt, the .GlobalEnv substitution, and the
         _R_NO_REPORT_MISSING_NAMESPACES_ reporting rules (R's own
         registry is legacy non-API, so the eval is unconditional; it
         lands only on SC_ENV_NS reads — non-bytecode namespace closures,
         rare: installed package functions are bytecode and never reach
         the codec) */
      SEXP getns =
        PROTECT(Rf_findFun(Rf_install("..getNamespace"), R_BaseNamespace));
      SEXP spec = PROTECT(Rf_ScalarString(nm));
      SEXP call = PROTECT(Rf_lang3(getns, spec, Rf_mkString("<unknown>")));
      env = Rf_eval(call, R_BaseNamespace);
      UNPROTECT(3);
      UNPROTECT(1);
      break;
    }
    default:
      Rf_error("mizu: corrupt payload stream");
    }
    PROTECT(env);
    SEXP formals = PROTECT(scr_node(r, depth + 1));
    SEXP body = PROTECT(scr_node(r, depth + 1));
    SEXP cl = PROTECT(R_mkClosure(formals, body, env));
    if (tag & SC_HASATTR) scr_attrs(r, cl, depth);
    if (tag & SC_HASS4) cl = Rf_asS4(cl, TRUE, 0);
    UNPROTECT(4);
    return cl;
  }
  case SC_S4: {
    /* born with the bit; attributes are the whole payload */
    SEXP s = PROTECT(Rf_allocS4Object());
    if (tag & SC_HASATTR) scr_attrs(r, s, depth);
    UNPROTECT(1);
    return s;
  }
  }
  Rf_error("mizu: corrupt payload stream");
}

static SEXP scr_node(mizu_scr *r, unsigned depth) {
  return scr_body(r, scr_u8(r), depth);
}

/* Read a codec stream of len bytes at buf (magic byte included). Raises
   on any malformation; the transport never produces one, the checks are
   the corrupt-payload discipline of the other tiers. */
SEXP mizu_codec_read(const unsigned char *buf, size_t len) {
  mizu_scr r = { buf, buf + len };
  if (len == 0 || scr_u8(&r) != MIZU_CODEC_MAGIC)
    Rf_error("mizu: corrupt payload stream");
  SEXP out = scr_node(&r, 0);
  if (r.p != r.end)
    Rf_error("mizu: corrupt payload stream");
  return out;
}

/* Task-frame read: a pool task payload is list(expr, args) — on this tier
   a SC_VEC of length 2. Reading the frame directly skips materializing
   the outer list, and an empty args reads as the shared empty vector (the
   worker binds from it read-only), so a constant task allocates nothing
   here at all. On success *expr and *args cross UNPROTECTED — the caller
   PROTECTs before any allocation (nothing allocates between). A
   non-task-frame is a 0 return, never an error: the caller falls back to
   the generic read, whose shape validation stays the single authority. A
   truncated or trailing stream raises, exactly as the generic read
   raises. */
int mizu_codec_read_task(const unsigned char *buf, size_t len, SEXP *expr,
                         SEXP *args) {
  if (len < 2 || buf[0] != MIZU_CODEC_MAGIC) return 0;
  mizu_scr r = { buf + 1, buf + len };
  uint32_t tag = scr_u8(&r);
  if ((tag & 0x0f) != SC_VEC ||
      (tag & (SC_HASATTR | SC_HASTAG | SC_HASS4)))
    return 0;
  if (scr_u64(&r) != 2) return 0;
  *expr = PROTECT(scr_node(&r, 1));
  const unsigned char *save = r.p;
  uint32_t atag = scr_u8(&r);
  if ((atag & 0x0f) == SC_VEC &&
      !(atag & (SC_HASATTR | SC_HASTAG | SC_HASS4)) && scr_u64(&r) == 0) {
    *args = mizu_empty_args();
  } else {
    r.p = save;
    *args = scr_node(&r, 1);
  }
  PROTECT(*args);
  if (r.p != r.end) {
    UNPROTECT(2);
    return 0;
  }
  UNPROTECT(2);
  return 1;
}

// .Call test surface -------------------------------------------------------------

/* Codec-write object into a fresh raw vector (NULL when outside the
   subset); mizu_codec_read_call reads one back. */
SEXP mizu_codec_write_call(SEXP object) {
  size_t n = mizu_codec_write(NULL, 0, object);
  if (n == 0) return R_NilValue;
  SEXP bytes = PROTECT(Rf_allocVector(RAWSXP, (R_xlen_t) n));
  size_t wrote = mizu_codec_write(RAW(bytes), (size_t) n, object);
  if (wrote != n) {
    UNPROTECT(1);
    Rf_error("mizu: codec write mismatch");
  }
  UNPROTECT(1);
  return bytes;
}

SEXP mizu_codec_read_call(SEXP bytes) {
  if (TYPEOF(bytes) != RAWSXP) Rf_error("mizu: expected a raw vector");
  return mizu_codec_read(RAW(bytes), (size_t) XLENGTH(bytes));
}
