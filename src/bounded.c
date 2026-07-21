/* Bounded single-pass serialize stream — the second mov-owned delta over the
   vendored core. The vendored streams are strict count-then-write; the ring's
   INLINE fast path serializes directly into the slot's inline region and
   flips to count-only mode on overflow: one pass total for payloads that fit
   (the dominant case in the target regime), and an exact total size for the
   spill allocation when they don't, at the cost of at most one inline budget
   of discarded copy. Same R_outpstream discipline as mori_serialize_into;
   the vendored two-pass invariant is untouched on the spill path. */

#include "mov.h"

typedef struct mov_bounded_s {
  unsigned char *buf;        /* NULL once overflowed: count-only mode */
  size_t limit;
  size_t total;
} mov_bounded;

static void mov_write_bounded(R_outpstream_t stream, void *src, int len) {
  mov_bounded *b = (mov_bounded *) stream->data;
  size_t n = (size_t) len;
  if (b->buf != NULL) {
    if (b->total + n <= b->limit)
      memcpy(b->buf + b->total, src, n);
    else
      b->buf = NULL;
  }
  b->total += n;
}

size_t mov_serialize_bounded(unsigned char *dst, size_t limit, SEXP object) {

  mov_bounded b = {.buf = dst, .limit = limit, .total = 0};
  struct R_outpstream_st out;

  R_InitOutPStream(&out, (R_pstream_data_t) &b, R_pstream_binary_format,
                   3, NULL, mov_write_bounded, NULL, R_NilValue);
  R_Serialize(object, &out);

  return b.total;
}

// .Call test surface -----------------------------------------------------------

/* Serialize object against a byte limit; returns list(size, bytes) where
   bytes is the complete stream if it fit within limit, else NULL. */
SEXP mov_bounded_call(SEXP object, SEXP limit) {
  double lim_in = Rf_asReal(limit);
  if (!(lim_in >= 0)) Rf_error("mov: invalid limit");
  size_t lim = (size_t) lim_in;

  unsigned char *buf = (unsigned char *) R_alloc(lim > 0 ? lim : 1, 1);
  size_t n = mov_serialize_bounded(buf, lim, object);

  SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(out, 0, Rf_ScalarReal((double) n));
  if (n <= lim) {
    SEXP bytes = Rf_allocVector(RAWSXP, (R_xlen_t) n);
    SET_VECTOR_ELT(out, 1, bytes);
    memcpy(RAW(bytes), buf, n);
  }
  UNPROTECT(1);
  return out;
}

SEXP mov_unserialize_call(SEXP bytes) {
  if (TYPEOF(bytes) != RAWSXP) Rf_error("mov: expected a raw vector");
  return mori_unserialize_from(RAW(bytes), (size_t) XLENGTH(bytes));
}
