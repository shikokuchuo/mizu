#include "view.h"

// Counting pass: compute exact serialized size -------------------------------

static void rei_view_count_bytes(R_outpstream_t stream, void *src, int len) {
  rei_view_buf *buf = (rei_view_buf *) stream->data;
  buf->cur += (size_t) len;
}

size_t rei_view_serialize_count(SEXP object) {

  rei_view_buf buf = {.buf = NULL, .len = 0, .cur = 0};
  struct R_outpstream_st out;

  R_InitOutPStream(&out, (R_pstream_data_t) &buf, R_pstream_binary_format,
                   3, NULL, rei_view_count_bytes, NULL, R_NilValue);
  R_Serialize(object, &out);

  return buf.cur;
}

// Fixed-buffer counted write: no realloc, no bounds check --------------------
// Capacity is the caller's guarantee (a counting pass, or a known position in
// an exactly-sized region). Returns the bytes written, so a caller that
// serializes into a known-final position reads the size off the cursor
// instead of running a separate counting pass.

static void rei_view_write_fixed(R_outpstream_t stream, void *src, int len) {
  rei_view_buf *buf = (rei_view_buf *) stream->data;
  memcpy(buf->buf + buf->cur, src, (size_t) len);
  buf->cur += (size_t) len;
}

size_t rei_view_serialize_into(unsigned char *dst, SEXP object) {

  rei_view_buf buf = {.buf = dst, .len = 0, .cur = 0};
  struct R_outpstream_st out;

  R_InitOutPStream(&out, (R_pstream_data_t) &buf, R_pstream_binary_format,
                   3, NULL, rei_view_write_fixed, NULL, R_NilValue);
  R_Serialize(object, &out);

  return buf.cur;
}

// Read callbacks for unserialize-from-buffer ---------------------------------

static void rei_view_read_bytes(R_inpstream_t stream, void *dst, int len) {
  rei_view_buf *buf = (rei_view_buf *) stream->data;
  size_t n = (size_t) len;
  if (buf->cur + n > buf->len) n = buf->len - buf->cur;
  memcpy(dst, buf->buf + buf->cur, n);
  buf->cur += n;
}

SEXP rei_view_unserialize_from(unsigned char *src, size_t size) {

  rei_view_buf buf = {.buf = src, .len = size, .cur = 0};
  struct R_inpstream_st in;

  R_InitInPStream(&in, (R_pstream_data_t) &buf, R_pstream_binary_format,
                  NULL, rei_view_read_bytes, NULL, R_NilValue);
  return R_Unserialize(&in);
}

