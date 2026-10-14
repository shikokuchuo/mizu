/* The Arrow frame paths (arrow.c): the send side a thin gate over the
   core's Arrow frame writer (the vendored mizu_arrow_* ext surface), with
   nanoarrow as the R-level source of the ABI structs (R/arrow.R's
   helpers); the receive side the fused decide-and-fill behind
   mizu_as_arrow(). Channels and pools. */

#ifndef MIZU_ARROW_H
#define MIZU_ARROW_H

#include "mizu.h"

/* The stage-dispatch gate: an ArrowTabular / RecordBatchReader environment
   or a nanoarrow_array_stream / nanoarrow_array external pointer. The two
   TYPEOF compares run ahead of any inherits call, so the vector hot path
   pays nothing. */
int mizu_arrow_check(SEXP x);

/* Stage x as one MIZL frame region through the core's mizu_arrow_stage.
   The size pre-gate (the region-span proxy) and every stage decline
   convert the held batches (or the unconsumed stream) to a value and
   re-enter through the stage-fn parameter (mizu_r_stage_channel or
   mizu_r_stage_pool); the writer's declines raise
   mizu_error_not_portable. caps is the capability word the core's
   conjunction gates on: the channel passes the peer's (rh->peer_caps),
   the pool this build's (MIZU_R_CAPS — a task frame is same-language by
   construction). */
void mizu_arrow_stage_r(SEXP x, mizu_slot_hdr *hdr, unsigned char *payload,
                        uint32_t inline_max, mizu_handle *h, void *ctx,
                        uint32_t caps, mizu_stage_fn reenter);

/* The pull: arrow_batches(x) — R calls; the fresh triple UNPROTECTED.
   batches NULL = a non-struct stream (the triple's third slot). */
SEXP mizu_arrow_pull(SEXP x);
/* The open + size over a pulled triple at caps. The core handle and the
   batch-pointer array it reads live in a malloc'd struct behind an extptr
   whose prot is the triple and whose finalizer closes and frees — the
   longjmp backstop for every R call that may run while it is open; the
   stage and the convert close early. *fsize = the frame span
   (mizu_arrow_size, memoized in the core). An open failure raises
   not-portable with the core's decline text, as the channel does today. */
SEXP mizu_arrow_open_r(SEXP triple, uint32_t caps, size_t *fsize);
/* The by-value form of a pulled triple: arrow_to_value on a non-struct
   stream, arrow_batches_to_value on held batches — R calls, fresh,
   UNPROTECTED. */
SEXP mizu_arrow_value(SEXP triple);
/* Close a held handle early (the finalizer frees the struct either way):
   the convert path's early close — the core's classification state need
   not wait on the GC. */
void mizu_arrow_close_held(SEXP xp);
/* The stage of an open handle into the stage's single checkout (pure C:
   mizu_arrow_stage, then close). 0 = staged, name/name_len filled, warn
   the conversion counts, the checkout held; nonzero = declined (the caps
   gate, a churn race, a region failure) with nothing claimed — the handle
   closed either way, the caller converts. The core's mizu_arrow_stage
   takes hdr/payload: the extptr struct carries a scratch slot header and
   name buffer for the call — a top-level caller copies the SHM_VEC stamp
   onto its own hdr/payload, the codec's frame leaf reads name/name_len
   off the scratch and the surrounding stream's header stays the codec
   tier's. name / name_len may be NULL (the top-level form reads the
   struct directly). */
int mizu_arrow_stage_held(SEXP xp, mizu_handle *h, unsigned char *name,
                          uint32_t *name_len, mizu_arrow_warn *warn);
/* The conversion counts raise after the write half completed: a handler
   longjmp abandons the stage here with the unpublished reservation
   reclaimed by the next send's rollback. */
void mizu_arrow_warn_raise(const mizu_arrow_warn *warn);

/* The receive side (mizu_as_arrow): one fused decide-and-fill call —
   every column locates uniformly through its own view identifier (a bare
   root by magic, a path through mizu_view_path_entry), returning the
   per-column borrow kinds (0 copy / 1 double / 2 int32 / 3 int64 /
   4 string / 5 uint64) and filling every borrowed column's pre-allocated
   ArrowArray
   in place, one counted loan per distinct region. */
SEXP mizu_arrow_export_call(SEXP x, SEXP arrays);

#endif /* MIZU_ARROW_H */
