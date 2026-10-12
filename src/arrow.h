/* The Arrow frame paths (arrow.c): the send side a thin gate over the
   core's Arrow frame writer (the vendored mizu_arrow_* ext surface), with
   nanoarrow as the R-level source of the ABI structs (R/arrow.R's
   helpers); the receive side the fused decide-and-fill behind
   mizu_as_arrow(). Channels only. */

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
   re-enter mizu_r_stage_channel; the writer's declines raise
   mizu_error_not_portable. */
void mizu_arrow_stage_r(SEXP x, mizu_slot_hdr *hdr, unsigned char *payload,
                        uint32_t inline_max, mizu_handle *h, void *ctx);

/* The receive side (mizu_as_arrow): one fused decide-and-fill call —
   every column locates uniformly through its own view identifier (a bare
   root by magic, a path through mizu_view_path_entry), returning the
   per-column borrow kinds (0 copy / 1 double / 2 int32 / 3 int64 /
   4 string) and filling every borrowed column's pre-allocated ArrowArray
   in place, one counted loan per distinct region. */
SEXP mizu_arrow_export_call(SEXP x, SEXP arrays);

#endif /* MIZU_ARROW_H */
