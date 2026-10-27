#include <R_ext/Rdynload.h>
#include "mizu.h"
#include "arrow.h"

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#include <errno.h>
#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434
#endif
#endif

/* Load-time guards. mizu is 64-bit only: the wire formats are built on
   64-bit monotonic positions whose lock-freedom the wrap-arithmetic and
   crash-atomicity arguments depend on; 32-bit targets would fall back to
   lock-based 64-bit atomics, which do not work across processes. (A
   compile-time constant — the check folds away on every supported target.)
   On Linux the death listener has no pre-pidfd fallback, by decision — a
   listener-less mode would turn host death under a parked non-interactive
   peer into a permanent hang, and a second liveness design would double
   exactly the surface this package works hardest to keep small. */
SEXP mizu_onload_probe(void) {
  if (sizeof(void *) < 8) {
    Rf_error("mizu requires 64-bit R: its cross-process wire formats depend on "
             "lock-free 64-bit atomics");
  }
#ifdef __linux__
  long fd = syscall(SYS_pidfd_open, (long) getpid(), 0);
  if (fd >= 0) {
    close((int) fd);
  } else if (errno == ENOSYS) {
    Rf_error("mizu requires Linux kernel >= 5.3: this kernel lacks pidfd_open, "
             "which the peer death listener depends on");
  }
#endif
  return R_NilValue;
}

/* glibc malloc tuning for the spawned peer/worker processes only — the
   host R process is left alone (set GLIBC_TUNABLES there instead). */
SEXP mizu_tune_malloc(void) {
  mizu_tune();
  return R_NilValue;
}

/* The entry-point prototypes live in mizu.h, sectioned by defining TU, so
   each definition compiles against its registration-time signature. */

static const R_CallMethodDef CallEntries[] = {
  {"mizu_onload_probe",           (DL_FUNC) &mizu_onload_probe,              0},
  {"mizu_tune_malloc",            (DL_FUNC) &mizu_tune_malloc,               0},
  {"mizu_bounded_call",           (DL_FUNC) &mizu_bounded_call,              2},
  {"mizu_unserialize_call",       (DL_FUNC) &mizu_unserialize_call,          1},
  {"mizu_codec_write_call",       (DL_FUNC) &mizu_codec_write_call,          1},
  {"mizu_interop_write_call",     (DL_FUNC) &mizu_interop_write_call,        1},
  {"mizu_interop_read_call",      (DL_FUNC) &mizu_interop_read_call,         1},
  {"mizu_interop_write_err_call", (DL_FUNC) &mizu_interop_write_err_call,    2},
  {"mizu_interop_write_task_call", (DL_FUNC) &mizu_interop_write_task_call,  4},
  {"mizu_interop_map_desc_call",  (DL_FUNC) &mizu_interop_map_desc_call,     3},
  {"mizu_interop_runner_call",    (DL_FUNC) &mizu_interop_runner_call,       5},
  {"mizu_interop_read_task_call", (DL_FUNC) &mizu_interop_read_task_call,    1},
  {"mizu_stream_read_call",       (DL_FUNC) &mizu_stream_read_call,          1},
  {"mizu_codec_read_call",        (DL_FUNC) &mizu_codec_read_call,           1},
  {"mizu_codec_cycle_call",       (DL_FUNC) &mizu_codec_cycle_call,          0},
  {"mizu_region_create",          (DL_FUNC) &mizu_region_create,             1},
  {"mizu_region_open",            (DL_FUNC) &mizu_region_open,               2},
  {"mizu_region_name",            (DL_FUNC) &mizu_region_name,               1},
  {"mizu_region_size",            (DL_FUNC) &mizu_region_size,               1},
  {"mizu_peek",                   (DL_FUNC) &mizu_peek,                      3},
  {"mizu_poke",                   (DL_FUNC) &mizu_poke,                      3},
  {"mizu_prune_call",             (DL_FUNC) &mizu_prune_call,                0},
  {"mizu_preamble_write_call",    (DL_FUNC) &mizu_preamble_write_call,       6},
  {"mizu_preamble_validate_call", (DL_FUNC) &mizu_preamble_validate_call,    1},
  {"mizu_park_call",              (DL_FUNC) &mizu_park_call,                 4},
  {"mizu_unpark_call",            (DL_FUNC) &mizu_unpark_call,               3},
  {"mizu_epoch_call",             (DL_FUNC) &mizu_epoch_call,                2},
  {"mizu_live_open_call",         (DL_FUNC) &mizu_live_open_call,            1},
  {"mizu_live_try_call",          (DL_FUNC) &mizu_live_try_call,             1},
  {"mizu_live_close_call",        (DL_FUNC) &mizu_live_close_call,           1},
  {"mizu_live_dir_call",          (DL_FUNC) &mizu_live_dir_call,             0},
  {"mizu_death_watch_call",       (DL_FUNC) &mizu_death_watch_call,          4},
  {"mizu_death_fired_call",       (DL_FUNC) &mizu_death_fired_call,          1},
  {"mizu_death_stop_call",        (DL_FUNC) &mizu_death_stop_call,           1},
  {"mizu_channel_create",         (DL_FUNC) &mizu_channel_create_call,       5},
  {"mizu_channel_suffix",         (DL_FUNC) &mizu_channel_suffix,            1},
  {"mizu_channel_ready_wait",     (DL_FUNC) &mizu_channel_ready_wait_call,   2},
  {"mizu_channel_destroy",        (DL_FUNC) &mizu_channel_destroy_call,      1},
  {"mizu_channel_attach",         (DL_FUNC) &mizu_channel_attach_call,       2},
  {"mizu_channel_ready_set",      (DL_FUNC) &mizu_channel_ready_set_call,    1},
  {"mizu_channel_send",           (DL_FUNC) &mizu_channel_send_call,         2},
  {"mizu_channel_send_error",     (DL_FUNC) &mizu_channel_send_error_call,   2},
  {"mizu_channel_send_batch",     (DL_FUNC) &mizu_channel_send_batch_call,   2},
  {"mizu_channel_recv",           (DL_FUNC) &mizu_channel_recv_call,         2},
  {"mizu_channel_recv_batch",     (DL_FUNC) &mizu_channel_recv_batch_call,   3},
  {"mizu_channel_close",          (DL_FUNC) &mizu_channel_close_call,        2},
  {"mizu_channel_close_signal",   (DL_FUNC) &mizu_channel_close_signal_call, 1},
  {"mizu_channel_alive",          (DL_FUNC) &mizu_channel_alive_call,        1},
  {"mizu_channel_keep_out",       (DL_FUNC) &mizu_channel_keep_out_call,   1},
  {"mizu_channel_stat",           (DL_FUNC) &mizu_channel_stat,              1},
  {"mizu_sentinel_check",         (DL_FUNC) &mizu_sentinel_check,            1},
  {"mizu_now_call",               (DL_FUNC) &mizu_now_call,                  0},
  {"mizu_pool_create",            (DL_FUNC) &mizu_pool_create_call,          6},
  {"mizu_pool_suffix",            (DL_FUNC) &mizu_pool_suffix,               1},
  {"mizu_pool_ready_wait",        (DL_FUNC) &mizu_pool_ready_wait_call,      3},
  {"mizu_pool_destroy",           (DL_FUNC) &mizu_pool_destroy_call,         1},
  {"mizu_pool_worker_join",       (DL_FUNC) &mizu_pool_worker_join_call,     3},
  {"mizu_pool_leave",             (DL_FUNC) &mizu_pool_leave_call,           1},
  {"mizu_pool_lame_duck",         (DL_FUNC) &mizu_pool_lame_duck_call,       1},
  {"mizu_pool_retire",            (DL_FUNC) &mizu_pool_retire_call,          2},
  {"mizu_pool_attach_call",       (DL_FUNC) &mizu_pool_attach_call,          1},
  {"mizu_pool_submit",            (DL_FUNC) &mizu_pool_submit_call,          4},
  {"mizu_pool_submit_batch",      (DL_FUNC) &mizu_pool_submit_batch_call,    5},
  {"mizu_pool_submit_expr",       (DL_FUNC) &mizu_pool_submit_expr,          5},
  {"mizu_pool_submit_spec",       (DL_FUNC) &mizu_pool_submit_spec,          4},
  {"mizu_pool_ident",             (DL_FUNC) &mizu_pool_ident_call,           1},
  {"mizu_pool_submit_try",        (DL_FUNC) &mizu_pool_submit_try,           4},
  {"mizu_pool_submit_map_runner", (DL_FUNC) &mizu_pool_submit_map_runner,    3},
  {"mizu_pool_step",              (DL_FUNC) &mizu_pool_step_call,            2},
  {"mizu_pool_run",               (DL_FUNC) &mizu_pool_run,                  2},
  {"mizu_pool_run_outcome",       (DL_FUNC) &mizu_pool_run_outcome,          2},
  {"mizu_pool_set_eval",          (DL_FUNC) &mizu_pool_set_eval,             1},
  {"mizu_current_pool_call",      (DL_FUNC) &mizu_current_pool_call,         0},
  {"mizu_pool_set_trace",         (DL_FUNC) &mizu_pool_set_trace_call,       2},
  {"mizu_pool_deque_pull",        (DL_FUNC) &mizu_pool_deque_pull_call,      2},
  {"mizu_pool_exec_fail",         (DL_FUNC) &mizu_pool_exec_fail_call,       2},
  {"mizu_pool_dump_call",         (DL_FUNC) &mizu_pool_dump_call,            1},
  {"mizu_pool_collect",           (DL_FUNC) &mizu_pool_collect_call,         2},
  {"mizu_pool_collect_try",       (DL_FUNC) &mizu_pool_collect_try,          2},
  {"mizu_pool_collect_settle",    (DL_FUNC) &mizu_pool_collect_settle,       1},
  {"mizu_pool_collect_any",       (DL_FUNC) &mizu_pool_collect_any_call,     2},
  {"mizu_pool_collect_all",       (DL_FUNC) &mizu_pool_collect_all_call,     2},
  {"mizu_pool_cancel",            (DL_FUNC) &mizu_pool_cancel_call,          1},
  {"mizu_pool_task_state",        (DL_FUNC) &mizu_pool_task_state_call,      1},
  {"mizu_pool_stop_call",         (DL_FUNC) &mizu_pool_stop_call,            2},
  {"mizu_pool_status_call",       (DL_FUNC) &mizu_pool_status_call,          1},
  {"mizu_pool_stats_call",        (DL_FUNC) &mizu_pool_stats_call,           1},
  {"mizu_pool_map_caps",          (DL_FUNC) &mizu_pool_map_caps_call,        1},
  {"mizu_pool_map_cache",         (DL_FUNC) &mizu_pool_map_cache,            1},
  {"mizu_pool_signals",           (DL_FUNC) &mizu_pool_signals_call,         1},
  {"mizu_pool_help_once",         (DL_FUNC) &mizu_pool_help_once_call,       1},
  {"mizu_chan_notify_peek",       (DL_FUNC) &mizu_chan_notify_peek,          1},
  {"mizu_chan_notify_set",        (DL_FUNC) &mizu_chan_notify_set,           2},
  {"mizu_pool_notify_peek",       (DL_FUNC) &mizu_pool_notify_peek,          1},
  {"mizu_pool_notify_set",        (DL_FUNC) &mizu_pool_notify_set,           2},
  {"mizu_task_pool",              (DL_FUNC) &mizu_task_pool_call,            1},
  {"mizu_chan_notify_arm",        (DL_FUNC) &mizu_chan_notify_arm_call,      2},
  {"mizu_chan_notify_disarm",     (DL_FUNC) &mizu_chan_notify_disarm_call,   1},
  {"mizu_pool_notify_enroll",     (DL_FUNC) &mizu_pool_notify_enroll,        6},
  {"mizu_pool_notify_scan",       (DL_FUNC) &mizu_pool_notify_scan,          2},
  {"mizu_pool_notify_remove",     (DL_FUNC) &mizu_pool_notify_remove,        2},
  {"mizu_pool_notify_find",       (DL_FUNC) &mizu_pool_notify_find,          1},
  {"mizu_pool_notify_count",      (DL_FUNC) &mizu_pool_notify_count,         1},
  {"mizu_map_eligible",           (DL_FUNC) &mizu_map_eligible,              1},
  {"mizu_map_stage",              (DL_FUNC) &mizu_map_stage,                 6},
  {"mizu_map_open",               (DL_FUNC) &mizu_map_open,                  2},
  {"mizu_map_desc",               (DL_FUNC) &mizu_map_desc,                  1},
  {"mizu_map_is_template",        (DL_FUNC) &mizu_map_is_template,           1},
  {"mizu_map_slice",              (DL_FUNC) &mizu_map_slice,                 3},
  {"mizu_map_xslice",             (DL_FUNC) &mizu_map_xslice,                3},
  {"mizu_map_write",              (DL_FUNC) &mizu_map_write,                 3},
  {"mizu_map_gather",             (DL_FUNC) &mizu_map_gather,                1},
  {"mizu_map_gather_view",        (DL_FUNC) &mizu_map_gather_view,           3},
  {"mizu_map_dim_call",           (DL_FUNC) &mizu_map_dim_call,              2},
  {"mizu_map_splice",             (DL_FUNC) &mizu_map_splice,                3},
  {"mizu_map_vsplice",            (DL_FUNC) &mizu_map_vsplice,               3},
  {"mizu_map_splice_ix",          (DL_FUNC) &mizu_map_splice_ix,             3},
  {"mizu_map_asplice",            (DL_FUNC) &mizu_map_asplice,               3},
  {"mizu_map_lost",               (DL_FUNC) &mizu_map_lost,                  2},
  {"mizu_map_next",               (DL_FUNC) &mizu_map_next,                  6},
  {"mizu_map_batch",              (DL_FUNC) &mizu_map_batch,                 10},
  {"mizu_map_abandon",            (DL_FUNC) &mizu_map_abandon,               2},
  {"mizu_map_cancel_set",         (DL_FUNC) &mizu_map_cancel_set,            1},
  {"mizu_map_close",              (DL_FUNC) &mizu_map_close,                 1},
  {"mizu_map_cancel_get",         (DL_FUNC) &mizu_map_cancel_get,            1},
  {"mizu_map_reset",              (DL_FUNC) &mizu_map_reset,                 1},
  {"mizu_map_swap_x",             (DL_FUNC) &mizu_map_swap_x,                2},
  {"mizu_map_info",               (DL_FUNC) &mizu_map_info,                  1},
  {"mizu_map_claim_state",        (DL_FUNC) &mizu_map_claim_state,           2},
  {"mizu_map_timeout_call",       (DL_FUNC) &mizu_map_timeout_call,          0},
  {"mizu_map_rng_base",           (DL_FUNC) &mizu_map_rng_base,              1},
  {"mizu_map_rng_seek",           (DL_FUNC) &mizu_map_rng_seek,              2},
  {"mizu_map_rng_install",        (DL_FUNC) &mizu_map_rng_install,           1},
  {"mizu_strip_srcref",           (DL_FUNC) &mizu_strip_srcref,              1},
  {"mizu_zc_view_check",          (DL_FUNC) &mizu_zc_view_check_call,        1},
  {"mizu_zc_refcount",            (DL_FUNC) &mizu_zc_refcount_call,          1},
  {"mizu_zc_view_name",           (DL_FUNC) &mizu_zc_view_name_call,         1},
  {"mizu_zc_armed_bytes",         (DL_FUNC) &mizu_zc_armed_bytes_call,       0},
  {"mizu_pool_zc_info",           (DL_FUNC) &mizu_pool_zc_info,              1},
  {"mizu_arrow_export",           (DL_FUNC) &mizu_arrow_export_call,         2},
  {NULL, NULL, 0}
};

/* The private precious root (mizu.h): one preserved VECSXP whose slots hold
   every mutable or cached package-level SEXP. Preserved first at load,
   released last at unload. */
SEXP mizu_precious;

static void mizu_priv_init(void) {
  mizu_precious = Rf_allocVector(VECSXP, MIZU_PRIV_MAX);
  R_PreserveObject(mizu_precious);
}

static void mizu_priv_fini(void) {
  R_ReleaseObject(mizu_precious);
  mizu_precious = R_NilValue;
}

void R_init_mizu(DllInfo *dll) {
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
  mizu_priv_init();
  mizu_cond_init();
  mizu_wrap_init();
  mizu_payload_init();
  mizu_entity_init();
  mizu_live_init();
  mizu_channel_init();
  mizu_pool_init();
  mizu_notify_init();
  mizu_map_init();
  mizu_zc_init();
  mizu_interop_init();
  SEXP wraps = PROTECT(Rf_allocVector(VECSXP, MIZU_VIEW_CACHE_MAX));
  mizu_priv_set(MIZU_PRIV_VIEW_WRAPS, wraps);
  mizu_view_altrep_init(dll, wraps);
  UNPROTECT(1);
}

/* Called by R if the DLL is ever unloaded (the package deliberately has no
   .onUnload — see R/mizu-package.R — so this is dev-tooling territory):
   death-listener teardown, then the singleton releases in reverse init
   order, then the precious root. The ALTREP class registrations stay — R
   has no unregister. */
void R_unload_mizu(DllInfo *dll) {
  /* the notify watches ride the death-listener machinery: stop them first */
  mizu_notify_fini();
  mizu_death_listener_teardown();
  mizu_view_altrep_fini();
  mizu_interop_fini();
  mizu_zc_fini();
  mizu_pool_fini();
  mizu_channel_fini();
  mizu_payload_fini();
  mizu_cond_fini();
  mizu_priv_fini();
}
