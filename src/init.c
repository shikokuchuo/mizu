#include <R_ext/Rdynload.h>
#include "mizu.h"

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

SEXP mizu_bounded_call(SEXP, SEXP);
SEXP mizu_unserialize_call(SEXP);
SEXP mizu_codec_write_call(SEXP);
SEXP mizu_codec_read_call(SEXP);
SEXP mizu_interop_write_call(SEXP);
SEXP mizu_interop_read_call(SEXP);
SEXP mizu_stream_read_call(SEXP);
SEXP mizu_region_create(SEXP);
SEXP mizu_region_open(SEXP, SEXP);
SEXP mizu_region_name(SEXP);
SEXP mizu_region_size(SEXP);
SEXP mizu_peek(SEXP, SEXP, SEXP);
SEXP mizu_poke(SEXP, SEXP, SEXP);
SEXP mizu_prune_call(void);
SEXP mizu_preamble_write_call(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_preamble_validate_call(SEXP);
SEXP mizu_park_call(SEXP, SEXP, SEXP, SEXP);
SEXP mizu_unpark_call(SEXP, SEXP, SEXP);
SEXP mizu_epoch_call(SEXP, SEXP);
SEXP mizu_live_open_call(SEXP);
SEXP mizu_live_try_call(SEXP);
SEXP mizu_live_close_call(SEXP);
SEXP mizu_live_dir_call(void);
SEXP mizu_death_watch_call(SEXP, SEXP, SEXP, SEXP);
SEXP mizu_death_fired_call(SEXP);
SEXP mizu_death_stop_call(SEXP);
SEXP mizu_channel_create_call(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_channel_suffix(SEXP);
SEXP mizu_channel_ready_wait_call(SEXP, SEXP);
SEXP mizu_channel_destroy_call(SEXP);
SEXP mizu_channel_attach_call(SEXP, SEXP);
SEXP mizu_channel_ready_set_call(SEXP);
SEXP mizu_channel_send_call(SEXP, SEXP);
SEXP mizu_channel_send_batch_call(SEXP, SEXP);
SEXP mizu_channel_recv_call(SEXP, SEXP);
SEXP mizu_channel_recv_batch_call(SEXP, SEXP, SEXP);
SEXP mizu_channel_close_call(SEXP, SEXP);
SEXP mizu_channel_close_signal_call(SEXP);
SEXP mizu_channel_alive_call(SEXP);
SEXP mizu_channel_stat(SEXP);
SEXP mizu_sentinel_check(SEXP);
SEXP mizu_now_call(void);
SEXP mizu_pool_create_call(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_pool_suffix(SEXP);
SEXP mizu_pool_ready_wait_call(SEXP, SEXP, SEXP);
SEXP mizu_pool_destroy_call(SEXP);
SEXP mizu_pool_worker_join_call(SEXP, SEXP, SEXP);
SEXP mizu_pool_leave_call(SEXP);
SEXP mizu_pool_lame_duck_call(SEXP);
SEXP mizu_pool_retire_call(SEXP, SEXP);
SEXP mizu_pool_attach_call(SEXP);
SEXP mizu_pool_submit_call(SEXP, SEXP, SEXP, SEXP);
SEXP mizu_pool_submit_batch_call(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_pool_submit_expr(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_pool_submit_try(SEXP, SEXP, SEXP, SEXP);
SEXP mizu_pool_step_call(SEXP, SEXP);
SEXP mizu_pool_run(SEXP, SEXP);
SEXP mizu_pool_run_outcome(SEXP, SEXP);
SEXP mizu_pool_set_eval(SEXP);
SEXP mizu_current_pool_call(void);
SEXP mizu_pool_set_trace_call(SEXP, SEXP);
SEXP mizu_pool_deque_pull_call(SEXP, SEXP);
SEXP mizu_pool_exec_fail_call(SEXP, SEXP);
SEXP mizu_pool_dump_call(SEXP);
SEXP mizu_pool_collect_call(SEXP, SEXP);
SEXP mizu_pool_collect_try(SEXP, SEXP);
SEXP mizu_pool_collect_any_call(SEXP, SEXP);
SEXP mizu_pool_collect_all_call(SEXP, SEXP);
SEXP mizu_pool_cancel_call(SEXP);
SEXP mizu_pool_task_state_call(SEXP);
SEXP mizu_pool_stop_call(SEXP, SEXP);
SEXP mizu_pool_status_call(SEXP);
SEXP mizu_pool_stats_call(SEXP);
SEXP mizu_pool_map_caps_call(SEXP);
SEXP mizu_pool_map_cache(SEXP);
SEXP mizu_pool_signals_call(SEXP);
SEXP mizu_pool_help_once_call(SEXP);
SEXP mizu_map_eligible(SEXP);
SEXP mizu_map_stage(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_map_open(SEXP, SEXP);
SEXP mizu_map_desc(SEXP);
SEXP mizu_map_slice(SEXP, SEXP, SEXP);
SEXP mizu_map_write(SEXP, SEXP, SEXP);
SEXP mizu_map_gather(SEXP);
SEXP mizu_map_gather_view(SEXP, SEXP, SEXP);
SEXP mizu_map_splice(SEXP, SEXP, SEXP);
SEXP mizu_map_lost(SEXP, SEXP);
SEXP mizu_map_next(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mizu_map_batch(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP,
                   SEXP);
SEXP mizu_map_abandon(SEXP, SEXP);
SEXP mizu_map_cancel_set(SEXP);
SEXP mizu_map_cancel_get(SEXP);
SEXP mizu_map_reset(SEXP);
SEXP mizu_map_swap_x(SEXP, SEXP);
SEXP mizu_map_info(SEXP);
SEXP mizu_map_claim_state(SEXP, SEXP);
SEXP mizu_map_timeout_call(void);
SEXP mizu_map_rng_base(SEXP);
SEXP mizu_map_rng_seek(SEXP, SEXP);
SEXP mizu_map_rng_install(SEXP);
SEXP mizu_strip_srcref(SEXP);
SEXP mizu_zc_view_check_call(SEXP);
SEXP mizu_zc_refcount_call(SEXP);
SEXP mizu_zc_view_name_call(SEXP);
SEXP mizu_pool_zc_info(SEXP);

static const R_CallMethodDef CallEntries[] = {
  {"mizu_onload_probe",           (DL_FUNC) &mizu_onload_probe,              0},
  {"mizu_tune_malloc",            (DL_FUNC) &mizu_tune_malloc,               0},
  {"mizu_bounded_call",           (DL_FUNC) &mizu_bounded_call,              2},
  {"mizu_unserialize_call",       (DL_FUNC) &mizu_unserialize_call,          1},
  {"mizu_codec_write_call",       (DL_FUNC) &mizu_codec_write_call,          1},
  {"mizu_interop_write_call",     (DL_FUNC) &mizu_interop_write_call,        1},
  {"mizu_interop_read_call",      (DL_FUNC) &mizu_interop_read_call,         1},
  {"mizu_stream_read_call",       (DL_FUNC) &mizu_stream_read_call,          1},
  {"mizu_codec_read_call",        (DL_FUNC) &mizu_codec_read_call,           1},
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
  {"mizu_channel_send_batch",     (DL_FUNC) &mizu_channel_send_batch_call,   2},
  {"mizu_channel_recv",           (DL_FUNC) &mizu_channel_recv_call,         2},
  {"mizu_channel_recv_batch",     (DL_FUNC) &mizu_channel_recv_batch_call,   3},
  {"mizu_channel_close",          (DL_FUNC) &mizu_channel_close_call,        2},
  {"mizu_channel_close_signal",   (DL_FUNC) &mizu_channel_close_signal_call, 1},
  {"mizu_channel_alive",          (DL_FUNC) &mizu_channel_alive_call,        1},
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
  {"mizu_pool_submit_try",        (DL_FUNC) &mizu_pool_submit_try,           4},
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
  {"mizu_map_eligible",           (DL_FUNC) &mizu_map_eligible,              1},
  {"mizu_map_stage",              (DL_FUNC) &mizu_map_stage,                 6},
  {"mizu_map_open",               (DL_FUNC) &mizu_map_open,                  2},
  {"mizu_map_desc",               (DL_FUNC) &mizu_map_desc,                  1},
  {"mizu_map_slice",              (DL_FUNC) &mizu_map_slice,                 3},
  {"mizu_map_write",              (DL_FUNC) &mizu_map_write,                 3},
  {"mizu_map_gather",             (DL_FUNC) &mizu_map_gather,                1},
  {"mizu_map_gather_view",        (DL_FUNC) &mizu_map_gather_view,           3},
  {"mizu_map_splice",             (DL_FUNC) &mizu_map_splice,                3},
  {"mizu_map_lost",               (DL_FUNC) &mizu_map_lost,                  2},
  {"mizu_map_next",               (DL_FUNC) &mizu_map_next,                  6},
  {"mizu_map_batch",              (DL_FUNC) &mizu_map_batch,                 10},
  {"mizu_map_abandon",            (DL_FUNC) &mizu_map_abandon,               2},
  {"mizu_map_cancel_set",         (DL_FUNC) &mizu_map_cancel_set,            1},
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
  {"mizu_pool_zc_info",           (DL_FUNC) &mizu_pool_zc_info,              1},
  {NULL, NULL, 0}
};

void R_init_mizu(DllInfo *dll) {
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
  mizu_wrap_init();
  mizu_payload_init();
  mizu_entity_init();
  mizu_channel_init();
  mizu_pool_init();
  mizu_map_init();
  mizu_zc_init();
  mizu_interop_init();
  mizu_view_altrep_init(dll);
}

/* Called by R if the DLL is ever unloaded (the package deliberately has no
   .onUnload — see R/mizu-package.R — so this is dev-tooling territory):
   death-listener teardown, then the preserved-object releases in reverse
   init order. The ALTREP class registrations stay — R has no unregister. */
void R_unload_mizu(DllInfo *dll) {
  mizu_death_listener_teardown();
  mizu_pool_fini();
  mizu_channel_fini();
  mizu_payload_fini();
}
