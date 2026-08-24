#include <R_ext/Rdynload.h>
#include "rei.h"

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#include <errno.h>
#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434
#endif
#endif

/* Load-time guards. rei is 64-bit only: the wire formats are built on
   64-bit monotonic positions whose lock-freedom the wrap-arithmetic and
   crash-atomicity arguments depend on; 32-bit targets would fall back to
   lock-based 64-bit atomics, which do not work across processes. (A
   compile-time constant — the check folds away on every supported target.)
   On Linux the death listener has no pre-pidfd fallback, by decision — a
   listener-less mode would turn host death under a parked non-interactive
   peer into a permanent hang, and a second liveness design would double
   exactly the surface this package works hardest to keep small. */
SEXP rei_onload_probe(void) {
  if (sizeof(void *) < 8) {
    Rf_error("rei requires 64-bit R: its cross-process wire formats depend on "
             "lock-free 64-bit atomics");
  }
#ifdef __linux__
  long fd = syscall(SYS_pidfd_open, (long) getpid(), 0);
  if (fd >= 0) {
    close((int) fd);
  } else if (errno == ENOSYS) {
    Rf_error("rei requires Linux kernel >= 5.3: this kernel lacks pidfd_open, "
             "which the peer death listener depends on");
  }
#endif
  return R_NilValue;
}

/* glibc malloc tuning for the spawned peer/worker processes only — the
   host R process is left alone (set GLIBC_TUNABLES there instead). */
SEXP rei_tune_malloc(void) {
  rei_tune();
  return R_NilValue;
}

SEXP rei_bounded_call(SEXP, SEXP);
SEXP rei_unserialize_call(SEXP);
SEXP rei_codec_write_call(SEXP);
SEXP rei_codec_read_call(SEXP);
SEXP rei_region_create(SEXP);
SEXP rei_region_open(SEXP, SEXP);
SEXP rei_region_name(SEXP);
SEXP rei_region_size(SEXP);
SEXP rei_peek(SEXP, SEXP, SEXP);
SEXP rei_poke(SEXP, SEXP, SEXP);
SEXP rei_prune_call(void);
SEXP rei_preamble_write_call(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP rei_preamble_validate_call(SEXP);
SEXP rei_park_call(SEXP, SEXP, SEXP, SEXP);
SEXP rei_unpark_call(SEXP, SEXP, SEXP);
SEXP rei_epoch_call(SEXP, SEXP);
SEXP rei_live_open_call(SEXP);
SEXP rei_live_try_call(SEXP);
SEXP rei_live_close_call(SEXP);
SEXP rei_live_dir_call(void);
SEXP rei_death_watch_call(SEXP, SEXP, SEXP, SEXP);
SEXP rei_death_fired_call(SEXP);
SEXP rei_death_stop_call(SEXP);
SEXP rei_channel_create_call(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP rei_channel_suffix(SEXP);
SEXP rei_channel_ready_wait_call(SEXP, SEXP);
SEXP rei_channel_destroy_call(SEXP);
SEXP rei_channel_attach_call(SEXP);
SEXP rei_channel_ready_set_call(SEXP);
SEXP rei_channel_send_call(SEXP, SEXP);
SEXP rei_channel_send_batch_call(SEXP, SEXP);
SEXP rei_channel_recv_call(SEXP, SEXP);
SEXP rei_channel_recv_batch_call(SEXP, SEXP, SEXP);
SEXP rei_channel_close_call(SEXP, SEXP);
SEXP rei_channel_close_signal_call(SEXP);
SEXP rei_channel_alive_call(SEXP);
SEXP rei_channel_stat(SEXP);
SEXP rei_sentinel_check(SEXP);
SEXP rei_now_call(void);
SEXP rei_pool_create_call(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP rei_pool_suffix(SEXP);
SEXP rei_pool_ready_wait_call(SEXP, SEXP, SEXP);
SEXP rei_pool_destroy_call(SEXP);
SEXP rei_pool_worker_join_call(SEXP, SEXP);
SEXP rei_pool_leave_call(SEXP);
SEXP rei_pool_lame_duck_call(SEXP);
SEXP rei_pool_retire_call(SEXP, SEXP);
SEXP rei_pool_attach_call(SEXP);
SEXP rei_pool_submit_call(SEXP, SEXP, SEXP, SEXP);
SEXP rei_pool_submit_batch_call(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP rei_pool_submit_expr(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP rei_pool_submit_try(SEXP, SEXP, SEXP, SEXP);
SEXP rei_pool_step_call(SEXP, SEXP);
SEXP rei_pool_run(SEXP, SEXP);
SEXP rei_pool_run_outcome(SEXP, SEXP);
SEXP rei_pool_set_eval(SEXP);
SEXP rei_pool_set_trace_call(SEXP, SEXP);
SEXP rei_pool_deque_pull_call(SEXP, SEXP);
SEXP rei_pool_dump_call(SEXP);
SEXP rei_pool_collect_call(SEXP, SEXP);
SEXP rei_pool_collect_try(SEXP, SEXP);
SEXP rei_pool_collect_any_call(SEXP, SEXP);
SEXP rei_pool_collect_all_call(SEXP, SEXP);
SEXP rei_pool_cancel_call(SEXP);
SEXP rei_pool_task_state_call(SEXP);
SEXP rei_pool_stop_call(SEXP, SEXP);
SEXP rei_pool_status_call(SEXP);
SEXP rei_pool_stats_call(SEXP);
SEXP rei_pool_map_caps_call(SEXP);
SEXP rei_pool_map_cache(SEXP);
SEXP rei_pool_signals_call(SEXP);
SEXP rei_pool_help_once_call(SEXP);
SEXP rei_map_eligible(SEXP);
SEXP rei_map_stage(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP rei_map_open(SEXP, SEXP);
SEXP rei_map_desc(SEXP);
SEXP rei_map_slice(SEXP, SEXP, SEXP);
SEXP rei_map_write(SEXP, SEXP, SEXP);
SEXP rei_map_gather(SEXP);
SEXP rei_map_gather_view(SEXP, SEXP, SEXP);
SEXP rei_map_splice(SEXP, SEXP, SEXP);
SEXP rei_map_lost(SEXP, SEXP);
SEXP rei_map_next(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP rei_map_batch(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP,
                   SEXP);
SEXP rei_map_abandon(SEXP, SEXP);
SEXP rei_map_cancel_set(SEXP);
SEXP rei_map_cancel_get(SEXP);
SEXP rei_map_reset(SEXP);
SEXP rei_map_swap_x(SEXP, SEXP);
SEXP rei_map_info(SEXP);
SEXP rei_map_claim_state(SEXP, SEXP);
SEXP rei_map_timeout_call(void);
SEXP rei_map_rng_base(SEXP);
SEXP rei_map_rng_seek(SEXP, SEXP);
SEXP rei_map_rng_install(SEXP);
SEXP rei_strip_srcref(SEXP);
SEXP rei_zc_view_check_call(SEXP);
SEXP rei_zc_refcount_call(SEXP);
SEXP rei_pool_zc_info(SEXP);

static const R_CallMethodDef CallEntries[] = {
  {"rei_onload_probe",           (DL_FUNC) &rei_onload_probe,              0},
  {"rei_tune_malloc",            (DL_FUNC) &rei_tune_malloc,               0},
  {"rei_bounded_call",           (DL_FUNC) &rei_bounded_call,              2},
  {"rei_unserialize_call",       (DL_FUNC) &rei_unserialize_call,          1},
  {"rei_codec_write_call",       (DL_FUNC) &rei_codec_write_call,          1},
  {"rei_codec_read_call",        (DL_FUNC) &rei_codec_read_call,           1},
  {"rei_region_create",          (DL_FUNC) &rei_region_create,             1},
  {"rei_region_open",            (DL_FUNC) &rei_region_open,               2},
  {"rei_region_name",            (DL_FUNC) &rei_region_name,               1},
  {"rei_region_size",            (DL_FUNC) &rei_region_size,               1},
  {"rei_peek",                   (DL_FUNC) &rei_peek,                      3},
  {"rei_poke",                   (DL_FUNC) &rei_poke,                      3},
  {"rei_prune_call",             (DL_FUNC) &rei_prune_call,                0},
  {"rei_preamble_write_call",    (DL_FUNC) &rei_preamble_write_call,       6},
  {"rei_preamble_validate_call", (DL_FUNC) &rei_preamble_validate_call,    1},
  {"rei_park_call",              (DL_FUNC) &rei_park_call,                 4},
  {"rei_unpark_call",            (DL_FUNC) &rei_unpark_call,               3},
  {"rei_epoch_call",             (DL_FUNC) &rei_epoch_call,                2},
  {"rei_live_open_call",         (DL_FUNC) &rei_live_open_call,            1},
  {"rei_live_try_call",          (DL_FUNC) &rei_live_try_call,             1},
  {"rei_live_close_call",        (DL_FUNC) &rei_live_close_call,           1},
  {"rei_live_dir_call",          (DL_FUNC) &rei_live_dir_call,             0},
  {"rei_death_watch_call",       (DL_FUNC) &rei_death_watch_call,          4},
  {"rei_death_fired_call",       (DL_FUNC) &rei_death_fired_call,          1},
  {"rei_death_stop_call",        (DL_FUNC) &rei_death_stop_call,           1},
  {"rei_channel_create",         (DL_FUNC) &rei_channel_create_call,       5},
  {"rei_channel_suffix",         (DL_FUNC) &rei_channel_suffix,            1},
  {"rei_channel_ready_wait",     (DL_FUNC) &rei_channel_ready_wait_call,   2},
  {"rei_channel_destroy",        (DL_FUNC) &rei_channel_destroy_call,      1},
  {"rei_channel_attach",         (DL_FUNC) &rei_channel_attach_call,       1},
  {"rei_channel_ready_set",      (DL_FUNC) &rei_channel_ready_set_call,    1},
  {"rei_channel_send",           (DL_FUNC) &rei_channel_send_call,         2},
  {"rei_channel_send_batch",     (DL_FUNC) &rei_channel_send_batch_call,   2},
  {"rei_channel_recv",           (DL_FUNC) &rei_channel_recv_call,         2},
  {"rei_channel_recv_batch",     (DL_FUNC) &rei_channel_recv_batch_call,   3},
  {"rei_channel_close",          (DL_FUNC) &rei_channel_close_call,        2},
  {"rei_channel_close_signal",   (DL_FUNC) &rei_channel_close_signal_call, 1},
  {"rei_channel_alive",          (DL_FUNC) &rei_channel_alive_call,        1},
  {"rei_channel_stat",           (DL_FUNC) &rei_channel_stat,              1},
  {"rei_sentinel_check",         (DL_FUNC) &rei_sentinel_check,            1},
  {"rei_now_call",               (DL_FUNC) &rei_now_call,                  0},
  {"rei_pool_create",            (DL_FUNC) &rei_pool_create_call,          6},
  {"rei_pool_suffix",            (DL_FUNC) &rei_pool_suffix,               1},
  {"rei_pool_ready_wait",        (DL_FUNC) &rei_pool_ready_wait_call,      3},
  {"rei_pool_destroy",           (DL_FUNC) &rei_pool_destroy_call,         1},
  {"rei_pool_worker_join",       (DL_FUNC) &rei_pool_worker_join_call,     2},
  {"rei_pool_leave",             (DL_FUNC) &rei_pool_leave_call,           1},
  {"rei_pool_lame_duck",         (DL_FUNC) &rei_pool_lame_duck_call,       1},
  {"rei_pool_retire",            (DL_FUNC) &rei_pool_retire_call,          2},
  {"rei_pool_attach_call",       (DL_FUNC) &rei_pool_attach_call,          1},
  {"rei_pool_submit",            (DL_FUNC) &rei_pool_submit_call,          4},
  {"rei_pool_submit_batch",      (DL_FUNC) &rei_pool_submit_batch_call,    5},
  {"rei_pool_submit_expr",       (DL_FUNC) &rei_pool_submit_expr,          5},
  {"rei_pool_submit_try",        (DL_FUNC) &rei_pool_submit_try,           4},
  {"rei_pool_step",              (DL_FUNC) &rei_pool_step_call,            2},
  {"rei_pool_run",               (DL_FUNC) &rei_pool_run,                  2},
  {"rei_pool_run_outcome",       (DL_FUNC) &rei_pool_run_outcome,          2},
  {"rei_pool_set_eval",          (DL_FUNC) &rei_pool_set_eval,             1},
  {"rei_pool_set_trace",         (DL_FUNC) &rei_pool_set_trace_call,       2},
  {"rei_pool_deque_pull",        (DL_FUNC) &rei_pool_deque_pull_call,      2},
  {"rei_pool_dump_call",         (DL_FUNC) &rei_pool_dump_call,            1},
  {"rei_pool_collect",           (DL_FUNC) &rei_pool_collect_call,         2},
  {"rei_pool_collect_try",       (DL_FUNC) &rei_pool_collect_try,          2},
  {"rei_pool_collect_any",       (DL_FUNC) &rei_pool_collect_any_call,     2},
  {"rei_pool_collect_all",       (DL_FUNC) &rei_pool_collect_all_call,     2},
  {"rei_pool_cancel",            (DL_FUNC) &rei_pool_cancel_call,          1},
  {"rei_pool_task_state",        (DL_FUNC) &rei_pool_task_state_call,      1},
  {"rei_pool_stop_call",         (DL_FUNC) &rei_pool_stop_call,            2},
  {"rei_pool_status_call",       (DL_FUNC) &rei_pool_status_call,          1},
  {"rei_pool_stats_call",        (DL_FUNC) &rei_pool_stats_call,           1},
  {"rei_pool_map_caps",          (DL_FUNC) &rei_pool_map_caps_call,        1},
  {"rei_pool_map_cache",         (DL_FUNC) &rei_pool_map_cache,            1},
  {"rei_pool_signals",           (DL_FUNC) &rei_pool_signals_call,         1},
  {"rei_pool_help_once",         (DL_FUNC) &rei_pool_help_once_call,       1},
  {"rei_map_eligible",           (DL_FUNC) &rei_map_eligible,              1},
  {"rei_map_stage",              (DL_FUNC) &rei_map_stage,                 6},
  {"rei_map_open",               (DL_FUNC) &rei_map_open,                  2},
  {"rei_map_desc",               (DL_FUNC) &rei_map_desc,                  1},
  {"rei_map_slice",              (DL_FUNC) &rei_map_slice,                 3},
  {"rei_map_write",              (DL_FUNC) &rei_map_write,                 3},
  {"rei_map_gather",             (DL_FUNC) &rei_map_gather,                1},
  {"rei_map_gather_view",        (DL_FUNC) &rei_map_gather_view,           3},
  {"rei_map_splice",             (DL_FUNC) &rei_map_splice,                3},
  {"rei_map_lost",               (DL_FUNC) &rei_map_lost,                  2},
  {"rei_map_next",               (DL_FUNC) &rei_map_next,                  6},
  {"rei_map_batch",              (DL_FUNC) &rei_map_batch,                 10},
  {"rei_map_abandon",            (DL_FUNC) &rei_map_abandon,               2},
  {"rei_map_cancel_set",         (DL_FUNC) &rei_map_cancel_set,            1},
  {"rei_map_cancel_get",         (DL_FUNC) &rei_map_cancel_get,            1},
  {"rei_map_reset",              (DL_FUNC) &rei_map_reset,                 1},
  {"rei_map_swap_x",             (DL_FUNC) &rei_map_swap_x,                2},
  {"rei_map_info",               (DL_FUNC) &rei_map_info,                  1},
  {"rei_map_claim_state",        (DL_FUNC) &rei_map_claim_state,           2},
  {"rei_map_timeout_call",       (DL_FUNC) &rei_map_timeout_call,          0},
  {"rei_map_rng_base",           (DL_FUNC) &rei_map_rng_base,              1},
  {"rei_map_rng_seek",           (DL_FUNC) &rei_map_rng_seek,              2},
  {"rei_map_rng_install",        (DL_FUNC) &rei_map_rng_install,           1},
  {"rei_strip_srcref",           (DL_FUNC) &rei_strip_srcref,              1},
  {"rei_zc_view_check",          (DL_FUNC) &rei_zc_view_check_call,        1},
  {"rei_zc_refcount",            (DL_FUNC) &rei_zc_refcount_call,          1},
  {"rei_pool_zc_info",           (DL_FUNC) &rei_pool_zc_info,              1},
  {NULL, NULL, 0}
};

void R_init_rei(DllInfo *dll) {
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
  rei_wrap_init();
  rei_payload_init();
  rei_entity_init();
  rei_channel_init();
  rei_pool_init();
  rei_map_init();
  rei_zc_init();
  mori_altrep_init(dll);
}

/* Called by R if the DLL is ever unloaded (the package deliberately has no
   .onUnload — see R/rei-package.R — so this is dev-tooling territory):
   death-listener teardown, then the preserved-object releases in reverse
   init order. The ALTREP class registrations stay — R has no unregister. */
void R_unload_rei(DllInfo *dll) {
  rei_death_listener_teardown();
  rei_pool_fini();
  rei_channel_fini();
  rei_payload_fini();
}
