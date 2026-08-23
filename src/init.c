#include <R_ext/Rdynload.h>
#include "sora.h"

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#include <errno.h>
#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434
#endif
#endif

/* Load-time guards. sora is 64-bit only: the wire formats are built on
   64-bit monotonic positions whose lock-freedom the wrap-arithmetic and
   crash-atomicity arguments depend on; 32-bit targets would fall back to
   lock-based 64-bit atomics, which do not work across processes. (A
   compile-time constant — the check folds away on every supported target.)
   On Linux the death listener has no pre-pidfd fallback, by decision — a
   listener-less mode would turn host death under a parked non-interactive
   peer into a permanent hang, and a second liveness design would double
   exactly the surface this package works hardest to keep small. */
SEXP sora_onload_probe(void) {
  if (sizeof(void *) < 8) {
    Rf_error("sora requires 64-bit R: its cross-process wire formats depend on "
             "lock-free 64-bit atomics");
  }
#ifdef __linux__
  long fd = syscall(SYS_pidfd_open, (long) getpid(), 0);
  if (fd >= 0) {
    close((int) fd);
  } else if (errno == ENOSYS) {
    Rf_error("sora requires Linux kernel >= 5.3: this kernel lacks pidfd_open, "
             "which the peer death listener depends on");
  }
#endif
  return R_NilValue;
}

SEXP sora_bounded_call(SEXP, SEXP);
SEXP sora_unserialize_call(SEXP);
SEXP sora_codec_write_call(SEXP);
SEXP sora_codec_read_call(SEXP);
SEXP sora_region_create(SEXP);
SEXP sora_region_open(SEXP, SEXP);
SEXP sora_region_name(SEXP);
SEXP sora_region_size(SEXP);
SEXP sora_peek(SEXP, SEXP, SEXP);
SEXP sora_poke(SEXP, SEXP, SEXP);
SEXP sora_prune_call(void);
SEXP sora_preamble_write_call(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP sora_preamble_validate_call(SEXP);
SEXP sora_park_call(SEXP, SEXP, SEXP, SEXP);
SEXP sora_unpark_call(SEXP, SEXP, SEXP);
SEXP sora_epoch_call(SEXP, SEXP);
SEXP sora_live_open_call(SEXP);
SEXP sora_live_try_call(SEXP);
SEXP sora_live_close_call(SEXP);
SEXP sora_live_dir_call(void);
SEXP sora_death_watch_call(SEXP, SEXP, SEXP, SEXP);
SEXP sora_death_fired_call(SEXP);
SEXP sora_death_stop_call(SEXP);
SEXP sora_channel_create(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP sora_channel_suffix(SEXP);
SEXP sora_channel_ready_wait(SEXP, SEXP);
SEXP sora_channel_destroy(SEXP);
SEXP sora_channel_attach(SEXP);
SEXP sora_channel_ready_set(SEXP);
SEXP sora_channel_send(SEXP, SEXP);
SEXP sora_channel_send_batch(SEXP, SEXP);
SEXP sora_channel_recv(SEXP, SEXP);
SEXP sora_channel_recv_batch(SEXP, SEXP, SEXP);
SEXP sora_channel_close(SEXP, SEXP);
SEXP sora_channel_close_signal(SEXP);
SEXP sora_channel_alive(SEXP);
SEXP sora_channel_stat(SEXP);
SEXP sora_sentinel_check(SEXP);
SEXP sora_now_call(void);
SEXP sora_pool_create(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP sora_pool_suffix(SEXP);
SEXP sora_pool_ready_wait(SEXP, SEXP, SEXP);
SEXP sora_pool_destroy(SEXP);
SEXP sora_pool_worker_join(SEXP, SEXP);
SEXP sora_pool_leave(SEXP);
SEXP sora_pool_lame_duck(SEXP);
SEXP sora_pool_retire(SEXP, SEXP);
SEXP sora_pool_attach_call(SEXP);
SEXP sora_pool_submit(SEXP, SEXP, SEXP, SEXP);
SEXP sora_pool_submit_batch(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP sora_pool_submit_expr(SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP sora_pool_submit_try(SEXP, SEXP, SEXP, SEXP);
SEXP sora_pool_step(SEXP, SEXP);
SEXP sora_pool_run(SEXP, SEXP);
SEXP sora_pool_run_outcome(SEXP, SEXP);
SEXP sora_pool_set_eval(SEXP);
SEXP sora_pool_set_trace(SEXP, SEXP);
SEXP sora_pool_deque_pull(SEXP, SEXP);
SEXP sora_pool_dump_call(SEXP);
SEXP sora_pool_collect(SEXP, SEXP);
SEXP sora_pool_collect_try(SEXP, SEXP);
SEXP sora_pool_collect_any(SEXP, SEXP);
SEXP sora_pool_collect_all(SEXP, SEXP);
SEXP sora_pool_cancel(SEXP);
SEXP sora_pool_task_state(SEXP);
SEXP sora_pool_stop_call(SEXP, SEXP);
SEXP sora_pool_status_call(SEXP);
SEXP sora_pool_stats_call(SEXP);
SEXP sora_pool_map_caps(SEXP);
SEXP sora_pool_map_cache(SEXP);
SEXP sora_pool_signals(SEXP);
SEXP sora_pool_help_once(SEXP);
SEXP sora_map_eligible(SEXP);
SEXP sora_map_stage(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP sora_map_open(SEXP, SEXP);
SEXP sora_map_desc(SEXP);
SEXP sora_map_slice(SEXP, SEXP, SEXP);
SEXP sora_map_write(SEXP, SEXP, SEXP);
SEXP sora_map_gather(SEXP);
SEXP sora_map_gather_view(SEXP, SEXP, SEXP);
SEXP sora_map_splice(SEXP, SEXP, SEXP);
SEXP sora_map_lost(SEXP, SEXP);
SEXP sora_map_next(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP sora_map_batch(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP,
                   SEXP);
SEXP sora_map_abandon(SEXP, SEXP);
SEXP sora_map_cancel_set(SEXP);
SEXP sora_map_cancel_get(SEXP);
SEXP sora_map_reset(SEXP);
SEXP sora_map_swap_x(SEXP, SEXP);
SEXP sora_map_info(SEXP);
SEXP sora_map_claim_state(SEXP, SEXP);
SEXP sora_map_timeout_call(void);
SEXP sora_map_rng_base(SEXP);
SEXP sora_map_rng_seek(SEXP, SEXP);
SEXP sora_map_rng_install(SEXP);
SEXP sora_strip_srcref(SEXP);
SEXP sora_zc_view_check_call(SEXP);
SEXP sora_zc_refcount_call(SEXP);
SEXP sora_pool_zc_info(SEXP);

static const R_CallMethodDef CallEntries[] = {
  {"sora_onload_probe",          (DL_FUNC) &sora_onload_probe,          0},
  {"sora_bounded_call",          (DL_FUNC) &sora_bounded_call,          2},
  {"sora_unserialize_call",      (DL_FUNC) &sora_unserialize_call,      1},
  {"sora_codec_write_call",      (DL_FUNC) &sora_codec_write_call,      1},
  {"sora_codec_read_call",       (DL_FUNC) &sora_codec_read_call,       1},
  {"sora_region_create",         (DL_FUNC) &sora_region_create,         1},
  {"sora_region_open",           (DL_FUNC) &sora_region_open,           2},
  {"sora_region_name",           (DL_FUNC) &sora_region_name,           1},
  {"sora_region_size",           (DL_FUNC) &sora_region_size,           1},
  {"sora_peek",                  (DL_FUNC) &sora_peek,                  3},
  {"sora_poke",                  (DL_FUNC) &sora_poke,                  3},
  {"sora_prune_call",            (DL_FUNC) &sora_prune_call,            0},
  {"sora_preamble_write_call",   (DL_FUNC) &sora_preamble_write_call,   6},
  {"sora_preamble_validate_call",(DL_FUNC) &sora_preamble_validate_call,1},
  {"sora_park_call",             (DL_FUNC) &sora_park_call,             4},
  {"sora_unpark_call",           (DL_FUNC) &sora_unpark_call,           3},
  {"sora_epoch_call",            (DL_FUNC) &sora_epoch_call,            2},
  {"sora_live_open_call",        (DL_FUNC) &sora_live_open_call,        1},
  {"sora_live_try_call",         (DL_FUNC) &sora_live_try_call,         1},
  {"sora_live_close_call",       (DL_FUNC) &sora_live_close_call,       1},
  {"sora_live_dir_call",         (DL_FUNC) &sora_live_dir_call,         0},
  {"sora_death_watch_call",      (DL_FUNC) &sora_death_watch_call,      4},
  {"sora_death_fired_call",      (DL_FUNC) &sora_death_fired_call,      1},
  {"sora_death_stop_call",       (DL_FUNC) &sora_death_stop_call,       1},
  {"sora_channel_create",        (DL_FUNC) &sora_channel_create,        5},
  {"sora_channel_suffix",        (DL_FUNC) &sora_channel_suffix,        1},
  {"sora_channel_ready_wait",    (DL_FUNC) &sora_channel_ready_wait,    2},
  {"sora_channel_destroy",       (DL_FUNC) &sora_channel_destroy,       1},
  {"sora_channel_attach",        (DL_FUNC) &sora_channel_attach,        1},
  {"sora_channel_ready_set",     (DL_FUNC) &sora_channel_ready_set,     1},
  {"sora_channel_send",          (DL_FUNC) &sora_channel_send,          2},
  {"sora_channel_send_batch",    (DL_FUNC) &sora_channel_send_batch,    2},
  {"sora_channel_recv",          (DL_FUNC) &sora_channel_recv,          2},
  {"sora_channel_recv_batch",    (DL_FUNC) &sora_channel_recv_batch,    3},
  {"sora_channel_close",         (DL_FUNC) &sora_channel_close,         2},
  {"sora_channel_close_signal",  (DL_FUNC) &sora_channel_close_signal,  1},
  {"sora_channel_alive",         (DL_FUNC) &sora_channel_alive,         1},
  {"sora_channel_stat",          (DL_FUNC) &sora_channel_stat,          1},
  {"sora_sentinel_check",        (DL_FUNC) &sora_sentinel_check,        1},
  {"sora_now_call",              (DL_FUNC) &sora_now_call,              0},
  {"sora_pool_create",           (DL_FUNC) &sora_pool_create,           6},
  {"sora_pool_suffix",           (DL_FUNC) &sora_pool_suffix,           1},
  {"sora_pool_ready_wait",       (DL_FUNC) &sora_pool_ready_wait,       3},
  {"sora_pool_destroy",          (DL_FUNC) &sora_pool_destroy,          1},
  {"sora_pool_worker_join",      (DL_FUNC) &sora_pool_worker_join,      2},
  {"sora_pool_leave",            (DL_FUNC) &sora_pool_leave,            1},
  {"sora_pool_lame_duck",        (DL_FUNC) &sora_pool_lame_duck,        1},
  {"sora_pool_retire",           (DL_FUNC) &sora_pool_retire,           2},
  {"sora_pool_attach_call",      (DL_FUNC) &sora_pool_attach_call,      1},
  {"sora_pool_submit",           (DL_FUNC) &sora_pool_submit,           4},
  {"sora_pool_submit_batch",     (DL_FUNC) &sora_pool_submit_batch,     5},
  {"sora_pool_submit_expr",      (DL_FUNC) &sora_pool_submit_expr,      5},
  {"sora_pool_submit_try",       (DL_FUNC) &sora_pool_submit_try,       4},
  {"sora_pool_step",             (DL_FUNC) &sora_pool_step,             2},
  {"sora_pool_run",              (DL_FUNC) &sora_pool_run,              2},
  {"sora_pool_run_outcome",      (DL_FUNC) &sora_pool_run_outcome,      2},
  {"sora_pool_set_eval",         (DL_FUNC) &sora_pool_set_eval,         1},
  {"sora_pool_set_trace",        (DL_FUNC) &sora_pool_set_trace,        2},
  {"sora_pool_deque_pull",       (DL_FUNC) &sora_pool_deque_pull,       2},
  {"sora_pool_dump_call",        (DL_FUNC) &sora_pool_dump_call,        1},
  {"sora_pool_collect",          (DL_FUNC) &sora_pool_collect,          2},
  {"sora_pool_collect_try",      (DL_FUNC) &sora_pool_collect_try,      2},
  {"sora_pool_collect_any",      (DL_FUNC) &sora_pool_collect_any,      2},
  {"sora_pool_collect_all",      (DL_FUNC) &sora_pool_collect_all,      2},
  {"sora_pool_cancel",           (DL_FUNC) &sora_pool_cancel,           1},
  {"sora_pool_task_state",       (DL_FUNC) &sora_pool_task_state,       1},
  {"sora_pool_stop_call",        (DL_FUNC) &sora_pool_stop_call,        2},
  {"sora_pool_status_call",      (DL_FUNC) &sora_pool_status_call,      1},
  {"sora_pool_stats_call",       (DL_FUNC) &sora_pool_stats_call,       1},
  {"sora_pool_map_caps",         (DL_FUNC) &sora_pool_map_caps,         1},
  {"sora_pool_map_cache",        (DL_FUNC) &sora_pool_map_cache,        1},
  {"sora_pool_signals",          (DL_FUNC) &sora_pool_signals,          1},
  {"sora_pool_help_once",        (DL_FUNC) &sora_pool_help_once,        1},
  {"sora_map_eligible",          (DL_FUNC) &sora_map_eligible,          1},
  {"sora_map_stage",             (DL_FUNC) &sora_map_stage,             6},
  {"sora_map_open",              (DL_FUNC) &sora_map_open,              2},
  {"sora_map_desc",              (DL_FUNC) &sora_map_desc,              1},
  {"sora_map_slice",             (DL_FUNC) &sora_map_slice,             3},
  {"sora_map_write",             (DL_FUNC) &sora_map_write,             3},
  {"sora_map_gather",            (DL_FUNC) &sora_map_gather,            1},
  {"sora_map_gather_view",       (DL_FUNC) &sora_map_gather_view,       3},
  {"sora_map_splice",            (DL_FUNC) &sora_map_splice,            3},
  {"sora_map_lost",              (DL_FUNC) &sora_map_lost,              2},
  {"sora_map_next",              (DL_FUNC) &sora_map_next,              6},
  {"sora_map_batch",             (DL_FUNC) &sora_map_batch,             10},
  {"sora_map_abandon",           (DL_FUNC) &sora_map_abandon,           2},
  {"sora_map_cancel_set",        (DL_FUNC) &sora_map_cancel_set,        1},
  {"sora_map_cancel_get",        (DL_FUNC) &sora_map_cancel_get,        1},
  {"sora_map_reset",             (DL_FUNC) &sora_map_reset,             1},
  {"sora_map_swap_x",            (DL_FUNC) &sora_map_swap_x,            2},
  {"sora_map_info",              (DL_FUNC) &sora_map_info,              1},
  {"sora_map_claim_state",       (DL_FUNC) &sora_map_claim_state,       2},
  {"sora_map_timeout_call",      (DL_FUNC) &sora_map_timeout_call,      0},
  {"sora_map_rng_base",          (DL_FUNC) &sora_map_rng_base,          1},
  {"sora_map_rng_seek",          (DL_FUNC) &sora_map_rng_seek,          2},
  {"sora_map_rng_install",       (DL_FUNC) &sora_map_rng_install,       1},
  {"sora_strip_srcref",          (DL_FUNC) &sora_strip_srcref,          1},
  {"sora_zc_view_check",         (DL_FUNC) &sora_zc_view_check_call,    1},
  {"sora_zc_refcount",           (DL_FUNC) &sora_zc_refcount_call,      1},
  {"sora_pool_zc_info",          (DL_FUNC) &sora_pool_zc_info,          1},
  {NULL, NULL, 0}
};

void R_init_sora(DllInfo *dll) {
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
  rei_tune();
  sora_wrap_init();
  sora_payload_init();
  sora_entity_init();
  sora_channel_init();
  sora_pool_init();
  sora_map_init();
  sora_zc_init();
  mori_altrep_init(dll);
}

/* Called by R if the DLL is ever unloaded (the package deliberately has no
   .onUnload — see R/sora-package.R — so this is dev-tooling territory):
   death-listener teardown, then the preserved-object releases in reverse
   init order. The ALTREP class registrations stay — R has no unregister. */
void R_unload_sora(DllInfo *dll) {
  rei_death_listener_teardown();
  sora_pool_fini();
  sora_channel_fini();
  sora_payload_fini();
}
