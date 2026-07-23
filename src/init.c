#include <R_ext/Rdynload.h>
#include "kioto.h"

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#include <errno.h>
#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434
#endif
#endif

/* Load-time guard (with the 64-bit refusal on the R side): the Linux death
   listener has no pre-pidfd fallback, by decision — a listener-less mode
   would turn host death under a parked non-interactive peer into a permanent
   hang, and a second liveness design would double exactly the surface this
   package works hardest to keep small. */
SEXP kio_onload_probe(void) {
#ifdef __linux__
  long fd = syscall(SYS_pidfd_open, (long) getpid(), 0);
  if (fd >= 0) {
    close((int) fd);
  } else if (errno == ENOSYS) {
    Rf_error("kioto requires Linux kernel >= 5.3: this kernel lacks pidfd_open, "
             "which the peer death listener depends on");
  }
#endif
  return R_NilValue;
}

SEXP kio_onunload(void) {
  kio_death_listener_teardown();
  return R_NilValue;
}

SEXP kio_bounded_call(SEXP, SEXP);
SEXP kio_unserialize_call(SEXP);
SEXP kio_region_create(SEXP);
SEXP kio_region_open(SEXP, SEXP);
SEXP kio_region_name(SEXP);
SEXP kio_region_size(SEXP);
SEXP kio_peek(SEXP, SEXP, SEXP);
SEXP kio_poke(SEXP, SEXP, SEXP);
SEXP kio_prune_call(void);
SEXP kio_preamble_write_call(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP kio_preamble_validate_call(SEXP);
SEXP kio_park_call(SEXP, SEXP, SEXP, SEXP);
SEXP kio_unpark_call(SEXP, SEXP, SEXP);
SEXP kio_epoch_call(SEXP, SEXP);
SEXP kio_live_open_call(SEXP);
SEXP kio_live_try_call(SEXP);
SEXP kio_live_close_call(SEXP);
SEXP kio_death_watch_call(SEXP, SEXP, SEXP, SEXP);
SEXP kio_death_fired_call(SEXP);
SEXP kio_death_stop_call(SEXP);
SEXP kio_channel_create(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP kio_channel_suffix(SEXP);
SEXP kio_channel_ready_wait(SEXP, SEXP);
SEXP kio_channel_destroy(SEXP);
SEXP kio_channel_attach(SEXP);
SEXP kio_channel_ready_set(SEXP);
SEXP kio_channel_send(SEXP, SEXP);
SEXP kio_channel_send_batch(SEXP, SEXP);
SEXP kio_channel_flush(SEXP);
SEXP kio_channel_recv(SEXP, SEXP);
SEXP kio_channel_recv_batch(SEXP, SEXP, SEXP);
SEXP kio_channel_close(SEXP, SEXP);
SEXP kio_channel_close_signal(SEXP);
SEXP kio_channel_alive(SEXP);
SEXP kio_channel_stat(SEXP);
SEXP kio_pool_create(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP kio_pool_suffix(SEXP);
SEXP kio_pool_ready_wait(SEXP, SEXP, SEXP);
SEXP kio_pool_destroy(SEXP);
SEXP kio_pool_worker_join(SEXP, SEXP);
SEXP kio_pool_leave(SEXP);
SEXP kio_pool_lame_duck(SEXP);
SEXP kio_pool_retire(SEXP, SEXP);
SEXP kio_pool_attach_call(SEXP);
SEXP kio_pool_submit(SEXP, SEXP, SEXP);
SEXP kio_pool_step(SEXP, SEXP);
SEXP kio_pool_fail_inflight(SEXP, SEXP);
SEXP kio_pool_set_eval(SEXP);
SEXP kio_pool_set_trace(SEXP, SEXP);
SEXP kio_pool_deque_pull(SEXP, SEXP);
SEXP kio_pool_dump_call(SEXP);
SEXP kio_pool_collect(SEXP, SEXP);
SEXP kio_pool_cancel(SEXP);
SEXP kio_pool_stop_call(SEXP, SEXP);
SEXP kio_pool_status_call(SEXP);
SEXP kio_pool_stats_call(SEXP);

static const R_CallMethodDef CallEntries[] = {
  {"kio_onload_probe",          (DL_FUNC) &kio_onload_probe,          0},
  {"kio_onunload",              (DL_FUNC) &kio_onunload,              0},
  {"kio_bounded_call",          (DL_FUNC) &kio_bounded_call,          2},
  {"kio_unserialize_call",      (DL_FUNC) &kio_unserialize_call,      1},
  {"kio_region_create",         (DL_FUNC) &kio_region_create,         1},
  {"kio_region_open",           (DL_FUNC) &kio_region_open,           2},
  {"kio_region_name",           (DL_FUNC) &kio_region_name,           1},
  {"kio_region_size",           (DL_FUNC) &kio_region_size,           1},
  {"kio_peek",                  (DL_FUNC) &kio_peek,                  3},
  {"kio_poke",                  (DL_FUNC) &kio_poke,                  3},
  {"kio_prune_call",            (DL_FUNC) &kio_prune_call,            0},
  {"kio_preamble_write_call",   (DL_FUNC) &kio_preamble_write_call,   6},
  {"kio_preamble_validate_call",(DL_FUNC) &kio_preamble_validate_call,1},
  {"kio_park_call",             (DL_FUNC) &kio_park_call,             4},
  {"kio_unpark_call",           (DL_FUNC) &kio_unpark_call,           3},
  {"kio_epoch_call",            (DL_FUNC) &kio_epoch_call,            2},
  {"kio_live_open_call",        (DL_FUNC) &kio_live_open_call,        1},
  {"kio_live_try_call",         (DL_FUNC) &kio_live_try_call,         1},
  {"kio_live_close_call",       (DL_FUNC) &kio_live_close_call,       1},
  {"kio_death_watch_call",      (DL_FUNC) &kio_death_watch_call,      4},
  {"kio_death_fired_call",      (DL_FUNC) &kio_death_fired_call,      1},
  {"kio_death_stop_call",       (DL_FUNC) &kio_death_stop_call,       1},
  {"kio_channel_create",        (DL_FUNC) &kio_channel_create,        6},
  {"kio_channel_suffix",        (DL_FUNC) &kio_channel_suffix,        1},
  {"kio_channel_ready_wait",    (DL_FUNC) &kio_channel_ready_wait,    2},
  {"kio_channel_destroy",       (DL_FUNC) &kio_channel_destroy,       1},
  {"kio_channel_attach",        (DL_FUNC) &kio_channel_attach,        1},
  {"kio_channel_ready_set",     (DL_FUNC) &kio_channel_ready_set,     1},
  {"kio_channel_send",          (DL_FUNC) &kio_channel_send,          2},
  {"kio_channel_send_batch",    (DL_FUNC) &kio_channel_send_batch,    2},
  {"kio_channel_flush",         (DL_FUNC) &kio_channel_flush,         1},
  {"kio_channel_recv",          (DL_FUNC) &kio_channel_recv,          2},
  {"kio_channel_recv_batch",    (DL_FUNC) &kio_channel_recv_batch,    3},
  {"kio_channel_close",         (DL_FUNC) &kio_channel_close,         2},
  {"kio_channel_close_signal",  (DL_FUNC) &kio_channel_close_signal,  1},
  {"kio_channel_alive",         (DL_FUNC) &kio_channel_alive,         1},
  {"kio_channel_stat",          (DL_FUNC) &kio_channel_stat,          1},
  {"kio_pool_create",           (DL_FUNC) &kio_pool_create,           7},
  {"kio_pool_suffix",           (DL_FUNC) &kio_pool_suffix,           1},
  {"kio_pool_ready_wait",       (DL_FUNC) &kio_pool_ready_wait,       3},
  {"kio_pool_destroy",          (DL_FUNC) &kio_pool_destroy,          1},
  {"kio_pool_worker_join",      (DL_FUNC) &kio_pool_worker_join,      2},
  {"kio_pool_leave",            (DL_FUNC) &kio_pool_leave,            1},
  {"kio_pool_lame_duck",        (DL_FUNC) &kio_pool_lame_duck,        1},
  {"kio_pool_retire",           (DL_FUNC) &kio_pool_retire,           2},
  {"kio_pool_attach_call",      (DL_FUNC) &kio_pool_attach_call,      1},
  {"kio_pool_submit",           (DL_FUNC) &kio_pool_submit,           3},
  {"kio_pool_step",             (DL_FUNC) &kio_pool_step,             2},
  {"kio_pool_fail_inflight",    (DL_FUNC) &kio_pool_fail_inflight,    2},
  {"kio_pool_set_eval",         (DL_FUNC) &kio_pool_set_eval,         1},
  {"kio_pool_set_trace",        (DL_FUNC) &kio_pool_set_trace,        2},
  {"kio_pool_deque_pull",       (DL_FUNC) &kio_pool_deque_pull,       2},
  {"kio_pool_dump_call",        (DL_FUNC) &kio_pool_dump_call,        1},
  {"kio_pool_collect",          (DL_FUNC) &kio_pool_collect,          2},
  {"kio_pool_cancel",           (DL_FUNC) &kio_pool_cancel,           1},
  {"kio_pool_stop_call",        (DL_FUNC) &kio_pool_stop_call,        2},
  {"kio_pool_status_call",      (DL_FUNC) &kio_pool_status_call,      1},
  {"kio_pool_stats_call",       (DL_FUNC) &kio_pool_stats_call,       1},
  {NULL, NULL, 0}
};

void R_init_kioto(DllInfo *dll) {
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
  kio_wrap_init();
  kio_entity_init();
  kio_channel_init();
  kio_pool_init();
}
