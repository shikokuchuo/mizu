#include <R_ext/Rdynload.h>
#include "mov.h"

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
SEXP mov_onload_probe(void) {
#ifdef __linux__
  long fd = syscall(SYS_pidfd_open, (long) getpid(), 0);
  if (fd >= 0) {
    close((int) fd);
  } else if (errno == ENOSYS) {
    Rf_error("mov requires Linux kernel >= 5.3: this kernel lacks pidfd_open, "
             "which the peer death listener depends on");
  }
#endif
  return R_NilValue;
}

SEXP mov_onunload(void) {
  mov_death_listener_teardown();
  return R_NilValue;
}

SEXP mov_bounded_call(SEXP, SEXP);
SEXP mov_unserialize_call(SEXP);
SEXP mov_region_create(SEXP);
SEXP mov_region_open(SEXP, SEXP);
SEXP mov_region_name(SEXP);
SEXP mov_region_size(SEXP);
SEXP mov_peek(SEXP, SEXP, SEXP);
SEXP mov_poke(SEXP, SEXP, SEXP);
SEXP mov_prune_call(void);
SEXP mov_preamble_write_call(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mov_preamble_validate_call(SEXP);
SEXP mov_park_call(SEXP, SEXP, SEXP, SEXP);
SEXP mov_unpark_call(SEXP, SEXP, SEXP);
SEXP mov_epoch_call(SEXP, SEXP);
SEXP mov_live_open_call(SEXP);
SEXP mov_live_try_call(SEXP);
SEXP mov_live_close_call(SEXP);
SEXP mov_death_watch_call(SEXP, SEXP, SEXP, SEXP);
SEXP mov_death_fired_call(SEXP);
SEXP mov_death_stop_call(SEXP);
SEXP mov_channel_create(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mov_channel_suffix(SEXP);
SEXP mov_channel_ready_wait(SEXP, SEXP);
SEXP mov_channel_destroy(SEXP);
SEXP mov_channel_attach(SEXP);
SEXP mov_channel_ready_set(SEXP);
SEXP mov_channel_send(SEXP, SEXP);
SEXP mov_channel_send_batch(SEXP, SEXP);
SEXP mov_channel_flush(SEXP);
SEXP mov_channel_recv(SEXP, SEXP);
SEXP mov_channel_recv_batch(SEXP, SEXP, SEXP);
SEXP mov_channel_close(SEXP, SEXP);
SEXP mov_channel_close_signal(SEXP);
SEXP mov_channel_alive(SEXP);
SEXP mov_channel_stat(SEXP);
SEXP mov_pool_create(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
SEXP mov_pool_suffix(SEXP);
SEXP mov_pool_ready_wait(SEXP, SEXP, SEXP);
SEXP mov_pool_destroy(SEXP);
SEXP mov_pool_worker_join(SEXP, SEXP);
SEXP mov_pool_leave(SEXP);
SEXP mov_pool_attach_call(SEXP);
SEXP mov_pool_submit(SEXP, SEXP, SEXP);
SEXP mov_pool_step(SEXP, SEXP, SEXP);
SEXP mov_pool_collect(SEXP, SEXP);
SEXP mov_pool_cancel(SEXP);
SEXP mov_pool_stop_call(SEXP, SEXP);
SEXP mov_pool_status_call(SEXP);

static const R_CallMethodDef CallEntries[] = {
  {"mov_onload_probe",          (DL_FUNC) &mov_onload_probe,          0},
  {"mov_onunload",              (DL_FUNC) &mov_onunload,              0},
  {"mov_bounded_call",          (DL_FUNC) &mov_bounded_call,          2},
  {"mov_unserialize_call",      (DL_FUNC) &mov_unserialize_call,      1},
  {"mov_region_create",         (DL_FUNC) &mov_region_create,         1},
  {"mov_region_open",           (DL_FUNC) &mov_region_open,           2},
  {"mov_region_name",           (DL_FUNC) &mov_region_name,           1},
  {"mov_region_size",           (DL_FUNC) &mov_region_size,           1},
  {"mov_peek",                  (DL_FUNC) &mov_peek,                  3},
  {"mov_poke",                  (DL_FUNC) &mov_poke,                  3},
  {"mov_prune_call",            (DL_FUNC) &mov_prune_call,            0},
  {"mov_preamble_write_call",   (DL_FUNC) &mov_preamble_write_call,   6},
  {"mov_preamble_validate_call",(DL_FUNC) &mov_preamble_validate_call,1},
  {"mov_park_call",             (DL_FUNC) &mov_park_call,             4},
  {"mov_unpark_call",           (DL_FUNC) &mov_unpark_call,           3},
  {"mov_epoch_call",            (DL_FUNC) &mov_epoch_call,            2},
  {"mov_live_open_call",        (DL_FUNC) &mov_live_open_call,        1},
  {"mov_live_try_call",         (DL_FUNC) &mov_live_try_call,         1},
  {"mov_live_close_call",       (DL_FUNC) &mov_live_close_call,       1},
  {"mov_death_watch_call",      (DL_FUNC) &mov_death_watch_call,      4},
  {"mov_death_fired_call",      (DL_FUNC) &mov_death_fired_call,      1},
  {"mov_death_stop_call",       (DL_FUNC) &mov_death_stop_call,       1},
  {"mov_channel_create",        (DL_FUNC) &mov_channel_create,        6},
  {"mov_channel_suffix",        (DL_FUNC) &mov_channel_suffix,        1},
  {"mov_channel_ready_wait",    (DL_FUNC) &mov_channel_ready_wait,    2},
  {"mov_channel_destroy",       (DL_FUNC) &mov_channel_destroy,       1},
  {"mov_channel_attach",        (DL_FUNC) &mov_channel_attach,        1},
  {"mov_channel_ready_set",     (DL_FUNC) &mov_channel_ready_set,     1},
  {"mov_channel_send",          (DL_FUNC) &mov_channel_send,          2},
  {"mov_channel_send_batch",    (DL_FUNC) &mov_channel_send_batch,    2},
  {"mov_channel_flush",         (DL_FUNC) &mov_channel_flush,         1},
  {"mov_channel_recv",          (DL_FUNC) &mov_channel_recv,          2},
  {"mov_channel_recv_batch",    (DL_FUNC) &mov_channel_recv_batch,    3},
  {"mov_channel_close",         (DL_FUNC) &mov_channel_close,         2},
  {"mov_channel_close_signal",  (DL_FUNC) &mov_channel_close_signal,  1},
  {"mov_channel_alive",         (DL_FUNC) &mov_channel_alive,         1},
  {"mov_channel_stat",          (DL_FUNC) &mov_channel_stat,          1},
  {"mov_pool_create",           (DL_FUNC) &mov_pool_create,           7},
  {"mov_pool_suffix",           (DL_FUNC) &mov_pool_suffix,           1},
  {"mov_pool_ready_wait",       (DL_FUNC) &mov_pool_ready_wait,       3},
  {"mov_pool_destroy",          (DL_FUNC) &mov_pool_destroy,          1},
  {"mov_pool_worker_join",      (DL_FUNC) &mov_pool_worker_join,      2},
  {"mov_pool_leave",            (DL_FUNC) &mov_pool_leave,            1},
  {"mov_pool_attach_call",      (DL_FUNC) &mov_pool_attach_call,      1},
  {"mov_pool_submit",           (DL_FUNC) &mov_pool_submit,           3},
  {"mov_pool_step",             (DL_FUNC) &mov_pool_step,             3},
  {"mov_pool_collect",          (DL_FUNC) &mov_pool_collect,          2},
  {"mov_pool_cancel",           (DL_FUNC) &mov_pool_cancel,           1},
  {"mov_pool_stop_call",        (DL_FUNC) &mov_pool_stop_call,        2},
  {"mov_pool_status_call",      (DL_FUNC) &mov_pool_status_call,      1},
  {NULL, NULL, 0}
};

void R_init_mov(DllInfo *dll) {
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
  mov_wrap_init();
  mov_entity_init();
  mov_channel_init();
  mov_pool_init();
}
