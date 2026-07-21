/* macOS kernel-wait primitive and death listener. The parker sleeps on the
   epoch word with __ulock_wait's UL_COMPARE_AND_WAIT_SHARED opcode (the plain
   compare-and-wait is process-local). __ulock_wait is undocumented but stable
   since 10.12, the _SHARED opcodes since 10.15; it underlies libc++
   std::atomic::wait, Rust parking_lot / crossbeam, and abseil — isolated in
   this file so a swap to mach ports would be local. The EINTR behavior under
   R's terminal SIGINT handler is verified empirically in
   tools/spike-ulock-eintr/. The death listener is a dispatch source on a
   per-watch serial queue — no thread of mov's own. */

#include "mov.h"

#ifdef __APPLE__

#include <dispatch/dispatch.h>
#include <stdlib.h>
#include <signal.h>
#include <errno.h>

extern int __ulock_wait(uint32_t operation, void *addr, uint64_t value,
                        uint32_t timeout_us);        /* 0 = forever */
extern int __ulock_wake(uint32_t operation, void *addr, uint64_t wake_value);

#define UL_COMPARE_AND_WAIT_SHARED 3
#define ULF_NO_ERRNO 0x01000000

// Parker -----------------------------------------------------------------------

int mov_parker_attach(mov_parker *pk, _Atomic uint32_t *epoch,
                      const char *region_name, int entity, int create) {
  (void) region_name; (void) entity; (void) create;
  pk->epoch = epoch;
  return 0;
}

void mov_parker_detach(mov_parker *pk) {
  pk->epoch = NULL;
}

int mov_park(mov_parker *pk, uint32_t snapshot, long timeout_ms) {
  if (atomic_load_explicit(pk->epoch, memory_order_acquire) != snapshot)
    return MOV_PARK_WOKEN;
  if (timeout_ms == 0) return MOV_PARK_TIMEOUT;

  /* Always timed (a 0 timeout means forever): MOV_PARK_NOMINAL_MS fits the
     uint32 microsecond argument, so the clamp below never produces 0. */
  long ms = timeout_ms < 0 ? MOV_PARK_NOMINAL_MS : timeout_ms;
  uint64_t us64 = (uint64_t) ms * 1000;
  uint32_t us = us64 > UINT32_MAX ? UINT32_MAX : (uint32_t) us64;
  int r = __ulock_wait(UL_COMPARE_AND_WAIT_SHARED | ULF_NO_ERRNO,
                       (void *) pk->epoch, snapshot, us);
  if (r >= 0) return MOV_PARK_WOKEN;
  switch (-r) {
  case ETIMEDOUT: return MOV_PARK_TIMEOUT;
  case EINTR:     return MOV_PARK_INTR;
  default:        return MOV_PARK_WOKEN;
  }
}

void mov_unpark(mov_parker *pk) {
  atomic_fetch_add_explicit(pk->epoch, 1, memory_order_release);
  /* -ENOENT (no waiter) is the common no-op case */
  __ulock_wake(UL_COMPARE_AND_WAIT_SHARED | ULF_NO_ERRNO,
               (void *) pk->epoch, 0);
}

// Death listener -----------------------------------------------------------------

struct mov_death_watch_s {
  dispatch_queue_t queue;            /* per-watch serial queue: the stop sync */
  dispatch_source_t source;
  _Atomic int *flag;
  mov_parker pk;
  int has_pk;
};

static void mov_dw_fire(void *ctx) {
  struct mov_death_watch_s *w = ctx;
  atomic_store_explicit(w->flag, 1, memory_order_release);
  if (w->has_pk) mov_unpark(&w->pk);
}

static void mov_dw_event(void *ctx) {
  struct mov_death_watch_s *w = ctx;
  mov_dw_fire(w);
  dispatch_source_cancel(w->source);
}

static void mov_dw_noop(void *ctx) {
  (void) ctx;
}

mov_death_watch *mov_death_watch_start(long pid, _Atomic int *flag,
                                       const mov_parker *pk) {
  struct mov_death_watch_s *w = calloc(1, sizeof(*w));
  if (w == NULL) return NULL;
  w->flag = flag;
  if (pk != NULL) {
    w->pk = *pk;
    w->has_pk = 1;
  }

  w->queue = dispatch_queue_create("mov.death", DISPATCH_QUEUE_SERIAL);
  if (w->queue == NULL) {
    free(w);
    return NULL;
  }
  w->source = dispatch_source_create(DISPATCH_SOURCE_TYPE_PROC,
                                     (uintptr_t) pid, DISPATCH_PROC_EXIT,
                                     w->queue);
  if (w->source == NULL) {
    dispatch_release(w->queue);
    free(w);
    return NULL;
  }
  dispatch_set_context(w->source, w);
  dispatch_source_set_event_handler_f(w->source, mov_dw_event);
  dispatch_resume(w->source);

  /* Close the create/exit race: a source armed for an already-dead pid never
     fires. Firing here can race the source's own event — both paths are
     idempotent (flag store; one spurious unpark, absorbed by re-check). */
  if (kill((pid_t) pid, 0) != 0 && errno == ESRCH) mov_dw_fire(w);

  return w;
}

void mov_death_watch_stop(mov_death_watch *w) {
  dispatch_source_cancel(w->source);
  /* Drain the serial queue: any in-flight event handler has completed once
     this returns, and a cancelled source submits no more, so freeing w (and
     the caller's flag/parker targets) is safe after this. */
  dispatch_sync_f(w->queue, NULL, mov_dw_noop);
  dispatch_release(w->source);
  dispatch_release(w->queue);
  free(w);
}

void mov_death_listener_teardown(void) {
}

#endif /* __APPLE__ */
