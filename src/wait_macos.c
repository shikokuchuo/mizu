/* macOS kernel-wait primitive and death listener. The parker sleeps on the
   epoch word with __ulock_wait's UL_COMPARE_AND_WAIT_SHARED opcode (the plain
   compare-and-wait is process-local). __ulock_wait is undocumented but stable
   since 10.12, the _SHARED opcodes since 10.15; it underlies libc++
   std::atomic::wait, Rust parking_lot / crossbeam, and abseil — isolated in
   this file so a swap to mach ports would be local. The EINTR behavior under
   R's terminal SIGINT handler was verified empirically by a standalone
   spike (spike-ulock-eintr, preserved in git history). The death listener
   is a dispatch source on a per-watch serial queue — no thread of kioto's
   own. */

#include "kioto.h"

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

int kio_parker_attach(kio_parker *pk, _Atomic uint32_t *epoch,
                      const char *region_name, int entity, int create) {
  (void) region_name; (void) entity; (void) create;
  pk->epoch = epoch;
  return 0;
}

void kio_parker_detach(kio_parker *pk) {
  pk->epoch = NULL;
}

int kio_park(kio_parker *pk, uint32_t snapshot, long timeout_ms) {
  if (atomic_load_explicit(pk->epoch, memory_order_acquire) != snapshot)
    return KIO_PARK_WOKEN;
  if (timeout_ms == 0) return KIO_PARK_TIMEOUT;

  /* Always timed (a 0 timeout means forever): KIO_PARK_NOMINAL_MS fits the
     uint32 microsecond argument, so the clamp below never produces 0. */
  long ms = timeout_ms < 0 ? KIO_PARK_NOMINAL_MS : timeout_ms;
  uint64_t us64 = (uint64_t) ms * 1000;
  uint32_t us = us64 > UINT32_MAX ? UINT32_MAX : (uint32_t) us64;
  int r = __ulock_wait(UL_COMPARE_AND_WAIT_SHARED | ULF_NO_ERRNO,
                       (void *) pk->epoch, snapshot, us);
  if (r >= 0) return KIO_PARK_WOKEN;
  switch (-r) {
  case ETIMEDOUT: return KIO_PARK_TIMEOUT;
  case EINTR:     return KIO_PARK_INTR;
  default:        return KIO_PARK_WOKEN;
  }
}

void kio_unpark(kio_parker *pk) {
  atomic_fetch_add_explicit(pk->epoch, 1, memory_order_release);
  /* -ENOENT (no waiter) is the common no-op case */
  __ulock_wake(UL_COMPARE_AND_WAIT_SHARED | ULF_NO_ERRNO,
               (void *) pk->epoch, 0);
}

// Death listener -----------------------------------------------------------------

struct kio_death_watch_s {
  dispatch_queue_t queue;            /* per-watch serial queue: the stop sync */
  dispatch_source_t source;
  _Atomic int *flag;
  kio_parker pk;
  int has_pk;
  void (*cb)(void *);                /* pure-C death callback (may be NULL) */
  void *cb_arg;
};

static void kio_dw_fire(void *ctx) {
  struct kio_death_watch_s *w = ctx;
  atomic_store_explicit(w->flag, 1, memory_order_release);
  if (w->has_pk) kio_unpark(&w->pk);
  if (w->cb != NULL) w->cb(w->cb_arg);
}

static void kio_dw_event(void *ctx) {
  struct kio_death_watch_s *w = ctx;
  kio_dw_fire(w);
  dispatch_source_cancel(w->source);
}

static void kio_dw_noop(void *ctx) {
  (void) ctx;
}

kio_death_watch *kio_death_watch_start2(long pid, _Atomic int *flag,
                                        const kio_parker *pk,
                                        void (*cb)(void *), void *cb_arg) {
  struct kio_death_watch_s *w = calloc(1, sizeof(*w));
  if (w == NULL) return NULL;
  w->flag = flag;
  if (pk != NULL) {
    w->pk = *pk;
    w->has_pk = 1;
  }
  w->cb = cb;
  w->cb_arg = cb_arg;

  w->queue = dispatch_queue_create("kioto.death", DISPATCH_QUEUE_SERIAL);
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
  dispatch_source_set_event_handler_f(w->source, kio_dw_event);
  dispatch_resume(w->source);

  /* Close the create/exit race: a source armed for an already-dead pid never
     fires. Firing here can race the source's own event — both paths are
     idempotent (flag store; one spurious unpark, absorbed by re-check). */
  if (kill((pid_t) pid, 0) != 0 && errno == ESRCH) kio_dw_fire(w);

  return w;
}

void kio_death_watch_stop(kio_death_watch *w) {
  dispatch_source_cancel(w->source);
  /* Drain the serial queue: any in-flight event handler has completed once
     this returns, and a cancelled source submits no more, so freeing w (and
     the caller's flag/parker targets) is safe after this. */
  dispatch_sync_f(w->queue, NULL, kio_dw_noop);
  dispatch_release(w->source);
  dispatch_release(w->queue);
  free(w);
}

void kio_death_listener_teardown(void) {
}

#endif /* __APPLE__ */
