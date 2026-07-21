/* Windows kernel-wait primitive and death listener. WaitOnAddress /
   WakeByAddress* is process-local by design and cannot synchronize across
   processes, so the parker is a named auto-reset kernel event per waiting
   entity: the epoch compare is not atomic with the sleep, but auto-reset
   stickiness substitutes — a SetEvent with no waiter leaves the event
   signalled and the next wait consumes it immediately. Events are created by
   the region's host before any peer attaches, named derivably from the
   region name, and refcounted by the kernel with no unlink step. The death
   listener is threadless: RegisterWaitForSingleObject on the OS thread pool
   turns process-handle signalling into the fire callback. */

#include "mov.h"

#ifdef _WIN32

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <stdlib.h>
#include <stdio.h>

// Parker -----------------------------------------------------------------------

int mov_parker_attach(mov_parker *pk, _Atomic uint32_t *epoch,
                      const char *region_name, int entity, int create) {
  char name[MORI_NAME_MAX + 16];
  int n = snprintf(name, sizeof(name), "%s.pk.%d", region_name, entity);
  if (n <= 0 || (size_t) n >= sizeof(name)) return -1;
  pk->epoch = epoch;
  pk->event = create ?
    CreateEventA(NULL, FALSE, FALSE, name) :
    OpenEventA(SYNCHRONIZE | EVENT_MODIFY_STATE, FALSE, name);
  return pk->event != NULL ? 0 : -1;
}

void mov_parker_detach(mov_parker *pk) {
  if (pk->event != NULL) CloseHandle((HANDLE) pk->event);
  pk->event = NULL;
  pk->epoch = NULL;
}

int mov_park(mov_parker *pk, uint32_t snapshot, long timeout_ms) {
  if (atomic_load_explicit(pk->epoch, memory_order_acquire) != snapshot)
    return MOV_PARK_WOKEN;
  if (timeout_ms == 0) return MOV_PARK_TIMEOUT;

  DWORD ms = timeout_ms < 0 ? INFINITE : (DWORD) timeout_ms;
  DWORD r = WaitForSingleObject((HANDLE) pk->event, ms);
  return r == WAIT_TIMEOUT ? MOV_PARK_TIMEOUT : MOV_PARK_WOKEN;
}

void mov_unpark(mov_parker *pk) {
  atomic_fetch_add_explicit(pk->epoch, 1, memory_order_release);
  SetEvent((HANDLE) pk->event);
}

// Death listener -----------------------------------------------------------------

struct mov_death_watch_s {
  HANDLE process;
  HANDLE wait;
  _Atomic int *flag;
  mov_parker pk;
  int has_pk;
};

static void mov_dw_fire(struct mov_death_watch_s *w) {
  atomic_store_explicit(w->flag, 1, memory_order_release);
  if (w->has_pk) mov_unpark(&w->pk);
}

static VOID CALLBACK mov_dw_cb(PVOID ctx, BOOLEAN timed_out) {
  (void) timed_out;
  mov_dw_fire((struct mov_death_watch_s *) ctx);
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

  w->process = OpenProcess(SYNCHRONIZE, FALSE, (DWORD) pid);
  if (w->process == NULL) {
    /* pid gone (reaped): fire immediately rather than error — matches the
       POSIX already-dead path. Access denial is a genuine failure. */
    if (GetLastError() == ERROR_INVALID_PARAMETER) {
      mov_dw_fire(w);
      return w;
    }
    free(w);
    return NULL;
  }

  /* An already-signalled handle (process exited between OpenProcess and
     here) fires the callback immediately. */
  if (!RegisterWaitForSingleObject(&w->wait, w->process, mov_dw_cb, w,
                                   INFINITE, WT_EXECUTEONLYONCE)) {
    CloseHandle(w->process);
    free(w);
    return NULL;
  }
  return w;
}

void mov_death_watch_stop(mov_death_watch *w) {
  /* Blocking unregister: returns only after any in-flight callback has
     completed, so freeing w (and the caller's flag/parker targets) is safe. */
  if (w->wait != NULL) UnregisterWaitEx(w->wait, INVALID_HANDLE_VALUE);
  if (w->process != NULL) CloseHandle(w->process);
  free(w);
}

void mov_death_listener_teardown(void) {
}

#endif /* _WIN32 */
