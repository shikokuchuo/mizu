/* Linux kernel-wait primitive and death listener. The parker sleeps on the
   epoch word with FUTEX_WAIT *without* FUTEX_PRIVATE_FLAG (cross-process
   futexes key on the physical page). The death listener is one dormant
   per-process thread blocked in epoll_wait over pidfd_open descriptors
   (kernel >= 5.3, enforced at package load): it never touches the R API,
   does no periodic work, and sleeps until a death event or teardown. */

#include "mov.h"

#ifdef __linux__

#include <linux/futex.h>
#include <sys/syscall.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>

#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434
#endif

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

  /* Always timed: an untimed FUTEX_WAIT returns ERESTARTSYS on signal and is
     silently restarted under SA_RESTART; only the timed wait's
     ERESTART_RESTARTBLOCK is converted to EINTR when a handler runs. */
  long ms = timeout_ms < 0 ? MOV_PARK_NOMINAL_MS : timeout_ms;
  struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
  long r = syscall(SYS_futex, (uint32_t *) pk->epoch, FUTEX_WAIT, snapshot,
                   &ts, NULL, 0);
  if (r == 0) return MOV_PARK_WOKEN;
  switch (errno) {
  case ETIMEDOUT: return MOV_PARK_TIMEOUT;
  case EINTR:     return MOV_PARK_INTR;
  default:        return MOV_PARK_WOKEN;    /* EAGAIN: epoch already moved */
  }
}

void mov_unpark(mov_parker *pk) {
  atomic_fetch_add_explicit(pk->epoch, 1, memory_order_release);
  syscall(SYS_futex, (uint32_t *) pk->epoch, FUTEX_WAKE, 1, NULL, NULL, 0);
}

// Death listener -----------------------------------------------------------------

enum { MOV_DW_ACTIVE, MOV_DW_FIRED, MOV_DW_STOPPED };

struct mov_death_watch_s {
  int pidfd;
  int state;
  int listed;                       /* on the registry; freed by the thread */
  _Atomic int *flag;
  mov_parker pk;
  int has_pk;
  void (*cb)(void *);               /* pure-C death callback (may be NULL) */
  void *cb_arg;
  struct mov_death_watch_s *prev, *next;
};

static pthread_mutex_t mov_dl_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_t mov_dl_thread;
static int mov_dl_running = 0;
static int mov_dl_shutdown = 0;
static int mov_dl_epfd = -1;
static int mov_dl_evfd = -1;
static pid_t mov_dl_pid = 0;
static struct mov_death_watch_s *mov_dl_watches = NULL;

/* A forked child inherits the parent's bookkeeping but not its thread (and
   possibly a mutex held at fork time): abandon everything and start fresh.
   Registered watches are the parent's — leaked in the child by design, like
   mori's macOS log fd is closed, never unlinked. Callers are single-threaded
   at this point (only the forking thread survives fork). */
static void mov_dl_fork_guard(void) {
  if (mov_dl_pid == 0 || mov_dl_pid == getpid()) return;
  if (mov_dl_epfd >= 0) close(mov_dl_epfd);
  if (mov_dl_evfd >= 0) close(mov_dl_evfd);
  mov_dl_epfd = mov_dl_evfd = -1;
  mov_dl_running = 0;
  mov_dl_shutdown = 0;
  mov_dl_watches = NULL;
  mov_dl_pid = 0;
  pthread_mutex_init(&mov_dl_mu, NULL);
}

static void mov_dw_fire(struct mov_death_watch_s *w) {
  atomic_store_explicit(w->flag, 1, memory_order_release);
  if (w->has_pk) mov_unpark(&w->pk);
  if (w->cb != NULL) w->cb(w->cb_arg);
}

static void *mov_dl_main(void *arg) {
  (void) arg;
  struct epoll_event evs[16];
  for (;;) {
    int n = epoll_wait(mov_dl_epfd, evs, 16, -1);
    if (n < 0) {
      if (errno == EINTR) continue;
      break;
    }
    pthread_mutex_lock(&mov_dl_mu);
    for (int i = 0; i < n; i++) {
      if (evs[i].data.ptr == NULL) {          /* eventfd tick */
        uint64_t v;
        ssize_t r = read(mov_dl_evfd, &v, sizeof(v));
        (void) r;
        continue;
      }
      struct mov_death_watch_s *w = evs[i].data.ptr;
      if (w->state == MOV_DW_ACTIVE) {
        mov_dw_fire(w);
        epoll_ctl(mov_dl_epfd, EPOLL_CTL_DEL, w->pidfd, NULL);
        close(w->pidfd);
        w->pidfd = -1;
        w->state = MOV_DW_FIRED;
      }
    }
    /* Reap stopped watches. Only this thread frees listed watches, so an
       event fetched in this batch can never reference an already-freed one. */
    for (struct mov_death_watch_s *w = mov_dl_watches, *next; w; w = next) {
      next = w->next;
      if (w->state == MOV_DW_STOPPED) {
        if (w->pidfd >= 0) {
          epoll_ctl(mov_dl_epfd, EPOLL_CTL_DEL, w->pidfd, NULL);
          close(w->pidfd);
        }
        if (w->prev) w->prev->next = w->next; else mov_dl_watches = w->next;
        if (w->next) w->next->prev = w->prev;
        free(w);
      }
    }
    int shut = mov_dl_shutdown;
    pthread_mutex_unlock(&mov_dl_mu);
    if (shut) break;
  }
  return NULL;
}

/* Callers hold mov_dl_mu. */
static int mov_dl_ensure_started(void) {
  if (mov_dl_running) return 0;
  mov_dl_epfd = epoll_create1(EPOLL_CLOEXEC);
  if (mov_dl_epfd < 0) return -1;
  mov_dl_evfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (mov_dl_evfd < 0) goto fail;
  struct epoll_event ev = {.events = EPOLLIN, .data = {.ptr = NULL}};
  if (epoll_ctl(mov_dl_epfd, EPOLL_CTL_ADD, mov_dl_evfd, &ev) != 0) goto fail;
  if (pthread_create(&mov_dl_thread, NULL, mov_dl_main, NULL) != 0) goto fail;
  mov_dl_running = 1;
  mov_dl_pid = getpid();
  return 0;

fail:
  if (mov_dl_epfd >= 0) close(mov_dl_epfd);
  if (mov_dl_evfd >= 0) close(mov_dl_evfd);
  mov_dl_epfd = mov_dl_evfd = -1;
  return -1;
}

static void mov_dl_poke(void) {
  uint64_t one = 1;
  ssize_t r = write(mov_dl_evfd, &one, sizeof(one));
  (void) r;
}

mov_death_watch *mov_death_watch_start2(long pid, _Atomic int *flag,
                                        const mov_parker *pk,
                                        void (*cb)(void *), void *cb_arg) {
  mov_dl_fork_guard();

  struct mov_death_watch_s *w = calloc(1, sizeof(*w));
  if (w == NULL) return NULL;
  w->flag = flag;
  if (pk != NULL) {
    w->pk = *pk;
    w->has_pk = 1;
  }
  w->cb = cb;
  w->cb_arg = cb_arg;

  int pidfd = (int) syscall(SYS_pidfd_open, (pid_t) pid, 0);
  if (pidfd < 0) {
    if (errno == ESRCH) {              /* already dead: fire immediately */
      w->pidfd = -1;
      w->state = MOV_DW_FIRED;
      mov_dw_fire(w);
      return w;
    }
    free(w);
    return NULL;
  }

  pthread_mutex_lock(&mov_dl_mu);
  if (mov_dl_ensure_started() != 0) {
    pthread_mutex_unlock(&mov_dl_mu);
    close(pidfd);
    free(w);
    return NULL;
  }
  w->pidfd = pidfd;
  w->state = MOV_DW_ACTIVE;
  w->listed = 1;
  w->next = mov_dl_watches;
  if (mov_dl_watches) mov_dl_watches->prev = w;
  mov_dl_watches = w;
  struct epoll_event ev = {.events = EPOLLIN, .data = {.ptr = w}};
  if (epoll_ctl(mov_dl_epfd, EPOLL_CTL_ADD, pidfd, &ev) != 0) {
    if (w->next) w->next->prev = NULL;
    mov_dl_watches = w->next;
    pthread_mutex_unlock(&mov_dl_mu);
    close(pidfd);
    free(w);
    return NULL;
  }
  pthread_mutex_unlock(&mov_dl_mu);
  return w;
}

void mov_death_watch_stop(mov_death_watch *w) {
  mov_dl_fork_guard();
  if (w->listed && mov_dl_running) {
    /* Taking the mutex synchronizes with a fire in flight: after it is
       released, the thread never touches this watch's flag/parker again. */
    pthread_mutex_lock(&mov_dl_mu);
    w->state = MOV_DW_STOPPED;
    pthread_mutex_unlock(&mov_dl_mu);
    mov_dl_poke();
    return;
  }
  if (w->pidfd >= 0) close(w->pidfd);
  free(w);
}

void mov_death_listener_teardown(void) {
  mov_dl_fork_guard();
  if (!mov_dl_running) return;
  pthread_mutex_lock(&mov_dl_mu);
  mov_dl_shutdown = 1;
  pthread_mutex_unlock(&mov_dl_mu);
  mov_dl_poke();
  pthread_join(mov_dl_thread, NULL);
  /* Thread joined: single-threaded from here. Close per-watch fds but leave
     the structs for their handles' finalizers, which free unlisted watches. */
  for (struct mov_death_watch_s *w = mov_dl_watches, *next; w; w = next) {
    next = w->next;
    if (w->state == MOV_DW_STOPPED) {
      if (w->pidfd >= 0) close(w->pidfd);
      free(w);
    } else {
      if (w->pidfd >= 0) close(w->pidfd);
      w->pidfd = -1;
      w->listed = 0;
    }
  }
  mov_dl_watches = NULL;
  close(mov_dl_epfd);
  close(mov_dl_evfd);
  mov_dl_epfd = mov_dl_evfd = -1;
  mov_dl_running = 0;
  mov_dl_shutdown = 0;
  mov_dl_pid = 0;
}

#endif /* __linux__ */
