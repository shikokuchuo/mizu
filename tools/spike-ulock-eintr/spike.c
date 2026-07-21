/* Phase 0 gate: verify SIGINT (Ctrl-C) EINTRs a timed __ulock_wait under
   R's terminal SIGINT handler (ipc-plan.md, Part I risk 2). Run via
   spike.sh inside an interactive R session on a pty; results are written
   to a file from C so they survive any R-level interrupt longjmp after
   the .Call returns. */

#include <R.h>
#include <Rinternals.h>
#include <stdint.h>
#include <stdio.h>
#include <signal.h>
#include <time.h>

#ifdef __APPLE__

extern int __ulock_wait(uint32_t operation, void *addr, uint64_t value,
                        uint32_t timeout);           /* timeout in us, 0 = forever */

#define UL_COMPARE_AND_WAIT_SHARED 3
#define ULF_NO_ERRNO 0x01000000                      /* return -errcode, no errno */

static uint32_t spike_word;

SEXP spike_ulock(SEXP timeout_s, SEXP path) {
  const char *out = CHAR(STRING_ELT(path, 0));
  uint32_t timeout_us = (uint32_t) (Rf_asReal(timeout_s) * 1e6);

  struct sigaction sa;
  sigaction(SIGINT, NULL, &sa);
  int sa_restart = (sa.sa_flags & SA_RESTART) != 0;
  int handler_installed = sa.sa_handler != SIG_DFL && sa.sa_handler != SIG_IGN;

  spike_word = 0;
  struct timespec t0, t1;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int ret = __ulock_wait(UL_COMPARE_AND_WAIT_SHARED | ULF_NO_ERRNO,
                         &spike_word, 0, timeout_us);
  clock_gettime(CLOCK_MONOTONIC, &t1);
  double elapsed = (double) (t1.tv_sec - t0.tv_sec) +
                   (double) (t1.tv_nsec - t0.tv_nsec) / 1e9;

  FILE *f = fopen(out, "w");
  if (f != NULL) {
    fprintf(f, "ret=%d elapsed=%.3f sa_restart=%d handler_installed=%d\n",
            ret, elapsed, sa_restart, handler_installed);
    fclose(f);
  }
  return R_NilValue;
}

#else

SEXP spike_ulock(SEXP timeout_s, SEXP path) {
  Rf_error("this spike is macOS-only");
}

#endif
