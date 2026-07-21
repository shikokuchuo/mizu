# macOS `__ulock_wait` EINTR spike (Phase 0 gate)

Verifies the assumption behind the lazy interrupt bound in ipc-plan.md's
*Hybrid wait* (Part I risk 2): that Ctrl-C interrupts a **timed**
`__ulock_wait(UL_COMPARE_AND_WAIT_SHARED)` with `EINTR` under R's real
terminal SIGINT handler, rather than being silently restarted under
`SA_RESTART`. Run via `sh spike.sh` — each case runs inside an
interactive R session on a pty (`script(1)`), SIGINT delivered
externally ~1.5 s into the wait; the C probe records the raw return and
elapsed time to a file so the result survives any R-level interrupt
longjmp.

## Environment

Apple M4 Pro, macOS 26.5.2, Apple clang 21.0.0, R 4.6.1 (2026-07-21).

## Results

| Case | Wait | Signal | ret | elapsed | Verdict |
|---|---|---|---|---|---|
| timed-sigint | 10 s timeout | SIGINT @ ~1.5 s | −4 (−EINTR) | 1.64 s | **PASS** — wait returns immediately on Ctrl-C |
| timed-quiet | 2 s timeout | none | −60 (−ETIMEDOUT) | 2.00 s | timeout semantics confirmed |
| untimed-sigint | 0 (forever) | SIGINT @ ~1.5 s | −4 (−EINTR) | 1.58 s | bonus: untimed also EINTRs on macOS |

Both signal cases ran with `sa_restart=1 handler_installed=1`: R's
terminal SIGINT handler **is** installed with `SA_RESTART`, confirming
the hazard analysis — restart behavior is decided by the kernel per
syscall, and XNU does not restart `__ulock_wait` (timed or untimed)
into a handler.

## Conclusions

- The lazy POSIX interrupt bound stands on macOS: terminal Ctrl-C wakes
  a parked waiter via `EINTR` immediately, so the bound only covers
  front-ends that set R's interrupt flag without delivering a signal
  (e.g. RStudio), and can stretch to seconds.
- macOS would tolerate untimed parks, but the always-timed rule stays:
  Linux restarts an *untimed* `FUTEX_WAIT` under `SA_RESTART` (it
  returns `ERESTARTSYS`; only the timed wait's `ERESTART_RESTARTBLOCK`
  is converted to `EINTR` when a handler runs), and one park discipline
  serves both platforms.
