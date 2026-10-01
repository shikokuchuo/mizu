# Remove Orphaned Shared Memory Regions

Removes the mizu shared memory regions that dead processes leave behind.
Needed only in exceptional circumstances. Cleanup after a crash is
automatic as long as any participant of the pool or channel survives it.
The survivor detects the death through the liveness lock and reclaims
the regions of the dead process itself. Orphans arise only when every
attached process dies at once (for example, the whole process group is
killed), and no survivor remains to run that cleanup. They persist until
pruned or until the machine reboots.

## Usage

``` r
mizu_prune()
```

## Value

Invisibly, a character vector of the region names removed, or `NULL` if
none were. On platforms whose shared memory namespace cannot be
enumerated (Windows, where orphans cannot exist), always `NULL`.

## Details

Regions of running processes are never touched, and mori regions are
invisible to mizu (and vice versa): the two packages keep disjoint
namespaces. If another mizu session on the machine is itself recovering
from a crash, do not run this. Payloads sent by a now-dead peer stay
deliverable to its survivor until drained, and this function reaps them.

A crashed process cannot clean up after itself, and a new process that
happens to reuse its PID cannot reap its orphans either (it reads its
own PID as alive). Run `mizu_prune()` while the PID is free, before
reuse.

## Examples

``` r
mizu_prune()
```
