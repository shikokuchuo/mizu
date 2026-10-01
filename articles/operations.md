# Operations

Deployment settings and failure semantics for mizu processes.

## Deploying on Linux

On Linux, POSIX shared memory is `/dev/shm`, a bounded tmpfs mount.
Everything mizu creates must fit in it: pool and channel regions, plus
the per-payload regions that carry anything past the in-slot budget.
Container runtimes set it to 64 MB by default. Use
`docker run --shm-size=4g`, or the equivalent in your orchestrator. Size
it to your peak in-flight payload traffic.

## Linux memory allocator

This section applies only to Linux with glibc.

When mizu starts a channel peer or a pool worker, that process changes
two settings of the C memory allocator. It raises the mmap threshold to
32 MB and the trim threshold to 128 MB. This keeps large payloads in
fast memory. Without this change, glibc asks the kernel to map and unmap
each large payload, and that work is slow.

Your own R process does not change. If you want the same settings there,
set them before R starts:

``` sh
export GLIBC_TUNABLES=glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=134217728
```

If you have set `GLIBC_TUNABLES`, mizu respects your values.

## When a process dies

The death of any participant is detected at OS notification latency,
with no heartbeats and no polling. Every process holds a lock that the
kernel releases the instant the process exits, for any reason. That
release is the verdict. A dead worker fails exactly the tasks that it
claimed, and collecting one of them raises an error. The surviving
workers consume the work still queued to the dead worker. On a channel,
the survivor sees `mizu_peer_gone`. This split is deliberate and holds
across the whole surface. Transport states — not yet, not now, stream
over — return as sentinel values for the receiving loop to handle. A
request that can never be satisfied raises a classed condition instead
(see
[`?mizu_error`](https://shikokuchuo.net/mizu/reference/mizu_error.md)).
Examples: the error of the task itself, a cancelled task, a dead worker,
a child that failed to start.

A crashed process cannot clean up after itself.
[`mizu_prune()`](https://shikokuchuo.net/mizu/reference/mizu_prune.md)
removes the shared-memory regions that dead processes leave behind.
Regions of running processes are never touched.
