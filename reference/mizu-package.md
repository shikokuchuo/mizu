# mizu: Lock-Free Shared-Memory Parallelism for R

Parallel computation and data exchange between R processes on the same
machine. Lock-free channels and work-stealing task pools over 'POSIX'
shared memory (Linux, macOS) or 'Win32' file mappings (Windows), with
the hot path entirely in user space. Inter-process communication cheap
enough that work can be divided at granularities usually reserved for
threads.

## Linux memory allocator

This section applies only to Linux with glibc.

When mizu starts a channel peer or a pool worker, that process changes
two settings of the C memory allocator. It raises the mmap threshold to
32 MB and the trim threshold to 128 MB. This keeps large payloads in
fast memory. Without this change, glibc asks the kernel to map and unmap
each large payload, and that work is slow.

Your own R process does not change. If you want the same settings there,
set 'GLIBC_TUNABLES' before R starts:


    export GLIBC_TUNABLES=glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=134217728

If you have set 'GLIBC_TUNABLES', mizu respects your values.

## See also

Useful links:

- <https://github.com/shikokuchuo/mizu>

- Report bugs at <https://github.com/shikokuchuo/mizu/issues>

## Author

**Maintainer**: Charlie Gao <charlie.gao@posit.co>
([ORCID](https://orcid.org/0000-0002-0750-061X))

Authors:

- Charlie Gao <charlie.gao@posit.co>
  ([ORCID](https://orcid.org/0000-0002-0750-061X))

Other contributors:

- Posit Software, PBC ([ROR](https://ror.org/03wc8by49)) \[copyright
  holder, funder\]

- Pierre L'Ecuyer (RngStreams library) \[copyright holder\]
