# Send and Receive over a Channel

`mizu_send()` publishes a message to the peer. The message is visible as
soon as the call returns, with no separate flush step. `mizu_recv()`
returns the next message, and waits up to `timeout` seconds for it.

## Usage

``` r
mizu_send(ch, x)

mizu_recv(ch, timeout = Inf)
```

## Arguments

- ch:

  a channel handle from
  [`mizu_channel()`](https://shikokuchuo.net/mizu/dev/reference/mizu_channel.md)
  (or the `ch` binding inside a peer expression).

- x:

  the payload: any R object.

- timeout:

  seconds to wait for a message before returning the `mizu_timeout`
  sentinel. `Inf` (the default) waits indefinitely; `0` does not wait.

## Value

`mizu_send()` returns `TRUE` (invisibly) on success, or a sentinel
otherwise. `mizu_recv()` returns the received payload or a sentinel.

## Details

Sends never block for ring space. Receives report every terminal state
as a class-tagged sentinel, not an error. Test for them with
`inherits(x, "mizu_sentinel")`, or with the specific classes:

- `mizu_full` — the ring is full (send). Back off until the peer drains,
  or drop the message.

- `mizu_timeout` — no message arrived within `timeout` (recv).

- `mizu_closed` — the other side closed the channel. A receive drains
  all published messages before it reports this.

- `mizu_peer_gone` — the peer died without closing. Death is detected
  through the liveness lock that the kernel releases, as soon as the OS
  reports it. A receive drains first here too, since the published
  messages of a dead peer are complete and valid. Sticky once returned.

`NULL` is a legal payload. Sentinels are ordinary values identified by
their class, never signalled conditions.
[`mizu_is_sentinel()`](https://shikokuchuo.net/mizu/dev/reference/mizu_is_sentinel.md)
checks identity where payloads are untrusted.

`NULL` crosses as an immediate, with no serialization and no allocation
on receive. A length-1 character vector that fits the inline budget
crosses with a single byte copy, encoding mark preserved. Attribute-free
non-ALTREP atomic vectors, and `integer64` vectors whose only attribute
is the class, take a serialization-free path with a byte-identical round
trip: inline within the budget, and past it as bare bytes in the arena
or a spill region. Other plain values (attributed vectors, strings,
lists, calls) cross as a compact binary stream written and read without
R's serializer. Anything else is R-serialized. Mori-shared objects
reduce to their identifiers through the mori hooks.

A NumPy or Arrow `int64` array from a Python peer arrives as an
`integer64` vector in bit64's layout, constructed without bit64. With
bit64 loaded the result is fully functional, and without it the bits
round-trip but print as raw doubles. The missing-value sentinel is
shared in both directions: a genuine `INT64_MIN` reads as
`NA_integer64_` in R, and an `NA_integer64_` arrives as `INT64_MIN` in
Python.

From a non-R peer, only vectors and strings are legal payloads. Anything
else (a pymizu codec stream or a pickle) is declined, and the receive
raises a classed `mizu_error_python_payload` error (see
[mizu_error](https://shikokuchuo.net/mizu/dev/reference/mizu_error.md)).
The declined message is consumed, so the channel keeps flowing.

Large payloads can arrive as zero-copy views over shared memory. A view
is a small R object that pins memory the garbage collector cannot see.
To return that memory promptly, the receive functions (and worker task
boundaries) run a minor garbage collection once 8 MiB of released view
memory has accumulated. Re-receiving the same region does not re-trigger
it. This is an occasional pause of about 0.2 ms, the largest latency
outlier in the system. Set `options(mizu.gc_boundary)` (in bytes; `0`
disables it) before loading the package to tune the threshold.

## Examples

``` r
ch <- mizu_channel(quote(mizu_send(ch, mizu_recv(ch))))
mizu_send(ch, list(1, "a"))
mizu_recv(ch, timeout = 5)
#> [[1]]
#> [1] 1
#> 
#> [[2]]
#> [1] "a"
#> 
mizu_close(ch)
```
