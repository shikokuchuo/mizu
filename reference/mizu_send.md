# Send and Receive over a Channel

`mizu_send()` publishes a message to the peer. The message is visible
the moment the call returns, with no separate flush step. `mizu_recv()`
returns the next message, and waits up to `timeout` seconds for it.

## Usage

``` r
mizu_send(ch, x)

mizu_recv(ch, timeout = Inf)
```

## Arguments

- ch:

  a channel handle from
  [`mizu_channel()`](https://shikokuchuo.net/mizu/reference/mizu_channel.md)
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

Sends never block for ring space. Receives surface every terminal state
as a class-tagged sentinel, not an error. Dispatch with
`inherits(x, "mizu_sentinel")`, or on the specific classes:

- `mizu_full` — the ring is full (send). Back off until the peer drains,
  or drop the message.

- `mizu_timeout` — no message arrived within `timeout` (recv).

- `mizu_closed` — the other side closed the channel. A receive drains
  all published messages before it reports this.

- `mizu_peer_gone` — the peer died without closing. The verdict comes
  from the kernel-released liveness lock, at OS death-notification
  latency. A receive drains first here too: the published messages of a
  dead peer are complete and valid. Sticky once returned.

`NULL` is a legal payload. Sentinels are ordinary values, identifiable
by class alone, and never signalled conditions.
[`mizu_is_sentinel()`](https://shikokuchuo.net/mizu/reference/mizu_is_sentinel.md)
checks identity where payloads are untrusted. `NULL` crosses as an
immediate: no serialization and no receive-side allocation. Length-1
character vectors that fit the inline budget cross with a single byte
copy, encoding mark preserved. Attribute-free non-ALTREP atomic vectors
— and `integer64` vectors whose only attribute is the class — ride a
serialization-free fast path with a byte-identical round-trip — inline
within the budget, and past it as bare bytes in the arena or a spill
region (no serialize, no parse). Other plain values — attributed
vectors, strings, lists, calls — cross as a compact binary stream
written and read without R's serializer. Anything else is R-serialized.
Mori-shared objects reduce to identifier wire forms through the mori
hooks.

Cross-language, a numpy `int64` array (or an Arrow one) from a Python
peer lands as an `integer64` vector — bit64's exact layout, constructed
without bit64: with bit64 loaded the result is fully functional, and
without it the bits round-trip but print as raw doubles. The missing
sentinel is shared both directions: a genuine `INT64_MIN` reads as
`NA_integer64_` in R, and an `NA_integer64_` arrives as `INT64_MIN` in
Python.

From a non-R peer, only vectors and strings are legal payloads. Anything
else (a pymizu codec stream or a pickle) is declined: the receive raises
a classed `mizu_error_python_payload` error (see
[mizu_error](https://shikokuchuo.net/mizu/reference/mizu_error.md)). The
declined message is consumed, so the channel keeps flowing.

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
