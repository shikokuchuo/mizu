# Remote Errors

A channel receive returns a remote error stream as a value, not a raised
condition: an error in the peer is data until user code decides
otherwise. The value is a `mizu_error_remote` condition (see
[mizu_error](https://shikokuchuo.github.io/mizu/reference/mizu_error.md))
— an ordinary condition object, so it prints, and
[`conditionMessage()`](https://rdrr.io/r/base/conditions.html) leads
with `remote_type: message`.

## Usage

``` r
mizu_is_remote_error(x)

mizu_raise(x)
```

## Arguments

- x:

  for `mizu_is_remote_error()`, any R object; for `mizu_raise()`, a
  `mizu_error_remote` condition as returned by
  [`mizu_recv()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)
  or
  [`mizu_recv_batch()`](https://shikokuchuo.github.io/mizu/reference/mizu_send_batch.md).

## Value

`mizu_is_remote_error()` returns `TRUE` or `FALSE`. `mizu_raise()` does
not return.

## Details

`mizu_is_remote_error()` tests the class. `mizu_raise()` signals the
condition with [`stop()`](https://rdrr.io/r/base/stop.html), so handlers
dispatch on its classes (`mizu_error_remote`, then `mizu_error`,
`error`, `condition`).

## Examples

``` r
ch <- mizu_channel(quote(stop("boom")))
x <- mizu_recv(ch, timeout = 5)
mizu_is_remote_error(x)
#> [1] TRUE
conditionMessage(x)
#> [1] "simpleError: boom"
```
