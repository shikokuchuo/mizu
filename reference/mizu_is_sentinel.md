# Test for a mizu Sentinel

Tests identity against the four sentinel values (`mizu_full`,
`mizu_timeout`, `mizu_closed`, `mizu_peer_gone`) that mizu's functions
return to signal terminal states. `inherits(x, "mizu_sentinel")` tests
the class alone, which any payload can carry, and even a genuine
sentinel forwarded over a channel arrives as an ordinary copy.
`mizu_is_sentinel()` returns `TRUE` only for the sentinel objects that
mizu's own calls returned in this process, so code that relays untrusted
values can tell its terminal states from look-alike payloads.

## Usage

``` r
mizu_is_sentinel(x)
```

## Arguments

- x:

  any R object.

## Value

`TRUE` or `FALSE`.

## Details

Sentinels are ordinary values, not R conditions: nothing is signalled,
and condition handlers never see them.

## Examples

``` r
mizu_is_sentinel(42)
#> [1] FALSE
# class alone does not make a sentinel:
mizu_is_sentinel(structure("x", class = c("mizu_timeout", "mizu_sentinel")))
#> [1] FALSE
```
