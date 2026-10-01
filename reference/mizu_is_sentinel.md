# Test for a mizu Sentinel

Identity comparison against the four interned sentinel singletons —
`mizu_full`, `mizu_timeout`, `mizu_closed`, `mizu_peer_gone` — that the
verbs of mizu return to tag terminal states.
`inherits(x, "mizu_sentinel")` tests the class alone, which any payload
can carry. This includes a genuine sentinel forwarded over a channel,
which arrives as an ordinary copy. `mizu_is_sentinel()` is provenance:
`TRUE` only for the exact objects that the own calls of mizu return in
this process. So code that relays untrusted values can distinguish its
terminal states from look-alike payloads.

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
