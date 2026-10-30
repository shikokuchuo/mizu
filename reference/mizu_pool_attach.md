# Attach to a Pool as a Submitter

Joins an existing pool from another process, claiming a free submitter
slot with its own injection ring and result slots. You pass the join
token of the pool to the joining process yourself: it is
`mizu_pool_status(p)$token` on the creator.

## Usage

``` r
mizu_pool_attach(token)
```

## Arguments

- token:

  the join token of the pool. The full region name
  (`mizu_pool_status(p)$name`) is also accepted.

## Value

A pool handle (class `"mizu_pool"`) holding a submitter slot.

## Examples

``` r
p <- mizu_pool()
token <- mizu_pool_status(p)[["token"]]
# pass `token` to the joining process yourself. There, run:
# pa <- mizu_pool_attach(token)
# mizu_collect(mizu_submit(pa, runif(3)))
mizu_pool_stop(p)
```
