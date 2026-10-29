# Attach to a Pool as a Submitter

Joins an existing pool from another process, claiming a free submitter
slot with its own injection ring and result slots. You pass the name of
the pool to the joining process yourself: it is
`mizu_pool_status(p)$name` on the creator.

## Usage

``` r
mizu_pool_attach(name)
```

## Arguments

- name:

  the region name of the pool, or its suffix (the part after the
  platform prefix).

## Value

A pool handle (class `"mizu_pool"`) holding a submitter slot.

## Examples

``` r
p <- mizu_pool()
name <- mizu_pool_status(p)[["name"]]
# pass `name` to the joining process yourself. There, run:
# pa <- mizu_pool_attach(name)
# mizu_collect(mizu_submit(pa, runif(3)))
mizu_pool_stop(p)
```
