# Attach to a Pool as a Submitter

Joins an existing pool from another process. Claims a free submitter
slot with its own injection ring and result-slot subrange. The name of
the pool travels out of band: it is `mizu_pool_status(p)$name` on the
creator.

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
# `name` travels out of band to the joining process, which runs:
# pa <- mizu_pool_attach(name)
# mizu_collect(mizu_submit(pa, runif(3)))
mizu_pool_stop(p)
```
