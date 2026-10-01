# Package index

## Channels

- [`mizu_channel()`](https://shikokuchuo.github.io/mizu/reference/mizu_channel.md)
  : Create a Channel and Spawn Its Peer
- [`mizu_send()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)
  [`mizu_recv()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)
  : Send and Receive over a Channel
- [`mizu_send_batch()`](https://shikokuchuo.github.io/mizu/reference/mizu_send_batch.md)
  [`mizu_recv_batch()`](https://shikokuchuo.github.io/mizu/reference/mizu_send_batch.md)
  : Batched Send and Receive
- [`mizu_close()`](https://shikokuchuo.github.io/mizu/reference/mizu_close.md)
  : Close a Channel
- [`mizu_alive()`](https://shikokuchuo.github.io/mizu/reference/mizu_alive.md)
  : Probe Peer Liveness

## Pools

- [`mizu_pool()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool.md)
  : Create a Task Pool and Spawn Its Workers
- [`mizu_pool_attach()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_attach.md)
  : Attach to a Pool as a Submitter
- [`mizu_submit()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
  [`mizu_collect()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
  : Submit a Task and Collect Its Result
- [`mizu_submit_batch()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit_batch.md)
  : Submit a Batch of Tasks
- [`mizu_collect_any()`](https://shikokuchuo.github.io/mizu/reference/mizu_collect_any.md)
  : Collect the First Available Result From Several Tasks
- [`mizu_collect_all()`](https://shikokuchuo.github.io/mizu/reference/mizu_collect_all.md)
  : Collect the Results of Several Tasks, in Order
- [`mizu_cancel()`](https://shikokuchuo.github.io/mizu/reference/mizu_cancel.md)
  : Cancel a Task
- [`mizu_current_pool()`](https://shikokuchuo.github.io/mizu/reference/mizu_current_pool.md)
  : The Evaluating Worker's Own Pool Handle
- [`mizu_spawn_workers()`](https://shikokuchuo.github.io/mizu/reference/mizu_spawn_workers.md)
  [`mizu_retire_worker()`](https://shikokuchuo.github.io/mizu/reference/mizu_spawn_workers.md)
  : Grow or Shrink the Worker Set of a Pool
- [`mizu_pool_stop()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_stop.md)
  : Stop a Pool

## Parallel map

- [`mizu_map()`](https://shikokuchuo.github.io/mizu/reference/mizu_map.md)
  : Parallel Map Over a Pool
- [`mizu_map_prepare()`](https://shikokuchuo.github.io/mizu/reference/mizu_map_prepare.md)
  [`mizu_map_run()`](https://shikokuchuo.github.io/mizu/reference/mizu_map_prepare.md)
  : Prepared Maps: Stage Once, Run Many

## Monitoring and debugging

- [`mizu_pool_status()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_status.md)
  : Inspect a Pool
- [`mizu_pool_stats()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_stats.md)
  : Cumulative Pool Counters
- [`mizu_pool_dump()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_dump.md)
  : Dump the Distributed State of a Pool
- [`mizu_pool_trace()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_trace.md)
  : Trace Task Lifecycle Events

## Cross-language interop

- [`mizu_call()`](https://shikokuchuo.github.io/mizu/reference/mizu_call.md)
  [`mizu_submit_call()`](https://shikokuchuo.github.io/mizu/reference/mizu_call.md)
  : Call a Function in Another Language: the Task Specification
- [`mizu_launcher()`](https://shikokuchuo.github.io/mizu/reference/mizu_launcher.md)
  : Default Child Process Launcher
- [`mizu_py_launcher()`](https://shikokuchuo.github.io/mizu/reference/mizu_py_launcher.md)
  : Python Channel Peer Launcher
- [`mizu_py_pool_launcher()`](https://shikokuchuo.github.io/mizu/reference/mizu_py_pool_launcher.md)
  : Python Pool Worker Launcher

## Conditions and sentinels

- [`mizu_error`](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)
  [`mizu_error_submit_timeout`](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)
  [`mizu_error_slots_exhausted`](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)
  [`mizu_error_stopped`](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)
  [`mizu_error_cancelled`](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)
  [`mizu_error_worker_died`](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)
  [`mizu_error_startup`](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)
  [`mizu_error_shm`](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)
  [`mizu_error_python_payload`](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)
  [`mizu_error_remote`](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)
  : Error Conditions
- [`mizu_is_remote_error()`](https://shikokuchuo.github.io/mizu/reference/mizu_is_remote_error.md)
  [`mizu_raise()`](https://shikokuchuo.github.io/mizu/reference/mizu_is_remote_error.md)
  : Remote Errors
- [`mizu_is_sentinel()`](https://shikokuchuo.github.io/mizu/reference/mizu_is_sentinel.md)
  : Test for a mizu Sentinel

## Utilities

- [`mizu_prune()`](https://shikokuchuo.github.io/mizu/reference/mizu_prune.md)
  : Remove Orphaned Shared Memory Regions
- [`print(`*`<mizu_channel>`*`)`](https://shikokuchuo.github.io/mizu/reference/print.mizu_channel.md)
  [`print(`*`<mizu_pool>`*`)`](https://shikokuchuo.github.io/mizu/reference/print.mizu_channel.md)
  [`print(`*`<mizu_task>`*`)`](https://shikokuchuo.github.io/mizu/reference/print.mizu_channel.md)
  [`print(`*`<mizu_map_prepared>`*`)`](https://shikokuchuo.github.io/mizu/reference/print.mizu_channel.md)
  [`print(`*`<mizu_sentinel>`*`)`](https://shikokuchuo.github.io/mizu/reference/print.mizu_channel.md)
  [`print(`*`<mizu_error_remote>`*`)`](https://shikokuchuo.github.io/mizu/reference/print.mizu_channel.md)
  : Print Methods for mizu Objects
