# Incumbent baseline (Phase 0 gate)

The socket-based stack mov's ring must measurably beat in its target
regime — high-rate small messages between two R processes on the same
machine (ipc-plan.md, Part I risk 4). Produced by
`Rscript tools/baseline/baseline.R`.

## Reference hardware

| | |
|---|---|
| Machine | Apple M4 Pro, 14 cores, 48 GiB |
| OS | macOS 26.5.2 (build 25F84) |
| R | 4.6.1 |
| nanonext | 1.10.1.9000 |
| mirai | 2.7.2.9000 |
| Date | 2026-07-21 |

## Numbers

### nanonext `ipc://` pair round-trip (8 B raw payload, blocking send/recv)

The latency floor for socket-based R IPC: UDS transport, no
serialization (`mode = "raw"`), synchronous round-trip driven from R.

| Metric | Value |
|---|---|
| Round-trips/sec | **31,516** |
| µs per round-trip | **31.73** (reps: 31.73 / 31.99 / 32.07) |

### mirai local dispatch (trivial `mirai(NULL)` tasks, 1 daemon)

| Configuration | Throughput (fire 10k, collect 10k) | Sequential round-trip |
|---|---|---|
| dispatcher = TRUE | 9,980 tasks/sec | 124.0 µs/task (8,065/sec) |
| dispatcher = FALSE | 20,661 tasks/sec | 63.0 µs/task (15,873/sec) |

## Reading

- The incumbent's small-message ceiling as experienced from R is
  ~31.5k round-trips/sec (one-way ~63k msg/s in a strict
  request-reply pattern). The ring's target regime is >100k msg/s
  sustained; the gate for Part I's benchmarks-as-tests is therefore:
  **beat 31.7 µs/round-trip and sustain >100k small messages/sec**
  where the incumbent cannot.
- Per-message R serialization costs ~1.3 µs (measured in the
  *Why no SHM_OBJ kind* investigation), so the incumbent's floor is
  kernel/socket overhead, not serialization — exactly the cost the
  user-space ring removes.
- mirai numbers are the task-model comparison, not the transport
  floor: they include promise/task bookkeeping and are quoted for the
  "should this collapse into a mirai task?" adoption question rather
  than as a ring target.
