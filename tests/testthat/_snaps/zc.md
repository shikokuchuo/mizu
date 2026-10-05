# a REF slot with len past inline_max is rejected before reading

    Code
      mizu_recv(p[["host"]], 5)
    Condition
      Error in `mizu_recv()`:
      ! mizu: corrupt payload slot

# remote-leaf declines are corrupt-or-newer shaped

    Code
      fx[["yb"]][[1L]]
    Condition
      Error:
      ! mizu: invalid MIZL remote leaf — referenced region not found: '/mizu_dead_beef'

---

    Code
      fx[["yb"]][[1L]]
    Condition
      Error:
      ! mizu: invalid MIZL remote leaf — claims do not match the referenced region

---

    Code
      fx[["yb"]][[1L]]
    Condition
      Error:
      ! mizu: invalid MIZL remote leaf — claims do not match the referenced region

---

    Code
      fx[["yb"]][[1L]]
    Condition
      Error:
      ! mizu: invalid MIZL remote leaf — claims do not match the referenced region

---

    Code
      fx[["yb"]][[1L]]
    Condition
      Error:
      ! mizu: invalid MIZL remote leaf — corrupt or newer region

---

    Code
      fx[["yb"]][[1L]]
    Condition
      Error:
      ! mizu: invalid MIZL remote leaf — corrupt or newer region

---

    Code
      fx[["yb"]][[1L]]
    Condition
      Error:
      ! mizu: invalid MIZL remote leaf — corrupt or newer region

