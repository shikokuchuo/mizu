# a REF slot with len past inline_max is rejected before reading

    Code
      mizu_recv(p[["host"]], 5)
    Condition
      Error in `doTryCatch()`:
      ! mizu: corrupt payload slot

# .onLoad validates and applies the mizu.gc_boundary option

    Code
      mizu:::.onLoad()
    Condition
      Error:
      ! mizu: option 'mizu.gc_boundary' must be a single non-negative number of bytes not exceeding 2^53

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

# remote-leaf header NA-claim rejections are corrupt-or-newer

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

# remote-leaf identifier parse failures are corrupt-or-newer

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

