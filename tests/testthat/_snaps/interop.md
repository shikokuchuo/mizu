# the attr decline names the offending attributes

    Code
      mizu_send(p$host, ts(1:12, frequency = 4, start = c(2020, 1)))
    Condition
      Error:
      ! mizu: value is not portable to the peer (an attribute set (tsp) outside the portable whitelist at x)

---

    Code
      mizu_send(p$host, structure(1, class = "Date", tzone = "UTC"))
    Condition
      Error:
      ! mizu: value is not portable to the peer (an attribute set outside the portable whitelist at x)

# the decline diagnoses a frame's failing column

    Code
      mizu_send(p$host, structure(list(y = ts(1:4, frequency = 4, start = c(2020, 1)),
      x = 1:4), class = "data.frame", row.names = c(NA_integer_, -4L)))
    Condition
      Error:
      ! mizu: value is not portable to the peer (an attribute set (tsp) outside the portable whitelist at x$y)

---

    Code
      mizu_send(p$host, structure(list(a = 1:2, t = as.POSIXlt(.POSIXct(c(0, 86400),
      tz = "UTC"))), class = "data.frame", row.names = c(NA_integer_, -2L)))
    Condition
      Error:
      ! mizu: value is not portable to the peer (a POSIXlt column has no portable home at x$t)

---

    Code
      mizu_send(p$host, structure(list(a = 1:2, l = list(1, 2)), class = "data.frame",
      row.names = c(NA_integer_, -2L)))
    Condition
      Error:
      ! mizu: value is not portable to the peer (a list column has no portable home at x$l)

---

    Code
      mizu_send(p$host, structure(list(a = 1:2, s = methods::new("ixS4col", 3:4)),
      class = "data.frame", row.names = c(NA_integer_, -2L)))
    Condition
      Error:
      ! mizu: value is not portable to the peer (an S4 object has no portable home at x$s)

---

    Code
      mizu_send(p$host, structure(list(v = 1:4, m = matrix(letters[1:4], 2)), class = "data.frame",
      row.names = c(NA_integer_, -4L)))
    Condition
      Error:
      ! mizu: value is not portable to the peer (a character matrix has no portable home at x$m)

---

    Code
      mizu_send(p$host, bad)
    Condition
      Error:
      ! mizu: value is not portable to the peer (a named atomic vector has no portable home at x$v)

# the decline diagnoses the frame's own shape and keeps prior text

    Code
      mizu_send(p$host, matrix(letters[1:4], 2))
    Condition
      Error:
      ! mizu: value is not portable to the peer (a character matrix has no portable home at x)

---

    Code
      mizu_send(p$host, structure(data.frame(x = 1:2), class = c("data.frame", "foo")))
    Condition
      Error:
      ! mizu: value is not portable to the peer (a data frame's class vector must end in "data.frame" at x)

---

    Code
      mizu_send(p$host, dfd)
    Condition
      Error:
      ! mizu: value is not portable to the peer (frame names must be unique, non-NA and UTF-8-writable at x)

---

    Code
      mizu_send(p$host, df)
    Condition
      Error:
      ! mizu: value is not portable to the peer (row.names must be an integer or character vector of the frame's length with no NA at x)

---

    Code
      mizu_send(p$host, c(a = "x"))
    Condition
      Error:
      ! mizu: value is not portable to the peer (a named atomic vector has no portable home at x)

---

    Code
      mizu_send(p$host, ordered(c("a", "b")))
    Condition
      Error:
      ! mizu: value is not portable to the peer (the peer predates the ordered factor capability at x)

---

    Code
      mizu_send(p$host, gd)
    Condition
      Error:
      ! mizu: value is not portable to the peer (an attribute set (formula) outside the portable whitelist at x)

---

    Code
      mizu_send(p$host, methods::new("ixS4top", 1:3))
    Condition
      Error:
      ! mizu: value is not portable to the peer (an S4 object has no portable home at x)

---

    Code
      mizu_send(p$host, lt)
    Condition
      Error:
      ! mizu: value is not portable to the peer (a POSIXlt has no portable home at x)

# corrupt and unknown streams raise informatively

    Code
      ix_read("49017f")
    Condition
      Error in `ix_read()`:
      ! mizu: unsupported interop tag 0x7F — the peer uses a newer format

---

    Code
      ix_read("490200")
    Condition
      Error in `ix_read()`:
      ! mizu: interop format version 0x02 — the peer uses a newer format

---

    Code
      ix_read("4901020102030405")
    Condition
      Error in `ix_read()`:
      ! mizu: truncated interop stream

---

    Code
      ix_read("49010f0f")
    Condition
      Error in `ix_read()`:
      ! mizu: malformed interop stream: an attr wraps an attr

---

    Code
      ix_read(ix_corpus()[["err-frame-shape"]])
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "names", "class", "row.names"; class: "data.frame")

---

    Code
      .Call(mizu:::mizu_interop_read_call, s)
    Condition
      Error:
      ! mizu: no portable home for an attributed value (attributes: "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk

# an attribute set outside the whitelist and a mismatched frame decline

    Code
      ix_read(bad)
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "foo", "class"; class: "bar")

# corrupt dim and row.names shapes decline at the writer

    Code
      ix_write(structure(integer(0), dim = c(2L, 0L)))
    Condition
      Error in `sprintf()`:
      ! mizu: not portable at x (an attribute set outside the portable whitelist)

# the runner stream writes and rejects as a plain task

    Code
      .Call(mizu:::mizu_interop_runner_call, "nm", 1, 5, 2L, 1)
    Condition
      Error:
      ! mizu: expected NULL or a numeric (seed, offset) pair

---

    Code
      .Call(mizu:::mizu_interop_runner_call, "nm", 1, c(1, NA), 2L, 1)
    Condition
      Error:
      ! mizu: expected integral (seed, offset) elements within int64 range

---

    Code
      .Call(mizu:::mizu_interop_runner_call, "nm", 1, c(1.5, 0), 2L, 1)
    Condition
      Error:
      ! mizu: expected integral (seed, offset) elements within int64 range

---

    Code
      .Call(mizu:::mizu_interop_runner_call, "nm", 1, c(2^63, 0), 2L, 1)
    Condition
      Error:
      ! mizu: expected integral (seed, offset) elements within int64 range

---

    Code
      .Call(mizu:::mizu_interop_runner_call, 42, 1, NULL, 2L, 1)
    Condition
      Error:
      ! mizu: expected a length-1 non-NA region name

---

    Code
      .Call(mizu:::mizu_interop_runner_call, "nm", 1:2, NULL, 2L, 1)
    Condition
      Error:
      ! mizu: expected a length-1 numeric generation field

---

    Code
      .Call(mizu:::mizu_interop_runner_call, "nm", NA_real_, NULL, 2L, 1)
    Condition
      Error:
      ! mizu: expected a non-negative integer generation field below 2^63

# a malformed foreign factor declines

    Code
      ix_read(ix_factor(1:2, c("a", NA)))
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "levels", "class"; class: "factor")

---

    Code
      ix_read(ix_factor(1:2, c("a", "a")))
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "levels", "class"; class: "factor")

---

    Code
      ix_read(ix_factor(c(1L, 5L), c("a", "b")))
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "levels", "class"; class: "factor")

