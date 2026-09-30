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

# an attribute set outside the whitelist and a mismatched frame decline

    Code
      ix_read(bad)
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "foo", "class"; class: "bar")

