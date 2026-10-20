# mizu_channel_create rejects fractional and NA geometry

    Code
      .Call(mizu:::mizu_channel_create, quote(NULL), 4.9, 256L, 0, FALSE)
    Condition
      Error:
      ! mizu: capacity must be a whole number

---

    Code
      .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 64.5, FALSE)
    Condition
      Error:
      ! mizu: arena_size must be a whole number

---

    Code
      .Call(mizu:::mizu_channel_create, quote(NULL), NA_integer_, 256L, 0, FALSE)
    Condition
      Error:
      ! mizu: capacity must be a whole number

# the channel attach entry rejects an NA suffix

    Code
      .Call(mizu:::mizu_channel_attach, NA_character_, NULL)
    Condition
      Error:
      ! mizu: suffix must not be NA

