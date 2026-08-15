# The load-time platform gate: the 64-bit check, then the pidfd probe
# (Linux). .onUnload is exercised only by an actual namespace unload.

test_that(".onLoad's platform probe passes on this platform", {
  expect_null(sora:::.onLoad(NULL, "sora"))
})
