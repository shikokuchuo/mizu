# The load-time platform gate (64-bit pointer width, then the pidfd probe
# on Linux) lives in rei_onload_probe; the unload path (R_unload_rei) is
# exercised only by an actual DLL unload.

test_that(".onLoad's platform probe passes on this platform", {
  expect_null(rei:::.onLoad(NULL, "rei"))
})
