# The load-time platform gate (64-bit pointer width, then the pidfd probe
# on Linux) lives in sora_onload_probe; the unload path (R_unload_sora) is
# exercised only by an actual DLL unload.

test_that(".onLoad's platform probe passes on this platform", {
  expect_null(sora:::.onLoad(NULL, "sora"))
})
