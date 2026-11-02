# channel handles print one line and survive release

    Code
      print(p[["host"]])
    Output
      <mizu_channel mizu_…: host, open>

---

    Code
      print(p[["peer"]])
    Output
      <mizu_channel mizu_…: peer, open>

---

    Code
      print(p[["host"]])
    Output
      <mizu_channel mizu_…: host, closed>

---

    Code
      print(p[["host"]])
    Output
      <mizu_channel: closed>

# pool and task handles print one line and survive destroy

    Code
      print(p[["ctrl"]])
    Output
      <mizu_pool mizu_…: controller, 2/2 workers live, 0 pending>

---

    Code
      print(p[["wk"]])
    Output
      <mizu_pool mizu_…: worker, 2/2 workers live, 0 pending>

---

    Code
      print(t)
    Output
      <mizu_task: pending>

---

    Code
      print(p[["ctrl"]])
    Output
      <mizu_pool mizu_…: controller, 2/2 workers live, 1 pending>

---

    Code
      print(t)
    Output
      <mizu_task: ok>

---

    Code
      print(t)
    Output
      <mizu_task: collected>

---

    Code
      print(t2)
    Output
      <mizu_task: err>

---

    Code
      print(t3)
    Output
      <mizu_task: cancel>

---

    Code
      print(t3)
    Output
      <mizu_task: dropped>

---

    Code
      print(p[["ctrl"]])
    Output
      <mizu_pool: closed>

# call specs print the head and argument counts

    Code
      print(mizu_call("stats::quantile", 1:10, probs = 0.5))
    Output
      <mizu_call: stats::quantile, 1 positional, 1 named>

---

    Code
      print(mizu_call(.source = "x + 1", x = 41L))
    Output
      <mizu_call: source "x + 1", 1 named>

---

    Code
      print(mizu_call(.source = "x + 1\nx + 2"))
    Output
      <mizu_call: source "x + 1">

---

    Code
      print(mizu_call(.source = strrep("x", 100L)))
    Output
      <mizu_call: source "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx...>

---

    Code
      print(mizu_call("base::sqrt"))
    Output
      <mizu_call: base::sqrt>

# sentinels print as their class

    Code
      print(tm)
    Output
      <mizu_timeout>

---

    Code
      print(fl)
    Output
      <mizu_full>

---

    Code
      print(cl)
    Output
      <mizu_closed>

# a finalized in-process peer handle reads closed at print

    Code
      print(host)
    Output
      <mizu_channel mizu_…: host, closed>

# a channel whose peer is gone prints the verdict

    Code
      print(ch)
    Output
      <mizu_channel mizu_…: host, peer gone>

# a prepared map prints its staging

    Code
      print(pm)
    Output
      <mizu_map_prepared: 4 elements, inline blob>

# a region-staged prepared map prints its region name

    Code
      print(pm)
    Output
      <mizu_map_prepared: 100000 elements, mizu_…>

# a remote error prints one line leading with type: message

    Code
      print(e)
    Output
      <mizu_error_remote: ValueError: boom>

# map handles print their submission state

    Code
      print(m)
    Output
      <mizu_map: 4 elements, inline blob, submitted>

---

    Code
      print(ms)
    Output
      <mizu_map: 4 elements, streaming, submitted>

---

    Code
      print(big)
    Output
      <mizu_map: 100000 elements, mizu_…, submitted>

---

    Code
      print(m)
    Output
      <mizu_map: 4 elements, collected>

