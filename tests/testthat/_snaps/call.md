# a ref candidate to a pool without TASKREF declines locally

    Code
      mizu_submit_call(p$ctrl, mizu_call("base::mean", big))
    Condition
      Error:
      ! mizu: value is not portable to the peer (the pool's workers cannot read by-reference task arguments at args)

