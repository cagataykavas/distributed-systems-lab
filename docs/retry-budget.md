# Retry amplification budget

Per-request attempt limits are necessary but insufficient during a broad outage. If
10,000 original requests each make two retries, the unhealthy dependency receives
30,000 attempts. `RetryBudget` adds a dependency-level admission boundary: original
traffic earns bounded credit and every additional network attempt consumes it.

```python
from resilience.primitives.retry_budget import RetryBudget, RetryBudgetPolicy

payments_budget = RetryBudget(
    RetryBudgetPolicy(
        initial_tokens=1,  # small cold-start reserve
        max_tokens=20,  # maximum retry burst
        tokens_per_original=0.10,  # long-run retry amplification target
    )
)

payments_budget.record_original()
result = call_payments()

if result.retryable:
    decision = payments_budget.try_acquire_retry()
    emit_metric(decision.reason.value)
    if decision.admitted:
        result = call_payments()
```

## Operational contract

- Use one budget per downstream dependency so an inventory outage cannot consume
  payment retry capacity.
- Record exactly one original attempt per logical top-level request. Retries never
  earn more credit.
- Acquire credit immediately before each additional network attempt. A rejected
  decision is terminal for that retry path and should be reported as load shedding.
- Keep ordinary per-request attempt, backoff, jitter, deadline, and circuit-breaker
  policies. The budget limits aggregate amplification; it does not replace them.
- Export admitted/rejected counters and available tokens from `snapshot()`. Decision
  evidence is deterministic JSON data and intentionally contains no request content.

The controller is thread-safe and uses constant memory. Decimal accounting avoids
floating-point drift around the admission threshold. Capacity is capped, so a long
healthy interval cannot accumulate an unbounded retry storm.

## Boundaries and calibration

This implementation is process-local. Multiple replicas each own a reserve and cap,
so fleet-wide amplification is approximately the per-instance budget multiplied by
the number of live replicas. A production rollout should either divide policy values
by replica count, coordinate through a low-latency shared limiter, or enforce another
budget at the gateway/mesh layer.

The budget trusts its caller to record each original request once. If redelivery can
repeat that call, deduplicate the request before crediting it. Process restarts also
restore the cold-start reserve; keep it small and monitor restart loops.

`tokens_per_original` is a traffic ratio, not a success-rate estimate. Calibrate it
from dependency capacity, retry success by error class, and incident load tests. The
next integration step is to place the budget directly in `ResilientExecutor` after
deadline and circuit-breaker checks, so stale or intentionally isolated calls do not
consume credit.
