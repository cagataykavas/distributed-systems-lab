# Adaptive dependency concurrency

Fixed connection/request limits prevent unbounded fan-out but are usually calibrated
for one operating point. A dependency that becomes slow can accumulate work and time
out even while the caller stays below that static limit. This module adds a bounded,
dependency-local AIMD admission controller without creating a hidden waiter queue.

## Control contract

`AdaptiveConcurrencyLimiter.try_acquire()` atomically admits work only while the
current in-flight count is below the limit. Excess work is rejected immediately with
`limit_reached`; callers can translate that into load shedding or a bounded upstream
retry policy. Every admitted request gets an opaque single-use lease.

Completed requests form a fixed-size decision window:

- if the failure fraction exceeds its budget, the limit decreases multiplicatively;
- otherwise, if p95 latency exceeds its budget, the limit decreases multiplicatively;
- otherwise, a sufficiently utilized window increases the limit additively;
- an underutilized window leaves the limit unchanged.

The configured minimum and maximum are hard bounds. Cancellation releases capacity
but is not counted as a dependency failure. `run()` makes success, failure and caller
cancellation bookkeeping cancellation-safe. Snapshots expose only bounded,
low-cardinality counters, the current generation and a canonical policy digest.

## Example

```python
limiter = AdaptiveConcurrencyLimiter(
    ConcurrencyPolicy(
        initial_limit=8,
        minimum_limit=2,
        maximum_limit=64,
        sample_window=50,
        latency_budget_seconds=0.2,
    )
)

response = await limiter.run(lambda: http_client.get(url))
```

Use a separate limiter per dependency or origin. Combine it with a real transport
deadline, circuit breaker and fleet-level retry budget. Treat `AdmissionRejected` as
backpressure; do not move rejected work into an unbounded local queue.

## Limitations

This is an in-process heuristic, not a distributed capacity oracle. A replica-local
limit must be calibrated against replica count, connection-pool size and downstream
quotas. Completion latency includes scheduler and client overhead, not just network or
server time. A short window reacts quickly but is noisy; a large window reacts slowly.
The controller does not distinguish HTTP status classes, enforce request deadlines,
cancel synchronous I/O or coordinate multiple callers. Long-lived streams need a
separate policy because their duration is not a useful request-latency signal.

The next step is an HTTP adapter that classifies retryable transport/server outcomes,
propagates remaining deadline and idempotency evidence, and exports adjustment/rejection
metrics without dependency-name cardinality.
