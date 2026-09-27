# Bounded request hedging

Tail latency can be dominated by a single slow connection, overloaded replica or
stalled upstream path. `resilience.primitives.hedging.hedged_call` starts one
candidate immediately and, only while it remains pending, admits later candidates
at a fixed interval. The first successful result wins and slower work is cancelled.

The primitive is intentionally fail closed:

- the caller must mark the logical operation as replay-safe;
- a caller-supplied request token is hashed rather than emitted;
- a maximum of eight candidates can be configured;
- one monotonic deadline covers every attempt;
- immediate failures accelerate the next candidate without resetting the deadline;
- exception classes and messages are excluded from evidence;
- caller cancellation propagates to every live attempt;
- cancellation is observed for a bounded grace period and reported if unconfirmed.

```python
from resilience.primitives.hedging import HedgePolicy, hedged_call

result = await hedged_call(
    [request_primary, request_secondary],
    request_token="logical-request-id",
    replay_safe=True,
    policy=HedgePolicy(
        hedge_delay_seconds=0.050,
        timeout_seconds=0.500,
        max_attempts=2,
    ),
)
```

Each operation receives its zero-based attempt index. Operations must be
cancellation-safe async adapters: close sockets, release response bodies and return
connection-pool slots in `finally` blocks.

## Operational boundary

Hedging trades additional load for lower tail latency. It must be enabled only for
read-only calls or writes protected by a downstream idempotency key shared across
all attempts. `replay_safe=True` is an explicit caller assertion; this module cannot
prove application semantics or downstream deduplication.

The delay should be calibrated from per-dependency latency distributions and should
normally be near a high percentile rather than a fixed fleet-wide constant. The
local candidate limit does not enforce a fleet-wide amplification budget. Compose it
with admission control, a retry budget and a circuit breaker; do not stack hedging
and ordinary retries without accounting for every network attempt.

Python cancellation is cooperative. Evidence reports
`cancellation_unconfirmed` when an adapter suppresses cancellation beyond the grace
period, but it cannot forcibly terminate arbitrary application code. The next step
is to bind this primitive to an HTTP adapter that propagates the shared request token
as an idempotency key and exports low-cardinality attempt/cancellation metrics.
