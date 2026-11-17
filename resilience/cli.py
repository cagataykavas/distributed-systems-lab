from __future__ import annotations

import argparse
import json
from pathlib import Path

from resilience.reporting import compare_reports
from resilience.scenarios import (
    circuit_breaker_scenario,
    network_partition_scenario,
    resilient_executor_scenario,
    retry_dlq_scenario,
    worker_pool_scenario,
)


def _render(payload: dict[str, object], output: str | None) -> None:
    text = json.dumps(payload, indent=2)
    if output:
        Path(output).write_text(text + "\n", encoding="utf-8")
    print(text)


def command_circuit(args: argparse.Namespace) -> int:
    report = circuit_breaker_scenario(
        requests=args.requests,
        failure_probability=args.failure_probability,
        failure_threshold=args.failure_threshold,
        seed=args.seed,
    )
    _render(report.as_dict(), args.output)
    return 0


def command_network_partition(args: argparse.Namespace) -> int:
    report = network_partition_scenario(
        requests=args.partition_requests,
        request_interval_ms=args.request_interval_ms,
        partition_start_ms=args.partition_start_ms,
        partition_duration_ms=args.partition_duration_ms,
        failure_threshold=args.partition_failure_threshold,
        recovery_timeout_ms=args.recovery_timeout_ms,
    )
    _render(report.as_dict(), args.output)
    return 0


def command_dlq(args: argparse.Namespace) -> int:
    report = retry_dlq_scenario(
        messages=args.messages,
        max_attempts=args.max_attempts,
        poison_every=args.poison_every,
    )
    _render(report.as_dict(), args.output)
    return 0


def command_executor(args: argparse.Namespace) -> int:
    _render(resilient_executor_scenario().as_dict(), args.output)
    return 0


def command_worker_pool(args: argparse.Namespace) -> int:
    report = worker_pool_scenario(
        items=args.items,
        queue_size=args.queue_size,
        max_concurrency=args.max_concurrency,
    )
    _render(report.as_dict(), args.output)
    return 0


def command_suite(args: argparse.Namespace) -> int:
    reports = [
        circuit_breaker_scenario(
            requests=args.requests,
            failure_probability=args.failure_probability,
            failure_threshold=args.failure_threshold,
            seed=args.seed,
        ),
        network_partition_scenario(
            requests=args.partition_requests,
            request_interval_ms=args.request_interval_ms,
            partition_start_ms=args.partition_start_ms,
            partition_duration_ms=args.partition_duration_ms,
            failure_threshold=args.partition_failure_threshold,
            recovery_timeout_ms=args.recovery_timeout_ms,
        ),
        retry_dlq_scenario(
            messages=args.messages,
            max_attempts=args.max_attempts,
            poison_every=args.poison_every,
        ),
        resilient_executor_scenario(),
        worker_pool_scenario(
            items=args.items,
            queue_size=args.queue_size,
            max_concurrency=args.max_concurrency,
        ),
    ]
    _render(compare_reports(reports), args.output)
    return 0


def _add_output(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", help="optional JSON output path")


def _add_partition_arguments(
    parser: argparse.ArgumentParser, *, request_flag: str = "--requests"
) -> None:
    parser.add_argument(request_flag, dest="partition_requests", type=int, default=12)
    parser.add_argument("--request-interval-ms", type=int, default=100)
    parser.add_argument("--partition-start-ms", type=int, default=300)
    parser.add_argument("--partition-duration-ms", type=int, default=400)
    parser.add_argument("--partition-failure-threshold", type=int, default=2)
    parser.add_argument("--recovery-timeout-ms", type=int, default=250)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="resilience-lab",
        description="Run deterministic distributed-system failure scenarios.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    circuit = subparsers.add_parser("circuit-breaker")
    circuit.add_argument("--requests", type=int, default=20)
    circuit.add_argument("--failure-probability", type=float, default=0.65)
    circuit.add_argument("--failure-threshold", type=int, default=3)
    circuit.add_argument("--seed", type=int, default=42)
    _add_output(circuit)
    circuit.set_defaults(handler=command_circuit)

    partition = subparsers.add_parser("network-partition")
    _add_partition_arguments(partition)
    _add_output(partition)
    partition.set_defaults(handler=command_network_partition)

    dlq = subparsers.add_parser("retry-dlq")
    dlq.add_argument("--messages", type=int, default=12)
    dlq.add_argument("--max-attempts", type=int, default=3)
    dlq.add_argument("--poison-every", type=int, default=4)
    _add_output(dlq)
    dlq.set_defaults(handler=command_dlq)

    executor = subparsers.add_parser("executor")
    _add_output(executor)
    executor.set_defaults(handler=command_executor)

    worker_pool = subparsers.add_parser("worker-pool")
    worker_pool.add_argument("--items", type=int, default=20)
    worker_pool.add_argument("--queue-size", type=int, default=3)
    worker_pool.add_argument("--max-concurrency", type=int, default=2)
    _add_output(worker_pool)
    worker_pool.set_defaults(handler=command_worker_pool)

    suite = subparsers.add_parser("suite")
    suite.add_argument("--requests", type=int, default=20)
    suite.add_argument("--failure-probability", type=float, default=0.65)
    suite.add_argument("--failure-threshold", type=int, default=3)
    suite.add_argument("--seed", type=int, default=42)
    suite.add_argument("--messages", type=int, default=12)
    suite.add_argument("--max-attempts", type=int, default=3)
    suite.add_argument("--poison-every", type=int, default=4)
    suite.add_argument("--items", type=int, default=20)
    suite.add_argument("--queue-size", type=int, default=3)
    suite.add_argument("--max-concurrency", type=int, default=2)
    _add_partition_arguments(suite, request_flag="--partition-requests")
    _add_output(suite)
    suite.set_defaults(handler=command_suite)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
