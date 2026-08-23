from __future__ import annotations

import argparse
import json
from pathlib import Path

from resilience.scenarios import (
    circuit_breaker_scenario,
    compare_reports,
    retry_dlq_scenario,
)


def _render(payload: dict, output: str | None) -> None:
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


def command_dlq(args: argparse.Namespace) -> int:
    report = retry_dlq_scenario(
        messages=args.messages,
        max_attempts=args.max_attempts,
        poison_every=args.poison_every,
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
        retry_dlq_scenario(
            messages=args.messages,
            max_attempts=args.max_attempts,
            poison_every=args.poison_every,
        ),
    ]
    _render(compare_reports(reports), args.output)
    return 0


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
    circuit.add_argument("--output")
    circuit.set_defaults(handler=command_circuit)

    dlq = subparsers.add_parser("retry-dlq")
    dlq.add_argument("--messages", type=int, default=12)
    dlq.add_argument("--max-attempts", type=int, default=3)
    dlq.add_argument("--poison-every", type=int, default=4)
    dlq.add_argument("--output")
    dlq.set_defaults(handler=command_dlq)

    suite = subparsers.add_parser("suite")
    suite.add_argument("--requests", type=int, default=20)
    suite.add_argument("--failure-probability", type=float, default=0.65)
    suite.add_argument("--failure-threshold", type=int, default=3)
    suite.add_argument("--seed", type=int, default=42)
    suite.add_argument("--messages", type=int, default=12)
    suite.add_argument("--max-attempts", type=int, default=3)
    suite.add_argument("--poison-every", type=int, default=4)
    suite.add_argument("--output")
    suite.set_defaults(handler=command_suite)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
