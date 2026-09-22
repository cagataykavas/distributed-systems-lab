"""Composable distributed-systems resilience primitives and executable scenarios."""

from resilience.deadline import DeadlineExceeded, DeadlineSnapshot, RequestDeadline
from resilience.executor import ExecutionResult, ResilientExecutor
from resilience.reporting import ScenarioEvent, ScenarioReport, compare_reports
from resilience.worker_pool import AsyncWorkerPool, WorkerPoolSnapshot

__version__ = "0.3.0"

__all__ = [
    "AsyncWorkerPool",
    "DeadlineExceeded",
    "DeadlineSnapshot",
    "ExecutionResult",
    "RequestDeadline",
    "ResilientExecutor",
    "ScenarioEvent",
    "ScenarioReport",
    "WorkerPoolSnapshot",
    "compare_reports",
]
