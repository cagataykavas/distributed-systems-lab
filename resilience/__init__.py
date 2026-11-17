"""Composable distributed-systems resilience primitives and executable scenarios."""

from resilience.executor import ExecutionResult, ResilientExecutor
from resilience.network_faults import (
    FaultSnapshot,
    FaultWindow,
    NetworkFaultInjector,
    NetworkFaultKind,
)
from resilience.reporting import ScenarioEvent, ScenarioReport, compare_reports
from resilience.worker_pool import AsyncWorkerPool, WorkerPoolSnapshot

__version__ = "0.4.0"

__all__ = [
    "AsyncWorkerPool",
    "ExecutionResult",
    "FaultSnapshot",
    "FaultWindow",
    "NetworkFaultInjector",
    "NetworkFaultKind",
    "ResilientExecutor",
    "ScenarioEvent",
    "ScenarioReport",
    "WorkerPoolSnapshot",
    "compare_reports",
]
