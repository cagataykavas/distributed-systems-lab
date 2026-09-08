"""Backward-compatible imports for the original retry/DLQ lab module."""

from resilience.primitives.dead_letter import Message, QueueOutcome, RetryQueue

__all__ = ["Message", "QueueOutcome", "RetryQueue"]
