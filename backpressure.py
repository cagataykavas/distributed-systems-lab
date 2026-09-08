"""Backward-compatible imports for the original backpressure lab module."""

from resilience.primitives.backpressure import BackpressureQueue, QueueStats

__all__ = ["BackpressureQueue", "QueueStats"]
