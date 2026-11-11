"""Backward-compatible imports for the original bulkhead lab module."""

from resilience.primitives.bulkhead import Bulkhead, BulkheadRejected, BulkheadStats

__all__ = ["Bulkhead", "BulkheadRejected", "BulkheadStats"]
