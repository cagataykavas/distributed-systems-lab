"""Backward-compatible imports for the original bulkhead lab module."""

from resilience.primitives.bulkhead import Bulkhead, BulkheadStats

__all__ = ["Bulkhead", "BulkheadStats"]
