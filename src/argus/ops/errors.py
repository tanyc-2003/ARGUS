"""Error taxonomy (v4 §9). Every failure that lands in the DLQ carries one of these."""

from __future__ import annotations

from enum import StrEnum


class ErrorClass(StrEnum):
    RATE_LIMIT_EXHAUSTED = "rate_limit_exhausted"  # budget spent — resume tomorrow, no error loop
    SOURCE_SCHEMA_DRIFT = "source_schema_drift"  # vendor silently changed shape
    VOTE_CONFLICT = "vote_conflict"  # all sources disagree (used from M3)
    TRANSPORT = "transport"  # network/HTTP failure after retries
    SOURCE_DOWN = "source_down"  # circuit open or credentials missing
    UNKNOWN = "unknown"


class ArgusError(Exception):
    """Base for classified errors."""

    error_class: ErrorClass = ErrorClass.UNKNOWN

    def __init__(self, message: str, *, source: str | None = None) -> None:
        super().__init__(message)
        self.source = source


class BudgetExhausted(ArgusError):
    error_class = ErrorClass.RATE_LIMIT_EXHAUSTED


class SchemaDrift(ArgusError):
    error_class = ErrorClass.SOURCE_SCHEMA_DRIFT


class TransportFailure(ArgusError):
    error_class = ErrorClass.TRANSPORT


class SourceDown(ArgusError):
    error_class = ErrorClass.SOURCE_DOWN
