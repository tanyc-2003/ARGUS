"""Error taxonomy (v4 §9). Every failure that lands in the DLQ carries one of these."""

from __future__ import annotations

from enum import StrEnum


class ErrorClass(StrEnum):
    RATE_LIMIT_EXHAUSTED = "rate_limit_exhausted"  # budget spent — resume tomorrow, no error loop
    SOURCE_SCHEMA_DRIFT = "source_schema_drift"  # vendor silently changed shape
    SOURCE_OVERSIZED = "source_oversized"  # payload legitimately exceeds our page cap
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


class PayloadTooLarge(ArgusError):
    """A (request_key) whose paginated payload exceeds the source's page cap.

    Deliberately NOT SchemaDrift: the vendor's shape is fine, the response is
    simply bigger than we are willing to page. Conflating the two both hides
    real drift and makes a volume problem look like a vendor problem.
    """

    error_class = ErrorClass.SOURCE_OVERSIZED


class TransportFailure(ArgusError):
    error_class = ErrorClass.TRANSPORT


class SourceDown(ArgusError):
    error_class = ErrorClass.SOURCE_DOWN
