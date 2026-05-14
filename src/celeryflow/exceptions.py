"""Exception hierarchy used by celeryflow."""
from __future__ import annotations


class CeleryFlowError(Exception):
    """Base exception for all celeryflow errors."""


class ConditionFailed(CeleryFlowError):
    """Raised (typically intercepted) when a task's pre-execution condition fails.

    ``apply_async`` on :class:`celeryflow.tasks.EventTask` raises this when the
    payload does not satisfy the configured ``condition`` mapping. Subclassing
    :class:`CeleryFlowError` lets callers catch a single base type.
    """

    def __init__(self, field: str, value: object, statements: dict[str, object]) -> None:
        self.field = field
        self.value = value
        self.statements = statements
        super().__init__(
            f"Condition check failed on field {field!r}: value={value!r} "
            f"did not satisfy {statements!r}"
        )


class FlowDefinitionError(CeleryFlowError):
    """Raised when a flow definition is malformed."""
