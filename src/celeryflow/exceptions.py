"""Exception hierarchy used by celeryflow."""
from __future__ import annotations


class CeleryFlowError(Exception):
    """Base exception for all celeryflow errors."""


class ConditionFailed(CeleryFlowError):
    """Raised when a step's condition doesn't match the payload.

    :meth:`celeryflow.tasks.EventTask.apply_async` raises this while it
    dispatches a gated step, before the step is queued, so that step and the
    steps after it don't run.

    ``args`` holds the constructor arguments ``(field, value, statements)``.
    Celery rebuilds a stored exception by calling ``cls(*args)``, so keeping
    them there lets ``result.get()`` raise ``ConditionFailed`` itself instead
    of falling back to a parent class.
    """

    def __init__(self, field: str, value: object, statements: dict[str, object]) -> None:
        super().__init__(field, value, statements)
        self.field = field
        self.value = value
        self.statements = statements

    def __str__(self) -> str:
        return (
            f"Condition check failed on field {self.field!r}: value={self.value!r} "
            f"did not satisfy {self.statements!r}"
        )


class FlowDefinitionError(CeleryFlowError):
    """Raised when a flow definition is malformed."""
