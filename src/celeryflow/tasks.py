"""Celery Task base classes used by celeryflow.

Two layers:

* :class:`FlowTask` — primitive base class. Owns the logic for splicing extra
  signatures into ``self.request.chain`` so that an in-flight task can extend
  the pipeline dynamically (used by the flow entry task to wire the body of
  the flow at runtime).
* :class:`EventTask` — adds optional pre-execution condition checking,
  structured logging hooks (success / failure / retry), and JSON-safe
  serialization of payloads.

Neither class assumes anything about the business domain. Pass a logger via
``EventTask.bind_logger`` if you want lifecycle events emitted; otherwise
logging is silent.
"""
from __future__ import annotations

import copy
import json
import logging
from typing import Any

from celery import Task, canvas, chain, group, signature

from celeryflow.conditions import check_conditions
from celeryflow.encoding import CeleryFlowJSONEncoder
from celeryflow.exceptions import ConditionFailed


class FlowTask(Task):
    """Base task with chain-manipulation helpers.

    The flow entry task uses :meth:`insert_to_chain` to inject the body of the
    flow (a Celery ``chain`` / ``group`` / single signature) after itself in
    ``self.request.chain``, which is how a single entry task expands into the
    full pipeline at runtime.
    """

    abstract = True

    def run(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - placeholder
        return None

    def insert_to_chain(self, task: Any) -> None:
        """Insert ``task`` (signature/chain/group/Task) into the current request chain."""
        if self.request.chain is None:
            self.request.chain = []

        if isinstance(task, group):
            current_chain = copy.deepcopy(self.request.chain)
            current_chain.reverse()
            reorder = [signature(t) for t in current_chain]
            next_chain = chain(task, *reorder)
            self.request.chain = [dict(next_chain)]
        elif isinstance(task, (chain, canvas._chain)):
            unchain_tasks = task.unchain_tasks()
            unchain_tasks.reverse()
            self.request.chain.extend(unchain_tasks)
        elif isinstance(task, Task):
            self.request.chain.append(task.s())
        elif isinstance(task, canvas.Signature):
            self.request.chain.append(task)
        else:
            raise TypeError(f"Cannot insert object of type {type(task)!r} into chain")

    def get_base_extra(self, payload: Any = None) -> dict[str, Any]:
        """Build the structured-logging extras for the current task invocation."""
        extra = {
            "task_id": self.request.id,
            "process_id": self.request.root_id,
            "parent_id": self.request.parent_id,
            "task_name": self.name,
        }
        if isinstance(payload, dict):
            cid = payload.get("correlation_id")
            if cid:
                extra["correlation_id"] = cid
        return extra


def _extract_payload(args: tuple, kwargs: dict) -> Any:
    """Best-effort payload extraction for callbacks.

    Convention: payload is either ``args[0]`` (most common — chain hands the
    previous task's return value as the first positional arg) or the
    ``payload`` keyword.
    """
    if args:
        return args[0]
    return kwargs.get("payload")


class EventTask(FlowTask):
    """Flow task with condition gating and lifecycle logging.

    Bind a logger globally via :meth:`bind_logger` (typically once at app
    startup); each subclass / task instance can override by assigning to its
    own ``logger`` attribute.

    Condition gating: when ``apply_async`` is called with an ``options['condition']``
    mapping (typically set by the FlowBuilder from the flow definition), the
    payload is evaluated against it before the task is actually queued. A
    failing condition raises :class:`celeryflow.exceptions.ConditionFailed`,
    which the caller may catch to "skip" the task while continuing the chain.
    """

    abstract = True
    logger: logging.Logger | None = None

    @classmethod
    def bind_logger(cls, logger: logging.Logger) -> None:
        """Install a logger to receive task lifecycle events."""
        cls.logger = logger

    # ------------------------------------------------------------------
    # public properties

    @property
    def task_name(self) -> str:
        """Short name (the last dotted component of ``self.name``)."""
        return self.name[self.name.rfind(".") + 1 :]

    @property
    def description(self) -> str:
        """Human-readable description used in log messages."""
        return self.task_name.replace("_", " ")

    # ------------------------------------------------------------------
    # Celery hooks

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        payload = _extract_payload(args, kwargs)
        self._log("debug", f"{self.description} Start", payload, input=payload)
        return super().__call__(*args, **kwargs)

    def apply_async(  # type: ignore[override]
        self,
        args: tuple | list | None = None,
        kwargs: dict | None = None,
        task_id: str | None = None,
        producer: Any = None,
        link: Any = None,
        link_error: Any = None,
        shadow: str | None = None,
        **options: Any,
    ) -> Any:
        condition = options.pop("condition", None)
        if condition:
            payload = (args or (None,))[0]
            if isinstance(payload, dict):
                # evaluate all clauses; the first failing clause raises
                for field, statements in condition.items():
                    if field in payload and not check_conditions(
                        {field: payload[field]}, {field: statements}
                    ):
                        raise ConditionFailed(field, payload[field], statements)
        return super().apply_async(
            args=args,
            kwargs=kwargs,
            task_id=task_id,
            producer=producer,
            link=link,
            link_error=link_error,
            shadow=shadow,
            **options,
        )

    def on_success(self, retval: Any, task_id: str, args: tuple, kwargs: dict) -> None:
        payload = _extract_payload(args, kwargs)
        self._log("info", f"{self.description} End", payload, output=retval)

    def on_failure(self, exc, task_id, args, kwargs, einfo) -> None:  # type: ignore[override]
        payload = _extract_payload(args or (), kwargs or {})
        self._log("error", f"{self.description} Failed", payload, reason=str(exc))

    def on_retry(self, exc, task_id, args, kwargs, einfo) -> None:  # type: ignore[override]
        payload = _extract_payload(args or (), kwargs or {})
        self._log("warning", f"{self.description} Retry", payload, reason=str(exc))

    # ------------------------------------------------------------------
    # internal helpers

    def _log(self, level: str, message: str, payload: Any, **extras: Any) -> None:
        if self.logger is None:
            return
        extra = self.get_base_extra(payload)
        for k, v in extras.items():
            try:
                extra[k] = json.dumps(v, ensure_ascii=False, cls=CeleryFlowJSONEncoder)
            except (TypeError, ValueError):
                extra[k] = repr(v)
        getattr(self.logger, level)(message, extra=extra)
