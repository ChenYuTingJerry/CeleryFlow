"""The ``CeleryFlow`` Celery subclass.

A thin wrapper that adds:

* Task-name normalisation — strips a ``{worker_name}.tasks.`` prefix so the
  declared task name matches the short name used in flow definitions
  (``order.task_a`` instead of ``my_worker.tasks.order.task_a``).
* :meth:`execute_flow` — convenience for synchronously running a registered
  flow against a payload, useful for tests and ad-hoc invocations.
"""
from __future__ import annotations

import logging
from typing import Any

from celery import Celery, chain

from celeryflow.tasks import EventTask


class CeleryFlow(Celery):
    """Celery app with flow-aware task naming and helpers."""

    def __init__(
        self,
        worker_name: str,
        *args: Any,
        config_obj: Any | None = None,
        logger: logging.Logger | None = None,
        **kwargs: Any,
    ) -> None:
        self.worker_name = worker_name
        self._pre_text = f"{worker_name}.tasks."
        super().__init__(worker_name, *args, **kwargs)
        if config_obj is not None:
            self.config_from_object(config_obj)
        if logger is not None:
            EventTask.bind_logger(logger)

    def gen_task_name(self, name: str, module: str) -> str:  # type: ignore[override]
        if module.startswith(self._pre_text):
            module = module[len(self._pre_text) :]
        return super().gen_task_name(name, module)

    def execute_flow(self, flow_name: str, payload: Any) -> Any:
        """Run a registered flow synchronously against ``payload``.

        Requires either ``task_always_eager=True`` in the Celery config, or a
        running worker connected to the configured broker. Returns the final
        result of the chain.
        """
        task = self.tasks[flow_name]
        steps = list(task.task_list)
        if not steps:
            return payload
        return chain(*steps).apply(args=(payload,))
