"""Integration test: a real Celery worker must boot with a flow registered.

Unlike ``test_integration_redis.py`` (which runs in ``apply`` mode and never
spins up a worker), this test starts an actual worker via Celery's
``start_worker`` helper and runs a task through it. That worker round-trip is
the only path that builds the per-task tracer, which is where the bind bug
showed up.

Regression being guarded: ``FlowBuilder._register_task`` used to call
``app.tasks.register`` (a plain dict insert) instead of ``app.register_task``.
The plain insert skips ``task.bind(app)``, so the registered flow entry task
has no ``request_stack``. When a worker boots, the consumer builds a strategy
(tracer) for every registered task; the unbound entry task crashes that build
at startup with ``AttributeError: 'NoneType' object has no attribute 'push'``,
and the whole worker fails to come up. Note the entry task itself is never
called here: just having it registered is enough to break boot.

Runs against both an in-process ``memory://`` broker (always) and a real Redis
broker (skipped when unreachable) via the ``worker_broker`` fixture.
"""
from __future__ import annotations

import pytest
from celery.contrib.testing.worker import start_worker

from celeryflow import CeleryFlow, EventTask, FlowBuilder

pytestmark = pytest.mark.integration


def test_worker_boots_and_runs_with_flow_registered(worker_broker: tuple[str, str]) -> None:
    broker_url, result_backend = worker_broker
    app = CeleryFlow("test_worker")
    app.conf.update(
        broker_url=broker_url,
        result_backend=result_backend,
        task_always_eager=False,
        broker_connection_retry_on_startup=True,
    )

    @app.task(name="order.leaf", base=EventTask)
    def leaf(payload):
        return {"ran": True}

    # Registering the flow puts a FlowEntryTask into app.tasks. On the buggy
    # code this entry task is unbound and poisons the worker's tracer build.
    FlowBuilder.from_dict(
        app,
        {"main-flows": [{"name": "BootFlow", "flows": [{"task": "order.leaf"}]}]},
    )

    # If the worker can boot and run an ordinary task, the registered entry
    # task was bound correctly. On the old code start_worker never reaches a
    # usable state and ``result.get`` times out.
    with start_worker(app, perform_ping_check=False, shutdown_timeout=20):
        result = app.tasks["order.leaf"].apply_async(args=({},))
        assert result.get(timeout=15) == {"ran": True}
