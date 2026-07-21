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
from celeryflow.exceptions import CeleryFlowError

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


def test_entry_task_splices_and_threads_payload(worker_broker: tuple[str, str]) -> None:
    """Dispatching the entry task itself must expand into the full pipeline.

    This is the only path that exercises the runtime chain-splicing mechanism.
    Eager mode and ``execute_flow`` both bypass it, so a real worker round-trip
    is the only place its behavior can be pinned. We assert the payload is
    threaded through every step: the entry task's own result must resolve to the
    final step's output (``self.replace`` freezes the flow chain with the entry
    task's id, so ``result.get()`` follows through to the tail).

    Regression guard: with the old ``insert_to_chain`` approach this returned the
    payload unchanged, because the flow body never ran on a worker.
    """
    broker_url, result_backend = worker_broker
    app = CeleryFlow("test_worker")
    app.conf.update(
        broker_url=broker_url,
        result_backend=result_backend,
        task_always_eager=False,
        broker_connection_retry_on_startup=True,
    )

    @app.task(name="order.step_a", base=EventTask)
    def step_a(payload):
        payload = dict(payload)
        payload["a_ran"] = True
        return payload

    @app.task(name="order.step_b", base=EventTask)
    def step_b(payload):
        payload = dict(payload)
        payload["b_ran"] = True
        return payload

    FlowBuilder.from_dict(
        app,
        {
            "main-flows": [
                {
                    "name": "ThreadFlow",
                    "flows": [{"task": "order.step_a"}, {"task": "order.step_b"}],
                },
            ],
        },
    )

    with start_worker(app, perform_ping_check=False, shutdown_timeout=20):
        result = app.tasks["ThreadFlow"].apply_async(args=({"sn": "x"},))
        assert result.get(timeout=15) == {"sn": "x", "a_ran": True, "b_ran": True}


def test_entry_task_condition_gates_a_spliced_step(worker_broker: tuple[str, str]) -> None:
    """A per-step condition must still gate that step through the real splice.

    The gate lives in ``EventTask.apply_async``; it only fires when a step is
    dispatched on a worker (eager chains bypass ``apply_async``). If gating did
    not fire, the gated step would run and the flow would succeed. So a raised
    ``CeleryFlowError`` on the unsatisfied payload proves the gate fired; a clean
    result on the satisfied payload proves it lets matching payloads through.
    """
    broker_url, result_backend = worker_broker
    app = CeleryFlow("test_worker")
    app.conf.update(
        broker_url=broker_url,
        result_backend=result_backend,
        task_always_eager=False,
        broker_connection_retry_on_startup=True,
    )

    @app.task(name="order.head", base=EventTask)
    def head(payload):
        payload = dict(payload)
        payload["head_ran"] = True
        return payload

    @app.task(name="order.gated", base=EventTask)
    def gated(payload):
        payload = dict(payload)
        payload["gated_ran"] = True
        return payload

    FlowBuilder.from_dict(
        app,
        {
            "main-flows": [
                {
                    "name": "GatedFlow",
                    "flows": [
                        {"task": "order.head"},
                        {
                            "task": "order.gated",
                            "condition": {"customer_type": {"$eq": "returning"}},
                        },
                    ],
                },
            ],
        },
    )

    with start_worker(app, perform_ping_check=False, shutdown_timeout=20):
        ok = app.tasks["GatedFlow"].apply_async(args=({"customer_type": "returning"},))
        assert ok.get(timeout=15) == {
            "customer_type": "returning",
            "head_ran": True,
            "gated_ran": True,
        }

        blocked = app.tasks["GatedFlow"].apply_async(args=({"customer_type": "new"},))
        with pytest.raises(CeleryFlowError):
            blocked.get(timeout=15)
