"""Integration tests that exercise CeleryFlow against a real Redis broker.

These are gated on Redis being reachable (see the ``redis_url`` fixture in
``conftest.py``). Locally, run ``docker compose up -d`` in the project root
first; in CI, the workflow brings up a Redis service container.

We don't spin up a separate Celery worker process — instead we use ``apply``
(synchronous execution within the test process) but with a real broker URL,
which validates the integration with kombu / redis transport up to the
queue-handoff boundary. For a full worker-roundtrip test you'd add a thread
running ``app.worker_main``; we keep this suite simple on purpose.
"""
from __future__ import annotations

import pytest

from celeryflow import EventTask, FlowBuilder

pytestmark = pytest.mark.integration


def test_broker_connection(broker_app):
    """Smoke test — can we open a connection to the configured broker?"""
    with broker_app.connection() as conn:
        conn.ensure_connection(max_retries=2, interval_start=0, interval_step=0)


def test_flow_executes_synchronously_with_real_broker(broker_app):
    """Run a flow in ``apply`` mode while broker_url points at real Redis.

    This validates that the FlowBuilder + EventTask machinery doesn't trip
    over the real kombu transport during chain construction.
    """
    @broker_app.task(name="real.task_a", base=EventTask)
    def task_a(payload):
        payload = dict(payload)
        payload["a"] = True
        return payload

    @broker_app.task(name="real.task_b", base=EventTask)
    def task_b(payload):
        payload = dict(payload)
        payload["b"] = True
        return payload

    FlowBuilder.from_dict(
        broker_app,
        {
            "main-flows": [
                {
                    "name": "RealBrokerFlow",
                    "flows": [{"task": "real.task_a"}, {"task": "real.task_b"}],
                },
            ],
        },
    )

    result = broker_app.execute_flow("RealBrokerFlow", {"x": 1})
    assert result.get() == {"x": 1, "a": True, "b": True}
