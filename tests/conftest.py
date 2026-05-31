"""Pytest fixtures shared across the celeryflow test-suite.

We avoid the ``pytest-celery`` plugin to keep the dependency surface small —
Celery's built-in ``task_always_eager`` mode is more than sufficient for
exercising the flow-building logic.
"""
from __future__ import annotations

import os
import socket

import pytest

from celeryflow import CeleryFlow
from celeryflow.conditions import reset_operators


@pytest.fixture
def eager_app() -> CeleryFlow:
    """A CeleryFlow app running tasks synchronously in-process (no broker)."""
    app = CeleryFlow("test_worker")
    app.conf.update(
        broker_url="memory://",
        result_backend="cache+memory://",
        task_always_eager=True,
        task_eager_propagates=True,
        task_store_eager_result=True,
    )
    return app


@pytest.fixture(autouse=True)
def _reset_condition_operators():
    """Make sure operator registrations from one test don't bleed into the next."""
    yield
    reset_operators()


# ---------------------------------------------------------------------------
# Integration fixtures (require a running Redis broker)

def _redis_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


@pytest.fixture
def redis_url() -> str:
    """Resolve the Redis URL from env or fall back to localhost.

    Skips the test if nothing is listening — handy for local runs that
    haven't spun up docker-compose.
    """
    url = os.environ.get("CELERYFLOW_TEST_REDIS_URL", "redis://localhost:6379/0")
    # crude parse: redis://host:port/db
    try:
        without_scheme = url.split("://", 1)[1]
        host_port = without_scheme.split("/", 1)[0]
        host, port_s = host_port.split(":", 1)
        port = int(port_s)
    except (IndexError, ValueError):
        pytest.skip(f"unparseable redis url {url!r}")
        return url  # pragma: no cover

    if not _redis_reachable(host, port):
        pytest.skip(f"Redis not reachable at {host}:{port} — start docker-compose to enable")
    return url


@pytest.fixture
def broker_app(redis_url: str) -> CeleryFlow:
    """A CeleryFlow app pointing at a real Redis broker."""
    app = CeleryFlow("test_worker")
    app.conf.update(
        broker_url=redis_url,
        result_backend=redis_url,
        task_always_eager=False,
        broker_connection_retry_on_startup=True,
    )
    return app


@pytest.fixture(params=["memory", "redis"])
def worker_broker(request) -> tuple[str, str]:
    """(broker_url, result_backend) for tests that start a real worker.

    Parametrised so the same worker-roundtrip test runs twice:

    * ``memory`` — kombu's in-process transport, always available (no service
      needed), so this variant runs everywhere including the sandbox and CI.
    * ``redis`` — a real Redis broker; skipped when nothing is listening, the
      same gate the ``redis_url`` fixture uses.
    """
    if request.param == "memory":
        return ("memory://", "cache+memory://")

    url = os.environ.get("CELERYFLOW_TEST_REDIS_URL", "redis://localhost:6379/0")
    try:
        host_port = url.split("://", 1)[1].split("/", 1)[0]
        host, port_s = host_port.split(":", 1)
        port = int(port_s)
    except (IndexError, ValueError):
        pytest.skip(f"unparseable redis url {url!r}")
    if not _redis_reachable(host, port):
        pytest.skip(f"Redis not reachable at {host}:{port} — start docker-compose to enable")
    return (url, url)
