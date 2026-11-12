"""Tests for the exception hierarchy.

The round-trip tests guard #15: a ``ConditionFailed`` raised on a worker has to
come back out of ``result.get()`` as ``ConditionFailed``, with its fields. Celery
rebuilds a stored exception by calling ``cls(*args)``, so ``args`` must hold the
constructor arguments.
"""
from __future__ import annotations

import json
import pickle

import pytest

from celeryflow import CeleryFlow
from celeryflow.exceptions import CeleryFlowError, ConditionFailed

FIELD, VALUE, STATEMENTS = "customer_type", "new", {"$eq": "returning"}


def _assert_same(exc: BaseException) -> None:
    assert type(exc) is ConditionFailed
    assert (exc.field, exc.value, exc.statements) == (FIELD, VALUE, STATEMENTS)


class TestConditionFailed:
    def test_message(self):
        exc = ConditionFailed(FIELD, VALUE, STATEMENTS)
        assert str(exc) == (
            "Condition check failed on field 'customer_type': value='new' "
            "did not satisfy {'$eq': 'returning'}"
        )

    def test_is_a_celeryflow_error(self):
        assert isinstance(ConditionFailed(FIELD, VALUE, STATEMENTS), CeleryFlowError)

    def test_args_hold_constructor_arguments(self):
        assert ConditionFailed(FIELD, VALUE, STATEMENTS).args == (FIELD, VALUE, STATEMENTS)

    def test_pickle_round_trip(self):
        _assert_same(pickle.loads(pickle.dumps(ConditionFailed(FIELD, VALUE, STATEMENTS))))

    @pytest.mark.parametrize("serializer", ["json", "pickle"])
    def test_result_backend_round_trip(self, serializer):
        """Store and rebuild the exception the way a result backend does."""
        app = CeleryFlow("test_worker")
        app.conf.update(result_backend="cache+memory://", result_serializer=serializer,
                        result_accept_content=["json", "pickle"])
        backend = app.backend

        stored = backend.prepare_exception(ConditionFailed(FIELD, VALUE, STATEMENTS), serializer)
        # Push it through the real codec, as the backend does when it saves the result.
        if serializer == "json":
            stored = json.loads(json.dumps(stored))
        else:
            stored = pickle.loads(pickle.dumps(stored))

        _assert_same(backend.exception_to_python(stored))
