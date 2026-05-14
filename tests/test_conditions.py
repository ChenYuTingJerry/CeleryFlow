"""Unit tests for the condition operator system."""
from __future__ import annotations

import pytest

from celeryflow.conditions import (
    check_conditions,
    get_operator,
    register_operator,
    reset_operators,
)


class TestCheckConditions:
    def test_empty_condition_passes(self):
        assert check_conditions({"sn": "1234"}, {}) is True

    def test_missing_field_passes(self):
        # missing fields are skipped, mirroring original CeleryFlow behaviour
        assert check_conditions({"other": 1}, {"sn": {"$eq": "1234"}}) is True

    def test_eq_match(self):
        assert check_conditions({"sn": "1234"}, {"sn": {"$eq": "1234"}}) is True

    def test_eq_no_match(self):
        assert check_conditions({"sn": "1234"}, {"sn": {"$eq": "9999"}}) is False

    def test_multiple_clauses_all_must_pass(self):
        payload = {"sn": "1234", "amount": 200}
        assert check_conditions(payload, {"sn": {"$eq": "1234"}, "amount": {"$gte": 100}}) is True
        assert check_conditions(payload, {"sn": {"$eq": "1234"}, "amount": {"$gte": 300}}) is False

    def test_in_operator(self):
        assert check_conditions({"region": "tw"}, {"region": {"$in": ["tw", "jp"]}}) is True
        assert check_conditions({"region": "us"}, {"region": {"$in": ["tw", "jp"]}}) is False

    def test_ne_operator(self):
        assert check_conditions({"sn": "1"}, {"sn": {"$ne": "2"}}) is True
        assert check_conditions({"sn": "2"}, {"sn": {"$ne": "2"}}) is False

    def test_range_operators(self):
        assert check_conditions({"x": 5}, {"x": {"$gt": 4, "$lt": 10}}) is True
        assert check_conditions({"x": 10}, {"x": {"$gt": 4, "$lt": 10}}) is False
        assert check_conditions({"x": 5}, {"x": {"$gte": 5, "$lte": 5}}) is True

    def test_unknown_operator_raises(self):
        with pytest.raises(KeyError):
            check_conditions({"x": 1}, {"x": {"$nope": 1}})


class TestRegisterOperator:
    def test_register_and_use(self):
        register_operator("$startswith", lambda a, b: isinstance(a, str) and a.startswith(b))
        try:
            assert check_conditions({"name": "celeryflow"}, {"name": {"$startswith": "celery"}}) is True
            assert check_conditions({"name": "django"}, {"name": {"$startswith": "celery"}}) is False
        finally:
            reset_operators()

    def test_register_rejects_non_callable(self):
        with pytest.raises(TypeError):
            register_operator("$bad", "not-callable")  # type: ignore[arg-type]

    def test_reset_restores_defaults(self):
        register_operator("$custom", lambda a, b: True)
        # the custom operator must exist before reset...
        assert get_operator("$custom") is not None
        reset_operators()
        # ...and be gone after.
        with pytest.raises(KeyError):
            get_operator("$custom")
