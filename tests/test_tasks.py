"""Tests for EventTask — condition gating, logging, and chain manipulation."""
from __future__ import annotations

import logging

import pytest

from celeryflow import EventTask
from celeryflow.exceptions import ConditionFailed


class TestEventTaskCondition:
    def test_apply_async_passes_when_condition_met(self, eager_app):
        @eager_app.task(name="check.passes", base=EventTask)
        def t(payload):
            return payload

        result = t.apply_async(args=({"sn": "1234"},), condition={"sn": {"$eq": "1234"}})
        assert result.get() == {"sn": "1234"}

    def test_apply_async_raises_when_condition_fails(self, eager_app):
        @eager_app.task(name="check.fails", base=EventTask)
        def t(payload):
            return payload

        with pytest.raises(ConditionFailed) as exc_info:
            t.apply_async(args=({"sn": "9999"},), condition={"sn": {"$eq": "1234"}})
        assert exc_info.value.field == "sn"
        assert exc_info.value.value == "9999"

    def test_condition_field_missing_passes_through(self, eager_app):
        """A condition on a field absent from the payload should not block."""
        @eager_app.task(name="check.missing", base=EventTask)
        def t(payload):
            return payload

        result = t.apply_async(args=({"other": "x"},), condition={"sn": {"$eq": "1234"}})
        assert result.get() == {"other": "x"}


class TestEventTaskLogging:
    def test_bind_logger_records_lifecycle(self, eager_app, caplog):
        EventTask.bind_logger(logging.getLogger("celeryflow.test"))
        try:
            @eager_app.task(name="log.task", base=EventTask)
            def t(payload):
                return payload

            with caplog.at_level(logging.DEBUG, logger="celeryflow.test"):
                t.apply(args=({"hello": "world"},))

            messages = [r.message for r in caplog.records]
            assert any("Start" in m for m in messages)
            assert any("End" in m for m in messages)
        finally:
            EventTask.logger = None

    def test_no_logger_is_silent(self, eager_app, caplog):
        EventTask.logger = None

        @eager_app.task(name="log.silent", base=EventTask)
        def t(payload):
            return payload

        with caplog.at_level(logging.DEBUG):
            t.apply(args=({"hello": "world"},))

        # nothing from celeryflow.*
        assert not [r for r in caplog.records if r.name.startswith("celeryflow")]


class TestEventTaskProperties:
    def test_task_name_strips_dotted_prefix(self, eager_app):
        @eager_app.task(name="some.module.do_thing", base=EventTask)
        def t(payload):
            return payload

        assert t.task_name == "do_thing"
        assert t.description == "do thing"
