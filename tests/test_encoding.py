"""Tests for the JSON encoder."""
from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from enum import Enum

import pytest

from celeryflow.encoding import CeleryFlowJSONEncoder


class Color(Enum):
    RED = "red"
    BLUE = "blue"


def test_encodes_decimal_as_string():
    out = json.dumps({"x": Decimal("10.50")}, cls=CeleryFlowJSONEncoder)
    assert json.loads(out) == {"x": "10.50"}


def test_encodes_enum_value():
    out = json.dumps({"c": Color.RED}, cls=CeleryFlowJSONEncoder)
    assert json.loads(out) == {"c": "red"}


def test_encodes_datetime_isoformat():
    when = dt.datetime(2026, 5, 14, 10, 30)
    out = json.dumps({"when": when}, cls=CeleryFlowJSONEncoder)
    assert json.loads(out)["when"].startswith("2026-05-14T10:30")


def test_unsupported_type_raises():
    class Mystery:
        pass

    with pytest.raises(TypeError):
        json.dumps({"x": Mystery()}, cls=CeleryFlowJSONEncoder)
