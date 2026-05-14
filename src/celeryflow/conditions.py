"""Condition operators for gating task execution inside a flow.

A *condition* is a mapping of ``{field_name: {operator: expected_value, ...}}``.
Each operator key (``$eq``, ``$ne``, ``$in`` ...) maps to a binary callable
``(actual, expected) -> bool``. The set of operators is extensible via
:func:`register_operator`.
"""
from __future__ import annotations

import operator as _op
from collections.abc import Callable, Mapping
from typing import Any

Operator = Callable[[Any, Any], bool]

_DEFAULT_OPERATORS: dict[str, Operator] = {
    "$eq": _op.eq,
    "$ne": _op.ne,
    "$gt": _op.gt,
    "$gte": _op.ge,
    "$lt": _op.lt,
    "$lte": _op.le,
    "$in": lambda actual, expected: actual in expected,
    "$nin": lambda actual, expected: actual not in expected,
}

_OPERATORS: dict[str, Operator] = dict(_DEFAULT_OPERATORS)


def register_operator(name: str, func: Operator) -> None:
    """Register (or replace) a condition operator.

    The ``name`` should follow the ``$<word>`` convention (mongo-style) to
    avoid colliding with payload field names.
    """
    if not callable(func):
        raise TypeError("operator must be callable")
    _OPERATORS[name] = func


def get_operator(name: str) -> Operator:
    try:
        return _OPERATORS[name]
    except KeyError as exc:
        raise KeyError(f"Unknown condition operator: {name!r}") from exc


def reset_operators() -> None:
    """Restore the default operator set. Mostly useful in tests."""
    _OPERATORS.clear()
    _OPERATORS.update(_DEFAULT_OPERATORS)


def check_conditions(payload: Mapping[str, Any], condition: Mapping[str, Mapping[str, Any]]) -> bool:
    """Evaluate a full condition mapping against ``payload``.

    A condition mapping looks like::

        {"sn": {"$eq": "1234"}, "amount": {"$gte": 100}}

    All operator clauses across all fields must pass. A field missing from the
    payload is treated as passing (matches the original CeleryFlow semantics —
    a task that depends on a field absent from the payload is simply
    skipped-as-passing rather than failed).
    """
    if not condition:
        return True
    for field, statements in condition.items():
        if field not in payload:
            continue
        actual = payload[field]
        for op_name, expected in statements.items():
            op = get_operator(op_name)
            if not op(actual, expected):
                return False
    return True
