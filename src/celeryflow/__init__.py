"""CeleryFlow: describe Celery workflows in YAML, JSON, or a Python dict.

Public API:

    from celeryflow import CeleryFlow, FlowBuilder, FlowTask, EventTask
    from celeryflow.conditions import check_conditions, register_operator
    from celeryflow.exceptions import CeleryFlowError, ConditionFailed
"""
from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

__all__ = [
    "CeleryFlow",
    "EventTask",
    "FlowBuilder",
    "FlowTask",
    "CeleryFlowError",
    "ConditionFailed",
]

try:
    __version__ = _pkg_version("celeryflow")
except PackageNotFoundError:
    # Package is not installed (e.g. running from a source checkout
    # without `pip install -e .`). Falling back to "0.0.0+unknown"
    # rather than crashing keeps tooling that introspects __version__
    # happy in development.
    __version__ = "0.0.0+unknown"

from celeryflow.app import CeleryFlow
from celeryflow.builder import FlowBuilder
from celeryflow.exceptions import CeleryFlowError, ConditionFailed
from celeryflow.tasks import EventTask, FlowTask
