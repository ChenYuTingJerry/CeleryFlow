"""Build runtime flow entry tasks from declarative definitions.

A *flow definition* is a list of step descriptors. Each step is either a
direct task reference::

    {"name": "order.task_a"}
    {"name": "order.task_b", "condition": {"sn": {"$eq": "1234"}}}

…or a nested workflow reference::

    {"flow": "common_preprocess"}

The builder resolves nested ``flow`` references against a registry of
named workflows (``work-flows`` in YAML / ``flow-definitions`` in JSON,
both supported), expands them inline, and registers a single Celery task
per top-level flow. That entry task — when invoked — splices the resolved
chain into ``self.request.chain`` so the rest of the pipeline runs after
it.
"""
from __future__ import annotations

import glob
import importlib
import json
from pathlib import Path
from typing import Any

from celery import Celery, Task, chain, signature

from celeryflow.exceptions import FlowDefinitionError
from celeryflow.tasks import EventTask


class FlowBuilder:
    """Stateless helper for translating flow definitions into Celery tasks.

    Designed for one-shot use — call :meth:`from_dict` / :meth:`from_yaml` /
    :meth:`from_json` against a Celery app to register the flow tasks. Each
    call clears the builder's internal workflow map, so repeated calls with
    different configs don't bleed into each other.
    """

    def __init__(self) -> None:
        self._flow_map: dict[str, list[Any]] = {}

    # ------------------------------------------------------------------
    # public entry points

    @classmethod
    def from_dict(
        cls,
        app: Celery,
        config: dict[str, Any],
        *,
        worker_name: str | None = None,
    ) -> FlowBuilder:
        """Build flows from an in-memory configuration dict.

        Expected schema (both the modern ``work-flows``/``main-flows`` shape
        and the legacy ``flow-definitions``/``flows`` shape are accepted)::

            {
              "tasks": ["order"],                  # modules to import; optional
              "work-flows": [                      # named workflows; optional
                {"name": "wf_a", "tasks": [...]},
              ],
              "flow-definitions": {                # alternative shape (CeleryFlow JSON)
                "test_flow": [{"name": "..."}, ...],
              },
              "main-flows": [                      # top-level entry flows
                {"name": "TestFlow",
                 "flows": [{"flow": "wf_a"}, {"task": "order.task_c"}]},
              ],
              "flows": {                           # alternative shape (CeleryFlow JSON)
                "TestFlow": {"type": "flow", "name": "test_flow"},
              },
            }
        """
        builder = cls()
        builder._parse_tasks(config.get("tasks") or [], worker_name=worker_name)
        builder._flow_map.update(builder._parse_work_flows(config.get("work-flows") or []))
        builder._flow_map.update(cls._normalize_flow_definitions(config.get("flow-definitions") or {}))

        # Support both shapes for the top-level entries
        if "main-flows" in config:
            builder._parse_main_flows(app, config["main-flows"])
        if "flows" in config:
            builder._parse_legacy_flows(app, config["flows"])
        return builder

    @classmethod
    def from_yaml(
        cls,
        app: Celery,
        path: str | Path,
        *,
        worker_name: str | None = None,
    ) -> FlowBuilder:
        """Load one or more YAML files (glob pattern supported) and build."""
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - exercised via extras
            raise ImportError(
                "PyYAML is required for FlowBuilder.from_yaml; "
                "install celeryflow with the 'yaml' extra, e.g. pip install celeryflow[yaml]"
            ) from exc

        files = sorted(glob.glob(str(path), recursive=True)) or [str(path)]
        merged: dict[str, Any] = {}
        for f in files:
            with open(f) as stream:
                conf = yaml.safe_load(stream) or {}
            merged = _deep_merge(merged, conf)
        return cls.from_dict(app, merged, worker_name=worker_name)

    @classmethod
    def from_json(
        cls,
        app: Celery,
        path: str | Path,
        *,
        worker_name: str | None = None,
    ) -> FlowBuilder:
        """Load one or more JSON files (glob pattern supported) and build."""
        files = sorted(glob.glob(str(path), recursive=True)) or [str(path)]
        merged: dict[str, Any] = {}
        for f in files:
            with open(f) as stream:
                conf = json.load(stream)
            merged = _deep_merge(merged, conf)
        return cls.from_dict(app, merged, worker_name=worker_name)

    # ------------------------------------------------------------------
    # legacy CeleryFlow-style flow-definitions normalisation

    @staticmethod
    def _normalize_flow_definitions(flow_defs: dict[str, list[dict[str, Any]]]) -> dict[str, list[Any]]:
        """Turn CeleryFlow's ``flow-definitions`` shape into the signature list shape."""
        result: dict[str, list[Any]] = {}
        for name, steps in flow_defs.items():
            sigs: list[Any] = []
            for step in steps:
                if "name" not in step:
                    raise FlowDefinitionError(
                        f"flow-definitions[{name!r}] step missing required 'name' key: {step!r}"
                    )
                options: dict[str, Any] = {}
                if "condition" in step:
                    options["condition"] = step["condition"]
                sig = signature(step["name"], options=options) if options else signature(step["name"])
                sigs.append(sig)
            result[name] = sigs
        return result

    # ------------------------------------------------------------------
    # parsing helpers

    def _parse_main_flows(self, app: Celery, flows: list[dict[str, Any]]) -> None:
        for attr in flows:
            self._build_main_flow(app, attr["name"], attr.get("flows", []))

    def _parse_legacy_flows(self, app: Celery, flows: dict[str, dict[str, Any]]) -> None:
        """Handle the CeleryFlow JSON ``flows: {Name: {type, name}}`` shape."""
        for name, spec in flows.items():
            kind = spec.get("type")
            if kind == "flow":
                target = spec["name"]
                if target not in self._flow_map:
                    raise FlowDefinitionError(f"flow {name!r} references unknown definition {target!r}")
                # re-wrap as a main-flow body
                self._build_main_flow(app, name, [{"flow": target}])
            elif kind == "task":
                self._build_main_flow(app, name, [{"task": spec["name"]}])
            else:
                raise FlowDefinitionError(f"flow {name!r} has unknown type {kind!r}")

    def _build_main_flow(self, app: Celery, name: str, items: list[dict[str, Any]]) -> None:
        tasks: list[Any] = []
        for item in items:
            if "task" in item:
                options: dict[str, Any] = {}
                if "condition" in item:
                    options["condition"] = item["condition"]
                sig = (
                    signature(item["task"], options=options)
                    if options
                    else signature(item["task"])
                )
                tasks.append(sig)
            elif "flow" in item:
                ref = item["flow"]
                if ref not in self._flow_map:
                    raise FlowDefinitionError(f"flow {name!r} references unknown flow {ref!r}")
                tasks.extend(self._flow_map[ref])
            else:
                raise FlowDefinitionError(f"flow {name!r} has invalid step: {item!r}")
        entry_run = _build_entry_task()
        attrs = {"name": name, "run": entry_run, "task_list": tasks}
        self._register_task(app, attrs, EventTask)

    @staticmethod
    def _parse_work_flows(work_flows: list[dict[str, Any]]) -> dict[str, list[Any]]:
        result: dict[str, list[Any]] = {}
        for attr in work_flows:
            name = attr["name"]
            sigs: list[Any] = []
            for t in attr.get("tasks", []):
                if isinstance(t, dict):
                    options: dict[str, Any] = {}
                    if "condition" in t:
                        options["condition"] = t["condition"]
                    sig = signature(t["name"], options=options) if options else signature(t["name"])
                else:
                    sig = signature(t)
                sigs.append(sig)
            result[name] = sigs
        return result

    @staticmethod
    def _parse_tasks(modules: list[str], *, worker_name: str | None) -> None:
        """Import the listed task modules so their @app.task decorators run."""
        for mod in modules:
            if worker_name:
                importlib.import_module(f"{worker_name}.tasks.{mod}")
            else:
                importlib.import_module(mod)

    @staticmethod
    def _register_task(app: Celery, attributes: dict[str, Any], base_task: type[Task]) -> Task:
        task = type("FlowEntryTask", (base_task,), attributes)()
        app.tasks.register(task)
        return task


def _build_entry_task():
    """Factory for the runtime entry-task ``run`` method.

    Captured here as a closure so it has access to the resolved ``task_list``
    via ``self`` (the registered FlowEntryTask sets ``task_list`` as a class
    attribute).
    """

    def entry_run(self, *args: Any, **kwargs: Any) -> Any:
        if "payload" in kwargs:
            payload = kwargs["payload"]
        elif len(args) == 1:
            payload = args[0]
        elif len(args) == 2:
            payload = args[1]
        else:
            payload = None

        chained: list[Any] = []
        for step in self.task_list:
            # step is already a celery Signature at this point
            chained.append(step)
        if chained:
            self.insert_to_chain(chain(*chained))
        return payload

    return entry_run


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Merge ``overlay`` into ``base`` non-destructively, recursing into dicts."""
    out = dict(base)
    for k, v in overlay.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        elif k in out and isinstance(out[k], list) and isinstance(v, list):
            out[k] = out[k] + v
        else:
            out[k] = v
    return out
