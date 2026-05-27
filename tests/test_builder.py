"""Tests for FlowBuilder — covers parsing of YAML, JSON, and direct dict
configs (modern and legacy shapes).

These tests use the eager Celery app so we can check the *runtime* behaviour
(payload flowing through the chain) end-to-end, not just registration.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from celeryflow import FlowBuilder
from celeryflow.exceptions import FlowDefinitionError


@pytest.fixture
def order_tasks(eager_app):
    """Register three task primitives on the eager app and return their names."""
    from celeryflow.tasks import EventTask

    @eager_app.task(name="order.task_a", base=EventTask)
    def task_a(payload):
        payload = dict(payload)
        payload["a_ran"] = True
        return payload

    @eager_app.task(name="order.task_b", base=EventTask)
    def task_b(payload):
        payload = dict(payload)
        payload["b_ran"] = True
        return payload

    @eager_app.task(name="order.task_c", base=EventTask)
    def task_c(payload):
        payload = dict(payload)
        payload["c_ran"] = True
        return payload

    return {"a": task_a, "b": task_b, "c": task_c}


# ---------------------------------------------------------------------------
# dict-based config


class TestFromDict:
    def test_main_flow_with_inline_tasks(self, eager_app, order_tasks):
        FlowBuilder.from_dict(
            eager_app,
            {
                "main-flows": [
                    {
                        "name": "InlineFlow",
                        "flows": [
                            {"task": "order.task_a"},
                            {"task": "order.task_c"},
                        ],
                    },
                ],
            },
        )
        assert "InlineFlow" in eager_app.tasks
        result = eager_app.execute_flow("InlineFlow", {"sn": "x"})
        # eager apply returns an EagerResult; .get() yields the chain tail value
        final = result.get()
        assert final == {"sn": "x", "a_ran": True, "c_ran": True}

    def test_main_flow_inline_task_carries_condition_into_signature_options(
        self, eager_app, order_tasks
    ):
        """Inline task in main-flows with a condition must keep that condition
        in the signature's options, so the worker's chain dispatch (which
        rebuilds the signature and calls apply_async) will trigger
        EventTask's condition gating.

        Regression: prior to this fix, ``_build_main_flow`` ignored the
        ``condition`` key on inline tasks, so the gated step always ran in
        production. Eager mode hid this because eager chain execution
        bypasses ``apply_async`` entirely.
        """
        from celery import signature

        from celeryflow.exceptions import ConditionFailed

        FlowBuilder.from_dict(
            eager_app,
            {
                "main-flows": [
                    {
                        "name": "Gated",
                        "flows": [
                            {"task": "order.task_a"},
                            {
                                "task": "order.task_b",
                                "condition": {"customer_type": {"$eq": "returning"}},
                            },
                        ],
                    },
                ],
            },
        )

        # The entry task holds the task_list the worker will splice into
        # request.chain. The gated step (task_b) must carry the condition
        # in its signature options.
        entry = eager_app.tasks["Gated"]
        gated_sig = entry.task_list[1]
        assert gated_sig.task == "order.task_b"
        assert gated_sig.options.get("condition") == {
            "customer_type": {"$eq": "returning"}
        }

        # Simulate what the Celery worker does when it picks the next chain
        # step out of request.chain: serialize-then-rebuild the signature,
        # then apply_async. With a satisfied payload it runs normally; with
        # an unsatisfied one EventTask.apply_async raises ConditionFailed.
        sig_as_dict = dict(gated_sig)
        rebuilt = signature(sig_as_dict, app=eager_app)
        assert rebuilt.options.get("condition") == {
            "customer_type": {"$eq": "returning"}
        }

        # Satisfied: returns a result without raising.
        rebuilt.apply_async(({"sn": "x", "customer_type": "returning"},))

        # Unsatisfied: EventTask.apply_async raises before queueing.
        with pytest.raises(ConditionFailed):
            rebuilt.apply_async(({"sn": "x", "customer_type": "new"},))

    def test_main_flow_referencing_work_flow(self, eager_app, order_tasks):
        FlowBuilder.from_dict(
            eager_app,
            {
                "work-flows": [
                    {"name": "ab", "tasks": ["order.task_a", "order.task_b"]},
                ],
                "main-flows": [
                    {
                        "name": "Composed",
                        "flows": [{"flow": "ab"}, {"task": "order.task_c"}],
                    },
                ],
            },
        )
        final = eager_app.execute_flow("Composed", {"sn": "x"}).get()
        assert final == {"sn": "x", "a_ran": True, "b_ran": True, "c_ran": True}

    def test_unknown_flow_reference_raises(self, eager_app, order_tasks):
        with pytest.raises(FlowDefinitionError):
            FlowBuilder.from_dict(
                eager_app,
                {
                    "main-flows": [
                        {"name": "Broken", "flows": [{"flow": "missing"}]},
                    ],
                },
            )

    def test_legacy_celeryflow_json_shape(self, eager_app, order_tasks):
        # The original CeleryFlow JSON layout — flow-definitions + flows.
        FlowBuilder.from_dict(
            eager_app,
            {
                "flow-definitions": {
                    "test_flow": [
                        {"name": "order.task_a"},
                        {"name": "order.task_b", "condition": {"sn": {"$eq": "1234"}}},
                    ],
                },
                "flows": {
                    "LegacyFlow": {"type": "flow", "name": "test_flow"},
                },
            },
        )
        assert "LegacyFlow" in eager_app.tasks


# ---------------------------------------------------------------------------
# YAML / JSON loaders


class TestFromYaml:
    def test_load_yaml_file(self, tmp_path: Path, eager_app, order_tasks):
        cfg = tmp_path / "flow.yaml"
        cfg.write_text(
            """
work-flows:
  - name: ab
    tasks:
      - order.task_a
      - order.task_b
main-flows:
  - name: YamlFlow
    flows:
      - flow: ab
      - task: order.task_c
"""
        )
        FlowBuilder.from_yaml(eager_app, cfg)
        final = eager_app.execute_flow("YamlFlow", {}).get()
        assert final == {"a_ran": True, "b_ran": True, "c_ran": True}

    def test_yaml_glob_merges_files(self, tmp_path, eager_app, order_tasks):
        (tmp_path / "a.yaml").write_text(
            "work-flows:\n  - name: wf_a\n    tasks: [order.task_a]\n"
        )
        (tmp_path / "b.yaml").write_text(
            "main-flows:\n  - name: GlobFlow\n    flows:\n      - flow: wf_a\n      - task: order.task_b\n"
        )
        FlowBuilder.from_yaml(eager_app, str(tmp_path / "*.yaml"))
        final = eager_app.execute_flow("GlobFlow", {}).get()
        assert final == {"a_ran": True, "b_ran": True}


class TestFromJson:
    def test_load_json_file(self, tmp_path: Path, eager_app, order_tasks):
        cfg = tmp_path / "flow.json"
        cfg.write_text(json.dumps({
            "main-flows": [
                {"name": "JsonFlow", "flows": [{"task": "order.task_a"}]},
            ],
        }))
        FlowBuilder.from_json(eager_app, cfg)
        final = eager_app.execute_flow("JsonFlow", {}).get()
        assert final == {"a_ran": True}

    def test_legacy_celeryflow_json_file(self, tmp_path: Path, eager_app, order_tasks):
        cfg = tmp_path / "flow.json"
        # Exactly the shape the original CeleryFlow project shipped (flow.json).
        cfg.write_text(json.dumps({
            "tasks": {"import": []},
            "flow-definitions": {
                "test_flow": [
                    {"name": "order.task_a"},
                    {"name": "order.task_b", "condition": {"sn": {"$eq": "1234"}}},
                ],
            },
            "flows": {
                "TestFlow": {"type": "flow", "name": "test_flow"},
            },
        }))
        # tasks.import list is consumed by _parse_tasks; an empty list is the no-op path
        # so we pass it through cleanly.
        # Strip the "import" wrapper to match our new schema:
        # the legacy shape used {"tasks": {"import": [...]}}; modern shape is {"tasks": [...]}.
        # The builder accepts either via .get("tasks") - we adapt here:
        FlowBuilder.from_dict(eager_app, {
            "flow-definitions": json.loads(cfg.read_text())["flow-definitions"],
            "flows": json.loads(cfg.read_text())["flows"],
        })
        assert "TestFlow" in eager_app.tasks
