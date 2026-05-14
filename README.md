# CeleryFlow

[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A small, declarative flow / pipeline orchestration layer on top of
[Celery](https://docs.celeryq.dev/). Define multi-step workflows in YAML,
JSON, or a Python dict, attach conditions to individual steps, and let
CeleryFlow wire them into Celery chains at runtime.

CeleryFlow is the open-source distillation of a workflow engine I built
and ran in production for several years on an e-commerce platform. The
domain-specific bits have been stripped — what's left is the generic
flow engine.

## Features

- **Declarative flows.** Compose pipelines from reusable named workflows.
- **Multi-format config.** YAML, JSON, or in-memory Python dict.
- **Condition gating.** Skip a step at runtime based on the payload, using
  a small Mongo-style operator DSL (`$eq`, `$ne`, `$in`, `$gte`, ...).
- **Lifecycle logging.** Optional structured logging on task start /
  success / failure / retry, with payload safely JSON-encoded.
- **Pluggable operators.** Register your own condition operators with
  `register_operator`.
- **Pure Celery.** Flows are real Celery tasks — they participate in
  retries, monitoring, and routing like any other task.

## Installation

```bash
pip install celeryflow              # core
pip install "celeryflow[yaml]"      # + YAML loader
pip install "celeryflow[redis]"     # + redis broker dependency
```

Python 3.11+ is required. CeleryFlow uses [uv](https://docs.astral.sh/uv/)
for dependency management during development.

## Quickstart

```python
from celeryflow import CeleryFlow, EventTask, FlowBuilder

app = CeleryFlow("my_worker")
app.conf.update(broker_url="redis://localhost:6379/0",
                result_backend="redis://localhost:6379/0")

# 1. Define a few primitive tasks.
@app.task(name="order.validate", base=EventTask)
def validate(payload):
    assert "sn" in payload
    return payload

@app.task(name="order.charge", base=EventTask)
def charge(payload):
    payload["charged"] = True
    return payload

@app.task(name="order.notify", base=EventTask)
def notify(payload):
    payload["notified"] = True
    return payload

# 2. Wire them into a flow.
FlowBuilder.from_dict(app, {
    "work-flows": [
        {"name": "checkout", "tasks": ["order.validate", "order.charge"]},
    ],
    "main-flows": [
        {
            "name": "PlaceOrder",
            "flows": [
                {"flow": "checkout"},
                {"task": "order.notify"},
            ],
        },
    ],
})

# 3. Run it.
result = app.execute_flow("PlaceOrder", {"sn": "1234"})
print(result.get())
# -> {"sn": "1234", "charged": True, "notified": True}
```

### YAML configuration

```yaml
# flows.yaml
work-flows:
  - name: checkout
    tasks:
      - order.validate
      - order.charge

main-flows:
  - name: PlaceOrder
    flows:
      - flow: checkout
      - task: order.notify
```

```python
FlowBuilder.from_yaml(app, "flows.yaml")
# Globs work too:
FlowBuilder.from_yaml(app, "config/flows/*.yaml")
```

### Conditional steps

```python
FlowBuilder.from_dict(app, {
    "main-flows": [
        {
            "name": "RefundFlow",
            "flows": [
                {"task": "order.validate"},
                # Only run charge() when buy_action is NEW_SUBSCRIPTION,
                # and the amount is >= 100.
                {"task": "order.charge",
                 "condition": {"buy_action": {"$eq": "NEW_SUBSCRIPTION"},
                               "amount":     {"$gte": 100}}},
            ],
        },
    ],
})
```

When a step's condition fails at runtime, `apply_async` raises
`celeryflow.exceptions.ConditionFailed`. Wrap it in a `link_error` /
custom callback if you want the chain to continue silently instead of
propagating the error.

### Custom operators

```python
from celeryflow.conditions import register_operator

register_operator("$startswith",
                  lambda actual, expected: isinstance(actual, str)
                                            and actual.startswith(expected))
```

### Lifecycle logging

```python
import logging
from celeryflow import EventTask

EventTask.bind_logger(logging.getLogger("orders"))
```

Each task invocation emits `Start` / `End` / `Retry` / `Failed` log
records with extras: `task_id`, `process_id`, `parent_id`, `task_name`,
and (when present) `correlation_id` from the payload.

## Development

```bash
git clone https://github.com/yu-ting/celeryflow.git
cd celeryflow
uv sync --all-extras

# Unit tests (no broker needed):
uv run pytest -m "not integration"

# Integration tests (need Redis):
docker compose up -d
uv run pytest -m integration
docker compose down

# Lint:
uv run ruff check src tests
```

## Project layout

```
celeryflow/
├── src/celeryflow/
│   ├── __init__.py        public API
│   ├── app.py             CeleryFlow Celery subclass
│   ├── builder.py         FlowBuilder — parse and register flows
│   ├── tasks.py           FlowTask / EventTask base classes
│   ├── conditions.py      operator DSL
│   ├── encoding.py        JSON encoder (Decimal / Enum / datetime)
│   ├── utils.py           StringConvert / DictConvert
│   └── exceptions.py
├── tests/
├── docker-compose.yml     Redis for integration tests
└── pyproject.toml         uv-managed project metadata
```

## Coming from plain Celery

If you already use Celery, CeleryFlow is additive — your existing
`@app.task` definitions, broker configuration, workers, and monitoring
all keep working unchanged. CeleryFlow only adds:

* `CeleryFlow(...)` — a `Celery` subclass. Drop-in replacement; accepts
  the same args plus a `worker_name` (used to strip a redundant
  ``{worker_name}.tasks.`` prefix from task names).
* `EventTask` — a `Task` subclass. Use it as `base=EventTask` on tasks
  that should participate in flows, or just use it everywhere if you
  want the lifecycle logging.
* `FlowBuilder.from_*` — registers extra "entry" tasks that splice
  the flow body into `self.request.chain` at runtime. The tasks you
  reference inside a flow are your normal Celery tasks.

A minimal migration looks like::

    # before
    from celery import Celery
    app = Celery("my_worker", broker="redis://...")

    @app.task
    def step_a(payload): ...

    @app.task
    def step_b(payload): ...

    # after
    from celeryflow import CeleryFlow, EventTask, FlowBuilder
    app = CeleryFlow("my_worker", broker="redis://...")

    @app.task(base=EventTask)
    def step_a(payload): ...

    @app.task(base=EventTask)
    def step_b(payload): ...

    FlowBuilder.from_dict(app, {
        "main-flows": [
            {"name": "MyFlow",
             "flows": [{"task": "step_a"}, {"task": "step_b"}]},
        ],
    })

    app.execute_flow("MyFlow", payload)

You can adopt it incrementally — flows live alongside any direct
`chain(...) / group(...) / chord(...)` you already build by hand.

## License

MIT — see [LICENSE](LICENSE).
