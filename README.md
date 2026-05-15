# CeleryFlow

[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

CeleryFlow lets you describe a multi-step Celery workflow as a config
file (YAML, JSON, or a plain Python dict) instead of wiring `chain()`,
`group()`, and `chord()` by hand. Each step can have a condition, so
you can skip parts of the flow based on the payload at runtime.

I used a version of this internally for a few years to run order and
billing pipelines. This is the cleaned-up open-source version with the
business-specific stuff removed.

## What you get

- Define flows in YAML, JSON, or a Python dict.
- Reuse named sub-flows inside larger flows.
- Skip steps with conditions like `{"sn": {"$eq": "1234"}}` —
  `$eq`, `$ne`, `$in`, `$gte`, etc. You can add your own.
- Optional structured logging on start / success / failure / retry.
- The flows are still regular Celery tasks. Retries, routing, and
  monitoring keep working the way you expect.

## Install

```bash
pip install celeryflow              # core
pip install "celeryflow[yaml]"      # if you want YAML configs
pip install "celeryflow[redis]"     # if you want Redis as the broker
```

Python 3.11 or newer. During development I use
[uv](https://docs.astral.sh/uv/), but you don't need it to install the
package.

## Quickstart

```python
from celeryflow import CeleryFlow, EventTask, FlowBuilder

app = CeleryFlow("my_worker")
app.conf.update(broker_url="redis://localhost:6379/0",
                result_backend="redis://localhost:6379/0")

# 1. Write some tasks.
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
```

2. Describe the flow. CeleryFlow accepts the definition in three
formats — a Python dict, a YAML file, or a JSON file. Pick whichever
fits your project.

Option A — Python dict, inline:

```python
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
```

Option B — YAML file:

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
# Globs work too — multiple files get merged:
FlowBuilder.from_yaml(app, "config/flows/*.yaml")
```

Option C — JSON file:

```json
{
  "work-flows": [
    {"name": "checkout", "tasks": ["order.validate", "order.charge"]}
  ],
  "main-flows": [
    {
      "name": "PlaceOrder",
      "flows": [
        {"flow": "checkout"},
        {"task": "order.notify"}
      ]
    }
  ]
}
```

```python
FlowBuilder.from_json(app, "flows.json")
```

All three produce the same `PlaceOrder` task. Then run it:

```python
# 3. Run it.
result = app.execute_flow("PlaceOrder", {"sn": "1234"})
print(result.get())
# -> {"sn": "1234", "charged": True, "notified": True}
```

### Conditional steps

```python
FlowBuilder.from_dict(app, {
    "main-flows": [
        {
            "name": "RefundFlow",
            "flows": [
                {"task": "order.validate"},
                # Only charge when it's a new subscription worth >= 100.
                {"task": "order.charge",
                 "condition": {"buy_action": {"$eq": "NEW_SUBSCRIPTION"},
                               "amount":     {"$gte": 100}}},
            ],
        },
    ],
})
```

When a condition doesn't match, `apply_async` raises
`celeryflow.exceptions.ConditionFailed`. Catch it in a `link_error` or
custom callback if you'd rather skip the step quietly instead of
failing the chain.

### Adding your own operator

```python
from celeryflow.conditions import register_operator

register_operator("$startswith",
                  lambda actual, expected: isinstance(actual, str)
                                            and actual.startswith(expected))
```

### Logging

```python
import logging
from celeryflow import EventTask

EventTask.bind_logger(logging.getLogger("orders"))
```

Each task call will log `Start`, `End`, `Retry`, or `Failed`. The log
records carry `task_id`, `process_id`, `parent_id`, `task_name`, and a
`correlation_id` if the payload has one.

## Development

```bash
git clone https://github.com/ChenYuTingJerry/CeleryFlow.git
cd CeleryFlow
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
│   ├── builder.py         FlowBuilder — parses and registers flows
│   ├── tasks.py           FlowTask / EventTask base classes
│   ├── conditions.py      condition operators
│   ├── encoding.py        JSON encoder (Decimal / Enum / datetime)
│   ├── utils.py           StringConvert / DictConvert
│   └── exceptions.py
├── tests/
├── docker-compose.yml     Redis for integration tests
└── pyproject.toml
```

## If you already use Celery

CeleryFlow doesn't change anything you already have. Your `@app.task`
definitions, broker, workers, and monitoring all keep working. The
three things CeleryFlow adds are:

- `CeleryFlow(...)` — a `Celery` subclass. Use it instead of `Celery`.
  Takes a `worker_name` and otherwise behaves the same.
- `EventTask` — a `Task` subclass. Pass `base=EventTask` to tasks you
  want in a flow. (You can also use it on every task if you just want
  the logging.)
- `FlowBuilder.from_*` — registers an entry task per flow. When that
  entry task runs, it splices the rest of the flow into the chain.

So a minimal switch looks like:

```python
# before
from celery import Celery
app = Celery("my_worker", broker="redis://...")

@app.task
def step_a(payload): ...

@app.task
def step_b(payload): ...
```

```python
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
```

You can adopt it bit by bit. Flows live alongside any `chain()` /
`group()` / `chord()` you already build by hand.

## License

MIT. See [LICENSE](LICENSE).
