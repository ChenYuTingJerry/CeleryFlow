"""End-to-end example you can run against an in-memory broker.

    python examples/quickstart.py

Prints the final payload after the flow runs in eager mode (no worker needed).
"""
from __future__ import annotations

from celeryflow import CeleryFlow, EventTask, FlowBuilder

app = CeleryFlow("demo")
app.conf.update(
    broker_url="memory://",
    result_backend="cache+memory://",
    task_always_eager=True,
    task_eager_propagates=True,
    task_store_eager_result=True,
)


@app.task(name="order.validate", base=EventTask)
def validate(payload):
    assert "sn" in payload, "sn is required"
    return payload


@app.task(name="order.charge", base=EventTask)
def charge(payload):
    payload = dict(payload)
    payload["charged"] = True
    return payload


@app.task(name="order.notify", base=EventTask)
def notify(payload):
    payload = dict(payload)
    payload["notified"] = True
    return payload


FlowBuilder.from_dict(
    app,
    {
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
    },
)


if __name__ == "__main__":
    result = app.execute_flow("PlaceOrder", {"sn": "1234"})
    print(result.get())
