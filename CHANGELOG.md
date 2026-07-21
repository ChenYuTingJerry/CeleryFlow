# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-07-21

### Removed

- `FlowTask.insert_to_chain`. The flow entry task now splices via
  `Task.replace` (see Fixed), so this method was unused, and it reached into
  Celery request/canvas internals (`self.request.chain`, `canvas._chain`).
  Removing it drops that coupling. This is why the next release is a minor
  bump.

### Fixed

- Dispatching a flow entry task to a worker on its own now runs the flow
  body. Before this, sending a flow with `.delay()` / `.apply_async()`
  returned the payload unchanged and none of the steps ran. The entry task
  spliced the flow by mutating `self.request.chain`, but `EventTask`'s
  lifecycle-logging `__call__` makes the worker run the entry task through a
  nested request, so that mutation was dropped before the worker's tracer
  read it. The flow body now uses Celery's public `Task.replace`, which
  dispatches the chain itself instead of relying on that mutation.
  `execute_flow()` (in-process) and flows dispatched as part of a larger
  chain were not affected.

## [0.1.1] - 2026-06-01

### Fixed

- Flow entry tasks are now bound to the app when registered, so a real
  Celery worker can boot with flows registered. Before this, registering
  any flow crashed the worker at startup with
  `AttributeError: 'NoneType' object has no attribute 'push'`. Eager mode
  and `execute_flow()` were not affected.
- Per-step conditions on `main-flows` inline tasks are now carried into
  the task signature options. Before this, a condition set on an inline
  task inside a main flow was dropped at build time.

## [0.1.0] - 2026-05-16

First stable release. No code changes since `0.1.0a1`. Promoted to
PyPI after verifying the full release pipeline through TestPyPI
(workflow trigger, OIDC publish, clean-venv install, quickstart run).

## [0.1.0a1] - 2026-05-15

First pre-release. Published to TestPyPI to dry-run the release
pipeline (Trusted Publisher OIDC, tag-driven workflow) before the
v0.1.0 stable release.

### Added

- `CeleryFlow`: `Celery` subclass that strips the
  `{worker_name}.tasks.` prefix from task names and provides an
  `execute_flow()` helper.
- `FlowTask` and `EventTask` base classes. `FlowTask` owns the
  chain-splicing logic. `EventTask` adds optional condition gating
  and lifecycle logging (`Start` / `End` / `Retry` / `Failed`).
- `FlowBuilder`: registers flow tasks from a Python dict, a YAML
  file, or a JSON file. Glob patterns and config merging supported.
  Accepts both the modern `work-flows` + `main-flows` schema and a
  legacy `flow-definitions` + `flows` schema.
- Mongo-style condition operators: `$eq`, `$ne`, `$gt`, `$gte`,
  `$lt`, `$lte`, `$in`, `$nin`. Extensible through
  `register_operator()`.
- `CeleryFlowJSONEncoder`: JSON encoder that handles `Decimal`,
  `Enum`, and `datetime` / `date`.
- `StringConvert` / `DictConvert`: recursive snake_case / camelCase
  conversion helpers.
- `__version__` resolved dynamically from package metadata so
  `pyproject.toml` is the single source of truth.
- 39 unit tests, 2 integration tests (Redis-backed), GitHub Actions
  CI across Python 3.11 / 3.12 / 3.13.

[Unreleased]: https://github.com/ChenYuTingJerry/CeleryFlow/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/ChenYuTingJerry/CeleryFlow/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/ChenYuTingJerry/CeleryFlow/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ChenYuTingJerry/CeleryFlow/releases/tag/v0.1.0
[0.1.0a1]: https://github.com/ChenYuTingJerry/CeleryFlow/releases/tag/v0.1.0a1
