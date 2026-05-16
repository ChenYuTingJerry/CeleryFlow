# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/ChenYuTingJerry/CeleryFlow/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ChenYuTingJerry/CeleryFlow/releases/tag/v0.1.0
[0.1.0a1]: https://github.com/ChenYuTingJerry/CeleryFlow/releases/tag/v0.1.0a1
