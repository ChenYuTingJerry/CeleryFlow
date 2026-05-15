# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-15

First public release.

### Added

- `CeleryFlow` — `Celery` subclass that strips the
  `{worker_name}.tasks.` prefix from task names and provides an
  `execute_flow()` helper.
- `FlowTask` and `EventTask` base classes. `FlowTask` owns the
  chain-splicing logic; `EventTask` adds optional condition gating
  and lifecycle logging (`Start` / `End` / `Retry` / `Failed`).
- `FlowBuilder` — registers flow tasks from a Python dict, a YAML
  file, or a JSON file. Glob patterns and config merging supported.
  Accepts both the modern `work-flows` + `main-flows` schema and a
  legacy `flow-definitions` + `flows` schema.
- Mongo-style condition operators: `$eq`, `$ne`, `$gt`, `$gte`,
  `$lt`, `$lte`, `$in`, `$nin`. Extensible through
  `register_operator()`.
- `CeleryFlowJSONEncoder` — JSON encoder that handles `Decimal`,
  `Enum`, and `datetime` / `date`.
- `StringConvert` / `DictConvert` — recursive snake_case /
  camelCase conversion helpers.
- 37 unit tests, 2 integration tests (Redis-backed), GitHub Actions
  CI across Python 3.11 / 3.12 / 3.13.

[Unreleased]: https://github.com/ChenYuTingJerry/CeleryFlow/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ChenYuTingJerry/CeleryFlow/releases/tag/v0.1.0
