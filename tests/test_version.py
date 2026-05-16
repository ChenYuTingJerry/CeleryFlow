"""Tests for celeryflow.__version__.

The runtime version is read dynamically from package metadata via
importlib.metadata, so it can drift out of sync with pyproject.toml in
two scenarios these tests catch:

1. The package isn't actually installed (e.g. someone ran pytest
   against a source tree without `pip install -e .`).
2. Two source-of-truth declarations got out of sync.
"""
from __future__ import annotations

from pathlib import Path

import celeryflow


def test_version_is_a_real_string():
    """__version__ should be non-empty and not the unknown fallback."""
    v = celeryflow.__version__
    assert isinstance(v, str) and v
    assert v != "0.0.0+unknown", (
        "celeryflow.__version__ fell back to the unknown placeholder. "
        "Is the package installed in the environment running the tests? "
        "Try `uv sync --all-extras` or `pip install -e .`."
    )


def test_version_matches_pyproject():
    """__version__ should match what pyproject.toml declares."""
    try:
        import tomllib  # Python 3.11+
    except ImportError:  # pragma: no cover - Python 3.10 dev environments only
        import tomli as tomllib  # type: ignore[import-not-found,no-redef]

    here = Path(__file__).resolve()
    for parent in here.parents:
        pyproject = parent / "pyproject.toml"
        if pyproject.exists():
            break
    else:
        raise AssertionError("could not locate pyproject.toml")

    declared = tomllib.loads(pyproject.read_text())["project"]["version"]
    assert celeryflow.__version__ == declared
