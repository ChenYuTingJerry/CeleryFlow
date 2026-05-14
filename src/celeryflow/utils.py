"""Generic string / dict conversion helpers used across event flows.

Useful when normalising payloads that come from different upstream services
(camelCase APIs, snake_case databases, etc.).
"""
from __future__ import annotations

import re
from typing import Any

_CAMEL_BOUNDARY_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_BOUNDARY_2 = re.compile(r"([a-z0-9])([A-Z])")


class StringConvert:
    """Case-style converters."""

    @staticmethod
    def to_snake_case(text: str) -> str:
        s1 = _CAMEL_BOUNDARY_1.sub(r"\1_\2", text)
        return _CAMEL_BOUNDARY_2.sub(r"\1_\2", s1).lower()

    @staticmethod
    def to_lower_camel_case(text: str) -> str:
        components = text.split("_")
        return components[0] + "".join(c.capitalize() for c in components[1:])

    @staticmethod
    def to_upper_camel_case(text: str) -> str:
        components = text.split("_")
        return "".join(c.capitalize() for c in components)


def _convert(obj: Any, key_fn) -> Any:
    if isinstance(obj, dict):
        return {key_fn(k): _convert(v, key_fn) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert(v, key_fn) for v in obj]
    return obj


class DictConvert:
    """Recursive key-case converters for nested dict/list structures."""

    @staticmethod
    def to_snake_case(obj: Any) -> Any:
        return _convert(obj, StringConvert.to_snake_case)

    @staticmethod
    def to_lower_camel_case(obj: Any) -> Any:
        return _convert(obj, StringConvert.to_lower_camel_case)

    @staticmethod
    def to_upper_camel_case(obj: Any) -> Any:
        return _convert(obj, StringConvert.to_upper_camel_case)
