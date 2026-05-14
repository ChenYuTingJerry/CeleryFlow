"""JSON encoding helpers.

Adds support for ``Decimal`` and ``Enum`` on top of the stdlib encoder, plus
``datetime``/``date`` (which the stock encoder also rejects). Useful when
logging task payloads or persisting flow state.
"""
from __future__ import annotations

import datetime as _dt
from decimal import Decimal
from enum import Enum
from json import JSONEncoder
from typing import Any


class CeleryFlowJSONEncoder(JSONEncoder):
    """JSON encoder that knows how to serialise common business-domain types."""

    def default(self, o: Any) -> Any:  # noqa: D401 - JSONEncoder API
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, Enum):
            return o.value
        if isinstance(o, (_dt.datetime, _dt.date)):
            return o.isoformat()
        return super().default(o)
