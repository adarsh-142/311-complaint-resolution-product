"""Utilities for converting rich Python/Spark values into JSON-safe data.

The API boundary should only emit standard Python primitives so response
serialization is deterministic and does not depend on Spark or NumPy internals.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any


def _is_numpy_scalar(value: Any) -> bool:
    try:
        import numpy as np
    except Exception:
        return False

    return isinstance(value, np.generic)


def to_json_safe(value: Any) -> Any:
    """Recursively convert rich objects into JSON-serializable Python values.

    Supported conversions:
    - Spark Row objects -> dict
    - Decimal -> int/float when exact, otherwise string
    - date / datetime -> ISO 8601 string
    - tuple / set -> list
    - numpy scalar -> native Python scalar
    - dict / list / nested structures -> recursively sanitized
    """
    if value is None:
        return None

    if isinstance(value, dict):
        return {str(key): to_json_safe(item) for key, item in value.items()}

    if isinstance(value, list):
        return [to_json_safe(item) for item in value]

    if hasattr(value, "asDict") and callable(value.asDict):
        return to_json_safe(value.asDict(recursive=True))

    if isinstance(value, tuple):
        return [to_json_safe(item) for item in value]

    if isinstance(value, set):
        return [to_json_safe(item) for item in sorted(value, key=lambda item: repr(item))]

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)

    if _is_numpy_scalar(value):
        return value.item()

    if hasattr(value, "__dict__") and not isinstance(value, type):
        return to_json_safe(vars(value))

    return value
