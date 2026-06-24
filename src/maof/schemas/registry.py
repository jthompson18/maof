"""Task-type -> schema validation.

MAOF ships **exactly one** built-in schema — ``generic_task.v1``. Every
domain-specific schema (``<task_type>.v1``, ``<task_type>.result.v1``) is
adopter-registered at runtime, keeping domain content out of the core
(domain schemas are always adopter-registered).
"""

from __future__ import annotations

from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

from maof.errors import SchemaValidationError

GENERIC_TASK_SCHEMA_ID = "generic_task.v1"

_GENERIC_TASK_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["task_type", "description", "priority"],
    "properties": {
        "task_type": {"const": "generic_task"},
        "description": {"type": "string", "minLength": 1},
        "priority": {"type": "integer", "minimum": 1, "maximum": 9},
    },
}


class SchemaRegistry:
    """Register JSON Schemas by ``schema_id`` and validate task bodies against them."""

    def __init__(self) -> None:
        self._validators: dict[str, Draft202012Validator] = {}
        self.register(GENERIC_TASK_SCHEMA_ID, _GENERIC_TASK_SCHEMA)

    def register(self, schema_id: str, schema: dict[str, Any]) -> None:
        try:
            Draft202012Validator.check_schema(schema)
        except SchemaError as exc:
            raise SchemaValidationError(
                f"invalid JSON Schema for {schema_id!r}: {exc.message}", schema_id=schema_id
            ) from exc
        self._validators[schema_id] = Draft202012Validator(schema)

    def validate(self, schema_id: str, body: dict[str, Any]) -> None:
        validator = self._validators.get(schema_id)
        if validator is None:
            raise SchemaValidationError(f"unknown schema_id: {schema_id!r}", schema_id=schema_id)
        try:
            validator.validate(body)
        except ValidationError as exc:
            raise SchemaValidationError(f"{schema_id}: {exc.message}", schema_id=schema_id) from exc

    def is_registered(self, schema_id: str) -> bool:
        return schema_id in self._validators

    def schema_ids(self) -> list[str]:
        return sorted(self._validators)


__all__ = ["SchemaRegistry", "GENERIC_TASK_SCHEMA_ID"]
