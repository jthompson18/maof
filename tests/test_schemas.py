"""Schema registry: the one built-in (generic_task.v1) + runtime registration."""

from __future__ import annotations

import pytest

from maof.errors import SchemaValidationError
from maof.schemas.registry import GENERIC_TASK_SCHEMA_ID, SchemaRegistry


def test_only_one_builtin_schema() -> None:
    reg = SchemaRegistry()
    assert reg.schema_ids() == [GENERIC_TASK_SCHEMA_ID]  # MAOF ships exactly one


def test_generic_task_accepts_valid() -> None:
    reg = SchemaRegistry()
    reg.validate(
        GENERIC_TASK_SCHEMA_ID,
        {"task_type": "generic_task", "description": "do the thing", "priority": 5},
    )


def test_generic_task_priority_bounds() -> None:
    reg = SchemaRegistry()
    for bad in (0, 10, -1):
        with pytest.raises(SchemaValidationError):
            reg.validate(
                GENERIC_TASK_SCHEMA_ID,
                {"task_type": "generic_task", "description": "x", "priority": bad},
            )


def test_generic_task_rejects_empty_description() -> None:
    reg = SchemaRegistry()
    with pytest.raises(SchemaValidationError):
        reg.validate(
            GENERIC_TASK_SCHEMA_ID,
            {"task_type": "generic_task", "description": "", "priority": 5},
        )


def test_generic_task_rejects_wrong_task_type() -> None:
    reg = SchemaRegistry()
    with pytest.raises(SchemaValidationError):
        reg.validate(
            GENERIC_TASK_SCHEMA_ID,
            {"task_type": "funds_commit", "description": "x", "priority": 5},
        )


def test_runtime_registration_and_validate() -> None:
    reg = SchemaRegistry()
    reg.register(
        "funds_commit.v1",
        {
            "type": "object",
            "required": ["task_type", "amount_usd"],
            "properties": {
                "task_type": {"const": "funds_commit"},
                "amount_usd": {"type": "number", "minimum": 0},
            },
        },
    )
    assert reg.is_registered("funds_commit.v1")
    reg.validate("funds_commit.v1", {"task_type": "funds_commit", "amount_usd": 1000})
    with pytest.raises(SchemaValidationError):
        reg.validate("funds_commit.v1", {"task_type": "funds_commit"})  # missing amount_usd


def test_unknown_schema_raises() -> None:
    reg = SchemaRegistry()
    with pytest.raises(SchemaValidationError):
        reg.validate("nope.v1", {"x": 1})


def test_register_invalid_schema_raises() -> None:
    reg = SchemaRegistry()
    with pytest.raises(SchemaValidationError):
        reg.register("bad.v1", {"type": "not-a-real-type"})
