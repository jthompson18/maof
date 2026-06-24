"""A2A cards verified against the official ``a2a-sdk`` types.

The dict-level mapping in maof.registry.a2a is validated here against the real
SDK's protobuf ``AgentCard``: exported cards must parse strictly (unknown or
ill-typed fields fail), and genuine SDK-built cards must import. Gated only on
the ``a2a-sdk`` package being installed.
"""

from __future__ import annotations

import pytest

pytest.importorskip("a2a")

from a2a.types import AgentCard, AgentInterface, AgentSkill  # noqa: E402
from google.protobuf.json_format import MessageToDict, ParseDict  # noqa: E402

from maof.registry.a2a import (  # noqa: E402
    MAOF_EXTENSION_URI,
    agent_card_to_manifest,
    manifest_to_agent_card,
)
from maof.registry.models import AgentManifest  # noqa: E402


def _manifest() -> AgentManifest:
    return AgentManifest(
        id="commitments",
        kind="l2_agent",
        name="Commitments",
        version="v1",
        endpoint="https://agents.example.com/commitments",
        capabilities=["funding"],
        accepted_task_types=["funds_commit", "reconciliation"],
        provided_schemas=["funds_commit.v1"],
        rbac_scopes=["buy:commit"],
        tenancy="tenant",
        description="Funds-committing platform agent",
    )


def test_exported_card_parses_as_sdk_agent_card() -> None:
    card = manifest_to_agent_card(_manifest())
    message = ParseDict(card, AgentCard())  # strict: unknown fields raise ParseError
    assert message.name == "Commitments"
    assert message.description == "Funds-committing platform agent"
    assert [s.id for s in message.skills] == ["funds_commit", "reconciliation"]
    assert message.supported_interfaces[0].url == "https://agents.example.com/commitments"
    extension = message.capabilities.extensions[0]
    assert extension.uri == MAOF_EXTENSION_URI
    params = dict(extension.params)
    assert params["maof_id"] == "commitments"
    assert list(params["rbac_scopes"]) == ["buy:commit"]


def test_sdk_built_card_imports_and_round_trips() -> None:
    sdk_card = AgentCard(
        name="External Expediter",
        description="Third-party expediting agent",
        version="2.1",
        supported_interfaces=[AgentInterface(url="https://partner.example.com/a2a")],
        skills=[
            AgentSkill(
                id="expedite_order",
                name="Expedite order",
                description="Pull in delivery dates",
                tags=["logistics"],
            )
        ],
    )
    manifest = agent_card_to_manifest(MessageToDict(sdk_card))
    assert manifest.name == "External Expediter"
    assert manifest.endpoint == "https://partner.example.com/a2a"
    assert manifest.accepted_task_types == ["expedite_order"]
    assert manifest.capabilities == ["logistics"]
    assert manifest.description == "Third-party expediting agent"
    # And the import is re-exportable as a valid card again.
    ParseDict(manifest_to_agent_card(manifest), AgentCard())


def test_legacy_subset_card_still_imports() -> None:
    legacy = {
        "name": "Legacy",
        "version": "v1",
        "url": "https://legacy.example.com",
        "skills": [{"id": "old_task", "name": "old_task", "tags": ["legacy"]}],
        "metadata": {"maof_id": "legacy-agent", "rbac_scopes": ["x:y"], "tenancy": "global"},
    }
    manifest = agent_card_to_manifest(legacy)
    assert manifest.id == "legacy-agent"
    assert manifest.endpoint == "https://legacy.example.com"
    assert manifest.rbac_scopes == ["x:y"]
    assert manifest.tenancy == "global"
