"""A2A Agent Card <-> AgentManifest mapping.

MCP is the tool/context protocol; A2A is the cross-organization agent protocol.
MAOF speaks both: an approved A2A Agent Card can be imported into the discovery
registry, and MAOF's agents can be exported as Agent Cards. Adapter only — the
core stays protocol-neutral (dict-level; no hard ``a2a-sdk`` dependency).

Export targets the A2A 1.x card schema (validated against the official SDK's
``AgentCard`` type in tests): endpoints ride ``supportedInterfaces`` and
MAOF-specific manifest fields ride a declared ``AgentExtension`` under
``capabilities.extensions`` — the spec's sanctioned slot for vendor data.
Import accepts both that shape and the legacy subset (top-level ``url`` +
``metadata``) so older cards keep working.
"""

from __future__ import annotations

from typing import Any

from maof.registry.models import AgentManifest

#: Extension URI under which MAOF manifest fields travel on an Agent Card.
MAOF_EXTENSION_URI = "urn:maof:manifest"

#: Transport binding declared on exported cards (A2A ``TransportProtocol.JSONRPC``).
_DEFAULT_BINDING = "JSONRPC"

_RESERVED_PARAMS = ("maof_id", "rbac_scopes", "provided_schemas", "tenancy")


def manifest_to_agent_card(manifest: AgentManifest) -> dict[str, Any]:
    """Export a manifest as an A2A 1.x Agent Card (JSON/camelCase wire shape)."""
    return {
        "name": manifest.name,
        "description": manifest.description,
        "version": manifest.version,
        "supportedInterfaces": [{"url": manifest.endpoint, "protocolBinding": _DEFAULT_BINDING}],
        "capabilities": {
            "streaming": False,
            "extensions": [
                {
                    "uri": MAOF_EXTENSION_URI,
                    "description": "MAOF discovery-registry manifest fields",
                    "params": {
                        "maof_id": manifest.id,
                        "rbac_scopes": list(manifest.rbac_scopes),
                        "provided_schemas": list(manifest.provided_schemas),
                        "tenancy": manifest.tenancy,
                        **manifest.metadata,
                    },
                }
            ],
        },
        "defaultInputModes": ["application/json"],
        "defaultOutputModes": ["application/json"],
        "skills": [
            {
                "id": task_type,
                "name": task_type,
                "description": f"Accepts MAOF task type {task_type!r}",
                "tags": list(manifest.capabilities),
            }
            for task_type in manifest.accepted_task_types
        ],
    }


def _first(card: dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Read a card field tolerating camelCase and proto snake_case spellings."""
    for key in keys:
        if key in card:
            return card[key]
    return default


def _maof_params(card: dict[str, Any]) -> dict[str, Any]:
    """MAOF fields from the declared extension, overlaid by legacy ``metadata``."""
    params: dict[str, Any] = {}
    capabilities = card.get("capabilities") or {}
    for extension in capabilities.get("extensions") or []:
        if extension.get("uri") == MAOF_EXTENSION_URI:
            params.update(extension.get("params") or {})
    params.update(card.get("metadata") or {})  # legacy subset wins on conflict
    return params


def agent_card_to_manifest(card: dict[str, Any]) -> AgentManifest:
    """Import an A2A Agent Card into a manifest (still passes the signed lifecycle)."""
    params = _maof_params(card)
    endpoint = str(card.get("url", ""))  # legacy top-level url
    if not endpoint:
        interfaces = _first(card, "supportedInterfaces", "supported_interfaces", default=[]) or []
        if interfaces:
            endpoint = str(interfaces[0].get("url", ""))
    skills = card.get("skills") or []
    task_types = [s["id"] for s in skills if "id" in s]
    return AgentManifest(
        id=str(params.get("maof_id", card["name"])),
        kind="l2_agent",
        name=card["name"],
        version=str(card.get("version", "v1")),
        endpoint=endpoint,
        capabilities=sorted({tag for s in skills for tag in s.get("tags", [])}),
        accepted_task_types=task_types,
        description=str(card.get("description", "")),
        provided_schemas=list(params.get("provided_schemas", [])),
        rbac_scopes=list(params.get("rbac_scopes", [])),
        tenancy=params.get("tenancy", "tenant"),
        metadata={k: v for k, v in params.items() if k not in _RESERVED_PARAMS},
    )


__all__ = ["manifest_to_agent_card", "agent_card_to_manifest", "MAOF_EXTENSION_URI"]
