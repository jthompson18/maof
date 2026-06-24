"""Source-of-truth agents — pure adopter code.

The catalog agent and shared datastore (datastore) agent are built by the platform vendor
product teams and INJECTED through the trust registry; MAOF hosts them. Here
they are small MCP-shaped clients (``read_resource`` / ``call_tool``) plus the
manifests that register them as a required context source, JIT resolvers
(``catalog://`` / ``datastore://``), and the post_result conformance authority.
"""

from __future__ import annotations

import re
from typing import Any

from maof.registry.models import AgentManifest, ContextDeclaration

#: The naming catalog the console surface manages: enumerated regions and the
#: placement-name grammar every vendor artifact must follow.
CATALOG_VERSION = "tax-v3"
CATALOG_REGIONS = ["east", "west", "central"]
ORDER_CODE_PATTERN = r"^PO_(EAST|WEST|CENTRAL)_[A-Z0-9_]+$"


class CatalogClient:
    """MCP-shaped client for the catalog agent: serves the current catalog
    slice as context and validates names on demand (consulted via ``ctx.agents``)."""

    def __init__(self, *, down: bool = False) -> None:
        self.down = down
        self.reads: list[str] = []
        self.validations: list[str] = []

    async def read_resource(self, ref: str) -> dict[str, Any]:
        if self.down:
            raise ConnectionError("catalog agent unreachable")
        self.reads.append(ref)
        return {
            "version": CATALOG_VERSION,
            "regions": list(CATALOG_REGIONS),
            "order_code_pattern": ORDER_CODE_PATTERN,
        }

    async def call_tool(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        if self.down:
            raise ConnectionError("catalog agent unreachable")
        if name != "validate":
            raise ValueError(f"unknown catalog tool: {name!r}")
        candidate = str(args.get("name", ""))
        self.validations.append(candidate)
        valid = re.match(ORDER_CODE_PATTERN, candidate) is not None
        return {
            "valid": valid,
            "reason": "" if valid else f"name must match {ORDER_CODE_PATTERN}",
        }


class DatastoreClient:
    """MCP-shaped client for the shared datastore: serves ``datastore://`` references
    (rate cards, plans, performance) just-in-time so big data never inlines."""

    RESOURCES: dict[str, dict[str, Any]] = {
        "rate-card/east": {"region": "east", "unit_price_usd": 32.0, "currency": "USD"},
        "rate-card/west": {"region": "west", "unit_price_usd": 18.5, "currency": "USD"},
        "performance/q2": {"orders_filled": 48_000_000, "unit_cost_usd": 41.2},
    }

    def __init__(self) -> None:
        self.reads: list[str] = []

    async def read_resource(self, ref: str) -> dict[str, Any]:
        self.reads.append(ref)
        return dict(self.RESOURCES.get(ref, {"ref": ref, "found": False}))

    async def call_tool(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        raise ValueError("the datastore agent serves resources only")


CATALOG_MANIFEST = AgentManifest(
    id="catalog",
    kind="context_source",
    name="Naming Catalog Agent",
    version="v3",
    endpoint="mcp://platform/catalog",
    context_tags=["naming"],
    tenancy="tenant",
    description=(
        "Naming catalog source of truth: purchase cycle/placement naming hierarchies, "
        "enumerated region values with semantic descriptions, name validation"
    ),
    resolver_schemes=["catalog"],
    required=True,  # planners MUST see the catalog: outage fails runs closed
    side_loaded_context=[
        ContextDeclaration(
            id="catalog_values",
            kind="lookup_table",
            description="enumerated naming values + placement grammar",
            scope="tenant",
            supplies=["naming"],
            mutable=False,  # immutable per version: cacheable across runs
        )
    ],
)

DATASTORE_MANIFEST = AgentManifest(
    id="datastore",
    kind="context_source",
    name="Shared Datastore Agent",
    version="v1",
    endpoint="mcp://platform/datastore",
    tenancy="tenant",
    description=(
        "Unified data layer: purchase plans, vendor rate cards, and performance "
        "data served just-in-time by reference"
    ),
    resolver_schemes=["datastore"],
    required=False,
)

__all__ = [
    "CatalogClient",
    "DatastoreClient",
    "CATALOG_MANIFEST",
    "DATASTORE_MANIFEST",
    "CATALOG_VERSION",
    "CATALOG_REGIONS",
    "ORDER_CODE_PATTERN",
]
