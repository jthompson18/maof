"""Discovery-registry models.

These are intentionally Pydantic models (the spec sketches them as dataclasses):
the registry canonicalizes and **signs** over them, so deterministic
serialization and validation are load-bearing here.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from maof.types import utcnow


class ContextDeclaration(BaseModel):
    """A specialized/third-party agent's declaration that it side-loads its own
    context. The L1 does NOT load ``source_ref``; it only records the
    delegation, de-duplicates ``supplies``, and verifies ``requires_from_l1``.
    """

    id: str
    kind: str  # "yaml_config" | "lookup_table" | "ruleset" | "embeddings" | "schema_profile" | ...
    description: str
    scope: Literal["global", "tenant"]
    supplies: list[str] = Field(default_factory=list)
    requires_from_l1: list[str] = Field(default_factory=list)
    source_ref: str | None = None  # provenance only (path/uri/pkg) — for audit
    mutable: bool = False


class AgentManifest(BaseModel):
    """A registry record describing an agent/MCP/context source. Aligned
    with A2A Agent Cards so MAOF interoperates across org boundaries."""

    id: str
    kind: Literal["l2_agent", "mcp_server", "context_source"]
    name: str
    version: str
    endpoint: str  # mcp stdio cmd / http url / python entrypoint
    capabilities: list[str] = Field(default_factory=list)
    accepted_task_types: list[str] = Field(default_factory=list)
    provided_schemas: list[str] = Field(default_factory=list)
    rbac_scopes: list[str] = Field(default_factory=list)
    context_tags: list[str] = Field(default_factory=list)
    tenancy: Literal["global", "tenant"] = "tenant"
    side_loaded_context: list[ContextDeclaration] = Field(default_factory=list)
    # Hosting + registry-intelligence surfaces:
    description: str = ""  # semantic capability description — embedded for search
    resolver_schemes: list[str] = Field(default_factory=list)  # adopter-defined JIT schemes served
    queue: str | None = None  # registry-driven routing target (fallback tasks.<task_type>)
    required: bool = False  # context_source only: fail runs closed when unavailable
    certification: dict[str, Any] | None = None  # {"dataset_ref", "min_pass_rate"} gate on approve
    canary_pct: float = 0.0  # deterministic per-run canary cohort for this entry version
    metadata: dict[str, Any] = Field(default_factory=dict)


class RegistryEntry(BaseModel):
    """A persisted registry record + its trust state. The loader returns
    only entries that are ``approved`` AND whose signature verifies."""

    manifest: AgentManifest
    status: Literal["pending", "approved", "revoked"] = "pending"
    signature: str | None = None
    kid: str | None = None
    submitted_at: str = Field(default_factory=utcnow)
    approved_at: str | None = None


__all__ = ["ContextDeclaration", "AgentManifest", "RegistryEntry"]
