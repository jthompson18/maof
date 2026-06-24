"""L1 processing of declared context delegation.

When a specialized/third-party agent declares that it side-loads its own context,
the L1 must: (1) de-duplicate — stop assembling what the agent supplies; (2) satisfy
the contract — verify every ``requires_from_l1`` is present; (3) record — stamp a
``delegated_context`` block + emit ``context_delegated``; (4) govern (policy/RBAC,
handled by the caller). An undeclared side-load is a trust/observability violation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.errors import MAOFError
from maof.observability.events import AuditEvent

if TYPE_CHECKING:
    from maof.observability.events import EventSink
    from maof.registry.models import ContextDeclaration
    from maof.types import ContextEnvelope


class ContextDelegationError(MAOFError):
    """A declared delegation's ``requires_from_l1`` could not be satisfied."""


def _available_context_keys(env: ContextEnvelope) -> set[str]:
    keys = set(env.policy_flags) | set(env.semantic_model)
    keys |= {dp.alias for dp in env.data_pointers}
    keys |= {tool.name for tool in env.toolset}
    return keys


async def process_context_delegations(
    env: ContextEnvelope,
    agent_name: str,
    declarations: list[ContextDeclaration],
    *,
    event_sink: EventSink | None = None,
    tenant_id: str = "",
    intent_id: str | None = None,
) -> None:
    available = _available_context_keys(env)
    for decl in declarations:
        # (2) satisfy the contract before routing to the agent
        missing = [req for req in decl.requires_from_l1 if req not in available]
        if missing:
            raise ContextDelegationError(
                f"agent {agent_name!r} declaration {decl.id!r} requires context "
                f"{missing} which the L1 did not assemble"
            )
        # (1) de-duplicate — do not redundantly carry context the agent self-supplies
        env.data_pointers = [dp for dp in env.data_pointers if dp.alias not in decl.supplies]
        for supplied in decl.supplies:
            env.semantic_model.pop(supplied, None)
        # (3) record the delegation in the envelope + audit
        block = env.extras.setdefault("delegated_context", [])
        block.append(
            {
                "agent": agent_name,
                "id": decl.id,
                "kind": decl.kind,
                "scope": decl.scope,
                "source_ref": decl.source_ref,
            }
        )
        if event_sink is not None:
            await event_sink.emit(
                AuditEvent(
                    tenant_id=tenant_id,
                    intent_id=intent_id,
                    event_type="context_delegated",
                    envelope={"agent": agent_name, "declaration": decl.id},
                    details={
                        "kind": decl.kind,
                        "scope": decl.scope,
                        "supplies": decl.supplies,
                        "source_ref": decl.source_ref,
                    },
                )
            )


__all__ = ["process_context_delegations", "ContextDelegationError"]
