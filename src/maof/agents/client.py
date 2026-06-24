"""Hosting machinery for source-of-truth agents.

- :class:`AgentClientFactory` — registry-resolved, RBAC-scoped MCP clients for
  agent→agent consultation (``ctx.agents``); every consultation is audited.
- :func:`attach_registry_context_sources` — approved ``context_source`` entries
  auto-attach to the ContextBuilder (``required`` fails closed; contributions
  cache per ``ContextDeclaration.mutable``).
- :func:`attach_registry_resolvers` — manifest ``resolver_schemes`` register as
  JIT reference loaders (adopter-defined schemes, e.g. ``catalog://``, ``datastore://``).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from maof.errors import MAOFError, RegistryTrustError
from maof.observability.events import AuditEvent
from maof.registry.loader import _scopes_granted

if TYPE_CHECKING:
    from maof.context.builder import ContextBuilder
    from maof.context.jit import DefaultReferenceResolver
    from maof.observability.events import EventSink
    from maof.registry.loader import RegistryLoader
    from maof.registry.models import AgentManifest
    from maof.types import ContextEnvelope, StageContext, TenantContext

#: Builds a connected client (MCP stdio/HTTP) for an approved manifest. Injected so
#: deployments choose their MCP client library; tests inject fakes.
ClientBuilder = Callable[["AgentManifest"], Any]


class AgentClientFactory:
    """Resolve an approved+signed registry agent into an RBAC-scoped client."""

    def __init__(
        self,
        loader: RegistryLoader,
        *,
        tenant: TenantContext,
        client_builder: ClientBuilder,
        event_sink: EventSink | None = None,
    ) -> None:
        self._loader = loader
        self._tenant = tenant
        self._client_builder = client_builder
        self._event_sink = event_sink
        self._cache: dict[str, Any] = {}

    async def client(self, agent_id: str) -> Any:
        if agent_id in self._cache:
            await self._emit(agent_id)
            return self._cache[agent_id]
        manifest = next((m for m in await self._loader.manifests() if m.id == agent_id), None)
        if manifest is None:
            raise RegistryTrustError(f"no approved registry agent {agent_id!r}")
        if not _scopes_granted(manifest, self._tenant):
            raise RegistryTrustError(
                f"tenant {self._tenant.tenant_id!r} lacks scopes for {agent_id!r}"
            )
        client = self._client_builder(manifest)
        self._cache[agent_id] = client
        await self._emit(agent_id)
        return client

    async def _emit(self, agent_id: str) -> None:
        if self._event_sink is None:
            return
        await self._event_sink.emit(
            AuditEvent(
                tenant_id=self._tenant.tenant_id,
                intent_id=None,
                event_type="agent_consulted",
                envelope={"agent": agent_id},
                details={},
            )
        )


class ContextSourceCache:
    """Caches context-source contributions keyed (entry_id, tenant, version)."""

    def __init__(self) -> None:
        self._cache: dict[tuple[str, str, str], Any] = {}

    def get(self, manifest: AgentManifest, tenant_id: str) -> Any | None:
        return self._cache.get((manifest.id, tenant_id, manifest.version))

    def set(self, manifest: AgentManifest, tenant_id: str, value: Any) -> None:
        self._cache[(manifest.id, tenant_id, manifest.version)] = value


class RegistryContextSource:
    """Wraps an approved ``context_source`` entry as a ContextBuilder source."""

    def __init__(
        self,
        manifest: AgentManifest,
        client: Any,
        *,
        cache: ContextSourceCache | None = None,
        event_sink: EventSink | None = None,
    ) -> None:
        # The stable registry id is the lookup key — display names are not contracts.
        self.name = manifest.id
        self._manifest = manifest
        self._client = client
        self._cache = cache
        self._event_sink = event_sink
        # mutable declarations force re-fetch every build (ContextDeclaration.mutable)
        self._cacheable = not any(d.mutable for d in manifest.side_loaded_context)

    async def contribute(self, sc: StageContext, env: ContextEnvelope) -> ContextEnvelope:
        data: Any | None = None
        if self._cache is not None and self._cacheable:
            data = self._cache.get(self._manifest, sc.tenant.tenant_id)
        if data is None:
            try:
                data = await self._client.read_resource(self._manifest.id)
            except Exception as exc:
                if self._manifest.required:
                    raise MAOFError(
                        f"required context source {self._manifest.id!r} unavailable "
                        f"(fail closed): {exc}"
                    ) from exc
                return env  # optional source: skip, keep planning
            if self._cache is not None and self._cacheable:
                self._cache.set(self._manifest, sc.tenant.tenant_id, data)
        env.semantic_model[self.name] = data
        env.extras.setdefault("registry_context_sources", []).append(self._manifest.id)
        if self._event_sink is not None:
            await self._event_sink.emit(
                AuditEvent(
                    tenant_id=sc.tenant.tenant_id,
                    intent_id=env.intent_id,
                    event_type="context_delegated",
                    envelope={"agent": self._manifest.id, "kind": "registry_context_source"},
                    details={"version": self._manifest.version},
                )
            )
        return env


async def attach_registry_context_sources(
    builder: ContextBuilder,
    loader: RegistryLoader,
    *,
    tenant: TenantContext,
    client_builder: ClientBuilder,
    cache: ContextSourceCache | None = None,
    event_sink: EventSink | None = None,
) -> list[AgentManifest]:
    """Attach every approved + RBAC-granted ``context_source`` to the builder."""
    attached: list[AgentManifest] = []
    for manifest in await loader.context_sources(tenant=tenant):
        builder.add_source(
            RegistryContextSource(
                manifest, client_builder(manifest), cache=cache, event_sink=event_sink
            )
        )
        attached.append(manifest)
    return attached


async def attach_registry_resolvers(
    resolver: DefaultReferenceResolver,
    loader: RegistryLoader,
    *,
    client_builder: ClientBuilder,
    tenant: TenantContext | None = None,
) -> list[str]:
    """Register every approved manifest's ``resolver_schemes`` as JIT loaders, so
    ``<scheme>://<ref>`` references resolve through the owning agent."""
    registered: list[str] = []
    manifests = await loader.manifests()
    for manifest in manifests:
        if tenant is not None and not _scopes_granted(manifest, tenant):
            continue
        for scheme in manifest.resolver_schemes:
            client = client_builder(manifest)

            async def _load(rest: str, _client: Any = client) -> str:
                data = await _client.read_resource(rest)
                return data if isinstance(data, str) else repr(data)

            resolver.register_loader(scheme, _load)
            registered.append(scheme)
    return registered


__all__ = [
    "ClientBuilder",
    "AgentClientFactory",
    "ContextSourceCache",
    "RegistryContextSource",
    "attach_registry_context_sources",
    "attach_registry_resolvers",
]
