"""Just-in-time retrieval.

Context carries lightweight references (``artifact://...``, ``note://...``, a data
pointer alias, or any adopter-registered scheme); the agent loads full content on
demand via this resolver rather than pre-stuffing the window.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from maof.runs.artifacts import ArtifactStore
    from maof.types import StageContext


@runtime_checkable
class ReferenceResolver(Protocol):
    async def resolve(self, ref: str, sc: StageContext) -> str: ...


class DefaultReferenceResolver:
    """Resolves ``scheme://rest`` references. Built-in: ``artifact://<ref>`` via the
    artifact store. Adopters register more schemes via ``loaders``. Bare references
    are matched against the envelope's data-pointer aliases."""

    def __init__(
        self,
        *,
        artifacts: ArtifactStore | None = None,
        loaders: dict[str, Callable[[str], Awaitable[str]]] | None = None,
    ) -> None:
        self._artifacts = artifacts
        self._loaders = dict(loaders) if loaders else {}

    def register_loader(self, scheme: str, loader: Callable[[str], Awaitable[str]]) -> None:
        """Register a scheme at runtime (registry resolver discovery)."""
        self._loaders[scheme] = loader

    async def resolve(self, ref: str, sc: StageContext) -> str:
        scheme, sep, rest = ref.partition("://")
        if sep:
            if scheme in self._loaders:
                return await self._loaders[scheme](rest)
            if scheme == "artifact" and self._artifacts is not None:
                data = await self._artifacts.get(rest)
                return data.decode("utf-8")
        if sc.envelope is not None:
            for pointer in sc.envelope.data_pointers:
                if pointer.alias == ref:
                    return pointer.uri
        raise KeyError(f"cannot resolve reference: {ref!r}")


__all__ = ["ReferenceResolver", "DefaultReferenceResolver"]
