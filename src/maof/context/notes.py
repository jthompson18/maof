"""Structured note-taking / agentic memory.

Durable notes the agent re-reads, persisted outside the context window and pulled
back on demand. This is the note *engine* the MemoryService facade delegates to —
no double implementation. Backed by the ``notes`` table; an in-memory store is
provided for tests/embedded use.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from maof.types import Note

if TYPE_CHECKING:
    from maof.persistence.postgres import Database


@runtime_checkable
class NoteStore(Protocol):
    async def write_note(self, run_id: str, note: Note) -> str: ...

    async def read_notes(self, run_id: str, *, tags: list[str] | None = None) -> list[Note]: ...


def _matches(note: Note, tags: list[str] | None) -> bool:
    return tags is None or bool(set(tags) & set(note.tags))


class InMemoryNoteStore:
    def __init__(self) -> None:
        self._notes: dict[str, list[Note]] = {}

    async def write_note(self, run_id: str, note: Note) -> str:
        bucket = self._notes.setdefault(run_id, [])
        note_id = note.id or f"note-{run_id}-{len(bucket) + 1}"
        bucket.append(note.model_copy(update={"id": note_id, "run_id": run_id}))
        return note_id

    async def read_notes(self, run_id: str, *, tags: list[str] | None = None) -> list[Note]:
        return [n for n in self._notes.get(run_id, []) if _matches(n, tags)]


class PostgresNoteStore:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def write_note(self, run_id: str, note: Note) -> str:
        note_id: str = await self._db.fetchval(
            "INSERT INTO notes (run_id, content, tags) VALUES ($1, $2, $3) RETURNING id",
            run_id,
            note.content,
            note.tags,
        )
        return note_id

    async def read_notes(self, run_id: str, *, tags: list[str] | None = None) -> list[Note]:
        rows = await self._db.fetch(
            "SELECT * FROM notes WHERE run_id = $1 ORDER BY created_at ASC, id ASC", run_id
        )
        notes = [
            Note(
                id=r["id"],
                run_id=run_id,
                content=r["content"],
                tags=list(r["tags"]),
                created_at=r["created_at"].isoformat(),
            )
            for r in rows
        ]
        return [n for n in notes if _matches(n, tags)]


if TYPE_CHECKING:
    _assert_mem: NoteStore = InMemoryNoteStore()
    _assert_pg: NoteStore = PostgresNoteStore.__new__(PostgresNoteStore)


__all__ = ["NoteStore", "InMemoryNoteStore", "PostgresNoteStore"]
