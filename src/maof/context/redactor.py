"""Functional PII redactor.

The reference left this a no-op; MAOF makes it real. When the ``pii=redact``
policy flag is set, common PII (emails, SSNs, formatted phone numbers) is scrubbed
from the envelope's free-text fields before the context reaches a prompt.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from maof.types import ContextEnvelope

REDACTION = "[REDACTED]"

_PATTERNS = [
    re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"),  # email
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),  # US SSN
    re.compile(  # formatted phone (separators required, so budgets/years are safe)
        r"\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}\b"
    ),
]


@runtime_checkable
class Redactor(Protocol):
    def redact(self, env: ContextEnvelope) -> ContextEnvelope: ...


class NoOpRedactor:
    def redact(self, env: ContextEnvelope) -> ContextEnvelope:
        return env


class RegexRedactor:
    def __init__(self, *, flag: str = "pii", value: str = "redact") -> None:
        self._flag = flag
        self._value = value

    def redact(self, env: ContextEnvelope) -> ContextEnvelope:
        if env.policy_flags.get(self._flag) != self._value:
            return env
        env.goal = self._scrub(env.goal)
        env.dialogue = [self._scrub(line) for line in env.dialogue]
        env.memories = [
            m.model_copy(update={"content": self._scrub(m.content)}) for m in env.memories
        ]
        env.data_pointers = [
            dp.model_copy(update={"note": self._scrub(dp.note)}) for dp in env.data_pointers
        ]
        return env

    @staticmethod
    def _scrub(text: str) -> str:
        for pattern in _PATTERNS:
            text = pattern.sub(REDACTION, text)
        return text


def scrub_text(text: str) -> str:
    """Scrub known PII patterns from free text (prompt-audit redaction)."""
    for pattern in _PATTERNS:
        text = pattern.sub(REDACTION, text)
    return text


__all__ = ["Redactor", "NoOpRedactor", "RegexRedactor", "REDACTION", "scrub_text"]
