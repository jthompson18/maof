"""Webhook event sink — push approval (or any domain) events to chat/webhook targets.

The HITL gate emits ``approval_requested`` / ``approval_granted`` / ``approval_denied``
audit events (approval/service.py); this sink turns them into Slack or Microsoft
Teams notifications so multi-party approvals reach the people who must sign.
Delivery is best-effort by design: a webhook outage must never break the
governance path, so failures are logged and swallowed — the audit store remains
the source of truth.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from typing import Any

import httpx

from maof.observability.events import AuditEvent

logger = logging.getLogger(__name__)

#: The approval lifecycle — the default trigger set for notifications.
APPROVAL_EVENT_TYPES = ("approval_requested", "approval_granted", "approval_denied")

#: Formatter contract: (event, approval_base_url) -> webhook JSON payload.
PayloadFormatter = Callable[[AuditEvent, str], dict[str, Any]]

_TITLES = {
    "approval_requested": "Approval requested",
    "approval_granted": "Approval granted",
    "approval_denied": "Approval denied",
}


def _fields(event: AuditEvent) -> tuple[str, str, str, str]:
    title = _TITLES.get(event.event_type, event.event_type)
    reason = str(event.details.get("reason", "")) or "(no reason given)"
    run_id = str(event.envelope.get("run_id", ""))
    approval_id = str(event.details.get("approval_id", ""))
    return title, reason, run_id, approval_id


def format_slack(event: AuditEvent, approval_base_url: str = "") -> dict[str, Any]:
    """Slack incoming-webhook payload (Block Kit) for an audit event."""
    title, reason, run_id, approval_id = _fields(event)
    blocks: list[dict[str, Any]] = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{title}*\n{reason}"},
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"tenant `{event.tenant_id or '-'}` · run `{run_id or '-'}` "
                    f"· approval `{approval_id or '-'}`",
                }
            ],
        },
    ]
    if approval_base_url and approval_id and event.event_type == "approval_requested":
        base = approval_base_url.rstrip("/")
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Approve"},
                        "style": "primary",
                        "url": f"{base}/approvals/{approval_id}/approve",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Deny"},
                        "style": "danger",
                        "url": f"{base}/approvals/{approval_id}/deny",
                    },
                ],
            }
        )
    return {"text": f"[maof] {title}: {reason}", "blocks": blocks}


def format_teams(event: AuditEvent, approval_base_url: str = "") -> dict[str, Any]:
    """Microsoft Teams incoming-webhook payload (MessageCard) for an audit event."""
    title, reason, run_id, approval_id = _fields(event)
    card: dict[str, Any] = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": f"[maof] {title}",
        "title": f"[maof] {title}",
        "text": reason,
        "sections": [
            {
                "facts": [
                    {"name": "tenant", "value": event.tenant_id or "-"},
                    {"name": "run", "value": run_id or "-"},
                    {"name": "approval", "value": approval_id or "-"},
                ]
            }
        ],
    }
    if approval_base_url and approval_id and event.event_type == "approval_requested":
        base = approval_base_url.rstrip("/")
        card["potentialAction"] = [
            {
                "@type": "OpenUri",
                "name": "Approve",
                "targets": [{"os": "default", "uri": f"{base}/approvals/{approval_id}/approve"}],
            },
            {
                "@type": "OpenUri",
                "name": "Deny",
                "targets": [{"os": "default", "uri": f"{base}/approvals/{approval_id}/deny"}],
            },
        ]
    return card


class WebhookEventSink:
    """POSTs formatted audit events to a webhook URL (Slack/Teams/custom).

    ``event_types=None`` forwards everything; the default forwards the approval
    lifecycle only. Inject ``client`` (an ``httpx.AsyncClient``) to control
    transport/lifecycle; otherwise a short-lived client is used per emit —
    approvals are low-volume by nature.
    """

    def __init__(
        self,
        url: str,
        *,
        event_types: Iterable[str] | None = APPROVAL_EVENT_TYPES,
        formatter: PayloadFormatter = format_slack,
        approval_base_url: str = "",
        timeout: float = 5.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._url = url
        self._event_types = frozenset(event_types) if event_types is not None else None
        self._formatter = formatter
        self._approval_base_url = approval_base_url
        self._timeout = timeout
        self._client = client

    async def emit(self, event: AuditEvent) -> None:
        if self._event_types is not None and event.event_type not in self._event_types:
            return
        payload = self._formatter(event, self._approval_base_url)
        try:
            if self._client is not None:
                response = await self._client.post(self._url, json=payload)
            else:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    response = await client.post(self._url, json=payload)
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001 - delivery is best-effort by design
            logger.warning(
                "webhook delivery failed for %s (%s): %s", event.event_type, self._url, exc
            )


__all__ = [
    "WebhookEventSink",
    "format_slack",
    "format_teams",
    "APPROVAL_EVENT_TYPES",
    "PayloadFormatter",
]
