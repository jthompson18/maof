"""Webhook approval notifier + fanout sink.

Delivery shape is asserted against captured requests (httpx MockTransport);
failure paths prove notification can never break the governance/audit path.
"""

from __future__ import annotations

from typing import Any

import httpx

from maof.observability.events import AuditEvent, FanoutEventSink
from maof.observability.sinks.webhook_sink import (
    WebhookEventSink,
    format_slack,
    format_teams,
)


def _approval_event(event_type: str = "approval_requested") -> AuditEvent:
    return AuditEvent(
        tenant_id="shared-buyer-001",
        intent_id=None,
        event_type=event_type,
        envelope={"run_id": "run-9"},
        details={"approval_id": "appr-run-9-1", "reason": "commitment exceeds the spend cap"},
    )


def _capturing_client(captured: list[dict[str, Any]], status_code: int = 200) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(
            {"url": str(request.url), "json": __import__("json").loads(request.content)}
        )
        return httpx.Response(status_code)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_slack_payload_carries_reason_ids_and_action_links() -> None:
    captured: list[dict[str, Any]] = []
    sink = WebhookEventSink(
        "https://hooks.slack.example/T000/B000",
        approval_base_url="https://approvals.example.com",
        client=_capturing_client(captured),
    )
    await sink.emit(_approval_event())
    assert len(captured) == 1
    payload = captured[0]["json"]
    assert "commitment exceeds the spend cap" in payload["text"]
    blocks = payload["blocks"]
    assert "Approval requested" in blocks[0]["text"]["text"]
    assert "run-9" in blocks[1]["elements"][0]["text"]
    buttons = blocks[2]["elements"]
    assert buttons[0]["url"].endswith("/approvals/appr-run-9-1/approve")
    assert buttons[1]["url"].endswith("/approvals/appr-run-9-1/deny")


async def test_teams_formatter_produces_message_card() -> None:
    card = format_teams(_approval_event(), "https://approvals.example.com")
    assert card["@type"] == "MessageCard"
    assert card["title"] == "[maof] Approval requested"
    facts = {f["name"]: f["value"] for f in card["sections"][0]["facts"]}
    assert facts["run"] == "run-9" and facts["approval"] == "appr-run-9-1"
    actions = card["potentialAction"]
    assert actions[0]["targets"][0]["uri"].endswith("/approve")


async def test_resolution_events_have_no_action_buttons() -> None:
    payload = format_slack(_approval_event("approval_granted"), "https://a.example.com")
    assert all(block["type"] != "actions" for block in payload["blocks"])


async def test_event_type_filtering() -> None:
    captured: list[dict[str, Any]] = []
    sink = WebhookEventSink("https://hooks.example", client=_capturing_client(captured))
    await sink.emit(AuditEvent(tenant_id="t", intent_id=None, event_type="policy_decision"))
    assert captured == []
    await sink.emit(_approval_event("approval_denied"))
    assert len(captured) == 1


async def test_delivery_failure_is_swallowed() -> None:
    captured: list[dict[str, Any]] = []
    sink = WebhookEventSink(
        "https://hooks.example", client=_capturing_client(captured, status_code=500)
    )
    await sink.emit(_approval_event())  # must not raise into the governance path
    assert len(captured) == 1


async def test_fanout_delivers_to_all_sinks_in_order() -> None:
    seen: list[str] = []

    class _Recorder:
        def __init__(self, name: str) -> None:
            self._name = name

        async def emit(self, event: AuditEvent) -> None:
            seen.append(f"{self._name}:{event.event_type}")

    fanout = FanoutEventSink([_Recorder("a"), _Recorder("b")])
    await fanout.emit(_approval_event())
    assert seen == ["a:approval_requested", "b:approval_requested"]
