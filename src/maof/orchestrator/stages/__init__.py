"""Default workflow stages (chat -> intent_synthesis -> action_plan -> approval -> publish)."""

from __future__ import annotations

from maof.orchestrator.stages.action_plan import ActionPlanStage, Planner
from maof.orchestrator.stages.approval import ApprovalStage
from maof.orchestrator.stages.chat import ChatStage
from maof.orchestrator.stages.intent import IntentStage
from maof.orchestrator.stages.publish import PublishStage

__all__ = [
    "ChatStage",
    "IntentStage",
    "ActionPlanStage",
    "Planner",
    "ApprovalStage",
    "PublishStage",
]
