"""Register the reference vendor L2 agents into MAOF's runtime registry on import.

Used by the worker containers, one per vendor queue:

    maof run-worker --queue suppliers.commitments.v1  --agents examples.po_demo.register
    maof run-worker --queue suppliers.fulfillment.v1 --agents examples.po_demo.register

(The TRUST registry — manifests, approval, signing — is separate: the
orchestrator entry submits + approves the Commitments/Fulfillment/catalog/datastore manifests
there; this module only makes the agent implementations available to workers.)
"""

from __future__ import annotations

from maof.agents.registry_runtime import default_registry

from .agents import CommitmentsAgent, FulfillmentAgent

default_registry.register_agent(CommitmentsAgent())
default_registry.register_agent(FulfillmentAgent())
