from dataclasses import dataclass
from typing import List
import random

from .operator_registry import Operator, OperatorRegistry
from .slot_model import SlotModel


@dataclass
class MutationPlanItem:
    slot: SlotModel
    operator: Operator


class MutationPlanner:
    def __init__(self, registry: OperatorRegistry) -> None:
        self.registry = registry

    def plan(self, slots: List[SlotModel]) -> List[MutationPlanItem]:
        plan: List[MutationPlanItem] = []
        for slot in slots:
            candidates = self.registry.get_candidates(slot)
            if not candidates:
                continue
            plan.append(MutationPlanItem(slot=slot, operator=random.choice(candidates)))
        return plan

    def plan_singletons(self, slots: List[SlotModel]) -> List[List[MutationPlanItem]]:
        """
        Generate one-slot mutation plans (control-variable style).
        Each plan contains exactly one slot + one operator.
        """
        plans: List[List[MutationPlanItem]] = []
        for slot in slots:
            candidates = self.registry.get_candidates(slot)
            if not candidates:
                continue
            plans.append([MutationPlanItem(slot=slot, operator=random.choice(candidates))])
        return plans
