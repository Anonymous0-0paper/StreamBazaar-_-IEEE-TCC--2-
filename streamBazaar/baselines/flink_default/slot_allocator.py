from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional


@dataclass
class TaskSlot:
    taskmanager_id: str
    slot_id: int
    cpu_share: float
    memory_share_mb: float
    assigned: List[Dict[str, object]] = field(default_factory=list)


def find_compatible_slot(subtask: Mapping[str, object], available_slots: List[TaskSlot]) -> Optional[TaskSlot]:
    """Flink slot sharing rules.

    - Same job + different operators can share a slot
    - Same operator subtasks cannot share a slot
    """

    job_id = str(subtask["job_id"])
    operator_id = str(subtask["operator_id"])

    for slot in available_slots:
        if not slot.assigned:
            return slot

        same_job = all(str(existing["job_id"]) == job_id for existing in slot.assigned)
        if not same_job:
            continue

        has_same_operator = any(str(existing["operator_id"]) == operator_id for existing in slot.assigned)
        if has_same_operator:
            continue

        return slot

    return None


def allocate_slots_statically(job: Mapping[str, object], cluster_config: Mapping[str, object]) -> Dict[str, TaskSlot]:
    """Allocate Flink slots once at startup (no runtime rebalancing)."""

    taskmanagers = cluster_config.get("taskmanagers", [])
    all_slots: List[TaskSlot] = []

    for tm in taskmanagers:
        tm_id = str(tm["id"])
        slots = int(tm["slots"])
        total_cpu = float(tm.get("cpu", 1.0))
        total_memory = float(tm.get("memory_mb", 1024.0))

        cpu_per_slot = total_cpu / max(slots, 1)
        memory_per_slot = total_memory / max(slots, 1)

        for slot_id in range(slots):
            all_slots.append(
                TaskSlot(
                    taskmanager_id=tm_id,
                    slot_id=slot_id,
                    cpu_share=cpu_per_slot,
                    memory_share_mb=memory_per_slot,
                )
            )

    assignments: Dict[str, TaskSlot] = {}

    for subtask in job.get("subtasks", []):
        subtask_id = str(subtask["subtask_id"])
        slot = find_compatible_slot(subtask, all_slots)
        if slot is None:
            raise RuntimeError(f"Insufficient slots for subtask {subtask_id}")

        slot.assigned.append(dict(subtask))
        assignments[subtask_id] = slot

    return assignments
