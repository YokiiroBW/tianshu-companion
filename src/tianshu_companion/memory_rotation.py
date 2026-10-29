"""Schedule accepted material fairly using the existing durable metadata store."""

from .contracts import digest


def _key(frame, person):
    return "memory-target-rotation:" + digest([frame, person])


def order(core, frame, materials):
    if not all(
        isinstance(item, dict) and isinstance(item.get("person_id"), str) for item in materials
    ):
        return materials  # The original descriptor validation owns malformed input.
    priorities = {}
    for item in materials:
        person = item["person_id"]
        if person not in priorities:
            row = core.store.get("metadata", _key(frame, person)) or {}
            priorities[person] = (row.get("covered_count", 0), row.get("last_covered_at", 0))
    return sorted(materials, key=lambda item: priorities[item["person_id"]])


def covered(core, item, receipt):
    if item.get("target_rotation_recorded"):
        return
    for result in receipt["results"]:
        person = result["subject_person_id"]
        key = _key(item["scope"], person)
        previous = core.store.get("metadata", key) or {}
        core.store.put(
            "metadata",
            dict(
                id=key,
                covered_count=previous.get("covered_count", 0) + 1,
                last_covered_at=core.clock(),
            ),
        )
    item["target_rotation_recorded"] = True
