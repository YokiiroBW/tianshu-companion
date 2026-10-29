from support import Harness

from tianshu_companion.memory_learning import QUEUE
from tianshu_companion.memory_rotation import covered, order
from tianshu_companion.memory_segments import MemorySegments


def test_rotation_preserves_all_twenty_real_materials_and_replay_counts_once(tmp_path):
    h = Harness(tmp_path / "rotation.sqlite", silence_ms=0)
    try:
        frame = dict(actor_id="actor:a", audience="group", conversation_id="room")
        materials = [
            dict(
                material_ref=f"m:{i}",
                person_id=f"p:{i}",
                text_bytes=1,
                member_count=1,
                source_key=f"s:{i}",
            )
            for i in range(20)
        ]
        item = dict(scope=frame)
        receipt = dict(results=[dict(subject_person_id=f"p:{i}") for i in range(8)])
        with h.core.store.transaction():
            covered(h.core, item, receipt)
            covered(h.core, item, receipt)
        prioritized = order(h.core, frame, materials)
        assert {row["person_id"] for row in prioritized[:12]} == {f"p:{i}" for i in range(8, 20)}
        assert order(h.core, dict(frame, actor_id="another"), materials) == materials
        MemorySegments(h.core, QUEUE).offer(frame, materials)
        segments = h.core.store.list("metadata", QUEUE)
        assert sorted(len(row["materials"]) for row in segments) == [4, 8, 8]
        assert {row["material_ref"] for segment in segments for row in segment["materials"]} == {
            f"m:{i}" for i in range(20)
        }
    finally:
        h.core.store.close()
