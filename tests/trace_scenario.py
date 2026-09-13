"""Produce a synthetic, persisted module trace; not an L0/L1 integration claim."""

import argparse
import asyncio
import json
import tempfile
from pathlib import Path

from support import Harness


async def run():
    snapshots = []
    with tempfile.TemporaryDirectory() as directory:
        h = Harness(Path(directory) / "trace.db", silence_ms=0, delivery_reconcile_timeout_ms=1000)

        def record(step):
            snapshots.append(
                dict(
                    step=step,
                    turns=[
                        dict(
                            sequence=t["sequence"],
                            phase=t["phase"],
                            version=t["version"],
                            model_calls=t["model_calls"],
                            delivery=t["delivery_state"],
                        )
                        for t in h.turns()
                    ],
                    replies=[
                        dict(sequence=r["sequence"], state=r["state"])
                        for r in h.core.store.list("replies")
                    ],
                    outbox=[
                        dict(state=o["state"], attempts=o.get("attempts"))
                        for o in h.core.store.list("outbox")
                    ],
                    native_submissions=len(h.sender.calls),
                )
            )

        h.gateway.gates[1] = asyncio.Event()
        h.sender.states = ["sent", "unknown"]
        await h.ingest(text="Synthetic T1")
        await h.ingest(text="Synthetic T2", actor="actor:b")
        await h.ingest(text="Synthetic T3")
        await h.cycles()
        record("T1 generating, T2 prepared, T3 queued")
        assert [t["phase"] for t in h.turns()] == ["generating", "ready_to_send", "queued"]
        h.gateway.gates[1].set()
        await h.cycles()
        record("T1 partial/unknown, no later submission")
        assert len(h.sender.calls) == 2
        await h.core.close()
        h.core = h.new_core()
        h.core.recover()
        record("database reopened, unknown retained")
        h.clock.advance(2)
        await h.cycles(80)
        record("reconciliation timeout releases slot and later turns send")
        assert [t["phase"] for t in h.turns()] == ["closed_unknown", "sent", "sent"]
        h.memory.fail_commit = True
        await h.core.flush_outbox()
        record("outbox dependency unavailable; persisted pending")
        h.memory.fail_commit = False
        h.clock.advance(2)
        await h.core.flush_outbox()
        record("outbox accepted by test consumer")
        assert len(h.memory.commits) == 3
        prior = h.sender.calls[1]
        h.core.record_receipt(prior["reply_id"], h.sender.receipt(prior))
        await h.core.flush_outbox()
        record("late sent fact updates projection without another memory commit")
        assert len(h.memory.commits) == 3 and len(h.sender.calls) == 6
        await h.core.close()
    return dict(
        evidence_level="local_modules_with_test_only_dependencies",
        contract_version="1.0.0",
        real_channel="not_run",
        real_memory="not_run",
        real_gateway="not_run",
        snapshots=snapshots,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(run())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(f"Recorded {len(result['snapshots'])} synthetic persisted checkpoints: {args.output}")
