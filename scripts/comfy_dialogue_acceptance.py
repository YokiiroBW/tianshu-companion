"""One isolated ordinary dialogue, actual Gateway, record-only outbound; default read-only."""

import argparse
import asyncio
import copy
import hashlib
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from support import Harness  # noqa: E402 -- existing isolated identity/Memory fixtures, no pytest
from tianshu_companion.clients import Gateway, JsonService, utc  # noqa: E402
from tianshu_companion.core import TERMINAL  # noqa: E402
from tianshu_companion.contracts import digest  # noqa: E402
from tianshu_companion.relationships import projection  # noqa: E402


class RecordedOutbound:
    """Synthetic receipts only. This class contains no actual channel client."""

    available = True

    def __init__(self, clock):
        self.clock, self.requests, self.receipts = clock, [], {}

    def expression_available(self, channel):
        return True

    async def send_expression(self, request):
        self.requests.append(copy.deepcopy(request))
        receipt = dict(
            schema_version=2,
            request_id=request["request_id"],
            expression_id=request["expression_id"],
            final=request["final"],
            state="sent",
            observed_at=utc(self.clock()),
            segments=[
                dict(
                    segment_id=s["segment_id"],
                    reply_id=s["reply_id"],
                    segment_sequence=s["segment_sequence"],
                    state="sent",
                    receipt_id="record-only:" + s["segment_id"],
                    retry_safe=False,
                    channel_message_ids=["record-only:" + s["segment_id"]],
                )
                for s in request["segments"]
            ],
        )
        self.receipts[request["expression_id"]] = receipt
        return receipt

    async def query_expression(self, expression_id):
        return self.receipts.get(expression_id)

    async def finalize_expression(self, expression_id, origin):
        self.receipts[expression_id]["final"] = True
        return self.receipts[expression_id]


class ObservedRelationship:
    """Preserve sampled expression fields, with isolated actor/person scope; no live write."""

    max_bytes = 8192

    def __init__(self, value):
        self.value = value
        self.client = self

    async def prepare(self, core, turn, context):
        return await projection.prepare(self, core, turn, context)

    async def read(self, origin, scope, clock):
        return dict(
            self.value,
            pair={k: scope[k] for k in ("actor_id", "person_id")},
            checked_at=utc(clock()),
        )

    async def check(self, origin, scope, expected_version):
        if expected_version != self.value["version"]:
            raise ValueError("Isolated relationship snapshot version changed")

    def recover(self, core):
        pass

    def queue(self, core, item, turn):
        pass

    async def flush(self, core):
        pass


async def run(args):
    os.environ["TIANSHU_CONTRACTS"] = args.contracts
    sample = json.loads(Path(args.private_input).read_text(encoding="utf-8"))
    settings = json.loads(Path(args.settings).read_text(encoding="utf-8"))
    peer = settings["services"]["gateway"]
    # Resolve only the registered service's existing env credential inside its host.
    # Never serialize the credential or copy it to the workstation.
    client = JsonService(peer["url"], os.environ[peer["token_env"]], ca_file=peer.get("ca_file"))
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    h = Harness(output / "isolated-dialogue.db", silence_ms=0)
    h.clock.now = time.time()
    await h.core.close()
    actor = sample["actor_id"]
    h.options["roles"] = {actor: sample["role"]}
    h.options["config_version"] = sample["config_version"]
    for binding in h.options["bindings"].values():
        binding["actor_ids"] = [actor]
    gateway = Gateway(h.contracts, client)
    queue = RecordedOutbound(h.clock)
    h.gateway, h.sender = gateway, queue
    h.core = h.new_core()
    h.core.life.gateway = gateway
    h.core.relationships = ObservedRelationship(sample["relationship_background"])
    h.core.images.staging = output / "originals"
    catalog = h.core.image_backend.catalog
    fingerprint = digest([sample, args.base_url, args.workflow])
    marker = h.core.store.get("metadata", "dialogue-acceptance")
    report = dict(
        mode="ordinary_ingest_native_auto",
        request_text=sample["request_text"],
        persona_sha256=digest(sample["role"]),
        relationship_snapshot_version=sample["relationship_background"]["version"],
        identity_memory="explicit_isolated_fixtures",
        gateway="actual_registered_service",
        outbound="record_only_no_qq",
        dialogue_state="not_started",
        image_jobs=[],
        artifacts=[],
        qq_sent=False,
    )

    def receipt():
        turns = h.turns()
        if turns:
            turn = turns[0]
            messages = (turn.get("native_execution") or {}).get("messages", [])
            report.update(
                turn_id=turn["id"],
                dialogue_state=turn["phase"],
                tool_calls=[
                    call["function"]["name"]
                    for message in messages
                    for call in message.get("tool_calls", [])
                ],
                assistant_text=[
                    message["content"]
                    for message in messages
                    if message["role"] == "assistant" and message.get("content")
                ],
                route_receipt_count=len(
                    (turn.get("native_execution") or {}).get("route_receipts", [])
                ),
                tool_receipts=[
                    {key: value[key] for key in ("state", "error_code") if key in value}
                    for message in messages
                    if message["role"] == "tool"
                    for value in [json.loads(message["content"])]
                ],
                failure=turn.get("failure"),
            )
        report["image_jobs"] = [
            {k: job.get(k) for k in ("id", "state", "submitted", "prompt_id", "failure")}
            for job in h.core.store.list("image_jobs")
        ]
        report["artifacts"] = []
        for job in h.core.store.list("image_jobs"):
            for artifact in job["artifacts"]:
                path = h.core.images.staging / artifact["staging_name"]
                report["artifacts"].append(
                    dict(
                        path=str(path),
                        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                        width=artifact["width"],
                        height=artifact["height"],
                        size=artifact["size"],
                    )
                )
        report["recorded_outbound_count"] = len(queue.requests)
        (output / "receipt.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

    try:
        if marker and marker["fingerprint"] != fingerprint:
            raise ValueError("Output directory belongs to different scenario inputs")
        if marker:
            h.origins.values = marker.get("fixture_origins", {})
            h.memory.accounts = {
                tuple(sorted(item["account"].items())): item["person_id"]
                for item in marker.get("fixture_accounts", [])
            }
        h.core.recover()
        if not catalog.connection():
            await catalog.call(
                "platform",
                "manage",
                dict(
                    schema_version=1,
                    actor_id=actor,
                    request_id="dialogue-connect",
                    operation="connection.configure",
                    expected_version=1,
                    value=dict(base_url=args.base_url, credential_ref=None, enabled=True),
                ),
            )
        else:
            await catalog.restore()
        if not catalog.actor(actor)["workflow_id"]:
            await catalog.call(
                "platform",
                "manage",
                dict(
                    schema_version=1,
                    actor_id=actor,
                    request_id="dialogue-select",
                    operation="workflow.select",
                    expected_version=1,
                    value=dict(workflow_id=args.workflow, bindings=None),
                ),
            )
        if not marker:
            marker = dict(id="dialogue-acceptance", fingerprint=fingerprint, state="prepared")
            h.core.store.put("metadata", marker)
        if args.run_dialogue and marker["state"] == "prepared":
            # Persist before any model request. Re-running never issues another dialogue.
            marker["state"] = "started"
            h.core.store.put("metadata", marker)
            await h.ingest(
                actor=actor, text=sample["request_text"], message="acceptance-original-input"
            )
            marker.update(
                fixture_origins=h.origins.values,
                fixture_accounts=[
                    dict(account=dict(key), person_id=value)
                    for key, value in h.memory.accounts.items()
                ],
            )
            h.core.store.put("metadata", marker)
            deadline = time.monotonic() + args.dialogue_timeout
            while time.monotonic() < deadline:
                h.clock.now = time.time()
                await h.core.tick()
                await asyncio.sleep(0.05)
                turns = h.turns()
                if turns and turns[0]["phase"] in TERMINAL and not h.core.jobs:
                    break
            marker["state"] = (
                "completed" if h.turns() and h.turns()[0]["phase"] in TERMINAL else "unknown"
            )
            h.core.store.put("metadata", marker)
        receipt()
        if args.generate:
            jobs = h.core.store.list("image_jobs")
            if len(jobs) != 1:
                report["generation_state"] = "no_single_image_job_no_submission"
            else:
                deadline = time.monotonic() + args.generation_timeout
                while time.monotonic() < deadline:
                    await h.core.images.work()
                    receipt()
                    job = h.core.images.get(jobs[0]["id"])
                    if job["state"] in {"completed", "failed", "cancelled"}:
                        await h.core.images.work()  # automatic original into record-only queue
                        break
                    await asyncio.sleep(2)
        report["scenario_state"] = (
            "prepared_no_model"
            if marker["state"] == "prepared"
            else (
                "image_requested"
                if h.core.store.list("image_jobs")
                else "image_tool_not_admitted"
                if "life_image_request" in report.get("tool_calls", [])
                else "assistant_without_image_tool"
            )
        )
        receipt()
        print(
            json.dumps(
                {
                    k: report[k]
                    for k in ("scenario_state", "dialogue_state", "image_jobs", "qq_sent")
                },
                ensure_ascii=False,
            )
        )
        print(json.dumps(dict(receipt=str(output / "receipt.json"), automatic_retry=False)))
        return 0 if marker["state"] in {"prepared", "completed"} else 2
    except Exception as error:
        report["execution_error"] = getattr(error, "code", type(error).__name__)
        receipt()
        print(
            json.dumps(
                dict(
                    error_code=report["execution_error"],
                    receipt=str(output / "receipt.json"),
                    automatic_retry=False,
                )
            )
        )
        return 2
    finally:
        await h.core.close()
        await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contracts", required=True)
    parser.add_argument(
        "--settings",
        required=True,
        help="Existing Companion settings; credential stays on this host",
    )
    parser.add_argument(
        "--private-input", required=True, help="Read-only sampled pinned persona/relationship/input"
    )
    parser.add_argument("--output", required=True, help="Dedicated isolated scenario directory")
    parser.add_argument("--base-url", default="http://192.168.31.61:8188")
    parser.add_argument("--workflow", default="角色/澄汐/澄汐-分类测试版.json")
    parser.add_argument(
        "--run-dialogue",
        action="store_true",
        help="Explicitly permit one ordinary dialogue with actual Gateway",
    )
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Submit/poll only that existing image job, never create another dialogue",
    )
    parser.add_argument("--dialogue-timeout", type=int, default=180)
    parser.add_argument("--generation-timeout", type=int, default=600)
    raise SystemExit(asyncio.run(run(parser.parse_args())))
