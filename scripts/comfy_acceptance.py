"""Isolated real ComfyUI acceptance. Defaults to discovery and compilation only."""

import argparse
import asyncio
import hashlib
import json
import time
from pathlib import Path

from tianshu_companion.clients import Gateway, JsonService, Memory, Origins, Sender
from tianshu_companion.contracts import Contracts
from tianshu_companion.core import Core
from tianshu_companion.store import Store


async def run(args):
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    contracts = Contracts(args.contracts)
    clients = [JsonService() for _ in range(3)]
    actor = "actor:comfy-acceptance"
    core = Core(
        Store(output / "isolated-companion.db"),
        contracts,
        Origins(contracts, {}),
        Memory(contracts, clients[0]),
        Gateway(contracts, clients[1]),
        Sender(contracts, clients[2]),
        bindings={},
        roles={actor: dict(version=1, system_prompt="Fictional character")},
        life_writing=False,
        image_options=dict(staging=output / "originals"),
    )
    catalog = core.image_backend.catalog
    report = dict(
        mode="generate" if args.generate else "readonly_compile",
        model_translation="not_called_explicit_english_intent",
        workflow_id=args.workflow,
        generation_state="not_started",
        artifacts=[],
    )

    def save_receipt(job=None):
        if job:
            report.update(
                job_id=job["id"],
                generation_state=job["state"],
                prompt_id=job["prompt_id"],
                submitted=job["submitted"],
                error_code=job.get("failure"),
                artifacts=[],
            )
            for artifact in job["artifacts"]:
                path = core.images.staging / artifact["staging_name"]
                report["artifacts"].append(
                    dict(
                        path=str(path),
                        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                        width=artifact["width"],
                        height=artifact["height"],
                        size=artifact["size"],
                    )
                )
        (output / "acceptance.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

    try:
        probe_config = dict(
            id="comfy-acceptance-config",
            base_url=args.base_url,
            workflow=args.workflow,
            width=args.width,
            height=args.height,
        )
        previous_config = core.store.get("metadata", probe_config["id"])
        if previous_config and previous_config != probe_config:
            raise ValueError("Existing receipt directory belongs to different acceptance inputs")
        core.store.put("metadata", probe_config)
        if not core.store.get("life_actors", actor):
            core.life.create_world("world:probe", setting="Fictional ocean observatory")
            core.life.create_room("room:probe", "world:probe")
            core.life.configure_actor(
                actor,
                "room:probe",
                personality_version=1,
                schedule=[dict(minute=0, activity="observing the ocean", controls={})],
            )
        if not catalog.connection():
            status = await catalog.status(actor)
            await catalog.call(
                "platform",
                "manage",
                dict(
                    schema_version=1,
                    request_id="probe-connection",
                    actor_id=actor,
                    operation="connection.configure",
                    expected_version=status["version"],
                    value=dict(base_url=args.base_url, credential_ref=None, enabled=True),
                ),
            )
        selected = catalog.actor(actor)
        if not selected["workflow_id"]:
            await catalog.call(
                "platform",
                "manage",
                dict(
                    schema_version=1,
                    request_id="probe-selection",
                    actor_id=actor,
                    operation="workflow.select",
                    expected_version=selected["version"],
                    value=dict(workflow_id=args.workflow, bindings=None),
                ),
            )
        intent = dict(
            outfit="ivory and ice-blue sea-observer dress, opaque modest fabric, subtle silver ornaments",
            pose="standing naturally on an ocean observatory balcony, relaxed hands, calm smile",
            background="ocean horizon, clear sky, soft morning daylight",
            camera="three-quarter view, full body, candid everyday moment",
            positive="safe fictional anime character illustration",
            negative="nudity, underwear, transparent clothing, sexual pose",
        )
        parameters = dict(width=args.width, height=args.height, seed=20261005)
        compilation = await catalog.call(
            "platform",
            "compile",
            dict(
                schema_version=1,
                request_id="probe-compile",
                actor_id=actor,
                intent=intent,
                parameters=parameters,
                assist_model=False,
            ),
        )
        report["compile"] = compilation["result"]
        save_receipt(core.store.get("image_jobs", "image:comfy-acceptance"))
        print(
            json.dumps(
                dict(
                    mode=report["mode"],
                    workflow_id=args.workflow,
                    state=compilation["result"]["state"],
                    dimensions=compilation["result"]["dimensions"],
                    output=str(output),
                ),
                ensure_ascii=False,
            ),
            flush=True,
        )
        if not args.generate:
            return 0
        request = dict(
            schema_version=2,
            request_id="probe-image-admission",
            actor_id=actor,
            operation="image.request",
            expected_version=0,
            value=dict(
                id="image:comfy-acceptance",
                parameters=parameters,
                outfit_id=None,
                activity_id=None,
                scene="Safe fictional day at the ocean observatory",
                edit_source_id=None,
                edit_source_ref=None,
                scope=None,
                query=None,
                intent=intent,
                assist_model=False,
            ),
        )
        await core.life_runtime.manage("platform", request)
        core.images.recover()
        save_receipt(core.images.get("image:comfy-acceptance"))
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            await core.images.work()
            job = core.images.get("image:comfy-acceptance")
            save_receipt(job)
            print(
                json.dumps(dict(job_id=job["id"], state=job["state"], submitted=job["submitted"])),
                flush=True,
            )
            if job["state"] in {"completed", "failed", "cancelled"}:
                break
            await asyncio.sleep(2)
        else:
            save_receipt(core.images.get("image:comfy-acceptance"))
        print(
            json.dumps(
                dict(
                    generation_state=report["generation_state"],
                    receipt=str(output / "acceptance.json"),
                    resume="Reuse exactly this output directory to reconcile the existing job",
                )
            ),
            flush=True,
        )
        return 0 if report["generation_state"] == "completed" else 2
    except Exception as error:
        job = core.store.get("image_jobs", "image:comfy-acceptance")
        report["execution_error"] = getattr(error, "code", type(error).__name__)
        save_receipt(job)
        print(
            json.dumps(
                dict(
                    generation_state=report["generation_state"],
                    error_code=report["execution_error"],
                    receipt=str(output / "acceptance.json"),
                )
            ),
            flush=True,
        )
        return 2
    finally:
        await core.close()
        for client in clients:
            await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--contracts", required=True, help="Published text-dialogue/v1 directory in the new closure"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Dedicated isolated test directory; never a production directory",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8188")
    parser.add_argument("--workflow", default="角色/澄汐/澄汐-分类测试版.json")
    parser.add_argument("--width", type=int, default=1024)
    parser.add_argument("--height", type=int, default=1536)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Explicitly authorize exactly the existing stable isolated image job",
    )
    raise SystemExit(asyncio.run(run(parser.parse_args())))
