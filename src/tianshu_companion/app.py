"""HTTP adapter. Configuration is explicit and secrets come from environment only."""

import asyncio
import hmac
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.background import BackgroundTask

from .clients import Gateway, JsonService, Memory, Origins, Sender, uid
from .contracts import Contracts, Fault, strict_json
from .core import Core, Policy
from .direct import SyntheticPlugin
from .store import Store
from .images import ComfyUI, Workflow
from .short_context import ShortContextPolicy

LOG = logging.getLogger(__name__)


def build_runtime(config):
    contracts = Contracts(config["contracts_path"])
    clients = []

    def client(name):
        entry = config.get("services", {}).get(name, {})
        value = JsonService(
            entry.get("url"),
            os.environ.get(entry.get("token_env", "")),
            ca_file=entry.get("ca_file"),
        )
        clients.append(value)
        return value

    incoming = {}
    issuers = {}
    for service, entry in config.get("callers", {}).items():
        token = os.environ.get(entry["token_env"])
        if token:
            incoming[service] = token
            if service in {"platform", "nonebot"}:
                issuers[service] = (entry["issuer"], client(entry["origin_service"]))
    image_options = None
    if config.get("images") is not None:
        image_config = config["images"]
        workflow = Workflow(
            image_config["api_graph"], image_config["bindings"], image_config["outputs"]
        )
        transport = ComfyUI(image_config["base_url"], token_env=image_config.get("token_env"))
        clients.append(transport)
        image_options = dict(
            transport=transport,
            workflow=workflow,
            staging=image_config["staging"],
        )
    outbound = Sender(contracts, client("nonebot"))
    # Explicit functional commands. With no `direct` section nothing is registered, so no
    # text is ever claimed as a command and every message keeps following the chat chain.
    # The delivery port is the existing outbound sender: no second message exit is created.
    routing = config.get("direct")
    direct_options = None
    if routing:
        adapter = None
        if routing.get("adapter") == "synthetic":
            adapter = SyntheticPlugin(time.time, slow=routing.get("synthetic_slow", False))
        direct_options = dict(
            adapter=adapter,
            deliver=outbound,
            timeout=routing.get("timeout", 20),
            request_expiry=routing.get("request_expiry", 600),
        )
    core = Core(
        Store(config["database_path"]),
        contracts,
        Origins(contracts, issuers),
        Memory(contracts, client("memory")),
        Gateway(contracts, client("gateway")),
        outbound,
        bindings=config.get("bindings", {}),
        roles=config.get("roles", {}),
        config_version=config.get("config_version"),
        policy=Policy(**config.get("policy", {})),
        short_context_policy=ShortContextPolicy(**config.get("short_context", {})),
        life_writing=config.get("life_writing", False),
        life_config_version=config.get("life_config_version"),
        image_options=image_options,
        writing_options=config.get("writing"),
        proactive_options=config.get("proactive"),
        direct_options=direct_options,
        web_sender=Sender(contracts, client("platform_sender")),
    )
    for spec in (routing or {}).get("commands", []):
        core.direct.register_command(**spec)
    return core, incoming, clients


def create_app(core=None, tokens=None):
    clients = []
    configured = core is not None
    if core is None and os.environ.get("TIANSHU_COMPANION_CONFIG"):
        config = json.loads(
            Path(os.environ["TIANSHU_COMPANION_CONFIG"]).read_text(encoding="utf-8")
        )
        core, tokens, clients = build_runtime(config)
        configured = True
    tokens = tokens or {}
    if len(set(tokens.values())) != len(tokens) or any(not token for token in tokens.values()):
        raise ValueError("Each caller needs its own non-empty service credential")

    async def worker():
        while True:
            try:
                await core.tick()
            except Exception as exc:
                LOG.error("Core tick failed: %s", type(exc).__name__)
            await asyncio.sleep(0.05)

    async def publisher():
        while True:
            try:
                await core.flush_outbox()
            except Exception as exc:
                LOG.error("Outbox worker failed: %s", type(exc).__name__)
            await asyncio.sleep(0.5)

    async def life_worker():
        while True:
            try:
                await core.life.work()
            except Exception as exc:
                LOG.error("Life worker failed: %s", type(exc).__name__)
            await asyncio.sleep(30)

    async def image_worker():
        while True:
            try:
                await core.images.work()
            except Exception as exc:
                LOG.error("Image worker failed: %s", type(exc).__name__)
            await asyncio.sleep(2)

    async def writing_worker():
        while True:
            try:
                await core.writing.work()
            except Exception as exc:
                LOG.error("Writing worker failed: %s", type(exc).__name__)
            await asyncio.sleep(5)

    async def proactive_worker():
        while True:
            try:
                await core.proactive.work()
            except Exception as exc:
                LOG.error("Proactive worker failed: %s", type(exc).__name__)
            await asyncio.sleep(2)

    async def direct_worker():
        # Commands execute here, off the chat path and off the ingest route, so a slow
        # plugin cannot block message admission or the light conversation schedule.
        while True:
            try:
                await core.direct.work()
            except Exception as exc:
                LOG.error("Direct command worker failed: %s", type(exc).__name__)
            await asyncio.sleep(0.5)

    @asynccontextmanager
    async def lifespan(app):
        jobs = []
        if core:
            core.recover()
            jobs = [
                asyncio.create_task(worker()),
                asyncio.create_task(publisher()),
                asyncio.create_task(life_worker()),
                asyncio.create_task(image_worker()),
                asyncio.create_task(writing_worker()),
                asyncio.create_task(proactive_worker()),
                asyncio.create_task(direct_worker()),
            ]
        yield
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        if core:
            await core.close()
        for client in clients:
            await client.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.core = core

    @app.get("/healthz")
    async def health():
        return dict(alive=True, configured=configured, external_dependencies="not_verified")

    async def dispatch(request, operation):
        request_id = uid("req")
        try:
            if core is None:
                raise Fault("dependency_unavailable")
            auth = request.headers.get("Authorization", "")
            service = next(
                (
                    name
                    for name, token in tokens.items()
                    if hmac.compare_digest(auth.encode(), ("Bearer " + token).encode())
                ),
                None,
            )
            if service is None:
                raise Fault("unauthorized")
            content = bytearray()
            async for chunk in request.stream():
                content.extend(chunk)
                if len(content) > 1_000_000:
                    raise Fault("invalid_input")
            try:
                body = strict_json(content)
            except (ValueError, UnicodeError):
                raise Fault("invalid_input") from None
            if isinstance(body, dict) and isinstance(body.get("command"), dict):
                candidate = body["command"].get("request_id")
                try:
                    core.contracts.check("common#id", candidate)
                    request_id = candidate
                except Fault:
                    pass
            elif isinstance(body, dict):
                candidate = body.get("request_id")
                try:
                    core.contracts.check("common#id", candidate)
                    request_id = candidate
                except Fault:
                    pass
            if operation == "ingest":
                result = await core.ingest(service, body, defer_processing=True)
                return JSONResponse(
                    result, background=BackgroundTask(core.acknowledge_ingest, result["receipt_id"])
                )
            if operation == "ingest-actors":
                result = await core.ingest_actors(service, body, defer_processing=True)

                async def release():
                    for outcome in result["outcomes"]:
                        if outcome["receipt"]:
                            await core.acknowledge_ingest(outcome["receipt"]["receipt_id"])

                return JSONResponse(result, background=BackgroundTask(release))
            if operation == "web-snapshot":
                return await core.web_snapshot(service, body)
            if operation == "facts":
                return core.source_facts(service, body)
            if operation == "direct-command":
                return await core.direct_command(service, body)
            if operation == "commands":
                return core.commands(service)
            if operation == "capability":
                return await core.capability(service, body)
            return await core.cancel(service, body)
        except Fault as error:
            return JSONResponse(error.wire(request_id), status_code=error.status)

    @app.post("/internal/v1/conversation/ingest")
    async def ingest(request: Request):
        return await dispatch(request, "ingest")

    @app.post("/internal/v1/conversation/cancel")
    async def cancel(request: Request):
        return await dispatch(request, "cancel")

    @app.post("/internal/v1/conversation/ingest-actors")
    async def ingest_actors(request: Request):
        return await dispatch(request, "ingest-actors")

    @app.post("/internal/v1/conversation/web-snapshot")
    async def web_snapshot(request: Request):
        return await dispatch(request, "web-snapshot")

    @app.post("/internal/v1/source-facts/read")
    async def source_facts(request: Request):
        return await dispatch(request, "facts")

    @app.post("/internal/v1/conversation/direct-command")
    async def direct_command(request: Request):
        return await dispatch(request, "direct-command")

    @app.post("/internal/v1/direct/commands")
    async def commands(request: Request):
        return await dispatch(request, "commands")

    @app.post("/internal/v1/capability/execute")
    async def capability(request: Request):
        return await dispatch(request, "capability")

    return app
