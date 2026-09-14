"""HTTP adapter. Configuration is explicit and secrets come from environment only."""

import asyncio
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.background import BackgroundTask

from .clients import Gateway, JsonService, Memory, Origins, Sender, uid
from .contracts import Contracts, Fault, strict_json
from .core import Core, Policy
from .store import Store
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
    core = Core(
        Store(config["database_path"]),
        contracts,
        Origins(contracts, issuers),
        Memory(contracts, client("memory")),
        Gateway(contracts, client("gateway")),
        Sender(contracts, client("nonebot")),
        bindings=config.get("bindings", {}),
        roles=config.get("roles", {}),
        config_version=config.get("config_version"),
        policy=Policy(**config.get("policy", {})),
        short_context_policy=ShortContextPolicy(**config.get("short_context", {})),
    )
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

    @asynccontextmanager
    async def lifespan(app):
        jobs = []
        if core:
            core.recover()
            jobs = [asyncio.create_task(worker()), asyncio.create_task(publisher())]
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
            if operation == "facts":
                return core.source_facts(service, body)
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

    @app.post("/internal/v1/source-facts/read")
    async def source_facts(request: Request):
        return await dispatch(request, "facts")

    return app
