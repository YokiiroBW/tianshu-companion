"""HTTP adapter. Configuration is explicit and secrets come from environment only."""

import asyncio
import hmac
import json
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.background import BackgroundTask

from .clients import (
    BotSenderRouter,
    Gateway,
    JsonService,
    Memory,
    Origins,
    PlatformBotSender,
    Sender,
    set_log_port,
    uid,
)
from .contracts import Contracts, Fault, strict_json
from .core import Core, Policy
from .direct import SyntheticPlugin
from . import health as health_module
from . import observability as obs
from .life_read import MAX_REQUEST_BYTES, LifeRead
from .life_read import readers as read_reader_map
from .life_read_queries import LifeReadQueries
from .store import Store
from .images import ComfyUI, Workflow
from .short_context import ShortContextPolicy
from .model_selection import HttpDefaultModelSelector
from .worker_health import WorkerHealth
from . import runtime_capabilities

LOG = logging.getLogger(__name__)

_JSON_MEDIA_TYPE = "application/json"
_TOKEN = frozenset("!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

# Background-loop backoff bounds. A failure is never retried in a tight loop: the delay
# grows from 50ms to a 30s ceiling, so a permanently broken dependency costs one attempt per
# 30 seconds instead of twenty per second, and each real failure is still reported.
BACKOFF_MIN_SECONDS = 0.05
BACKOFF_MAX_SECONDS = 30.0


def json_media_type(headers):
    """One settled `application/json` content type, or a refusal.

    The four authorized read routes accept a JSON document, so the request must say so before
    anything is parsed: a missing header, a different media type, a repeated header (RFC 9110
    makes `Content-Type` a single-occurrence field, so two of them are ambiguous however they
    read), or a parameter that is not a well-formed `charset` are all `invalid_input` - never
    quietly treated as JSON because the bytes happen to parse. A syntactically valid charset
    parameter is allowed and does not change the decoding, which stays UTF-8.
    """
    values = headers.getlist("content-type")
    if len(values) != 1:
        return False
    parts = values[0].split(";")
    if parts[0].strip().lower() != _JSON_MEDIA_TYPE:
        return False
    charset = False
    for parameter in parts[1:]:
        name, separator, value = parameter.partition("=")
        value = value.strip().strip('"')
        if (
            not separator
            or name.strip().lower() != "charset"
            or charset
            or not value
            or not set(value) <= _TOKEN
        ):
            return False
        charset = True
    return True


def log_adapter_from_environment(environ=None):
    """Assemble the runtime event port from the explicit deployment configuration.

    `TIANSHU_LOG_DIR` names the directory; the segment and directory budgets are optional
    explicit bounds. With no directory the adapter is the honest non-durable stderr channel
    and readiness says `non_durable`. An unusable value is a startup failure: a deployment
    that meant to keep full logs must not come up quietly writing nowhere.
    """
    environ = os.environ if environ is None else environ

    def count(name, default):
        raw = environ.get(name)
        if raw in (None, ""):
            return default
        try:
            value = int(raw)
        except (TypeError, ValueError):
            raise ValueError(f"{name} must be an integer byte count") from None
        return value

    directory = environ.get(obs.ENVIRONMENT_LOG_DIR) or None
    try:
        return obs.build_log_adapter(
            directory,
            max_segment_bytes=count(
                obs.ENVIRONMENT_LOG_SEGMENT_BYTES, obs.DEFAULT_MAX_SEGMENT_BYTES
            ),
            max_directory_bytes=count(
                obs.ENVIRONMENT_LOG_DIRECTORY_BYTES, obs.DEFAULT_MAX_DIRECTORY_BYTES
            ),
        )
    except ValueError as error:
        raise ValueError("Invalid runtime log configuration: " + str(error)) from None


async def _sleep_interruptibly(seconds, wait):
    """Cancelable bounded wait: a shutdown request ends the wait immediately."""
    if wait is None:
        await asyncio.sleep(seconds)
        return
    try:
        await asyncio.wait_for(wait.wait(), timeout=seconds)
    except (TimeoutError, asyncio.TimeoutError):
        pass


async def run_loop(
    name,
    work,
    *,
    port=None,
    interval=0.05,
    wait=None,
    counter=None,
    clock=time.monotonic,
    sleep=None,
    workers=None,
):
    """One background loop: bounded cancelable backoff, and one event per real outcome.

    Each iteration either did real work, did nothing, or failed. A failure is reported every
    single time it happens - never sampled, never filtered as a duplicate - and the next
    attempt is delayed by a bounded, cancelable backoff. Nothing is swallowed as "already
    seen".

    A loop's own lifecycle is not reported: starting, idling and stopping are not outcomes, and
    a record for each of the seven loops would be written before the process can serve at all -
    which is precisely where a hard limit on the channel turns noise into a stalled start.
    """
    pause = sleep if sleep is not None else _sleep_interruptibly

    def report(event, outcome, **fields):
        if port is not None:
            port.emit(event, outcome, **fields)

    failures = 0
    while True:
        if wait is not None and wait.is_set():
            break
        outcome, error = "idle", None
        # The baseline belongs to this pass: read it before the work runs, so an idle pass
        # cannot inherit the previous pass's progress and report work it did not do.
        before = counter(clock) if counter is not None else None
        try:
            await work()
            outcome = "worked" if counter is None or counter(clock) != before else "idle"
        except asyncio.CancelledError:
            # A cancelled pass is a shutdown, not a failure: it is not reported as one, it is
            # not retried, and the loop ends instead of rewriting the cancellation into a
            # bounded-delay retry.
            raise
        except Exception as failure:  # noqa: BLE001 - every real failure is reported below
            outcome = "failed"
            error = failure
        if workers is not None:
            workers.completed(name, failed=outcome == "failed")
        if outcome != "failed":
            failures = 0
        if outcome == "worked":
            report("runtime.background_work", "succeeded", level="DEBUG")
        elif outcome == "failed":
            failures += 1
            report(
                "runtime.background_failed",
                "failed",
                level="ERROR",
                error_code=obs.failure_class(error),
            )
        # An idle pass is deliberately not reported. Nothing happened, so a record would be a
        # false account of the pass - and at these intervals idle records would outnumber real
        # ones by orders of magnitude, filling the declared directory budget with noise while
        # the events an operator needs to see were crowded out.
        delay = (
            # The first failure waits the documented minimum and each further failure doubles
            # it, so the delay is driven by the earlier failures, not by this one.
            min(BACKOFF_MAX_SECONDS, BACKOFF_MIN_SECONDS * (2 ** min(failures - 1, 16)))
            if failures
            else interval
        )
        await pause(delay, wait)


class RuntimeEvents:
    """Application middleware: one correlation ID, one admitted start, one truthful end.

    Everything that answers a probe rather than a caller is deliberately excluded. A probe must
    be purely read-only, which includes not writing a log record, so the middleware never sees
    it and the log directory is byte-for-byte identical before and after a probe.

    Admission is the gate, and it is a *durable* one: the start record is written and fsynced
    before the application is called, awaited outside any transaction, so a request that cannot
    be accounted for in the log never runs. The alternative - checking a health flag and hoping
    the later write succeeds - would let side effects happen with no record of them.

    The end record reports what actually happened: the status the application sent, the
    exception it raised, or the cancellation it received. A rejected or failed request is not
    reported as a success.
    """

    def __init__(self, app, port, enabled=True):
        self.app, self.port, self.enabled = app, port, enabled

    async def __call__(self, scope, receive, send):
        path = scope.get("path") if scope["type"] == "http" else None
        if (
            not self.enabled
            or scope["type"] != "http"
            or path in health_module.PROBE_PATHS
            or path == runtime_capabilities.PATH
        ):
            await self.app(scope, receive, send)
            return
        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in (scope.get("headers") or [])
        }
        offered = headers.get(obs.CORRELATION_HEADER.lower())
        # An invalid value - including one that looks like a secret - is replaced and never
        # echoed back, so no caller can plant a string of its choosing in the event stream.
        correlation = offered if obs.valid_correlation_id(offered) else obs.new_correlation_id()
        started = time.perf_counter()
        announced = refusal = False
        status = {"code": None}
        outcome = {"value": "succeeded", "code": None, "level": None}

        async def wrapped_send(message):
            nonlocal announced
            if message["type"] == "http.response.start":
                if refusal:
                    # The 503 has already been sent; the application's own response is not
                    # written on top of it, so the caller sees exactly one answer.
                    return
                status["code"] = message["status"]
                announced = True
            elif message["type"] == "http.response.body" and not announced:
                return
            await send(message)

        with obs.correlation_scope(correlation):
            try:
                admitted = await obs.admit(
                    self.port, "service.request.started", "started", correlation_id=correlation
                )
            except asyncio.CancelledError:
                raise
            if not admitted:
                # No durable record, so no business. Nothing has run yet, which is exactly why
                # refusing here cannot lose an in-flight side effect - and why no request body
                # needs to be read first.
                refusal = True
                code = "log_capacity_exhausted" if self._full() else "log_unavailable"
                obs.emit(
                    self.port,
                    "service.request.finished",
                    "rejected",
                    correlation_id=correlation,
                    error_code=code,
                    duration_ms=round((time.perf_counter() - started) * 1000, 3),
                )
                await self._refuse(send, code)
                return
            try:
                await self.app(scope, receive, wrapped_send)
            except asyncio.CancelledError:
                outcome.update(value="cancelled", code="cancelled", level="WARNING")
                raise
            except Exception as error:
                outcome.update(value="failed", code=obs.failure_class(error), level="ERROR")
                raise
            finally:
                self._report_finished(status["code"], outcome, correlation, started)

    def _report_finished(self, status, outcome, correlation, started):
        """Record the real terminal state of the request, never a blanket success."""
        if outcome["value"] == "succeeded" and isinstance(status, int) and status >= 400:
            outcome.update(
                value="rejected" if status < 500 else "failed",
                code="unauthorized" if status in (401, 403) else "invalid_input",
                level="WARNING" if status < 500 else "ERROR",
            )
        obs.emit(
            self.port,
            "service.request.finished",
            outcome["value"],
            level=outcome["level"],
            correlation_id=correlation,
            error_code=outcome["code"],
            duration_ms=round((time.perf_counter() - started) * 1000, 3),
        )

    async def _refuse(self, send, code):
        """Refuse a request that has no accounting record, naming the real reason.

        The header says what actually happened: `exhausted` only when the directory budget really
        ran out, `unavailable` when the destination could not take the record at all. Reporting
        "exhausted" for a failed writer would send an operator to look at the wrong thing.
        """
        body = json.dumps({"code": code}).encode()
        reason = b"exhausted" if code == "log_capacity_exhausted" else b"unavailable"
        await send(
            {
                "type": "http.response.start",
                "status": 503,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                    (b"x-tianshu-log-capacity", reason),
                    (b"connection", b"close"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    def _full(self):
        return bool(self.port is not None and getattr(self.port, "capacity_exhausted", False))

    def _refused(self):
        return self.port is not None and not self.port.accepts_business()


def build_runtime(config):
    automatic_memory_candidates = runtime_capabilities.candidates_enabled(
        config.get("automatic_memory_candidates", True)
    )
    contracts = Contracts(config["contracts_path"])
    clients = []

    def client(name, label=None):
        entry = config.get("services", {}).get(name, {})
        value = JsonService(
            entry.get("url"),
            os.environ.get(entry.get("token_env", "")),
            ca_file=entry.get("ca_file"),
            label=label,
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
                issuers[service] = (entry["issuer"], client(entry["origin_service"], "origins"))
    # Registered character personas: the deployment mapping seeds one absolute initial
    # version, and every later version is an explicit operator act. The management
    # credential is separate from the chat/ingest credentials so no channel or bridge
    # credential can ever reach a persona write.
    persona_config = config.get("personas")
    persona_token = None
    if persona_config:
        persona_token = os.environ.get(persona_config["admin_token_env"])
        if persona_token:
            incoming["persona_admin"] = persona_token
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
    platform_client = client("platform_sender", "channel")
    bot_binding_management = config.get("bot_binding_management_enabled", False)
    if type(bot_binding_management) is not bool:
        raise ValueError("bot_binding_management_enabled must be boolean")
    if bot_binding_management:
        if (
            bot_binding_management is not True
            or config.get("callers", {}).get("platform", {}).get("issuer") != "platform"
            or not platform_client.url
            or not platform_client.token
        ):
            raise ValueError(
                "Bot binding management requires the registered platform issuer and sender"
            )
    outbound = BotSenderRouter(
        Sender(contracts, client("nonebot", "channel")),
        PlatformBotSender(contracts, platform_client),
        config.get("bot_platform_bindings", []),
        config.get("bindings", {}),
    )
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
    provider_selector = None
    if config.get("provider_self_service", False):
        provider_selector = HttpDefaultModelSelector(client("provider_selector", "platform"))
    qq_admin = None
    qq_admission = (
        any(binding.get("namespace") == "qq" for binding in config.get("bindings", {}).values())
        or bot_binding_management
        or config.get("bot_observation_enabled", False)
    )
    if qq_admission and "qq_admin" not in config.get("services", {}):
        raise ValueError("QQ admission requires the Platform QQ administrator reader")
    if "qq_admin" in config.get("services", {}):
        from .qq_identity import QQAdminClient

        qq_client = client("qq_admin", "platform")
        if not qq_client.url or not qq_client.token:
            raise ValueError("QQ administrator reader requires a registered HTTPS service")
        qq_admin = QQAdminClient(qq_client)
    from .relationships import assemble as assemble_relationships

    memory = Memory(contracts, client("memory", "memory"))
    relationships = assemble_relationships(config.get("relationships"), memory.client)
    core = Core(
        Store(config["database_path"]),
        contracts,
        Origins(contracts, issuers),
        memory,
        Gateway(contracts, client("gateway", "gateway")),
        outbound,
        bindings=config.get("bindings", {}),
        roles=config.get("roles", {}),
        config_version=config.get("config_version"),
        default_model_selector=provider_selector,
        qq_admin=qq_admin,
        qq_identity_required=qq_admission,
        relationships=relationships,
        policy=Policy(**config.get("policy", {})),
        short_context_policy=ShortContextPolicy(**config.get("short_context", {})),
        life_writing=config.get("life_writing", False),
        automatic_memory_candidates=automatic_memory_candidates,
        life_config_version=config.get("life_config_version"),
        image_options=image_options,
        writing_options=config.get("writing"),
        proactive_options=config.get("proactive"),
        direct_options=direct_options,
        web_sender=Sender(contracts, platform_client),
        personas=bool(persona_config),
        # The whole deployment document: the persona module owns the rule for where a
        # character is declared, so startup and the maintenance CLI read it identically.
        persona_import=config if persona_config else None,
    )
    if bot_binding_management:
        from .bot_bindings import BotBindings

        core.bot_bindings = BotBindings(core, outbound, config.get("bindings", {}))
    observe_enabled = config.get("bot_observation_enabled", False)
    if type(observe_enabled) is not bool:
        raise ValueError("bot_observation_enabled must be boolean")
    if observe_enabled:
        if config.get("callers", {}).get("platform", {}).get("issuer") != "platform":
            raise ValueError("Bot observation requires the registered Platform caller")
        from .observation import Observations

        core.observations = Observations(
            config["database_path"] + ".observations.sqlite", core.memory.client
        )
    for spec in (routing or {}).get("commands", []):
        core.direct.register_command(**spec)
    # The optional authorized read port. Its deployment mapping is validated here, where the
    # caller registry is known, so an entry naming a service that was never configured fails
    # startup instead of becoming a reader that can never authenticate.
    life_readers = config.get("life_readers")
    if life_readers is not None:
        read_reader_map(life_readers, set(config.get("callers", {})))
    return core, incoming, clients, life_readers


def apply_deployment_overrides(config, environ=None):
    """Let the explicitly deployed paths win over the ones written in the document.

    The runtime entry point resolves `--database` / `--contracts` (then their environment
    variables, then the platform default) and exports the result as `TIANSHU_COMPANION_DATABASE`
    and `TIANSHU_CONTRACTS`. Those are *deployment* decisions: a mounted volume and a mounted
    contract pack. The configuration document is a description of the deployment, and a stale
    one must not silently send the process to a different database or a contract pack that is
    not the one mounted.

    Precedence, from strongest to weakest: explicit deployment variable, value in the document,
    platform default. The document is copied, never mutated, so a caller that keeps its own
    reference still sees what it loaded.
    """
    environ = os.environ if environ is None else environ
    resolved = dict(config)
    for key, variable in (
        ("database_path", "TIANSHU_COMPANION_DATABASE"),
        ("contracts_path", "TIANSHU_CONTRACTS"),
    ):
        deployed = environ.get(variable)
        if deployed:
            resolved[key] = deployed
    return resolved


def create_app(core=None, tokens=None, life_readers=None):
    clients = []
    configured = core is not None
    if core is None and os.environ.get("TIANSHU_COMPANION_CONFIG"):
        config = json.loads(
            Path(os.environ["TIANSHU_COMPANION_CONFIG"]).read_text(encoding="utf-8")
        )
        core, tokens, clients, life_readers = build_runtime(apply_deployment_overrides(config))
        configured = True
    tokens = tokens or {}
    if len(set(tokens.values())) != len(tokens) or any(not token for token in tokens.values()):
        raise ValueError("Each caller needs its own non-empty service credential")
    # Authorized life reading is assembled here rather than inside Core: the port needs no
    # chat state, and Core must not grow a second read path. With no `life_readers` section
    # the four routes answer 503 and nothing else about the deployment changes.
    reads = None
    if core is not None:
        # The registry is passed in so an entry naming a service that holds no credential is
        # a startup failure instead of a deployment that silently never authenticates.
        mapping = read_reader_map(life_readers, set(tokens))
        if mapping is not None:
            reads = LifeRead(
                LifeReadQueries(core.store.db),
                readers=mapping,
                contracts=core.contracts,
            )

    log_port = log_adapter_from_environment()
    set_log_port(log_port)
    stopping = asyncio.Event()
    workers = WorkerHealth()

    def _counter(owner, name):
        """A read-only view of the pass counter the work itself advances."""
        return lambda clock=None: owner.pass_count(name)

    def loop(name, work, interval, counter=None):
        """One background loop, reported and backed off through the shared loop contract.

        Each pass reports whether it did real work or was idle, and every real failure is
        reported as it happens - nothing is swallowed as "already seen". A failure delays the
        next attempt by a bounded, cancelable backoff instead of retrying in a tight loop.
        """
        task = asyncio.create_task(
            run_loop(
                name,
                work,
                port=log_port,
                interval=interval,
                wait=stopping,
                counter=counter,
                clock=time.monotonic,
                workers=workers,
            )
        )
        workers.register(name, interval, task)
        return task

    @asynccontextmanager
    async def lifespan(app):
        jobs = []
        try:
            if core:
                await obs.admit(log_port, "runtime.started", "succeeded")
                core.recover()
                jobs = [
                    loop("core.tick", core.tick, 0.05, _counter(core, "core.tick")),
                    loop("core.outbox", core.flush_outbox, 0.5, _counter(core, "core.outbox")),
                    loop("life.work", core.life.work, 30, _counter(core, "life.work")),
                    loop("images.work", core.images.work, 2, _counter(core, "images.work")),
                    loop("writing.work", core.writing.work, 5, _counter(core, "writing.work")),
                    loop(
                        "proactive.work",
                        core.proactive.work,
                        2,
                        _counter(core, "proactive.work"),
                    ),
                    # Commands execute here, off the chat path and off the ingest route, so a
                    # slow plugin cannot block message admission or the light conversation
                    # schedule.
                    loop("direct.work", core.direct.work, 0.5, _counter(core, "direct.work")),
                ]
                if hasattr(core, "observations"):

                    async def observation_flush():
                        while not stopping.is_set():
                            await core.observations.flush()
                            try:
                                await asyncio.wait_for(stopping.wait(), 2)
                            except asyncio.TimeoutError:
                                pass

                    jobs.append(asyncio.create_task(observation_flush()))
            yield
        finally:
            workers.stopping = True
            # Shutdown is itself reported durably, and in the only order that makes the
            # records trustworthy: `stopped` is confirmed on the disk *before* the writer is
            # closed, so the last line of the stream is never a record that was still queued
            # when the handle went away.
            await obs.admit(log_port, "runtime.stopping", "started")
            stopping.set()
            for job in jobs:
                job.cancel()
            await asyncio.gather(*jobs, return_exceptions=True)
            if core:
                await core.close()
            for client in clients:
                await client.close()
            await obs.admit(log_port, "runtime.stopped", "succeeded")
            close = getattr(log_port, "aclose", None)
            if close is not None:
                await close()
            else:
                log_port.close()

    health_view = health_module.Health(
        os.environ.get(health_module.TOKEN_ENV), core=core, log=log_port, workers=workers
    )
    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.core = core
    app.state.log = log_port
    app.state.health = health_view
    app.state.stopping = stopping

    @app.get("/healthz")
    async def health():
        return dict(alive=True, configured=configured, external_dependencies="not_verified")

    @app.get("/health/live")
    async def live():
        # Public minimum, and purely read-only: no credential, no dependency, no log write.
        return JSONResponse(health_module.live_payload())

    @app.get("/health/ready")
    async def ready(request: Request):
        # Its own credential, never a chat, bridge, ingest or persona-management one. An
        # undeployed diagnostics credential answers 503; a missing or wrong one answers 401.
        if not health_view.configured:
            return JSONResponse(
                {"code": "dependency_unavailable", "detail": "diagnostics_not_configured"},
                status_code=503,
            )
        if not health_view.authorized(request.headers.get("Authorization")):
            return JSONResponse({"code": "unauthorized"}, status_code=401)
        # The status code is the verdict, not decoration: a body that says `not_ready` while
        # the response says 200 is exactly the kind of false readiness a load balancer trusts.
        answer = health_view.ready()
        return JSONResponse(answer, status_code=200 if answer["status"] == "ready" else 503)

    @app.get(runtime_capabilities.PATH)
    async def capabilities(request: Request):
        if not health_view.configured:
            return JSONResponse({"code": "dependency_unavailable"}, status_code=503)
        if not health_view.authorized(request.headers.get("Authorization")):
            return JSONResponse({"code": "unauthorized"}, status_code=401)
        if core is None or getattr(core, "closed", False):
            return JSONResponse({"code": "dependency_unavailable"}, status_code=503)
        try:
            return JSONResponse(runtime_capabilities.read_capabilities(core))
        except sqlite3.Error:
            return JSONResponse({"code": "dependency_unavailable"}, status_code=503)

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
            if operation == "persona":
                return core.manage_persona(service, body)
            if operation == "role-runtime":
                return core.manage_role(service, body)
            if operation == "bot-binding-apply":
                if not hasattr(core, "bot_bindings"):
                    raise Fault("dependency_unavailable")
                return core.bot_bindings.apply(service, body)
            if operation == "bot-binding-status":
                if not hasattr(core, "bot_bindings"):
                    raise Fault("dependency_unavailable")
                return core.bot_bindings.status(service, body)
            if operation == "observation-ingest":
                if not hasattr(core, "observations"):
                    raise Fault("dependency_unavailable")
                return await asyncio.to_thread(core.observations.ingest, service, body)
            if operation == "observation-query":
                if not hasattr(core, "observations"):
                    raise Fault("dependency_unavailable")
                return await core.observations.query_archive(service, body)
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

    @app.post("/internal/v1/bot-bindings/apply")
    async def bot_binding_apply(request: Request):
        return await dispatch(request, "bot-binding-apply")

    @app.post("/internal/v1/bot-bindings/status")
    async def bot_binding_status(request: Request):
        return await dispatch(request, "bot-binding-status")

    @app.post("/internal/v2/observations/ingest")
    async def observation_ingest(request: Request):
        return await dispatch(request, "observation-ingest")

    @app.post("/internal/v2/observations/query")
    async def observation_query(request: Request):
        return await dispatch(request, "observation-query")

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

    @app.post("/internal/v1/persona/manage")
    async def persona_manage(request: Request):
        # Writer endpoint. It exists only when the dedicated management credential is
        # configured; with no credential `persona_admin` is not a known caller, so this
        # answers 401 and no unauthenticated remote write is possible.
        return await dispatch(request, "persona")

    @app.post("/internal/v1/role-runtime/manage")
    async def role_runtime_manage(request: Request):
        return await dispatch(request, "role-runtime")

    async def life_read_dispatch(request, operation):
        """The authorized read boundary: authenticate, bound the request, then read.

        This adapter does four things and nothing more: it maps the bearer credential to a
        service name, it requires the request to declare itself as JSON, it refuses a raw body
        over the documented ceiling before parsing, and it hands the parsed document to the
        read port. No reader identity is ever taken from the request body, and no fault of the
        read port's own rules is re-decided here. The credential is still settled first, so an
        unauthenticated caller learns only that it is unauthenticated.
        """
        request_id = uid("req")
        try:
            if reads is None:
                # No `life_readers` section: the port is not deployed, exactly as an
                # unconfigured dependency is reported anywhere else in this service.
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
            if not json_media_type(request.headers):
                # A media type is a claim about the document; refusing it here keeps a
                # mislabelled request out of the read port entirely instead of leaving the
                # only content-type judgement to whether the bytes happen to parse.
                raise Fault("invalid_input")
            content = bytearray()
            async for chunk in request.stream():
                content.extend(chunk)
                if len(content) > MAX_REQUEST_BYTES:
                    raise Fault("invalid_input")
            try:
                body = strict_json(content)
            except (ValueError, UnicodeError):
                raise Fault("invalid_input") from None
            return JSONResponse(reads.handle(service, operation, body))
        except Fault as error:
            return JSONResponse(error.wire(request_id), status_code=error.status)

    @app.post("/internal/v1/life-read/actors")
    async def life_read_actors(request: Request):
        return await life_read_dispatch(request, "actors")

    @app.post("/internal/v1/life-read/snapshot")
    async def life_read_snapshot(request: Request):
        return await life_read_dispatch(request, "snapshot")

    @app.post("/internal/v1/life-read/diaries")
    async def life_read_diaries(request: Request):
        return await life_read_dispatch(request, "diaries")

    @app.post("/internal/v1/life-read/revision")
    async def life_read_revision(request: Request):
        return await life_read_dispatch(request, "revision")

    # Installed last so it wraps every route above, and skipped for the two health probes so
    # a probe stays purely read-only. Nothing here decides a business question: it supplies a
    # validated correlation ID, records the request lifecycle, and refuses new business only
    # while the log budget is full.
    app.add_middleware(RuntimeEvents, port=log_port)
    return app
