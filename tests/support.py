"""Deterministic synthetic dependencies. Never imported by application code."""

import asyncio
import copy
import json
import os
import time
import httpx
from pathlib import Path

from tianshu_companion.clients import command, uid, utc
from tianshu_companion.contracts import Contracts, Fault
from tianshu_companion.core import Core, Policy
from tianshu_companion.store import Store


def native_sse(handler):
    """Present synthetic completion fixtures on the actual native streaming boundary."""
    import inspect

    def convert(request, response):
        if request.url.path != "/v1/chat/completions" or response.status_code != 200:
            return response
        value = response.json()
        chunks = []
        for index, choice in enumerate(value["choices"]):
            chunks.append(
                dict(choices=[dict(index=index, delta=choice["message"], finish_reason=None)])
            )
            chunks.append(
                dict(choices=[dict(index=index, delta={}, finish_reason=choice["finish_reason"])])
            )
        body = "".join(
            "data: " + json.dumps(chunk, ensure_ascii=False) + "\n\n" for chunk in chunks
        )
        return httpx.Response(
            200, text=body + "data: [DONE]\n\n", headers={"Content-Type": "text/event-stream"}
        )

    if inspect.iscoroutinefunction(handler):

        async def stream(request):
            return convert(request, await handler(request))
    else:

        def stream(request):
            return convert(request, handler(request))

    return stream


def contracts():
    if os.environ.get("TIANSHU_CONTRACTS"):
        return Contracts(os.environ["TIANSHU_CONTRACTS"])
    context = json.loads(
        (Path(__file__).parents[1] / ".runtime/workspace-context.json").read_text(encoding="utf-8")
    )
    return Contracts(Path(context["workspace"]) / "contracts/text-dialogue/v1")


class Clock:
    def __init__(self):
        self.now = time.time()

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class FakeOrigins:
    def __init__(self, clock):
        self.clock, self.values = clock, {}
        self.unavailable = False

    async def resolve(self, service, envelope, now):
        if self.unavailable:
            raise Fault("dependency_unavailable")
        ctx = self.values.get(envelope["origin"]["assertion_ref"])
        if not ctx or ctx["authenticated_service"] != service or ctx.get("revoked"):
            raise Fault("forbidden")
        return copy.deepcopy(ctx)


class FakeMemory:
    def __init__(self, clock):
        self.clock, self.accounts = clock, {}
        self.selections, self.commits = [], []
        self.scope_version = 1
        self.unavailable = False
        self.fail_commit = False
        self.profile_selections = []
        self.profile_version = 1

    async def identity(self, origin, account, now):
        if self.unavailable:
            raise Fault("dependency_unavailable")
        key = tuple(sorted(account.items()))
        return self.accounts.setdefault(key, uid("person")), 1

    async def resolve_identity(self, origin, account):
        return self.accounts.get(tuple(sorted(account.items())))

    async def select(
        self, origin, scope, text, budget, known_version=None, *, known=None, time_range=None
    ):
        if self.unavailable:
            raise Fault("dependency_unavailable")
        if known_version is not None and known_version != self.scope_version:
            raise Fault("scope_changed")
        self.selections.append(
            dict(scope=scope, budget=budget, text=text, known_version=known_version)
        )
        return dict(
            schema_version=1,
            version_domain="memory-context/v1",
            association_version=1,
            scope_checks=[
                dict(
                    scope=scope,
                    scope_version=self.scope_version,
                    association_id=None,
                    association_version=1,
                )
            ],
            coverage=dict(
                matched_groups=0,
                returned_groups=0,
                complete=True,
                time_basis="source_sent_at",
                history_complete=False,
                missing_source_times=0,
            ),
            request_id=uid("req"),
            effective_scope=scope,
            scope_version=self.scope_version,
            verified_at=utc(self.clock()),
            valid_until=utc(self.clock() + 3600),
            selected_units=[],
            dependency_groups=[],
            budget_used=dict(tokens=0, bytes=0),
            omissions=["no_match"],
        )

    async def profiles(self, origin, scope, target, text, selection, budget, known_version=None):
        if self.unavailable:
            raise Fault("dependency_unavailable")
        if known_version is not None and known_version != self.profile_version:
            raise Fault("scope_changed")
        self.profile_selections.append(
            dict(
                origin=origin,
                scope=scope,
                target=target,
                text=text,
                selection=selection,
                budget=budget,
                known_version=known_version,
            )
        )
        return dict(
            schema_version=1,
            version_domain="profile-memory/v1",
            request_id=uid("req"),
            requester_scope=scope,
            target=target,
            scope_version=self.profile_version,
            verified_at=utc(self.clock()),
            valid_until=utc(self.clock() + 3600),
            selected_units=[],
            dependency_groups=[],
            budget_used=dict(tokens=0, bytes=0),
            omissions=["no_match"],
        )

    async def commit(self, event):
        if self.fail_commit:
            raise Fault("dependency_unavailable")
        duplicate = any(e["event_id"] == event["event_id"] for e in self.commits)
        self.commits.append(copy.deepcopy(event))
        return dict(
            schema_version=1,
            event_id=event["event_id"],
            turn_id=event["aggregate_id"],
            input_revision=event["input_revision"],
            state="duplicate" if duplicate else "accepted",
            candidate_job_ref="candidate:fixture",
            confirmed_memory_written=False,
        )

    async def check_sources(self, turn, sources):
        if self.unavailable:
            raise Fault("dependency_unavailable")
        return self.scope_version


class FakeGateway:
    def __init__(self):
        self.calls, self.gates = [], {}
        self.segments = ["合成回复一", "合成回复二"]
        self.fail = False

    async def generate(self, turn, messages):
        self.calls.append((turn["sequence"], copy.deepcopy(messages)))
        if turn["sequence"] in self.gates:
            await self.gates[turn["sequence"]].wait()
        if self.fail:
            raise Fault("dependency_unavailable")
        return self.segments, dict(usage=None, fixture_only=True)


class FakeSender:
    def __init__(self, clock):
        self.clock = clock
        self.calls, self.answers = [], {}
        self.states = []
        self.gate = None

    def receipt(self, request, state="sent"):
        return dict(
            schema_version=1,
            request_id=request["command"]["request_id"],
            reply_id=request["reply_id"],
            segment_sequence=request["segment_sequence"],
            attempt_id="attempt:" + request["reply_id"],
            state=state,
            channel_message_ids=["synthetic:" + request["reply_id"]] if state == "sent" else [],
            observed_at=utc(self.clock()),
            retry_safe=state == "failed",
        )

    async def send(self, request):
        self.calls.append(copy.deepcopy(request))
        if self.gate:
            await self.gate.wait()
        state = self.states.pop(0) if self.states else "sent"
        return self.receipt(request, state)

    async def reconcile(self, request):
        return self.answers.get(request["reply_id"])


class DirectPlugin:
    """Synthetic plugin adapter for routing tests.

    It never models a real GsCore or group-management API; it exists to drive the port.
    `reply_for_core` deliberately violates the single-reply-owner rule so the engine's
    refusal can be tested.
    """

    adapter = "synthetic"
    available = True

    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.gate = None
        self.slow = set()
        self.states = []
        self.reply_for_core = False
        self.reply = None

    async def execute(self, request):
        self.calls.append(copy.deepcopy(request))
        if self.gate:
            await self.gate.wait()
        state = self.states.pop(0) if self.states else "completed"
        if state != "completed":
            return dict(
                adapter=self.adapter,
                request_id=request["request_id"],
                attempt_id=request["attempt_id"],
                state=state,
                task_ref=None,
                result=None,
                reply=None,
                channel_message_ids=[],
            )
        if request["command_id"] in self.slow:
            return dict(
                adapter=self.adapter,
                request_id=request["request_id"],
                attempt_id=request["attempt_id"],
                state="accepted",
                task_ref="synthetic:" + request["attempt_id"],
                result=None,
                reply=None,
                channel_message_ids=[],
            )
        reply = None
        if request["reply_to"] == "bridge" or self.reply_for_core:
            reply = dict(text=self.reply or ("[合成] " + request["command_id"]))
        return dict(
            adapter=self.adapter,
            request_id=request["request_id"],
            attempt_id=request["attempt_id"],
            state="completed",
            task_ref=None,
            result=dict(kind="synthetic", parameters=request["parameters"]),
            reply=reply,
            channel_message_ids=[],
        )

    def complete(self, request_id, attempt_id, *, reply=None):
        return dict(
            adapter=self.adapter,
            request_id=request_id,
            attempt_id=attempt_id,
            state="completed",
            task_ref=None,
            result=dict(kind="synthetic", late=True),
            reply=dict(text=reply or "[合成] 完成"),
            channel_message_ids=[],
        )

    async def reconcile(self, request):
        return None


class FakeDelivery:
    """Synthetic outbound port; the production port is the existing bridge send path."""

    available = True

    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.states = []
        self.answers = {}
        self.gate = None
        self.retry_safe = False

    def receipt(self, request, state="sent"):
        return dict(
            schema_version=1,
            request_id=request["command"]["request_id"],
            reply_id=request["reply_id"],
            segment_sequence=request["segment_sequence"],
            attempt_id="attempt:" + request["reply_id"],
            state=state,
            channel_message_ids=["synthetic:" + request["reply_id"]] if state == "sent" else [],
            observed_at=utc(self.clock()),
            retry_safe=state == "failed" and self.retry_safe,
        )

    async def send(self, request):
        self.calls.append(copy.deepcopy(request))
        if self.gate:
            await self.gate.wait()
        state = self.states.pop(0) if self.states else "sent"
        return self.receipt(request, state)

    async def reconcile(self, request):
        return self.answers.get(request["reply_id"])


def persona_config(version=1, **entries):
    """A deployment document shape, exactly as the service and the CLI both read it."""
    return dict(
        config_version=version,
        roles=entries
        or {
            "actor:a": dict(version=1, persona="Role A"),
            "actor:b": dict(version=1, persona="Role B"),
        },
    )


class Harness:
    def __init__(self, path=":memory:", *, direct_options=None, personas=None, **policy):
        self.clock, self.contracts = Clock(), contracts()
        self.origins, self.memory = FakeOrigins(self.clock), FakeMemory(self.clock)
        self.gateway, self.sender = FakeGateway(), FakeSender(self.clock)
        self.direct_plugin, self.delivery = DirectPlugin(self.clock), FakeDelivery(self.clock)
        self.path = path
        self.options = dict(
            bindings={
                "qq-private": dict(
                    service="nonebot",
                    namespace="qq",
                    audience="self_private",
                    actor_ids=["actor:a", "actor:b"],
                    classification=dict(
                        value="real",
                        basis="registered_input_mode",
                        policy_ref="fixture:real",
                        policy_version=1,
                    ),
                ),
                "qq-group": dict(
                    service="nonebot",
                    namespace="qq",
                    audience="group",
                    actor_ids=["actor:a", "actor:b"],
                    classification=dict(
                        value="real",
                        basis="registered_input_mode",
                        policy_ref="fixture:real",
                        policy_version=1,
                    ),
                ),
            },
            roles={
                "actor:a": dict(version=1, persona="Role A"),
                "actor:b": dict(version=1, persona="Role B"),
            },
            config_version=1,
            policy=Policy(**policy),
            clock=self.clock,
            direct_options={
                "adapter": self.direct_plugin,
                "deliver": self.delivery,
                **(direct_options or {}),
            },
            **(dict(personas=True, persona_import=personas) if personas is not None else {}),
        )
        self.core = self.new_core()

    def new_core(self, **overrides):
        return Core(
            Store(self.path),
            self.contracts,
            self.origins,
            self.memory,
            self.gateway,
            self.sender,
            **{**self.options, **overrides},
        )

    def request(
        self,
        text="你好",
        *,
        account="a",
        channel="private:a",
        group=False,
        actor="actor:a",
        message=None,
        revision=1,
        kind="message",
        targets=None,
    ):
        message = message or uid("msg")
        ref = uid("origin")
        key = dict(
            namespace="qq",
            binding_id="qq-group" if group else "qq-private",
            channel_conversation_id=channel,
            thread_id=None,
        )
        author = dict(namespace="qq", immutable_account_id=account)
        self.origins.values[ref] = dict(
            issuer="nonebot",
            authenticated_service="nonebot",
            audience_service="companion",
            assertion_ref=ref,
            verified_account=author,
            principal_id=None,
            verified_channel=key,
            allowed_scope=dict(
                actor_id=actor,
                person_id=None,
                audience="group" if group else "self_private",
                conversation_id=None,
            ),
            expires_at=utc(self.clock() + 3600),
            revoked=False,
        )
        return dict(
            command=command(dict(assertion_ref=ref), uid("key"), self.clock(), 3600),
            message_key=dict(channel=key, message_id=message, revision=revision),
            author=author,
            sent_at=utc(self.clock()),
            kind=kind,
            parts=[] if kind == "retract" else [dict(kind="text", text=text)],
            reply_refs=[],
            mentioned_accounts=[],
            target_actor_ids=[actor] if targets is None else targets,
        )

    async def ingest(self, **kwargs):
        request = self.request(**kwargs)
        return await self.core.ingest("nonebot", request)

    async def cycles(self, count=30):
        for _ in range(count):
            await self.core.tick()
            await asyncio.sleep(0)
        for task in [*self.core.jobs.values(), *self.core.send_jobs.values()]:
            if task.done() and not task.cancelled() and task.exception():
                raise task.exception()

    def turns(self):
        return self.core.store.list("turns")

    async def cancel(self, turn):
        request = dict(
            command=command(turn["origin"], uid("cancel"), self.clock()),
            conversation_id=turn["conversation_id"],
            turn_id=turn["id"],
            expected_version=turn["version"],
            reason="explicit_user_cancel",
        )
        return await self.core.cancel("nonebot", request)
