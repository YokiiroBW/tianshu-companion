"""Deterministic synthetic dependencies. Never imported by application code."""

import asyncio
import copy
import json
import os
import time
from pathlib import Path

from tianshu_companion.clients import command, uid, utc
from tianshu_companion.contracts import Contracts, Fault
from tianshu_companion.core import Core, Policy
from tianshu_companion.store import Store


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

    async def select(self, origin, scope, text, budget, known_version=None):
        if self.unavailable:
            raise Fault("dependency_unavailable")
        if known_version is not None and known_version != self.scope_version:
            raise Fault("scope_changed")
        self.selections.append(
            dict(scope=scope, budget=budget, text=text, known_version=known_version)
        )
        return dict(
            schema_version=1,
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


class Harness:
    def __init__(self, path=":memory:", **policy):
        self.clock, self.contracts = Clock(), contracts()
        self.origins, self.memory = FakeOrigins(self.clock), FakeMemory(self.clock)
        self.gateway, self.sender = FakeGateway(), FakeSender(self.clock)
        self.path = path
        self.options = dict(
            bindings={
                "qq-private": dict(
                    service="nonebot",
                    namespace="qq",
                    audience="self_private",
                    actor_ids=["actor:a", "actor:b"],
                ),
                "qq-group": dict(
                    service="nonebot",
                    namespace="qq",
                    audience="group",
                    actor_ids=["actor:a", "actor:b"],
                ),
            },
            roles={
                "actor:a": dict(version=1, persona="Role A"),
                "actor:b": dict(version=1, persona="Role B"),
            },
            config_version=1,
            policy=Policy(**policy),
            clock=self.clock,
        )
        self.core = self.new_core()

    def new_core(self):
        return Core(
            Store(self.path),
            self.contracts,
            self.origins,
            self.memory,
            self.gateway,
            self.sender,
            **self.options,
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
