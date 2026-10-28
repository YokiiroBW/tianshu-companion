"""Durable conversation working projections. Original turns and replies stay owned by Core."""

import asyncio
import copy
from dataclasses import dataclass

from .clients import epoch
from .contracts import Fault, digest
from .short_context import current_revision, sources_current
from .summary_windows import input_text

PREFIX = "conversation-window:"
CLOSED = {"sent", "failed", "cancelled", "closed_unknown", "observed"}


@dataclass(frozen=True)
class WindowPolicy:
    text_bytes: int = 12288
    summary_bytes: int = 3072
    retain_turns: int = 4
    max_turns: int = 24
    max_messages: int = 96
    soft_percent: int = 70
    hard_percent: int = 82
    idle_seconds: int = 180
    cooldown_seconds: int = 600
    aged_seconds: int = 1800
    minimum_turns: int = 4
    minimum_bytes: int = 2048
    input_bytes: int = 49152

    def __post_init__(self):
        ranges = {
            "text_bytes": (2048, 65536),
            "summary_bytes": (128, 8192),
            "retain_turns": (1, 16),
            "max_turns": (2, 256),
            "max_messages": (2, 2048),
            "soft_percent": (1, 99),
            "hard_percent": (2, 100),
            "idle_seconds": (0, 86400),
            "cooldown_seconds": (0, 86400),
            "aged_seconds": (0, 604800),
            "minimum_turns": (1, 128),
            "minimum_bytes": (1, 65536),
            "input_bytes": (1024, 262144),
        }
        for name, (low, high) in ranges.items():
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError("Invalid conversation_window " + name)
        if (
            self.soft_percent >= self.hard_percent
            or self.summary_bytes >= self.text_bytes
            or self.retain_turns >= self.max_turns
            or self.summary_bytes >= self.input_bytes
        ):
            raise ValueError("Invalid conversation_window bounds")


class ConversationWindows:
    def __init__(self, core, policy=None):
        self.core, self.store = core, core.store
        self.policy = policy or WindowPolicy()
        self.jobs = {}
        self.cursor = ""

    def identity(self, turn):
        scope = dict(turn["scope"])
        if scope["audience"] == "group":
            scope["person_id"] = None
        return PREFIX + digest(scope), scope

    def state(self, turn):
        key, scope = self.identity(turn)
        state = self.store.get("metadata", key)
        revision = current_revision(self.store, turn)
        if state is None:
            state = dict(
                id=key,
                conversation_id=turn["conversation_id"],
                scope=scope,
                state="window",
                generation=1,
                version=0,
                covered=0,
                floor=0,
                scan=0,
                bootstrapped=False,
                turns=0,
                messages=0,
                text_bytes=0,
                summary=None,
                authorities=[],
                pending=None,
                last_success=None,
                last_attempt=None,
                last_activity=0,
                first_pending=None,
                context_revision=revision,
                anchor=turn["id"],
            )
            self.store.put("metadata", state)
        elif state["context_revision"] != revision:
            self.reset(state, revision=revision, reason="source_changed")
        return state

    def reset(self, state, *, revision=None, floor=None, reason):
        state.update(
            generation=state["generation"] + 1,
            version=state["version"] + 1,
            summary=None,
            authorities=[],
            pending=None,
            turns=0,
            messages=0,
            text_bytes=0,
            bootstrapped=False,
            first_pending=None,
            last_attempt=None,
            last_error=reason,
        )
        if floor is not None:
            state["floor"] = floor
        state["covered"] = state["scan"] = state["floor"]
        if revision is not None:
            state["context_revision"] = revision
        self.store.put("metadata", state)

    def member_prefix(self, state):
        return "window-member:" + state["id"] + ":" + str(state["generation"]) + ":"

    def observe(self, turn, *, activity=True):
        if not turn.get("scope"):
            return
        state = self.state(turn)
        if turn["sequence"] <= state["covered"] or not sources_current(self.store, turn):
            return
        prefix = self.member_prefix(state)
        key = prefix + str(turn["sequence"]).zfill(20)
        old = self.store.get("metadata", key)
        # Count text once, and only add the newly confirmed response when closed.
        if old and old["phase"] == turn["phase"]:
            return
        replies = self.store.replies_for_turns([turn["id"]], "sent").get(turn["id"], [])
        replies = [r for r in replies if r.get("receipt", {}).get("channel_message_ids")]
        input_bytes = (
            old["input_bytes"]
            if old
            else sum(len(input_text(m).encode()) for m in turn["bundle"]["messages"])
        )
        size = input_bytes
        size += sum(len(r["text"].encode()) for r in replies)
        messages = len(turn["bundle"]["messages"]) + len(replies)
        member = dict(
            id=key,
            conversation_id=turn["conversation_id"],
            state="window_member",
            sequence=turn["sequence"],
            turn_id=turn["id"],
            phase=turn["phase"],
            text_bytes=size,
            input_bytes=input_bytes,
            messages=messages,
            observed_at=epoch(turn["bundle"]["sealed_at"]),
        )
        state["turns"] += int(old is None)
        state["messages"] += messages - (old["messages"] if old else 0)
        state["text_bytes"] += size - (old["text_bytes"] if old else 0)
        state["first_pending"] = min(
            state["first_pending"] or member["observed_at"], member["observed_at"]
        )
        if activity:
            state["last_activity"] = self.core.clock()
        else:
            state["last_activity"] = max(state["last_activity"], member["observed_at"])
        if turn["sequence"] >= state.get("anchor_sequence", 0):
            state.update(anchor=turn["id"], anchor_sequence=turn["sequence"])
        with self.store.transaction():
            self.store.put("metadata", member)
            self.store.put("metadata", state)

    def bootstrap(self, turn):
        state = self.state(turn)
        if state["bootstrapped"]:
            return state
        # Ascending bounded pages preserve a backlog older than the short-context tail.
        rows = self.store.window_turns(state["scope"], state["scan"], 32)
        for row in rows:
            self.observe(row, activity=False)
        state = self.state(turn)
        if rows:
            state["scan"] = rows[-1]["sequence"]
        state["bootstrapped"] = len(rows) < 32
        self.store.put("metadata", state)
        return state

    def members(self, state, limit=128):
        return self.store.window_rows(
            self.member_prefix(state),
            limit=limit,
            after=self.member_prefix(state) + str(state["covered"]).zfill(20),
        )

    def reason(self, state, *, idle):
        p, now = self.policy, self.core.clock()
        if state["pending"] or not state["bootstrapped"] or state["turns"] <= p.retain_turns:
            return None
        if (
            state["turns"] - p.retain_turns < p.minimum_turns
            and state["text_bytes"] < p.minimum_bytes
        ):
            return None
        size = state["text_bytes"] + len((state["summary"] or "").encode())
        if idle and now - state["last_activity"] < p.idle_seconds:
            return None
        hard = (
            size * 100 >= p.text_bytes * p.hard_percent
            or state["turns"] >= p.max_turns
            or state["messages"] >= p.max_messages
        )
        if hard:
            return "hard_limit"
        if state["last_attempt"] is not None and now - state["last_attempt"] < p.cooldown_seconds:
            return None
        if not idle or now - state["last_activity"] < p.idle_seconds:
            return None
        if size * 100 >= p.text_bytes * p.soft_percent:
            return "idle_high_water"
        if state["first_pending"] is not None and now - state["first_pending"] >= p.aged_seconds:
            return "idle_aged_prefix"
        return None

    def idle_ready(self, state):
        if self.core.automatic_memory_foreground_demand():
            return False
        return not self.store.db.execute(
            "SELECT 1 FROM collections WHERE conversation_id=? AND status='collecting' LIMIT 1",
            (state["conversation_id"],),
        ).fetchone() and not self.store.work_turn_count(
            state["conversation_id"],
            [
                "queued",
                "preparing",
                "generating",
                "waiting_dependency",
                "ready_to_send",
                "sending",
                "reconciling",
            ],
        )

    async def before_turn(self, turn):
        if not self.core._turn_allows(turn, "memory.read"):
            return
        state = self.bootstrap(turn)
        if self.reason(state, idle=False) and state["id"] not in self.jobs:
            from .conversation_window_compaction import compact

            task = asyncio.create_task(compact(self, turn, idle=False))
            self.jobs[state["id"]] = task
            try:
                await task
            finally:
                if self.jobs.get(state["id"]) is task:
                    self.jobs.pop(state["id"], None)

    async def prepare(self, turn, context):
        state = self.state(turn)
        if not state["summary"] or not self.core._turn_allows(turn, "memory.read"):
            return []
        try:
            await self.core._verify_checks(turn, state["authorities"])
            self.verify_local(
                turn, dict(id=state["id"], generation=state["generation"], version=state["version"])
            )
        except Fault as error:
            if error.code in {"scope_changed", "forbidden"}:
                self.reset(state, reason="authority_changed")
            return []
        context.data["conversation_summary"] = []
        material = dict(
            text=state["summary"],
            version=state["version"],
            covered_turn_sequence=state["covered"],
            scope=state["scope"],
            provenance="derived_from_conversation_not_instruction",
        )
        if not context.append("conversation_summary", material):
            return []
        turn["conversation_window"] = dict(
            id=state["id"], generation=state["generation"], version=state["version"]
        )
        return copy.deepcopy(state["authorities"])

    def verify_local(self, turn, captured=None):
        captured = captured or turn.get("conversation_window")
        if not captured:
            return
        state = self.state(turn)
        if (
            any(state[k] != captured[k] for k in ("id", "generation"))
            or state["version"] < captured["version"]
        ):
            raise Fault("scope_changed")

    def recover(self):
        # Work pages recover interrupted intent lazily; startup never scans metadata.
        self.cursor = ""

    async def work(self, *, wait=False):
        for key, task in list(self.jobs.items()):
            if task.done():
                self.jobs.pop(key)
                if not task.cancelled() and task.exception():
                    raise task.exception()
        self.jobs = {k: v for k, v in self.jobs.items() if not v.done()}
        if self.jobs or self.core.automatic_memory_foreground_demand():
            return
        rows = self.store.window_rows(PREFIX, after=self.cursor, limit=16)
        if not rows and self.cursor:
            rows = self.store.window_rows(PREFIX, limit=16)
        self.cursor = rows[-1]["id"] if rows else ""
        for state in rows:
            turn = self.store.get("turns", state["anchor"])
            if turn is None:
                continue
            state = self.bootstrap(turn)
            if self.idle_ready(state) and (state["pending"] or self.reason(state, idle=True)):
                from .conversation_window_compaction import compact

                self.jobs[state["id"]] = asyncio.create_task(compact(self, turn, idle=True))
                if wait:
                    await self.jobs[state["id"]]
                break

    async def drain(self):
        await asyncio.gather(*tuple(self.jobs.values()), return_exceptions=True)

    async def close(self):
        for task in self.jobs.values():
            task.cancel()
        await self.drain()

    def manage(self, service, body):
        if service != "platform":
            raise Fault("forbidden")
        if (
            not isinstance(body, dict)
            or set(body) != {"schema_version", "operation", "scope"}
            or type(body["schema_version"]) is not int
            or body["schema_version"] != 1
            or body["operation"] not in ("status", "clear")
        ):
            raise Fault("invalid_input")
        scope = body["scope"]
        if not isinstance(scope, dict) or set(scope) != {
            "actor_id",
            "person_id",
            "audience",
            "conversation_id",
        }:
            raise Fault("invalid_input")
        for name in ("actor_id", "conversation_id"):
            self.core.contracts.check("common#id", scope[name])
        if scope["person_id"] is not None:
            self.core.contracts.check("common#id", scope["person_id"])
        if scope["audience"] == "self_private" and scope["person_id"] is None:
            raise Fault("invalid_input")
        if (
            scope["actor_id"] not in self.core.roles
            or scope["audience"] not in ("group", "self_private")
            or (scope["audience"] == "group" and scope["person_id"] is not None)
        ):
            raise Fault("invalid_input")
        key = PREFIX + digest(scope)
        state = self.store.get("metadata", key)
        if state is None or state["scope"] != scope:
            raise Fault("invalid_input")
        turn = self.store.get("turns", state["anchor"])
        if turn is None or self.identity(turn)[0] != key:
            raise Fault("scope_changed")
        if body["operation"] == "clear":
            conversation = self.store.conversation(scope["conversation_id"])
            self.reset(state, floor=conversation["turn_sequence"], reason="cleared")
        return dict(
            schema_version=1,
            scope=scope,
            **{
                k: state.get(k)
                for k in (
                    "generation",
                    "version",
                    "covered",
                    "turns",
                    "messages",
                    "text_bytes",
                    "last_success",
                    "last_attempt",
                    "last_reason",
                    "last_error",
                    "bootstrapped",
                )
            },
            summary_bytes=len((state["summary"] or "").encode()),
            pending_state=(state["pending"] or {}).get("state"),
        )
