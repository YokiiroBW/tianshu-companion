"""Fixed NoneBot 2.5.0 completion evidence for our standalone OneBot entry.

Public hooks execute normally. The version-specific registry reads only verify
the closed graph; they never execute filters or replace framework functions.
"""

from dataclasses import dataclass

import anyio
from nonebot import get_loaded_plugins
from nonebot.adapters import Bot, Event
from nonebot.dependencies import Dependent
from nonebot.internal.adapter import Bot as InternalBot
from nonebot.matcher import Matcher, matchers
from nonebot.params import Depends
from nonebot.rule import TrieRule
from nonebot.typing import T_State
import nonebot.message as dispatch

from . import participation_protocol as wire
from .rpc import transaction
from .sdk import event_enabled

STATE_KEY = "_tianshu_participation_execution"


@dataclass
class Execution:
    owner: object
    handler_digest: str | None
    native: dict | None = None
    pre_completed: bool = False
    post_completed: bool = False
    failed: bool = False
    eligible: bool = True
    accepted: bool = True

    def captured_value(self, value):
        if not self.eligible:
            return
        service = self.owner.host.service
        self.native = service.participation.native(
            dict(
                platform_id=service.instance_id,
                self_id=value["account_id"],
                conversation_id=value["conversation"],
                account_id=value["author"],
                event_id=value["event_id"],
                revision=1,
            )
        )

    async def step(self, name, outcome):
        if self.native is None:
            self.failed = True
            return
        service = self.owner.host.service
        async with service.lock:
            with transaction(service.db):
                service.participation.step(self.native, name, outcome)


def execution(state):
    """The shallow-copied state holds a handle, never the authoritative receipts."""
    return state.get(STATE_KEY) if isinstance(state, dict) else None


class ControlledHost:
    def __init__(self, host):
        self.host = host
        self.enabled = False
        self.functions = {}
        self.expected = (
            (host.observer, host._observe, host._observed, 1, False),
            (
                host.observation_reply,
                host._claim_observation_reply,
                host._claimed_observation_reply,
                2,
                True,
            ),
            (
                host.matcher,
                host._scope,
                host._handled,
                host.settings.tianshu_adapter_priority,
                True,
            ),
        )
        host.service.participation.controlled = self.available

    def graph(self):
        if not self.enabled or self.host.service.participation.framework != {
            "name": "nonebot2",
            "version": "2.5.0",
        }:
            return None
        try:
            if {plugin.module_name for plugin in get_loaded_plugins()} != {
                "tianshu_nonebot.adapter_plugin"
            }:
                return None
            if {matcher for group in matchers.values() for matcher in group} != {
                item[0] for item in self.expected
            }:
                return None
            groups = {
                "event_pre": dispatch._event_preprocessors,
                "event_post": dispatch._event_postprocessors,
                "run_pre": dispatch._run_preprocessors,
                "run_post": dispatch._run_postprocessors,
            }
            for name, registered in groups.items():
                if {item.call for item in registered} != {self.functions[name]}:
                    return None
            if (
                InternalBot._calling_api_hook
                or InternalBot._called_api_hook
                or len(TrieRule.prefix)
            ):
                return None
            description = []
            for matcher, rule, handler, priority, block in self.expected:
                if (
                    matcher.priority != priority
                    or matcher.block is not block
                    or matcher.type != "message"
                    or matcher.temp
                    or matcher.expire_time is not None
                    or matcher.permission.checkers
                    or {item.call for item in matcher.rule.checkers} != {rule}
                    or [item.call for item in matcher.handlers] != [handler]
                    or matcher._default_state
                    or matcher._default_type_updater
                    or matcher._default_permission_updater
                ):
                    return None
                # No added handler/rule dependencies may run outside the known receipt paths.
                for item in (*matcher.rule.checkers, *matcher.handlers):
                    if not isinstance(item, Dependent) or item.parameterless:
                        return None
                description.append(
                    [rule.__module__, rule.__qualname__, handler.__qualname__, priority, block]
                )
            return wire.digest(
                dict(
                    framework="nonebot2:2.5.0/onebot:2.4.0",
                    matchers=description,
                    processors=sorted(self.functions),
                    tasks="awaited-owned-only",
                )
            )
        except (AttributeError, TypeError, ValueError):
            return None

    def available(self):
        return self.graph() is not None

    def enable_owned_entry(self):
        """Called only by create_host after constructing the actual isolated host."""
        self.enabled = True

    def install(self):
        async def lifetime(bot: Bot, event: Event):
            async with self.host.lifecycle.operation(event=True) as accepted:
                eligible = accepted and event_enabled(
                    bot, event, self.host.service.participation_enabled
                )
                proof = Execution(
                    self, self.graph() if eligible else None, eligible=eligible, accepted=accepted
                )
                try:
                    yield proof
                except BaseException:
                    proof.failed = True
                    raise
                finally:
                    # Cancellation may enter this block. Normal closure is an explicit
                    # conjunction of receipts, not inferred from reaching this finally.
                    if proof.native is not None:
                        with anyio.CancelScope(shield=True):
                            async with self.host.service.lock:
                                with transaction(self.host.service.db):
                                    current = self.graph()
                                    self.host.service.participation.finish(
                                        proof.native,
                                        proof.handler_digest or "0" * 64,
                                        not proof.failed
                                        and proof.pre_completed
                                        and proof.post_completed
                                        and proof.handler_digest is not None
                                        and current == proof.handler_digest,
                                    )

        async def event_pre(state: T_State, proof=Depends(lifetime)):
            state[STATE_KEY] = proof
            proof.pre_completed = True

        async def event_post(state: T_State, proof=Depends(lifetime)):
            if execution(state) is not proof:
                proof.failed = True
                return
            proof.post_completed = True

        async def run_pre(matcher: Matcher, proof=Depends(lifetime)):
            # Observation's rule normally returns False. Every actual handler
            # (including our old exact/explicit paths) preempts semantic ownership.
            if proof.native is not None:
                async with self.host.service.lock:
                    with transaction(self.host.service.db):
                        self.host.service.participation.entered(proof.native)

        async def run_post(exception: Exception | None, proof=Depends(lifetime)):
            if exception is not None:
                proof.failed = True

        self.functions = dict(
            event_pre=event_pre, event_post=event_post, run_pre=run_pre, run_post=run_post
        )
        dispatch.event_preprocessor(event_pre)
        dispatch.event_postprocessor(event_post)
        dispatch.run_preprocessor(run_pre)
        dispatch.run_postprocessor(run_post)

    def close(self):
        self.enabled = False
        for name, registered in (
            ("event_pre", dispatch._event_preprocessors),
            ("event_post", dispatch._event_postprocessors),
            ("run_pre", dispatch._run_preprocessors),
            ("run_post", dispatch._run_postprocessors),
        ):
            registered.difference_update(
                item for item in list(registered) if item.call is self.functions[name]
            )


def install(host):
    owner = ControlledHost(host)
    owner.install()
    return owner
