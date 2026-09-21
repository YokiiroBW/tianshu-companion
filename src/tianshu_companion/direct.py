"""Explicit functional commands: registration, routing, one execution, one reply owner.

Trusted in-process port. It owns three things and nothing else:

1. the **registry** of already-registered complete commands - name, parameter shape, the
   platforms and audiences where the command exists, the actor/conversation authority it
   needs and its single reply owner. None of that is inferred from chat text or a model line.
2. the **durable request**, bound to the authenticated message version
   (channel + message_id + revision) or to an explicit capability key, so a duplicate SDK
   event and two concurrent entries converge on exactly one execution.
3. the **single reply owner** of that request: either the structured result returns to Core
   (the natural-language entry organizes the reply) or the bridge delivers the plugin's
   native result. Never both, and the owner is frozen on the durable request.

Deliberate boundaries
---------------------
* The command fast path never waits for the chat silence window and never takes a chat turn
  slot or a model slot, so a slow plugin cannot block message admission.
* Persona, affection and model output are never authority. This port takes no persona field
  at all, and the host `guard` re-derives authority from Core's own durable facts - never
  from anything a caller typed.
* A plugin is never triggered by re-injecting a synthetic user message: the adapter receives
  a trusted context and has no path back into `Core.ingest`.
* An `unknown` delivery is never resent automatically. An explicit `retry` may re-run the
  plugin, but a re-delivery is only allowed once the bridge proved the earlier attempt did
  not execute (`retry_safe=true`); otherwise `reconcile` is the only way out.

GsCore / group-management reality gap
-------------------------------------
This module ships the port plus an explicitly synthetic reference adapter
(`SyntheticPlugin`). It does not guess any real GsCore or group-management API, and nothing
the synthetic adapter produces is ever reported as a verified real result: only
`adapter="contract"` counts as a real capability.
"""

import asyncio
import json
import re
from collections.abc import Callable

from .clients import command
from .contracts import Fault, canonical, digest
from .life import text, timestamp
from . import observability as obs

ENTRIES = {"command", "capability"}
# The single reply owner of one request. A request has exactly one of these, for ever.
REPLY_OWNERS = {"core", "bridge"}
# Only the command entry may not depend on the chat model, so a registration whose reply
# owner is Core is capability-only: bare command text for it follows the companion chain.
COMMAND_ENTRY_OWNER = "bridge"
ADAPTERS = {"contract", "synthetic", "unknown"}
VERIFYING_ADAPTER = "contract"

REQUEST_STATES = {
    "pending",
    "dispatching",
    "awaiting_result",
    "completed",
    "failed",
    "unknown",
    "cancelled",
    "superseded",
}
IN_FLIGHT = {"dispatching", "awaiting_result"}
SETTLED = {"completed", "failed", "unknown", "cancelled", "superseded"}
RETRYABLE = {"failed", "unknown"}

FIELD_TYPES = {"token", "rest", "integer"}
MAX_FIELDS = 8
TEXT_LIMIT = 4096
NAME = re.compile(r"^\S{1,32}$")
INTEGER = re.compile(r"-?(?:0|[1-9][0-9]{0,17})")

CONTRACT_GAP = (
    "text-dialogue/v1 has no dedicated direct-function-reply document. The only frozen "
    "outbound document is conversation#send_request, whose owning unit is named by turn_id "
    "and whose position is the conversation-serial (turn_sequence, segment_sequence). A "
    "direct request owns its own reply - it is not a companion turn and never claims to be "
    "one - and its message version is a real authenticated inbound SDK event, so reusing "
    "the frozen document is safe. What is still missing is a frozen transport for a Core "
    "process delivering to a bridge in another process; the deployment must supply it."
)
PLUGIN_GAP = (
    "No real group-management or GsCore plugin is attached. The shipped SyntheticPlugin is "
    "explicitly synthetic: it never produces a verified real result, and no real game state "
    "or group action is claimed."
)


def usage_line(name, fields):
    parts = [name]
    for field in fields:
        token = "<" + field["name"] + ">"
        parts.append(token if field["required"] else "[" + token + "]")
    return " ".join(parts)


def normalize_fields(parameters):
    """Validate an explicit parameter shape once, at registration."""
    if not isinstance(parameters, dict) or set(parameters) != {"fields"}:
        raise ValueError("Parameter shape must declare exactly its fields")
    fields = parameters["fields"]
    if not isinstance(fields, list) or len(fields) > MAX_FIELDS:
        raise ValueError("Invalid parameter field count")
    names, optional_seen, rest_seen = set(), False, False
    normalized = []
    for field in fields:
        if not isinstance(field, dict) or set(field) - {"name", "type", "required", "choices"}:
            raise ValueError("Invalid parameter field")
        if set(field) < {"name", "type"}:
            raise ValueError("Parameter field needs a name and a type")
        name = field["name"]
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", name):
            raise ValueError("Invalid parameter field name")
        if name in names:
            raise ValueError("Duplicate parameter field")
        names.add(name)
        if field["type"] not in FIELD_TYPES:
            raise ValueError("Unsupported parameter field type")
        if rest_seen:
            raise ValueError("A rest field must be the last field")
        rest_seen = field["type"] == "rest"
        required = field.get("required", True)
        if not isinstance(required, bool):
            raise ValueError("Invalid parameter required flag")
        if not required:
            optional_seen = True
        elif optional_seen:
            # Optional-then-required is ambiguous when parsing positionally.
            raise ValueError("A required field may not follow an optional one")
        choices = field.get("choices")
        if choices is not None:
            if field["type"] == "rest" or not isinstance(choices, list) or not choices:
                raise ValueError("Choices need a positional field and a non-empty list")
            if any(not isinstance(c, str) or not c for c in choices):
                raise ValueError("Invalid parameter choice")
            if len(set(choices)) != len(choices):
                raise ValueError("Duplicate parameter choice")
        normalized.append(
            dict(name=name, type=field["type"], required=required, choices=list(choices or []))
        )
    return normalized


def parse_parameters(rest, fields):
    """Parse the declared shape; a violation is a reportable result, never a guess."""
    tokens, values, index = rest.split(), {}, 0
    for field in fields:
        raw = None
        if field["type"] == "rest":
            if index < len(tokens):
                raw = " ".join(tokens[index:])
            index = len(tokens)
        elif index < len(tokens):
            raw = tokens[index]
            index += 1
        if raw is None or raw == "":
            continue
        if field["type"] == "integer":
            if INTEGER.fullmatch(raw) is None:
                raise ValueError("invalid_argument:" + field["name"])
            raw = int(raw)
        if field["choices"] and raw not in field["choices"]:
            raise ValueError("invalid_argument:" + field["name"])
        values[field["name"]] = raw
    if index < len(tokens):
        raise ValueError("unexpected_argument")
    for field in fields:
        if field["required"] and field["name"] not in values:
            raise ValueError("missing_argument:" + field["name"])
    return values


class SyntheticPlugin:
    """Explicitly synthetic reference adapter for the plugin port.

    It is only constructed when a deployment names it, every result is labelled
    `adapter="synthetic"`, and nothing it returns is ever a verified real result. It covers
    one read-only query and one deliberately slow task, which is the minimum this task
    requires; it does not model, guess or call any real GsCore or group-management API.
    """

    adapter = "synthetic"
    available = True
    QUERY = "companion.synthetic.query"
    SLOW = "companion.synthetic.slow"

    def __init__(self, clock, *, slow=False):
        self.clock = clock
        self.slow = slow
        self.calls = []

    @staticmethod
    def registrations():
        return [
            dict(
                command_id=SyntheticPlugin.QUERY,
                version=1,
                name="/查询",
                aliases=["/query"],
                platforms=["qq", "tg"],
                audiences=["self_private", "group"],
                parameters={
                    "fields": [
                        dict(name="subject", type="token", required=True),
                        dict(name="detail", type="rest", required=False),
                    ]
                },
                reply_to="bridge",
                description="Synthetic read-only query.",
                synthetic=True,
            ),
            dict(
                command_id=SyntheticPlugin.SLOW,
                version=1,
                name="/慢任务",
                aliases=[],
                platforms=["qq"],
                audiences=["self_private", "group"],
                parameters={"fields": [dict(name="steps", type="integer", required=False)]},
                reply_to="bridge",
                description="Synthetic slow task settled only by a later callback.",
                synthetic=True,
                timeout=60,
            ),
        ]

    async def execute(self, request):
        self.calls.append(request["command_id"])
        if request["command_id"] == self.SLOW or self.slow:
            # Submitted, not finished: only a trusted settle() may report the outcome.
            return dict(
                adapter=self.adapter,
                request_id=request["request_id"],
                attempt_id=request["attempt_id"],
                state="accepted",
                task_ref="synthetic-task:" + request["attempt_id"],
                result=None,
                reply=None,
                channel_message_ids=[],
            )
        subject = request["parameters"]["subject"]
        return dict(
            adapter=self.adapter,
            request_id=request["request_id"],
            attempt_id=request["attempt_id"],
            state="completed",
            task_ref=None,
            result=dict(
                kind="read_only_query",
                subject=subject,
                detail=request["parameters"].get("detail"),
                synthetic=True,
            ),
            reply=dict(text="[合成适配器] " + subject),
            channel_message_ids=[],
        )

    async def reconcile(self, request):
        """No real plugin is attached, so no receipt can be looked up; never a resend."""
        return None


class Direct:
    """Host must authorize callers BEFORE invoking any registry or request method here."""

    def __init__(
        self,
        store,
        clock,
        guard,
        *,
        adapter=None,
        deliver=None,
        bands=None,
        events=None,
        timeout=20,
        request_expiry=600,
        max_commands=256,
        max_history=32,
        max_pruned=64,
        max_scan=64,
        max_read=64,
    ):
        if not callable(guard):
            raise ValueError("A host authority guard is required")
        self.store, self.clock, self.guard = store, clock, guard
        self.adapter, self.delivery_port = adapter, deliver
        # The injected runtime-event port. Routing, authorization, single execution and the
        # single reply owner are all decided above and are not restated here; this port only
        # records what the durable rows already say, and its failure changes no verdict.
        self.events = events
        # The host's shared outbound coordinator. Without it this engine can only take the
        # conversation's next position for itself, which is honest but cannot wait for a
        # companion turn that is still sending its own segments.
        self.bands = bands
        for name, value, ceiling in (
            ("timeout", timeout, 600),
            ("request_expiry", request_expiry, 86400),
            ("max_commands", max_commands, 4096),
            ("max_history", max_history, 256),
            ("max_pruned", max_pruned, 512),
            ("max_scan", max_scan, 512),
            ("max_read", max_read, 512),
        ):
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError("Invalid routing bound: " + name)
            setattr(self, name, value)
        self.lock = asyncio.Lock()
        self.waiters = {}
        self._next_tick = 0

    # ------------------------------------------------------------------- registry

    def register_command(
        self,
        command_id,
        version,
        *,
        name,
        platforms,
        audiences,
        parameters,
        reply_to,
        aliases=(),
        actor_allowlist=None,
        description=None,
        synthetic=False,
        timeout=None,
        expected=None,
    ):
        """Register one complete command. The content is immutable per (command_id, version).

        `reply_to` names the single owner of this command's reply. Only `reply_to="bridge"`
        may be claimed by the command fast path: a registration that needs Core to organize
        the reply is capability-only, because a direct command must never start depending on
        the chat model being reachable.
        """
        text(command_id, 128)
        if type(version) is not int or version < 1:
            raise ValueError("Invalid command version")
        if not isinstance(name, str) or NAME.fullmatch(name) is None:
            raise ValueError("A command name is one non-space token of at most 32 characters")
        names = [name, *aliases]
        if any(not isinstance(a, str) or NAME.fullmatch(a) is None for a in names):
            raise ValueError("Invalid command alias")
        if len(set(names)) != len(names) or len(names) > 8:
            raise ValueError("Duplicate or excessive command names")
        if reply_to not in REPLY_OWNERS:
            raise ValueError("Unsupported reply owner")
        if not isinstance(platforms, list) or not platforms:
            raise ValueError("A command must name its platforms")
        if not isinstance(audiences, list) or not audiences:
            raise ValueError("A command must name its audiences")
        for value in platforms:
            text(value, 64)
        for value in audiences:
            if value not in {"self_private", "group"}:
                raise ValueError("Unsupported audience")
        fields = normalize_fields(parameters)
        allowed = None
        if actor_allowlist is not None:
            if not isinstance(actor_allowlist, list) or not actor_allowlist:
                raise ValueError("An actor allowlist must be a non-empty list")
            for actor in actor_allowlist:
                text(actor, 128)
            allowed = list(actor_allowlist)
        if not isinstance(synthetic, bool):
            raise ValueError("Invalid synthetic flag")
        if timeout is not None and (type(timeout) is not int or not 1 <= timeout <= 600):
            raise ValueError("Invalid command timeout")
        if description is not None:
            text(description, 256)
        key = digest([command_id, version])
        record = dict(
            command_id=command_id,
            command_version=version,
            names=names,
            name=name,
            platforms=sorted(platforms),
            audiences=sorted(audiences),
            parameters=fields,
            reply_to=reply_to,
            actor_allowlist=allowed,
            description=description,
            synthetic=synthetic,
            timeout=timeout,
            usage=usage_line(name, fields),
        )
        prior = self.store.get("direct_commands", key)
        if prior is not None:
            if any(prior[name] != value for name, value in record.items()):
                raise ValueError("Command version is immutable")
            if prior["state"] != "registered":
                if expected is None or expected != version:
                    raise ValueError("Command is revoked; re-register with its exact version")
                with self.store.transaction():
                    prior.update(state="registered", revoked_reason=None, updated_at=self.clock())
                    self.store.put("direct_commands", prior)
            return key
        with self.store.transaction():
            if expected is not None:
                current = self._active(command_id)
                if current is None or current["command_version"] != expected:
                    raise ValueError("Stale version")
            # One name may not be claimed twice inside one platform/audience scope: two
            # owners for one typed command is exactly the double reply this task forbids.
            for other in self._commands():
                if other["command_id"] == command_id:
                    # A newer version of the same command replaces its predecessor; it is not
                    # a second owner.
                    continue
                if set(other["platforms"]) & set(platforms) and set(other["audiences"]) & set(
                    audiences
                ):
                    if set(other["names"]) & set(names):
                        raise ValueError("Command name already registered for this scope")
            self.store.put(
                "direct_commands",
                dict(
                    id=key,
                    conversation_id=command_id,
                    state="registered",
                    deadline=None,
                    version=1,
                    sequence=version,
                    **record,
                ),
            )
            # A newer version replaces its predecessor, so one name is never registered twice.
            for other in self._commands():
                if other["id"] != key and other["command_id"] == command_id:
                    other.update(state="superseded", updated_at=self.clock())
                    self.store.put("direct_commands", other)
        return key

    def revoke_command(self, command_id, *, reason, expected=None):
        """Explicit revocation; an undispatched request for it is refused before any call."""
        text(reason, 256)
        with self.store.transaction():
            command = self._active(command_id)
            if command is None:
                raise KeyError(command_id)
            if expected is not None and command["command_version"] != expected:
                raise ValueError("Stale version")
            command.update(state="revoked", revoked_reason=reason, updated_at=self.clock())
            self.store.put("direct_commands", command)
        return command["id"]

    def _commands(self):
        rows = self.store.db.execute(
            "SELECT body FROM direct_commands WHERE status='registered' "
            "ORDER BY position,id LIMIT ?",
            (self.max_commands,),
        ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def _active(self, command_id):
        row = self.store.db.execute(
            "SELECT body FROM direct_commands WHERE conversation_id=? "
            "AND status != 'revoked' ORDER BY json_extract(body,'$.command_version') DESC "
            "LIMIT 1",
            (command_id,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def _require_command(self, command_id, version):
        command = self.store.get("direct_commands", digest([command_id, version]))
        if command is None or command["state"] != "registered":
            raise Fault("not_found")
        return command

    @staticmethod
    def command_view(command):
        return {
            key: command[key]
            for key in (
                "id",
                "command_id",
                "command_version",
                "name",
                "names",
                "platforms",
                "audiences",
                "parameters",
                "reply_to",
                "actor_allowlist",
                "description",
                "synthetic",
                "timeout",
                "usage",
                "state",
            )
        }

    def commands(self, *, namespace=None):
        return [
            self.command_view(item)
            for item in self._commands()
            if namespace is None or namespace in item["platforms"]
        ]

    def registry_version(self):
        """A stable fingerprint of what is registered, so a consumer can spot drift."""
        return digest(
            [
                [
                    item["id"],
                    item["command_version"],
                    item["names"],
                    item["platforms"],
                    item["audiences"],
                    item["reply_to"],
                ]
                for item in self._commands()
            ]
        )

    # -------------------------------------------------------------------- routing

    def match(self, content, *, namespace, audience):
        """Claim a text only when it is a registered, complete command.

        The first whitespace-delimited token must equal a registered name or alias inside
        this platform and audience scope. Anything else - a keyword inside a sentence, an
        unfinished phrase, an unknown token, a command registered only elsewhere - is not a
        command match and keeps following the existing companion chain. A registration whose
        reply owner is Core is never claimed here, and an ambiguous registration is refused
        rather than guessed.
        """
        if not isinstance(content, str) or not content.strip():
            return dict(matched=False, reason="empty")
        token = content.strip().split(None, 1)[0]
        hits = [
            item
            for item in self._commands()
            if token in item["names"]
            and namespace in item["platforms"]
            and audience in item["audiences"]
        ]
        if not hits:
            return dict(matched=False, reason="not_a_command")
        if len({item["command_id"] for item in hits}) != 1:
            return dict(matched=False, reason="ambiguous_registration")
        item = max(hits, key=lambda c: c["command_version"])
        if item["reply_to"] != COMMAND_ENTRY_OWNER:
            return dict(matched=False, reason="reply_owner_core")
        view = dict(
            matched=True,
            reason=None,
            command_id=item["command_id"],
            command_version=item["command_version"],
            name=token,
            reply_to=item["reply_to"],
            parameters=None,
            parameters_valid=False,
            parameter_error=None,
            usage=item["usage"],
        )
        try:
            view["parameters"] = parse_parameters(
                content.strip()[len(token) :].strip(), item["parameters"]
            )
            view["parameters_valid"] = True
        except ValueError as error:
            view["parameter_error"] = str(error)
        return view

    @staticmethod
    def command_request_key(channel, message_id, revision):
        """Durable identity of one message version; never a mutable local timestamp."""
        return digest(["direct", channel, message_id, revision])

    # ------------------------------------------------------------------- requests

    def open_from_command(
        self,
        content,
        *,
        namespace,
        audience,
        actor_id,
        person_id,
        conversation_id,
        channel,
        message_key,
        origin_ref,
        match,
        expires_in=None,
    ):
        """Durable request for the command fast path, bound to the message version.

        The requester identity is NOT taken from the caller: Core derives actor, person and
        audience from the authenticated origin context before calling this method.
        """
        if not isinstance(match, dict) or not match.get("matched"):
            raise ValueError("A matched, registered command is required")
        if (
            not isinstance(content, str)
            or not message_key
            or match["name"] != content.strip().split(None, 1)[0]
        ):
            raise Fault("invalid_input")
        command = self._require_command(match["command_id"], match["command_version"])
        if match["reply_to"] != command["reply_to"] or command["reply_to"] != COMMAND_ENTRY_OWNER:
            raise Fault("invalid_input")
        request_key = self.command_request_key(
            message_key["channel"], message_key["message_id"], message_key["revision"]
        )
        return self._open(
            entry="command",
            request_key=request_key,
            record=self._record(
                command=command,
                actor_id=actor_id,
                person_id=person_id,
                audience=audience,
                conversation_id=conversation_id,
                channel=channel,
                message_key=message_key,
                origin_ref=origin_ref,
                parameters=match["parameters"],
                parameter_error=match["parameter_error"],
                reply_to=command["reply_to"],
                entry="command",
                expires_in=expires_in,
                supersede=True,
            ),
        )

    def open_from_capability(
        self,
        *,
        entry_ref,
        command_id,
        command_version,
        actor_id,
        person_id,
        audience,
        conversation_id,
        channel,
        origin_ref,
        parameters,
        reply_to,
        message_key=None,
        request_key=None,
        expires_in=None,
    ):
        """Durable request for Core's explicit capability call.

        When the call answers a real user message it carries that message version, and the
        derived key is identical to the command entry's key - so both entries share one
        execution instead of running the plugin twice.
        """
        text(entry_ref, 128)
        command = self._require_command(command_id, command_version)
        if reply_to != command["reply_to"]:
            # The reply owner is registered once; an entry may request it, never move it.
            raise Fault("invalid_input")
        if message_key is not None:
            derived = self.command_request_key(
                message_key["channel"], message_key["message_id"], message_key["revision"]
            )
            if request_key is not None and request_key != derived:
                raise Fault("invalid_input")
            request_key = derived
        elif request_key is None:
            request_key = digest(["direct", "capability", entry_ref])
        return self._open(
            entry="capability",
            request_key=request_key,
            record=self._record(
                command=command,
                actor_id=actor_id,
                person_id=person_id,
                audience=audience,
                conversation_id=conversation_id,
                channel=channel,
                message_key=message_key,
                origin_ref=origin_ref,
                parameters=self._check_parameters(command, parameters),
                parameter_error=None,
                reply_to=reply_to,
                entry="capability",
                expires_in=expires_in,
                supersede=message_key is not None,
            ),
        )

    @staticmethod
    def _check_parameters(command, parameters):
        if parameters is None:
            parameters = {}
        if not isinstance(parameters, dict):
            raise Fault("invalid_input")
        fields = command["parameters"]
        if set(parameters) - {field["name"] for field in fields}:
            # No extra field can smuggle in a persona, a score or a permission claim.
            raise Fault("invalid_input")
        checked = {}
        for field in fields:
            if field["name"] not in parameters:
                if field["required"]:
                    raise Fault("invalid_input")
                continue
            value = parameters[field["name"]]
            if field["type"] == "integer":
                if type(value) is not int:
                    raise Fault("invalid_input")
            elif not isinstance(value, str) or not value:
                raise Fault("invalid_input")
            if field["choices"] and value not in field["choices"]:
                raise Fault("invalid_input")
            checked[field["name"]] = value
        return checked

    def _open(self, *, entry, request_key, record):
        with self.store.transaction():
            existing = self.store.get("direct_requests", request_key)
            if existing is not None:
                if existing["signature"] != record["signature"]:
                    raise Fault("idempotency_conflict")
                if entry not in existing["entries"]:
                    existing["entries"] = [*existing["entries"], entry]
                    self.store.put("direct_requests", existing)
                return self.request_view(request_key)
            if record["supersede"]:
                self._supersede(record["channel"], record["message_id"], request_key)
            record.pop("supersede")
            record.pop("command")
            self.store.put(
                "direct_requests",
                dict(
                    id=request_key,
                    state="pending",
                    deadline=record["expires_at"],
                    sequence=int(self.clock() * 1000),
                    version=1,
                    **record,
                ),
            )
            # A genuinely new durable request: one admission, one event. A replay or a
            # second entry on the same request returns above and is not admitted again.
            obs.emit(self.events, "direct.request.queued", "started")
        return self.request_view(request_key)

    def _supersede(self, channel, message_id, keep):
        """One durable owner per (channel, message id): an edit never double-answers."""
        rows = self.store.db.execute(
            "SELECT body FROM direct_requests WHERE status IN ('pending','dispatching',"
            "'awaiting_result') AND json_extract(body,'$.message_id')=? "
            "AND json_extract(body,'$.channel_key')=? ORDER BY position,id LIMIT ?",
            (str(message_id), canonical(channel), self.max_scan),
        ).fetchall()
        for row in rows:
            item = json.loads(row[0])
            if item["id"] == keep:
                continue
            item.update(state="superseded", superseded_by=keep, updated_at=self.clock())
            item["version"] += 1
            self.store.put("direct_requests", item)

    def _record(
        self,
        *,
        command,
        actor_id,
        person_id,
        audience,
        conversation_id,
        channel,
        message_key,
        origin_ref,
        parameters,
        parameter_error,
        reply_to,
        entry,
        expires_in,
        supersede,
    ):
        text(actor_id, 128)
        text(person_id, 128)
        if audience not in {"self_private", "group"}:
            raise ValueError("Unsupported audience")
        text(conversation_id, 128)
        if not isinstance(channel, dict) or set(channel) != {
            "namespace",
            "binding_id",
            "channel_conversation_id",
            "thread_id",
        }:
            raise ValueError("A full channel key is required")
        if not isinstance(origin_ref, dict) or set(origin_ref) != {"assertion_ref"}:
            raise ValueError("A complete origin reference is required")
        seconds = self.request_expiry if expires_in is None else expires_in
        if type(seconds) is not int or not 1 <= seconds <= 86400:
            raise ValueError("Invalid request expiry")
        signature = digest(
            dict(
                command_id=command["command_id"],
                command_version=command["command_version"],
                actor_id=actor_id,
                person_id=person_id,
                audience=audience,
                conversation_id=conversation_id,
                channel=channel,
                parameters=parameters,
                parameter_error=parameter_error,
                reply_to=reply_to,
            )
        )
        return dict(
            entry=entry,
            entries=[entry],
            command=command,
            command_id=command["command_id"],
            command_version=command["command_version"],
            command_key=command["id"],
            signature=signature,
            actor_id=actor_id,
            person_id=person_id,
            audience=audience,
            conversation_id=conversation_id,
            channel=channel,
            channel_key=canonical(channel),
            message_key=message_key,
            message_id=None if message_key is None else message_key["message_id"],
            message_revision=None if message_key is None else message_key["revision"],
            origin_ref=origin_ref,
            parameters=parameters,
            parameter_error=parameter_error,
            reply_to=reply_to,
            reply_state="not_started",
            attempt_id=None,
            attempt_count=0,
            result=None,
            result_receipt=None,
            reply=None,
            delivery=None,
            delivery_receipt=None,
            blocked_reason=None,
            cancel_requested=False,
            cancel_reason=None,
            superseded_by=None,
            unresolved=False,
            synthetic=command["synthetic"],
            created_at=self.clock(),
            updated_at=self.clock(),
            expires_at=self.clock() + seconds,
            supersede=supersede,
        )

    def request_view(self, request_id):
        request = self.store.get("direct_requests", request_id)
        if request is None:
            raise KeyError(request_id)
        receipt = request.get("result_receipt")
        delivery = request.get("delivery_receipt")
        owner = request["reply_to"]
        if owner == "bridge":
            verified = bool(
                delivery and delivery["state"] == "sent" and delivery["channel_message_ids"]
            )
            evidence = (
                "published_contract_receipt"
                if verified
                else ("delivery_" + delivery["state"] if delivery else None)
            )
        else:
            verified = False
            evidence = None
        return {
            key: request.get(key)
            for key in (
                "id",
                "entry",
                "entries",
                "command_id",
                "command_version",
                "actor_id",
                "person_id",
                "audience",
                "conversation_id",
                "channel",
                "message_key",
                "parameters",
                "parameter_error",
                "reply_to",
                "reply_state",
                "state",
                "attempt_id",
                "attempt_count",
                "result",
                "reply",
                "delivery_receipt",
                "blocked_reason",
                "deferred_reason",
                "deferred_on",
                "cancel_requested",
                "cancel_reason",
                "superseded_by",
                "unresolved",
                "synthetic",
                "created_at",
                "updated_at",
                "expires_at",
                "version",
            )
        } | dict(
            reply_owner=owner,
            reply_owner_enforced=True,
            delivery_verified=verified,
            delivery_evidence=evidence,
            result_verified=bool(receipt and receipt["adapter"] == VERIFYING_ADAPTER),
            result_evidence=(
                None
                if receipt is None
                else (
                    "published_contract_result"
                    if receipt["adapter"] == VERIFYING_ADAPTER
                    else "synthetic_adapter_result"
                )
            ),
            adapter_available=self._adapter_available(),
            delivery_port_available=self._deliver_available(),
            contract_gap=(
                CONTRACT_GAP if owner == "bridge" and not self._deliver_available() else None
            ),
            plugin_gap=(
                None
                if self.adapter is not None
                and getattr(self.adapter, "adapter", None) == VERIFYING_ADAPTER
                else PLUGIN_GAP
            ),
        )

    def _adapter_available(self):
        return self.adapter is not None and bool(getattr(self.adapter, "available", False))

    def _deliver_available(self):
        return self.delivery_port is not None and bool(
            getattr(self.delivery_port, "available", False)
        )

    # --------------------------------------------------------------- authorization

    def _authorize(self, request):
        """Registration scope plus the host's local authority re-check.

        The verdict is derived from the durable request, the frozen registration and Core's
        own facts. No persona, affection score or model statement takes part, and there is
        no field a caller could set to grant itself permission.
        """
        command = self.store.get("direct_commands", request.get("command_key"))
        reasons = []
        if command is None or command["state"] != "registered":
            reasons.append("registration_missing")
        else:
            if command["command_version"] != request["command_version"]:
                reasons.append("registration_changed")
            if request["channel"]["namespace"] not in command["platforms"]:
                reasons.append("platform_not_registered")
            if request["audience"] not in command["audiences"]:
                reasons.append("audience_not_registered")
            if (
                command["actor_allowlist"] is not None
                and request["actor_id"] not in command["actor_allowlist"]
            ):
                reasons.append("actor_not_allowed")
        verdict = self.guard(request) or {}
        reasons.extend(
            verdict.get("reasons") or ([verdict["reason"]] if verdict.get("reason") else [])
        )
        return dict(
            allowed=not reasons,
            reason=reasons[0] if reasons else None,
            reasons=reasons,
            registration=command["id"] if command else None,
            evidence=verdict.get("evidence"),
        )

    # ------------------------------------------------------------------ execution

    def tick(self, *, force=False):
        """Bounded, fair sweep: expire and de-authorize requests; never call a plugin."""
        now = timestamp(self.clock())
        if not force and now < self._next_tick:
            return
        with self.store.transaction():
            for request_id in self._pending_window():
                request = self.store.get("direct_requests", request_id)
                if request is None or request["state"] != "pending":
                    continue
                if request["expires_at"] <= now:
                    self._settle_request(request, "cancelled", reason="expired")
                    continue
                verdict = self._authorize(request)
                if not verdict["allowed"]:
                    self._settle_request(
                        request,
                        "cancelled",
                        reason="authorization_revoked:" + str(verdict["reason"]),
                    )
        self._next_tick = now + 1

    def _pending_window(self):
        """Rotating bounded window over pending requests, so none is starved.

        A durable cursor walks the (position, id) ordered set and wraps, so every request is
        reached within `ceil(N / max_scan)` passes even when the head rows never change.
        Work per pass stays bounded by `max_scan`: the window after the cursor and the
        wrap-around share that budget, so this is never an unbounded full scan.
        """
        cursor = self.store.get("metadata", "direct_scan_pending") or {}
        position, row_id = cursor.get("position"), cursor.get("row_id")
        rows = []
        if position is not None:
            rows = [
                tuple(row)
                for row in self.store.db.execute(
                    "SELECT id,position FROM direct_requests WHERE status='pending' "
                    "AND (position,id) > (?,?) ORDER BY position,id LIMIT ?",
                    (position, row_id, self.max_scan),
                ).fetchall()
            ]
        if len(rows) < self.max_scan:
            clause, extra = "", []
            if position is not None:
                clause = " AND (position,id) <= (?,?)"
                extra = [position, row_id]
            rows.extend(
                tuple(row)
                for row in self.store.db.execute(
                    "SELECT id,position FROM direct_requests WHERE status='pending'"
                    f"{clause} ORDER BY position,id LIMIT ?",
                    (*extra, self.max_scan - len(rows)),
                ).fetchall()
            )
        # The window after the cursor and the wrap-around are two queries, so the last
        # appended row is not necessarily the last row in (position, id) order.
        if rows:
            scanned = max(rows, key=lambda row: (row[1], row[0]))
            self.store.put(
                "metadata",
                dict(id="direct_scan_pending", position=scanned[1], row_id=scanned[0]),
            )
        return [row[0] for row in rows]

    def _settle_request(self, request, state, *, reason=None):
        request.update(
            state=state,
            blocked_reason=reason,
            reply_state="not_required",
            unresolved=state == "unknown",
            updated_at=self.clock(),
            version=request["version"] + 1,
        )
        self.store.put("direct_requests", request)
        self._notify(request["id"])
        if state == "cancelled":
            # The durable verdict already exists; the event repeats the stored reason.
            obs.emit(
                self.events,
                "direct.request.cancelled",
                "cancelled",
                error_code=(reason or "").split(":", 1)[0] or None,
            )
        return request

    def _notify(self, request_id):
        waiter = self.waiters.get(request_id)
        if waiter is not None:
            waiter.set()

    async def work(self):
        """One bounded execution and one bounded delivery per pass, off the chat path.

        With no adapter configured the pass still records the honest reason on the head
        request instead of silently leaving it pending for ever.
        """
        if self.lock.locked():
            return
        async with self.lock:
            self.tick(force=True)
            row = self.store.db.execute(
                "SELECT id FROM direct_requests WHERE status='pending' ORDER BY position,id LIMIT 1"
            ).fetchone()
            if row is not None:
                await self.dispatch(row[0])
            for request_id in self._deliverable():
                await self.deliver(request_id)

    def _deliverable(self):
        rows = self.store.db.execute(
            "SELECT id FROM direct_requests WHERE status IN ('completed','failed') "
            "AND json_extract(body,'$.reply_state')='ready_to_deliver' "
            "ORDER BY position,id LIMIT ?",
            (self.max_read,),
        ).fetchall()
        return [row[0] for row in rows]

    def _claim(self, request_id):
        """Single-execution claim. The submit intent is durable before any transport call."""
        with self.store.transaction():
            request = self.store.get("direct_requests", request_id)
            if request is None:
                raise KeyError(request_id)
            if request["state"] != "pending":
                return None
            now = self.clock()
            if request["expires_at"] <= now:
                self._settle_request(request, "cancelled", reason="expired")
                return None
            verdict = self._authorize(request)
            if not verdict["allowed"]:
                self._settle_request(
                    request,
                    "cancelled",
                    reason="authorization_revoked:" + str(verdict["reason"]),
                )
                return None
            command = self.store.get("direct_commands", request["command_key"])
            if request["parameter_error"] is not None:
                # A registered command with a broken parameter shape is still owned by this
                # request, but it never reaches an executor and never reaches a model.
                self._reject_parameters(request, command)
                return None
            attempt_no = request["attempt_count"] + 1
            attempt_id = "direct-attempt:" + digest([request_id, attempt_no])
            # The registration's own bound is honoured, never expanded.
            bound = (
                self.timeout
                if command["timeout"] is None
                else min(self.timeout, command["timeout"])
            )
            exchange = dict(
                schema_version=1,
                request_id=request_id,
                attempt_id=attempt_id,
                command_id=request["command_id"],
                command_version=request["command_version"],
                entry=request["entry"],
                actor_id=request["actor_id"],
                person_id=request["person_id"],
                audience=request["audience"],
                conversation_id=request["conversation_id"],
                channel=request["channel"],
                parameters=request["parameters"],
                parameter_error=None,
                reply_to=request["reply_to"],
                usage=command["usage"],
                submitted_at=now,
            )
            request.update(
                state="dispatching",
                attempt_id=attempt_id,
                attempt_count=attempt_no,
                blocked_reason=None,
                updated_at=now,
                version=request["version"] + 1,
            )
            self.store.put("direct_requests", request)
            self.store.put(
                "direct_attempts",
                dict(
                    id=attempt_id,
                    conversation_id=request["conversation_id"],
                    state="submitted",
                    deadline=now + self.timeout,
                    sequence=attempt_no,
                    request_id=request_id,
                    attempt_no=attempt_no,
                    request=exchange,
                    bound=bound,
                    response_received=False,
                    stale=False,
                    receipt=None,
                    settled_at=None,
                    version=1,
                ),
            )
            return exchange, bound

    def _reject_parameters(self, request, command):
        result = dict(
            kind="invalid_parameters",
            parameter_error=request["parameter_error"],
            usage=command["usage"],
        )
        request.update(
            state="failed",
            blocked_reason="invalid_parameters",
            result=result,
            updated_at=self.clock(),
            version=request["version"] + 1,
        )
        if request["reply_to"] == "core":
            request.update(reply=None, reply_state="awaiting_core")
        else:
            # The plugin owns this reply, so the miss is answered with the registered usage
            # line, which is derived from the registration rather than written by a model.
            request.update(reply=dict(text=command["usage"]))
            if self._deliver_available():
                request.update(reply_state="ready_to_deliver")
            else:
                request.update(
                    reply_state="not_started", blocked_reason="delivery_port_unavailable"
                )
        self.store.put("direct_requests", request)
        self._notify(request["id"])

    async def dispatch(self, request_id, *, wait=0.0):
        """Run one pending request exactly once and settle it.

        A second caller - a duplicate SDK event, a concurrent capability entry, a later
        `work()` pass - sees a request that is no longer `pending` and never re-executes it.
        """
        request = self.store.get("direct_requests", request_id)
        if request is None:
            raise KeyError(request_id)
        if not self._adapter_available():
            with self.store.transaction():
                fresh = self.store.get("direct_requests", request_id)
                if fresh["state"] == "pending":
                    fresh.update(blocked_reason="plugin_unavailable", updated_at=self.clock())
                    self.store.put("direct_requests", fresh)
            return self.request_view(request_id)
        claim = self._claim(request_id)
        if claim is None:
            return self.request_view(request_id)
        exchange, bound = claim
        await self._execute(exchange, bound)
        if wait:
            await self._await_settled(request_id, wait)
        return self.request_view(request_id)

    async def _await_settled(self, request_id, wait):
        if self.store.get("direct_requests", request_id)["state"] not in IN_FLIGHT:
            return
        waiter = self.waiters.setdefault(request_id, asyncio.Event())
        try:
            await asyncio.wait_for(waiter.wait(), timeout=wait)
        except (TimeoutError, asyncio.TimeoutError):
            pass
        finally:
            self.waiters.pop(request_id, None)

    async def _execute(self, exchange, bound):
        # One correlation ID for this attempt: the plugin call and its settled outcome
        # belong to the same operational trace.
        with obs.correlation_scope():
            await self._execute_attempt(exchange, bound)

    async def _execute_attempt(self, exchange, bound):
        attempt_id = exchange["attempt_id"]
        obs.emit(self.events, "direct.attempt.started", "started")
        try:
            result = await asyncio.wait_for(self.adapter.execute(dict(exchange)), timeout=bound)
            self._check_result(exchange, result)
        except asyncio.CancelledError:
            self.settle(attempt_id, self._unknown(exchange))
            raise
        except Exception:
            # A timeout, a crash or a dropped reply is never proof of non-execution.
            result = self._unknown(exchange)
        self.settle(attempt_id, result)
        if result["state"] == "completed":
            await self._after_result(exchange["request_id"])

    @staticmethod
    def _unknown(exchange):
        return dict(
            adapter="unknown",
            request_id=exchange["request_id"],
            attempt_id=exchange["attempt_id"],
            state="unknown",
            task_ref=None,
            result=None,
            reply=None,
            channel_message_ids=[],
        )

    @staticmethod
    def _check_result(exchange, result):
        if not isinstance(result, dict):
            raise Fault("invalid_input")
        if (
            result.get("request_id") != exchange["request_id"]
            or result.get("attempt_id") != exchange["attempt_id"]
            or result.get("state") not in {"completed", "accepted", "failed", "unknown"}
            or result.get("adapter") not in ADAPTERS
        ):
            raise Fault("invalid_input")
        ids = result.get("channel_message_ids")
        if not isinstance(ids, list) or any(not isinstance(i, str) or not i for i in ids):
            raise Fault("invalid_input")
        if result["state"] != "completed" and result.get("result") is not None:
            raise Fault("invalid_input")
        reply = result.get("reply")
        # Exactly one reply owner: a Core-owned request must not carry a native reply, and a
        # bridge-owned completed request must carry exactly one plain-text segment.
        if exchange["reply_to"] == "core":
            if reply is not None:
                raise Fault("invalid_input")
        elif result["state"] == "completed":
            if (
                not isinstance(reply, dict)
                or set(reply) != {"text"}
                or not isinstance(reply["text"], str)
                or not reply["text"]
                or len(reply["text"]) > TEXT_LIMIT
            ):
                raise Fault("invalid_input")

    def settle(self, attempt_id, result):
        """Trusted adapter callback; a late one for a superseded attempt stays on it."""
        outcome = None
        with self.store.transaction():
            attempt = self.store.get("direct_attempts", attempt_id)
            if attempt is None:
                raise KeyError(attempt_id)
            self._check_result(attempt["request"], result)
            if attempt["state"] in {"completed", "failed"}:
                if attempt["receipt"] != result:
                    raise Fault("idempotency_conflict")
                return self.request_view(attempt["request_id"])
            request_id = attempt["request_id"]
            request = self.store.get("direct_requests", request_id)
            owns = (
                request is not None
                and request["attempt_id"] == attempt_id
                and request["state"] in IN_FLIGHT
            )
            attempt.update(
                state=result["state"],
                receipt=result,
                response_received=True,
                settled_at=self.clock(),
                stale=not owns,
                version=attempt["version"] + 1,
            )
            self.store.put("direct_attempts", attempt)
            if not owns:
                # A late reply for an older attempt never rewrites the request or a newer one.
                outcome = result["state"]
            else:
                self._apply_result(request, result)
        if outcome is None:
            outcome = result["state"]
        obs.emit(
            self.events,
            "direct.attempt.finished",
            outcome,
            level="INFO" if outcome == "completed" else "WARNING",
        )
        return self.request_view(request_id)

    def _apply_result(self, request, result):
        state = result["state"]
        base = dict(
            result=result.get("result"),
            result_receipt=result,
            reply=result.get("reply"),
            updated_at=self.clock(),
            version=request["version"] + 1,
        )
        if state == "accepted":
            request.update(
                **base,
                state="awaiting_result",
                reply_state="not_started" if request["reply_to"] == "bridge" else "awaiting_core",
            )
        else:
            request.update(
                **base,
                state=state,
                unresolved=state == "unknown",
            )
            if request["cancel_requested"]:
                request.update(
                    state="cancelled",
                    reply_state="not_required",
                    blocked_reason=request["cancel_reason"],
                )
            elif state != "completed":
                request.update(reply_state="not_required")
            elif request["reply_to"] == "core":
                # The structured result is handed back to Core; Core organizes the reply.
                request.update(reply_state="awaiting_core")
            elif not self._deliver_available():
                request.update(
                    reply_state="not_started", blocked_reason="delivery_port_unavailable"
                )
            else:
                request.update(reply_state="ready_to_deliver")
        self.store.put("direct_requests", request)
        self._notify(request["id"])

    async def _after_result(self, request_id):
        request = self.store.get("direct_requests", request_id)
        if request is not None and request["reply_state"] == "ready_to_deliver":
            await self.deliver(request_id)

    # ------------------------------------------------------------------- delivery

    def delivery_request(self, request):
        """Build the frozen outbound document for a bridge-owned reply.

        The direct request owns its reply, so it is the owning unit named by `turn_id`, and
        `turn_sequence` comes from the conversation's single serial counter so chat replies
        and functional replies never overtake or interleave each other.
        """
        conversation = self.store.get("conversations", digest(request["channel"]))
        if conversation is None:
            return None
        if conversation["conversation_id"] != request["conversation_id"]:
            # The conversation was re-mapped under this request: a different failure from a
            # conversation that does not exist, and it must not be reported as the same one.
            raise Fault("conversation_mismatch")
        if not request["reply"] or not request["reply"].get("text"):
            return None
        return dict(
            conversation_id=conversation["conversation_id"],
            turn_id=request["id"],
            # Filled by `deliver` from the conversation's single serial counter, so a chat
            # reply and a functional reply never claim the same outbound position.
            turn_sequence=None,
            reply_id=digest([request["id"], "reply"]),
            actor_id=request["actor_id"],
            destination=request["channel"],
            segment_sequence=1,
            segment_count=1,
            text=request["reply"]["text"],
        )

    async def deliver(self, request_id):
        """One delivery intent per request; an `unknown` outcome is never resent."""
        if not self._deliver_available():
            return self.request_view(request_id)
        with self.store.transaction():
            request = self.store.get("direct_requests", request_id)
            if request is None or request["reply_state"] != "ready_to_deliver":
                return self.request_view(request_id)
            if request["reply_to"] != "bridge":
                return self.request_view(request_id)
            if request["cancel_requested"]:
                self._settle_request(request, "cancelled", reason=request["cancel_reason"])
                return self.request_view(request_id)
            try:
                delivery = self.delivery_request(request)
            except Fault as error:
                # The conversation was re-mapped under this request. Report the real reason
                # instead of sending it somewhere else or calling it "unmapped".
                request.update(
                    reply_state="not_started",
                    blocked_reason=error.code,
                    updated_at=self.clock(),
                    version=request["version"] + 1,
                )
                self.store.put("direct_requests", request)
                return self.request_view(request_id)
            if delivery is None:
                request.update(
                    reply_state="not_started",
                    blocked_reason="conversation_unmapped",
                    updated_at=self.clock(),
                    version=request["version"] + 1,
                )
                self.store.put("direct_requests", request)
                return self.request_view(request_id)
            band, waiting = self._open_band(request_id, request)
            if waiting is not None:
                return self.request_view(request_id)
            delivery.update(
                turn_sequence=band,
                # The envelope is minted at delivery time from the request's own stored
                # origin reference, so a long task does not inherit an expired deadline.
                command=command(
                    request["origin_ref"], digest([request_id, "delivery"]), self.clock()
                ),
            )
            self._record_delivery_intent(request, delivery)
        # The frozen delivery document is durable before this point, so the event can never
        # be the only record that a send was attempted.
        obs.emit(self.events, "direct.delivery.started", "started")
        try:
            receipt = await asyncio.wait_for(
                self.delivery_port.send(delivery), timeout=self.timeout
            )
            self._check_delivery(delivery, receipt)
        except asyncio.CancelledError:
            self._settle_delivery(request_id, None)
            raise
        except Exception:
            receipt = None
        self._settle_delivery(request_id, receipt)
        return self.request_view(request_id)

    def _open_band(self, request_id, request):
        """Take this request's place in the conversation's shared outbound order.

        Waiting is only ever for a companion turn that still has segments to hand to the
        exit. That is a legal message boundary and the wait is bounded by that turn's
        remaining segments - never by the chat model, which has already finished generating.
        While waiting the request stays deliverable, so the worker simply tries again.
        """
        conversation_key = digest(request["channel"])
        if self.bands is None:
            conversation = self.store.get("conversations", conversation_key)
            if conversation is None:
                return None, None
            # No host coordinator: still never step below a band already handed out, so the
            # conversation's outbound mark stays monotone on this path too.
            band = (
                max(
                    conversation.get("turn_sequence", 0),
                    conversation.get("send_band") or 0,
                )
                + 1
            )
            conversation["turn_sequence"] = band
            conversation["send_band"] = band
            conversation["send_band_owner"] = request["id"]
            self.store.put("conversations", conversation)
            return band, None
        band, waiting = self.bands(
            conversation_key, unit_id=request["id"], current=None, wait_for_turn=True
        )
        if waiting is not None:
            conversation = self.store.get("conversations", conversation_key)
            if (
                request.get("deferred_reason") != "outbound_band_busy"
                or request.get("deferred_on") != waiting
            ):
                request.update(
                    deferred_reason="outbound_band_busy",
                    deferred_on=waiting,
                    conversation_id=conversation and conversation["conversation_id"],
                    updated_at=self.clock(),
                    version=request["version"] + 1,
                )
                self.store.put("direct_requests", request)
                # Waiting on a legal message boundary is a real, reportable state, recorded
                # once per distinct wait rather than on every retry of the same wait.
                obs.emit(
                    self.events,
                    "direct.delivery.deferred",
                    "degraded",
                    error_code="outbound_band_busy",
                )
            return None, waiting
        return band, None

    def waiting_for_band(self, conversation_id):
        """Is a functional reply parked on a companion turn's outbound band for this channel?"""
        row = self.store.db.execute(
            "SELECT 1 FROM direct_requests WHERE status IN ('completed','failed') "
            "AND json_extract(body,'$.reply_state')='ready_to_deliver' "
            "AND json_extract(body,'$.deferred_reason')='outbound_band_busy' "
            "AND json_extract(body,'$.conversation_id')=? LIMIT 1",
            (conversation_id,),
        ).fetchone()
        return row is not None

    def _record_delivery_intent(self, request, delivery):
        """Persist `completed + submitting` before any IO.

        This durable row is what an ungraceful exit leaves behind, and it is the evidence
        `recover` and `reconcile` use afterwards: the same frozen document, the same reply id,
        the same request id. Nothing about the reply is ever re-derived from a new decision.
        """
        request.update(
            reply_state="submitting",
            delivery=delivery,
            deferred_reason=None,
            deferred_on=None,
            updated_at=self.clock(),
            version=request["version"] + 1,
        )
        self.store.put("direct_requests", request)

    def _check_delivery(self, delivery, receipt):
        if (
            not isinstance(receipt, dict)
            or receipt.get("reply_id") != delivery["reply_id"]
            or receipt.get("segment_sequence") != delivery["segment_sequence"]
            or receipt.get("request_id") != delivery["command"]["request_id"]
            or receipt.get("state") not in {"sent", "failed", "unknown"}
        ):
            raise Fault("invalid_input")

    def _settle_delivery(self, request_id, receipt):
        with self.store.transaction():
            request = self.store.get("direct_requests", request_id)
            # `unknown` is re-enterable only through `reconcile`, which never sends again.
            if request is None or request["reply_state"] not in {"submitting", "unknown"}:
                return
            if receipt is None:
                receipt = dict(
                    state="unknown",
                    reply_id=request["delivery"]["reply_id"],
                    request_id=request["delivery"]["command"]["request_id"],
                    segment_sequence=1,
                    attempt_id="attempt:unknown",
                    channel_message_ids=[],
                    retry_safe=False,
                    observed_at=None,
                )
            request.update(
                reply_state=receipt["state"],
                delivery_receipt=receipt,
                unresolved=receipt["state"] == "unknown",
                # A reason recorded for the interrupted state must not outlive it, otherwise a
                # settled reply would still read as "blocked".
                blocked_reason=(
                    None
                    if request.get("blocked_reason") == "interrupted_delivery"
                    else request.get("blocked_reason")
                ),
                updated_at=self.clock(),
                version=request["version"] + 1,
            )
            if request["cancel_requested"] and receipt["state"] != "sent":
                request.update(state="cancelled", blocked_reason=request["cancel_reason"])
            self.store.put("direct_requests", request)
            self._notify(request_id)
        # One delivery intent, one reported outcome. An `unknown` receipt stays unknown.
        obs.emit(
            self.events,
            "direct.delivery.finished",
            receipt["state"],
            level="INFO" if receipt["state"] == "sent" else "WARNING",
        )

    async def reconcile(self, request_id):
        """Read-only receipt lookup. Never sends again, even while the local state is unknown."""
        request = self.store.get("direct_requests", request_id)
        if request is None:
            raise KeyError(request_id)
        if request["reply_state"] != "unknown" or not self._deliver_available():
            return self.request_view(request_id)
        lookup = getattr(self.delivery_port, "reconcile", None)
        if lookup is None:
            return self.request_view(request_id)
        try:
            receipt = await asyncio.wait_for(lookup(request["delivery"]), timeout=self.timeout)
        except Exception:
            receipt = None
        if not isinstance(receipt, dict):
            return self.request_view(request_id)
        self._check_delivery(request["delivery"], receipt)
        self._settle_delivery(request_id, receipt)
        return self.request_view(request_id)

    # ------------------------------------------------------- cancel / retry / restart

    def cancel(self, request_id, *, reason, expected=None):
        """Cancel before any transport call; a submitted attempt settles itself."""
        text(reason, 256)
        with self.store.transaction():
            request = self.store.get("direct_requests", request_id)
            if request is None:
                raise KeyError(request_id)
            if expected is not None and request["version"] != expected:
                raise ValueError("Stale version")
            if request["state"] in SETTLED:
                raise ValueError("Request is already settled")
            if request["state"] == "pending":
                self._settle_request(request, "cancelled", reason=reason)
            else:
                # In flight: the reply is withheld and the attempt still settles itself.
                request.update(
                    cancel_requested=True,
                    cancel_reason=reason,
                    updated_at=self.clock(),
                    version=request["version"] + 1,
                )
                self.store.put("direct_requests", request)
        return self.request_view(request_id)

    def retry(self, request_id, *, reason, expected=None, redeliver=False):
        """Explicit retry only. A settled unknown outcome is never resent automatically."""
        text(reason, 256)
        with self.store.transaction():
            request = self.store.get("direct_requests", request_id)
            if request is None:
                raise KeyError(request_id)
            if expected is not None and request["version"] != expected:
                raise ValueError("Stale version")
            if redeliver:
                receipt = request.get("delivery_receipt") or {}
                delivery = request.get("delivery") or {}
                if request["reply_to"] != "bridge":
                    raise ValueError("Only a bridge-owned reply can be redelivered")
                if receipt.get("state") == "unknown" or request["reply_state"] == "unknown":
                    # Not proven non-executed: resending would be a blind replay.
                    raise ValueError("Delivery outcome is unknown; reconcile before redelivering")
                if not (receipt.get("state") == "failed" and receipt.get("retry_safe") is True):
                    raise ValueError("Delivery was not proven unexecuted")
                if receipt.get("reply_id") != delivery.get("reply_id") or receipt.get(
                    "segment_sequence"
                ) != delivery.get("segment_sequence"):
                    # A receipt for another document proves nothing about this one.
                    raise ValueError("Delivery receipt does not belong to this reply")
                if request["state"] in SETTLED:
                    # The execution already stands; only the reply is submitted again, and the
                    # reply id is unchanged so the bridge still de-duplicates it.
                    request.update(
                        reply_state="ready_to_deliver",
                        retry_reason=reason,
                        updated_at=self.clock(),
                        version=request["version"] + 1,
                    )
                    self.store.put("direct_requests", request)
                    return self.request_view(request_id)
            if request["state"] not in RETRYABLE:
                raise ValueError("Request is not retryable")
            request["reply_state"] = "not_started" if redeliver else request["reply_state"]
            request.update(
                state="pending",
                attempt_id=None,
                result=None,
                result_receipt=None,
                reply=None,
                unresolved=False,
                blocked_reason=None,
                cancel_requested=False,
                cancel_reason=None,
                retry_reason=reason,
                updated_at=self.clock(),
                version=request["version"] + 1,
            )
            self.store.put("direct_requests", request)
        return self.request_view(request_id)

    def recover(self):
        """Once after acquiring the owner lock: an interrupted submit is `unknown`, never a resend."""
        with self.store.transaction():
            for request in self.store.list("direct_requests", states=sorted(IN_FLIGHT)):
                self._settle_request(
                    request,
                    "unknown",
                    reason=(
                        "interrupted_dependency_call"
                        if request["state"] == "dispatching"
                        else "interrupted_task"
                    ),
                )
            for request in self._interrupted_deliveries():
                # The delivery intent was durable but no definite receipt was ever recorded,
                # so the reply may or may not have reached the channel. It becomes `unknown`
                # and unresolved: the only way forward is a read-only reconcile against the
                # very same delivery document, never a plugin re-run and never a resend.
                request.update(
                    reply_state="unknown",
                    unresolved=True,
                    blocked_reason="interrupted_delivery",
                    deferred_reason=None,
                    deferred_on=None,
                    updated_at=self.clock(),
                    version=request["version"] + 1,
                )
                self.store.put("direct_requests", request)
            for request in self.store.list("direct_requests", states=["pending"]):
                if request.get("blocked_reason") == "plugin_unavailable":
                    request["blocked_reason"] = None
                    self.store.put("direct_requests", request)
        self._prune()

    def _interrupted_deliveries(self):
        """Requests parked in `submitting`: a committed intent that never got a receipt."""
        rows = self.store.db.execute(
            "SELECT body FROM direct_requests "
            "WHERE json_extract(body,'$.reply_state')='submitting' "
            "ORDER BY position,id LIMIT ?",
            (self.max_read,),
        ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def _prune(self):
        for request in self.store.list("direct_requests", states=sorted(SETTLED)):
            rows = self.store.db.execute(
                "SELECT id FROM direct_attempts WHERE json_extract(body,'$.request_id')=? "
                "ORDER BY position DESC,id LIMIT ?",
                (request["id"], self.max_history + self.max_pruned + 1),
            ).fetchall()
            keys = [row[0] for row in rows]
            for key in keys[self.max_history : self.max_history + self.max_pruned]:
                self.store.delete("direct_attempts", key)

    def attempts(self, request_id):
        rows = self.store.db.execute(
            "SELECT body FROM direct_attempts WHERE json_extract(body,'$.request_id')=? "
            "ORDER BY position,id LIMIT ?",
            (request_id, self.max_history),
        ).fetchall()
        return [
            {
                key: json.loads(row[0])[key]
                for key in ("id", "request_id", "attempt_no", "state", "stale", "settled_at")
            }
            for row in rows
        ]

    def requests(self, *, states=None, limit=None):
        bounded = self.max_read if limit is None else min(limit, self.max_read)
        return [self.request_view(item["id"]) for item in self._request_rows(states, bounded)]

    def _request_rows(self, states, limit):
        query = "SELECT body FROM direct_requests WHERE 1=1"
        args = []
        if states:
            query += " AND status IN (" + ",".join("?" for _ in states) + ")"
            args.extend(states)
        query += " ORDER BY position,id LIMIT ?"
        args.append(limit)
        return [json.loads(row[0]) for row in self.store.db.execute(query, args)]


def configured_matcher(names_by_scope: dict) -> Callable:
    """Deployment-side coarse matcher for the bridge process.

    The bridge owns the outbound pipeline and decides ownership before Core sees the
    message, so it needs the registered command names for its own platform. Core stays
    authoritative: it re-matches the whole command and its parameter shape and refuses a
    request whose command is not registered there. Drift can therefore cause an honest
    refusal, never a double reply.
    """

    def match(content, *, namespace, audience):
        if not isinstance(content, str) or not content.strip():
            return dict(matched=False, reason="empty")
        names = names_by_scope.get((namespace, audience)) or names_by_scope.get(namespace) or []
        token = content.strip().split(None, 1)[0]
        matched = token in names
        return dict(matched=matched, reason=None if matched else "not_a_command")

    return match
