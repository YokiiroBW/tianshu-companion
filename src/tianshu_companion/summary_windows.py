"""Core-owned dialogue windows for background consolidation, not another chat store."""

import copy
import hashlib

from .clients import utc
from .context import MEMORY_CONTEXT_DOMAIN, reply_provenance_checks
from .contracts import Fault, canonical, digest
from .short_context import sources_current
from .source_sync import ensure_channel, link_learning_channel, selector

MAX_NEW_TURNS = 1
MAX_TURNS = 12
MAX_MEMBERS = 32
MAX_TEXT_BYTES = 48 * 1024
MAX_REQUEST_BYTES = 256 * 1024
MAX_SOURCES = 256
DEPENDENCIES = ("group_dependencies", "episode_dependencies", "source_dependencies")


def text_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def input_text(message):
    # Non-text input remains its actual structured payload, not an invented
    # caption. Text-only messages retain the original speaker's full wording.
    parts = message["parts"]
    if any(part["kind"] != "text" for part in parts):
        return canonical(parts)
    return "\n".join(part["text"] for part in parts)


def sent_digest(reply):
    return digest(
        dict(
            reply_id=reply["id"],
            state="sent",
            channel_message_ids=reply["receipt"]["channel_message_ids"],
        )
    )


def source_upper_bound(manifest):
    # Memory's source_key is opaque. Do not copy its private identity algorithm:
    # the disjoint counts can overcount an intersection but never undercount it.
    human = {
        canonical(selector(member["reference"]["source"], member["source_scope"]["actor_id"]))
        for member in manifest["members"]
        if member["role"] == "user"
    }
    dependencies = {
        row["source_key"]
        for member in manifest["members"]
        for row in member["memory_dependencies"]["source_dependencies"]
    }
    return len(human) + len(dependencies)


class SummaryWindows:
    def __init__(self, core):
        self.core, self.store = core, core.store

    def _current(self, turn, scope, channel):
        if (
            turn is None
            or turn["cancelled"]
            or turn.get("display_invalidated")
            or not self.core._turn_allows(turn, "memory.write")
            or any(
                turn["scope"][key] != scope[key]
                for key in ("actor_id", "audience", "conversation_id")
            )
            or (scope["audience"] != "group" and turn["scope"] != scope)
            or turn["bundle"]["collection_key"]["channel"] != channel
            or not sources_current(self.store, turn)
        ):
            raise Fault("scope_changed")
        ensure_channel(self.store, channel)
        self.core._check_role(turn)
        for message in turn["bundle"]["messages"]:
            admission = self.store.get("admissions", digest(selector(message, scope["actor_id"])))
            if admission is None or admission["fact"]["binding_version"] != turn["binding_version"]:
                raise Fault("scope_changed")

    def _memory_dependencies(self, turn, *, bundle=None):
        if bundle is None:
            bundle = (turn.get("memory_context") or {}).get("bundle")
        if bundle is None or any(type(bundle.get(key)) is not list for key in DEPENDENCIES):
            # Old replies have no complete captured Memory provenance. Their
            # independent human inputs remain eligible; never invent proof.
            return None
        result = {key: copy.deepcopy(bundle[key]) for key in DEPENDENCIES}
        try:
            for key, definition in (
                ("group_dependencies", "group_dependency"),
                ("episode_dependencies", "episode_dependency"),
                ("source_dependencies", "source_dependency"),
            ):
                for dependency in result[key]:
                    self.core.contracts.check("memory-context-v2#" + definition, dependency)
                    if key == "episode_dependencies":
                        continue
                    owner = dependency["owner_scope"]
                    if (turn["scope"]["audience"] != "group" and owner != turn["scope"]) or any(
                        owner[key] != turn["scope"][key]
                        for key in ("actor_id", "audience", "conversation_id")
                    ):
                        return None
        except Fault:
            return None
        source_ids = {row["dependency_id"] for row in result["source_dependencies"]}
        if any(
            row["source_dependency_id"] not in source_ids for row in result["episode_dependencies"]
        ):
            return None
        return result

    def _reply_dependencies(self, turn, turns, visiting=None):
        """Only replies whose complete captured Memory/input closure fits here."""
        visiting = set() if visiting is None else visiting
        if turn is None or turn["id"] in visiting or turn.get("profile_checks"):
            # profiles() records a check only after nonempty selected_units
            # were actually appended to the model context, not for candidates.
            # These old profile records lack exact v2 dependency arrays.
            return None
        recorded = reply_provenance_checks(turn)
        if turn.get("reply_context_provenance") is not None and recorded is None:
            # A new mixed/incomplete dispatch journal is not an old reply whose
            # latest three arrays can stand in for every actual model input.
            return None
        dependencies = (
            {key: [] for key in DEPENDENCIES}
            if recorded is not None
            else self._memory_dependencies(turn)
        )
        if dependencies is None:
            return None
        visiting = visiting | {turn["id"]}
        inherited = []
        for check in recorded if recorded is not None else turn.get("context_checks", []):
            identity = check.get("source_turn_id")
            if not identity or identity not in turns:
                return None
            if check["version_domain"] not in {
                "text-input/v1",
                "text-dialogue/v1",
                MEMORY_CONTEXT_DOMAIN,
            }:
                return None
            try:
                self.core._context_source(check)
            except Fault as error:
                if not getattr(error, "context_dependency_invalid", False):
                    raise
                return None
            if check["version_domain"] == MEMORY_CONTEXT_DOMAIN:
                bundle = check["memory_bundle"]
                if (
                    bundle.get("retrieval_mode") != "context"
                    or bundle.get("effective_scope") != check["scope"]
                ):
                    return None
                selected = self._memory_dependencies(turns[identity], bundle=bundle)
                if selected is None:
                    return None
                for key in DEPENDENCIES:
                    dependencies[key].extend(selected[key])
            if check["version_domain"] == "text-dialogue/v1" and identity != turn["id"]:
                inherited.append(identity)
        inherited.extend(dependency["turn_id"] for dependency in turn["bundle"]["dependencies"])
        for identity in dict.fromkeys(inherited):
            if identity not in turns:
                return None
            previous = self._reply_dependencies(turns[identity], turns, visiting)
            if previous is None:
                return None
            for key in DEPENDENCIES:
                dependencies[key].extend(previous[key])
        result = {
            key: list({canonical(value): value for value in values}.values())
            for key, values in dependencies.items()
        }
        # The wire carries complete arrays, never truncated provenance. A
        # reply whose inherited closure cannot fit is optional context only.
        return None if any(len(values) > 64 for values in result.values()) else result

    def _input_members(self, turn, purpose):
        common = dict(
            turn_id=turn["id"],
            turn_sequence=turn["sequence"],
            source_scope=copy.deepcopy(turn["scope"]),
            binding_version=turn["binding_version"],
            purpose=purpose,
        )
        values = []
        for message in turn["bundle"]["messages"]:
            text = input_text(message)
            if not text:
                continue
            reference = dict(
                input_revision=turn["bundle"]["collection_revision"], source=message["source"]
            )
            values.append(
                (
                    dict(
                        common,
                        member_id="input:" + digest([turn["id"], reference]),
                        role="user",
                        speaker=dict(
                            kind="person", person_id=message["person_id"], account=message["author"]
                        ),
                        reference=reference,
                        observed_at=message["sent_at"],
                        content_sha256=text_hash(text),
                        memory_dependencies={key: [] for key in DEPENDENCIES},
                    ),
                    text,
                )
            )
        return values

    def _members(self, turn, purpose, *, turns):
        """Reconstruct complete members still required by older frozen windows."""
        values = self._input_members(turn, purpose)
        dependencies = self._reply_dependencies(turn, turns)
        if dependencies is None or turn["result_version"] < 1:
            return values
        common = dict(
            turn_id=turn["id"],
            turn_sequence=turn["sequence"],
            source_scope=copy.deepcopy(turn["scope"]),
            binding_version=turn["binding_version"],
        )
        for reply in self.store.replies_for_turns([turn["id"]], "sent")[turn["id"]]:
            receipt = reply.get("receipt")
            if not receipt or receipt["state"] != "sent" or not receipt.get("channel_message_ids"):
                continue
            reference = dict(
                reply_id=reply["id"],
                segment_sequence=reply["segment_sequence"],
                result_version=turn["result_version"],
                receipt_digest=sent_digest(reply),
            )
            values.append(
                (
                    dict(
                        common,
                        member_id="reply:" + digest(reference),
                        role="assistant",
                        purpose="context",
                        speaker=dict(kind="actor", actor_id=turn["scope"]["actor_id"]),
                        reference=reference,
                        observed_at=utc(reply["attempted_at"]),
                        content_sha256=text_hash(reply["text"]),
                        memory_dependencies=dependencies,
                    ),
                    reply["text"],
                )
            )
        return values

    def read(self, new_turn_ids, *, max_bytes=MAX_REQUEST_BYTES):
        """Capture full new human inputs and bounded owner-proved context.

        Group segments include shared same-room dialogue; private context remains
        exact-scope. Optional sent replies require their existing full provenance
        and never replace a new human member.
        """
        if not 1 <= len(new_turn_ids) <= MAX_NEW_TURNS or len(set(new_turn_ids)) != len(
            new_turn_ids
        ):
            raise Fault("invalid_input")
        fresh = [self.store.get("turns", identity) for identity in new_turn_ids]
        if any(turn is None for turn in fresh):
            raise Fault("scope_changed")
        fresh.sort(key=lambda turn: turn["sequence"])
        anchor = fresh[-1]
        scope, channel = anchor["scope"], anchor["bundle"]["collection_key"]["channel"]
        for turn in fresh:
            if turn["scope"] != scope or self.core._event_reality(turn) != self.core._event_reality(
                anchor
            ):
                raise Fault("scope_changed")
            self._current(turn, scope, channel)
        recent = self.store.recent_turns(
            anchor["conversation_id"],
            anchor["sequence"] + 1,
            MAX_TURNS + 1,
            scope=None if scope["audience"] == "group" else scope,
        )
        selected = {turn["id"]: turn for turn in fresh}
        omitted = len(recent) > MAX_TURNS
        for candidate in recent:
            if candidate["id"] in selected:
                continue
            if len(selected) >= MAX_TURNS:
                omitted = True
                break
            turn = self.store.get("turns", candidate["id"])
            try:
                self._current(turn, scope, channel)
            except Fault as error:
                if error.code not in {"scope_changed", "forbidden", "not_found"}:
                    raise
                omitted = True
                continue
            selected[turn["id"]] = turn

        # New input groups take precedence; an optional older group is omitted
        # whole if it cannot fit. No member text is cut mid-sentence.
        groups, count, size = {}, 0, 0
        ordered = fresh + sorted(
            (turn for key, turn in selected.items() if key not in new_turn_ids),
            key=lambda turn: turn["sequence"],
            reverse=True,
        )
        for turn in ordered:
            members = self._input_members(turn, "new" if turn["id"] in new_turn_ids else "context")
            added = sum(len(text.encode("utf-8")) for _, text in members)
            if count + len(members) > MAX_MEMBERS or size + added > MAX_TEXT_BYTES:
                if turn["id"] in new_turn_ids:
                    raise Fault("budget_exceeded")
                omitted = True
                continue
            groups[turn["id"]] = members
            count, size = count + len(members), size + added

        def replies():
            """Rebuild optional BOT text only against the humans actually kept."""
            nonlocal omitted
            turns = {identity: selected[identity] for identity in groups}
            result, used, text_size = {}, count, size
            for turn in ordered:
                if turn["id"] not in turns:
                    continue
                extra = [
                    pair
                    for pair in self._members(
                        turn, "new" if turn["id"] in new_turn_ids else "context", turns=turns
                    )
                    if pair[0]["role"] == "assistant"
                ]
                if not extra and turn.get("delivery_state") == "sent":
                    omitted = True
                added = sum(len(text.encode("utf-8")) for _, text in extra)
                if used + len(extra) <= MAX_MEMBERS and text_size + added <= MAX_TEXT_BYTES:
                    result[turn["id"]] = extra
                    used, text_size = used + len(extra), text_size + added
                else:
                    omitted = True
            return result

        reply_groups = replies()

        def package():
            pairs = [
                pair
                for turn in sorted(selected.values(), key=lambda turn: turn["sequence"])
                for pair in groups.get(turn["id"], []) + reply_groups.get(turn["id"], [])
            ]
            manifest = dict(
                schema_version=1,
                scope=copy.deepcopy(scope),
                channel=copy.deepcopy(channel),
                anchor=dict(
                    turn_id=anchor["id"], input_revision=anchor["bundle"]["collection_revision"]
                ),
                new_turns=[
                    dict(turn_id=turn["id"], input_revision=turn["bundle"]["collection_revision"])
                    for turn in fresh
                ],
                members=[member for member, _ in pairs],
                coverage=dict(complete=not omitted, omitted_before=omitted, omitted_after=False),
            )
            manifest["window_id"] = digest(manifest)
            return dict(
                manifest=manifest,
                members=[dict(member_id=member["member_id"], text=text) for member, text in pairs],
            )

        window = package()
        while (
            len(canonical(window).encode("utf-8")) > min(MAX_REQUEST_BYTES, max_bytes)
            or source_upper_bound(window["manifest"]) > MAX_SOURCES
        ):
            # Omit optional owner history whole; never trim a new input.
            older = next(
                (identity for identity in reversed(groups) if identity not in new_turn_ids), None
            )
            if older is not None:
                del groups[older]
                count = sum(len(values) for values in groups.values())
                size = sum(
                    len(text.encode("utf-8")) for values in groups.values() for _, text in values
                )
                reply_groups = replies()
            elif reply_groups:
                reply_groups = {}
            else:
                raise Fault("budget_exceeded")
            omitted = True
            window = package()
        self.core.contracts.check("memory-extraction#summary_window", window)
        return window

    def learning_frame(self, service, request):
        """Prove Core's channel identity, not observation or reply permission.

        The authenticated Memory owner obtains the current observation actor
        and channel from Platform. Core alone assigns the conversation ID; no
        source admission, person, turn or dialogue capability is created here.
        """
        if service not in {"memory", "platform"}:
            raise Fault("forbidden")
        self.core.contracts.check("memory-extraction#segment_frame_request", request)
        actor, channel = request["actor_id"], request["channel"]
        if actor not in self.core.roles or (
            service == "memory" and not self.core._role_allows(actor, "memory.write")
        ):
            raise Fault("forbidden")
        binding = self.core.bindings.get(channel["binding_id"])
        if binding is not None:
            if binding["namespace"] != channel["namespace"] or actor not in binding["actor_ids"]:
                raise Fault("forbidden")
            audience = binding["audience"]
        elif channel["namespace"] == "qq" and channel["channel_conversation_id"].startswith(
            ("group:", "private:")
        ):
            audience = (
                "group"
                if channel["channel_conversation_id"].startswith("group:")
                else "self_private"
            )
        else:
            raise Fault("forbidden")
        delivery = request["delivery_channel"]
        if delivery is not None and any(
            channel[key] != delivery[key]
            for key in ("namespace", "channel_conversation_id", "thread_id")
        ):
            raise Fault("forbidden")
        if delivery is not None and delivery["binding_id"] in self.core.bindings:
            current_binding = self.core.bindings[delivery["binding_id"]]
            if (
                current_binding["namespace"] != delivery["namespace"]
                or current_binding["audience"] != audience
                or actor not in current_binding["actor_ids"]
            ):
                raise Fault("forbidden")
        with self.store.transaction():
            current = link_learning_channel(
                self.store, channel, delivery, conversation_id=request["conversation_id"]
            )
            response = dict(
                schema_version=1,
                valid=True,
                frame=dict(
                    actor_id=actor, audience=audience, conversation_id=current["conversation_id"]
                ),
                channel=copy.deepcopy(channel),
                delivery_channel=copy.deepcopy(delivery),
            )
            self.core.contracts.check("memory-extraction#segment_frame_response", response)
            return response

    def check(self, service, request):
        if service != "memory":
            raise Fault("forbidden")
        if isinstance(request, dict) and "manifest" not in request:
            return self.learning_frame(service, request)
        self.core.contracts.check("memory-extraction#summary_window_check_request", request)
        manifest = request["manifest"]
        if manifest["window_id"] != digest(
            {key: value for key, value in manifest.items() if key != "window_id"}
        ):
            raise Fault("invalid_input")
        identities = [member["member_id"] for member in manifest["members"]]
        turn_ids = list(dict.fromkeys(member["turn_id"] for member in manifest["members"]))
        if len(set(identities)) != len(identities) or len(turn_ids) > MAX_TURNS:
            raise Fault("invalid_input")
        if source_upper_bound(manifest) > MAX_SOURCES:
            raise Fault("budget_exceeded")
        new_ids = [entry["turn_id"] for entry in manifest["new_turns"]]
        if not new_ids or len(set(new_ids)) != len(new_ids):
            raise Fault("invalid_input")
        turns = {key: self.store.get("turns", key) for key in set(turn_ids) | set(new_ids)}
        new = [turns[key] for key in new_ids]
        if any(turn is None for turn in new):
            raise Fault("scope_changed")
        # This synchronous owner check has one fixed manifest/turn set. Reuse
        # reconstruction only inside this call, not across an await or a later
        # window check. A multi-segment BOT reply otherwise repeats the same
        # recursive dependency and sent-reply reads for every member.
        validated, captured_members = set(), {}
        reply_turn_ids = {
            member["turn_id"] for member in manifest["members"] if member["role"] == "assistant"
        }

        def current(turn):
            if turn is None or turn["id"] not in validated:
                self._current(turn, manifest["scope"], manifest["channel"])
                validated.add(turn["id"])

        def members(turn):
            identity = turn["id"]
            if identity not in captured_members:
                purpose = "new" if identity in new_ids else "context"
                captured_members[identity] = (
                    self._members(turn, purpose, turns=turns)
                    if identity in reply_turn_ids
                    else self._input_members(turn, purpose)
                )
            return captured_members[identity]

        for entry, turn in zip(manifest["new_turns"], new, strict=True):
            current(turn)
            if (
                turn["scope"] != manifest["scope"]
                or entry["input_revision"] != turn["bundle"]["collection_revision"]
            ):
                raise Fault("scope_changed")
        latest = max(new, key=lambda turn: turn["sequence"])
        if manifest["anchor"] != dict(
            turn_id=latest["id"], input_revision=latest["bundle"]["collection_revision"]
        ):
            raise Fault("invalid_input")
        if new_ids != [turn["id"] for turn in sorted(new, key=lambda turn: turn["sequence"])]:
            raise Fault("invalid_input")
        invalid, text_bytes = [], 0
        for member in manifest["members"]:
            turn = turns[member["turn_id"]]
            try:
                current(turn)
                if member["purpose"] != (
                    "new"
                    if member["role"] == "user" and member["turn_id"] in new_ids
                    else "context"
                ):
                    raise Fault("invalid_input")
                actual = next(
                    (pair for pair in members(turn) if pair[0]["member_id"] == member["member_id"]),
                    None,
                )
                expected = actual[0] if actual else None
                if actual:
                    text_bytes += len(actual[1].encode("utf-8"))
                if expected != member:
                    invalid.append(member["member_id"])
            except Fault as error:
                if error.code not in {"scope_changed", "forbidden", "not_found"}:
                    raise
                invalid.append(member["member_id"])
        for turn in new:
            present = [
                member
                for member in manifest["members"]
                if member["turn_id"] == turn["id"] and member["role"] == "user"
            ]
            expected = [member for member, _ in members(turn) if member["role"] == "user"]
            if expected != present:
                invalid.extend(member["member_id"] for member in present)
                if not present:
                    raise Fault("scope_changed")
        for identity in turn_ids:
            assistants = [
                member
                for member in manifest["members"]
                if member["turn_id"] == identity and member["role"] == "assistant"
            ]
            if not assistants or turns[identity] is None:
                continue
            expected_inputs = [
                member for member, _ in members(turns[identity]) if member["role"] == "user"
            ]
            actual_inputs = [
                member
                for member in manifest["members"]
                if member["turn_id"] == identity and member["role"] == "user"
            ]
            if actual_inputs != expected_inputs:
                invalid.extend(member["member_id"] for member in assistants)
        if text_bytes > MAX_TEXT_BYTES or [
            member["turn_sequence"] for member in manifest["members"]
        ] != sorted(member["turn_sequence"] for member in manifest["members"]):
            raise Fault("invalid_input")
        invalid = list(dict.fromkeys(invalid))
        return dict(
            schema_version=1,
            request_id=request["request_id"],
            request_digest=digest(request),
            window_id=manifest["window_id"],
            state="invalid" if invalid else "valid",
            invalid_member_ids=invalid,
            checked_at=utc(self.core.clock()),
        )
