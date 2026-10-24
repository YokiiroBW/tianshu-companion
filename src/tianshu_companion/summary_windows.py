"""Core-owned dialogue windows for background consolidation, not another chat store."""

import copy
import hashlib

from .clients import utc
from .contracts import Fault, canonical, digest
from .short_context import sources_current
from .source_sync import ensure_channel, selector


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

    def _memory_dependencies(self, turn):
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

    def _reply_dependencies(self, turn, turns, visiting=None, *, blocked_replies=()):
        """Only replies whose complete captured Memory/input closure fits here."""
        visiting = set() if visiting is None else visiting
        if turn is None or turn["id"] in visiting or turn.get("profile_checks"):
            # profiles() records a check only after nonempty selected_units
            # were actually appended to the model context, not for candidates.
            # These old profile records lack exact v2 dependency arrays.
            return None
        dependencies = self._memory_dependencies(turn)
        if dependencies is None:
            return None
        visiting = visiting | {turn["id"]}
        inherited = []
        for check in turn.get("context_checks", []):
            identity = check.get("source_turn_id")
            if not identity or identity not in turns:
                return None
            if check["version_domain"] not in {"text-input/v1", "text-dialogue/v1"}:
                return None
            try:
                self.core._context_source(check)
            except Fault as error:
                if not getattr(error, "context_dependency_invalid", False):
                    raise
                return None
            if check["version_domain"] == "text-dialogue/v1" and identity != turn["id"]:
                if identity in blocked_replies:
                    return None
                inherited.append(identity)
        inherited.extend(dependency["turn_id"] for dependency in turn["bundle"]["dependencies"])
        for identity in dict.fromkeys(inherited):
            if identity not in turns:
                return None
            if identity in blocked_replies:
                return None
            previous = self._reply_dependencies(
                turns[identity], turns, visiting, blocked_replies=blocked_replies
            )
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

    def _members(self, turn, purpose, *, turns):
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
        dependencies = self._reply_dependencies(turn, turns)
        if dependencies is None or turn["result_version"] < 1:
            return values
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
            anchor["conversation_id"], anchor["sequence"] + 1, MAX_TURNS + 1
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
            if self.core._event_reality(turn) != self.core._event_reality(anchor):
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
            members = self._members(
                turn, "new" if turn["id"] in new_turn_ids else "context", turns=selected
            )
            if (
                self._reply_dependencies(turn, selected) is None
                and self.store.replies_for_turns([turn["id"]], "sent")[turn["id"]]
            ):
                omitted = True
            added = sum(len(text.encode("utf-8")) for _, text in members)
            if turn["id"] in new_turn_ids and (
                count + len(members) > MAX_MEMBERS or size + added > MAX_TEXT_BYTES
            ):
                # BOT context is optional, including replies to a new input.
                # The complete new human text can never be silently dropped.
                members = [pair for pair in members if pair[0]["role"] == "user"]
                added = sum(len(text.encode("utf-8")) for _, text in members)
                omitted = True
            if count + len(members) > MAX_MEMBERS or size + added > MAX_TEXT_BYTES:
                if turn["id"] in new_turn_ids:
                    raise Fault("budget_exceeded")
                omitted = True
                continue
            groups[turn["id"]] = members
            count, size = count + len(members), size + added

        def package():
            nonlocal omitted
            # A dropped context group cannot remain a hidden prerequisite of
            # a BOT reply, including a group omitted by the wire-byte budget.
            included = {identity: selected[identity] for identity in groups}
            for identity, members in groups.items():
                if (
                    any(pair[0]["role"] == "assistant" for pair in members)
                    and self._reply_dependencies(selected[identity], included) is None
                ):
                    groups[identity] = [pair for pair in members if pair[0]["role"] == "user"]
                    omitted = True
            pairs = [
                pair
                for turn in sorted(selected.values(), key=lambda turn: turn["sequence"])
                for pair in groups.get(turn["id"], [])
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
            # The request includes exact dependency arrays as well as text.
            # Omit optional context whole; never trim a covered new input.
            assistants = next(
                (
                    identity
                    for identity, members in groups.items()
                    if any(pair[0]["role"] == "assistant" for pair in members)
                ),
                None,
            )
            older = next(
                (identity for identity in reversed(groups) if identity not in new_turn_ids), None
            )
            if assistants is not None:
                groups[assistants] = [
                    pair for pair in groups[assistants] if pair[0]["role"] == "user"
                ]
            elif older is not None:
                del groups[older]
            else:
                raise Fault("budget_exceeded")
            omitted = True
            window = package()
        self.core.contracts.check("memory-extraction#summary_window", window)
        return window

    def without_context(self, window, invalid_ids):
        """A proved unclaimed batch may discard optional context once, never refill."""
        manifest = copy.deepcopy(window["manifest"])
        by_id = {member["member_id"]: member for member in manifest["members"]}
        if (
            not invalid_ids
            or len(set(invalid_ids)) != len(invalid_ids)
            or any(
                identity not in by_id or by_id[identity]["purpose"] != "context"
                for identity in invalid_ids
            )
        ):
            raise Fault("invalid_input")
        new_ids = {turn["turn_id"] for turn in manifest["new_turns"]}
        removed = {
            by_id[identity]["turn_id"]
            for identity in invalid_ids
            if by_id[identity]["turn_id"] not in new_ids
        }
        blocked_replies = {
            by_id[identity]["turn_id"]
            for identity in invalid_ids
            if by_id[identity]["role"] == "assistant"
        }
        members = [
            member
            for member in manifest["members"]
            if member["turn_id"] not in removed
            and not (member["role"] == "assistant" and member["turn_id"] in blocked_replies)
        ]
        turns = {
            member["turn_id"]: self.store.get("turns", member["turn_id"]) for member in members
        }
        members = [
            member
            for member in members
            if member["role"] != "assistant"
            or self._reply_dependencies(
                turns[member["turn_id"]], turns, blocked_replies=blocked_replies
            )
            is not None
        ]
        manifest["members"] = members
        manifest["coverage"] = dict(
            complete=False, omitted_before=True, omitted_after=manifest["coverage"]["omitted_after"]
        )
        manifest["window_id"] = digest(
            {key: value for key, value in manifest.items() if key != "window_id"}
        )
        retained = {member["member_id"] for member in members}
        result = dict(
            manifest=manifest,
            members=[
                copy.deepcopy(member)
                for member in window["members"]
                if member["member_id"] in retained
            ],
        )
        self.core.contracts.check("memory-extraction#summary_window", result)
        return result

    def check(self, service, request):
        if service != "memory":
            raise Fault("forbidden")
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
        for entry, turn in zip(manifest["new_turns"], new, strict=True):
            self._current(turn, manifest["scope"], manifest["channel"])
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
                self._current(turn, manifest["scope"], manifest["channel"])
                if member["purpose"] != (
                    "new"
                    if member["role"] == "user" and member["turn_id"] in new_ids
                    else "context"
                ):
                    raise Fault("invalid_input")
                actual = next(
                    (
                        pair
                        for pair in self._members(turn, member["purpose"], turns=turns)
                        if pair[0]["member_id"] == member["member_id"]
                    ),
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
            expected = [
                member
                for member, _ in self._members(turn, "new", turns=turns)
                if member["role"] == "user"
            ]
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
            purpose = "new" if identity in new_ids else "context"
            expected_inputs = [
                member
                for member, _ in self._members(turns[identity], purpose, turns=turns)
                if member["role"] == "user"
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
