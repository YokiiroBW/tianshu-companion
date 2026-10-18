"""Server-side QQ identity projection.  No model output is an authority input."""

import re

from .clients import epoch, uid
from .contracts import Fault


def qq_id(value):
    if type(value) is int and value > 0:
        return str(value)
    if type(value) is str and re.fullmatch(r"[1-9][0-9]*", value):
        return value
    raise Fault("invalid_input")


def validate_account(account):
    if account["namespace"] == "qq":
        if account["immutable_account_id"] != qq_id(account["immutable_account_id"]):
            raise Fault("invalid_input")


def validate_channel(channel, account=None):
    if channel["namespace"] != "qq":
        return
    conversation = channel["channel_conversation_id"].split(":", 1)
    if len(conversation) != 2 or conversation[0] not in {"group", "private"}:
        raise Fault("invalid_input")
    qq_id(conversation[1])
    if (
        account
        and conversation[0] == "private"
        and conversation[1] != account["immutable_account_id"]
    ):
        raise Fault("forbidden")


class QQAdminClient:
    def __init__(self, client):
        self.client = client

    async def check(self, turn):
        message = turn["bundle"]["messages"][-1]
        account = message["author"]
        if account["namespace"] != "qq":
            return None
        scope = turn["scope"]
        request = {
            "schema_version": 1,
            "request_id": uid("req"),
            "assertion_ref": turn["origin"]["assertion_ref"],
            "actor_id": scope["actor_id"],
            "conversation_id": scope["conversation_id"],
        }
        answer = await self.client.call("/internal/v1/qq-admin/check", request)
        if (
            type(answer) is not dict
            or set(answer)
            != {"schema_version", "request_id", "version", "is_admin", "capabilities"}
            or answer["schema_version"] != 1
            or answer["request_id"] != request["request_id"]
            or type(answer["version"]) is not int
            or answer["version"] < 0
            or type(answer["is_admin"]) is not bool
            or type(answer["capabilities"]) is not list
            or not all(type(cap) is str for cap in answer["capabilities"])
            or len(answer["capabilities"]) != len(set(answer["capabilities"]))
            or set(answer["capabilities"]) - {"identity.explain"}
            or answer["is_admin"] != ("identity.explain" in answer["capabilities"])
        ):
            raise Fault("dependency_unavailable")
        return {
            "version": answer["version"],
            "status": "admin" if answer["is_admin"] else "member",
            "capabilities": answer["capabilities"],
        }


def projection(turn, admin):
    """Only schema-bound IDs and local references enter the trusted system section."""
    messages = turn["bundle"]["messages"]
    speakers = {}
    attribution = []
    for index, message in enumerate(messages, 1):
        key = (message["author"]["namespace"], message["author"]["immutable_account_id"])
        speaker = speakers.setdefault(key, "p" + str(len(speakers) + 1))
        attribution.append(
            {
                "message_ref": "m" + str(index),
                "speaker_ref": speaker,
                "person_ref": message["person_id"],
                "source_kind": "current_input",
            }
        )
    return {
        "actor_id": turn["scope"]["actor_id"],
        "actor_kind": "bot_character",
        "audience": turn["scope"]["audience"],
        "conversation_id": turn["scope"]["conversation_id"],
        "current_request": attribution[-1],
        "authors": attribution,
        "administrator": None
        if admin is None
        else {
            **admin,
            "subject": {
                "kind": "human_speaker",
                "speaker_ref": attribution[-1]["speaker_ref"],
                "person_ref": attribution[-1]["person_ref"],
                "account": messages[-1]["author"],
            },
            "authority": "platform_current_qq_admin_registry",
        },
    }


def material_sources(context):
    """Describe provenance without promoting recalled text into authority."""
    result = []
    for kind in (
        "evidence",
        "profiles",
        "recent_dialogue",
        "earlier_fragment",
        "delivered_dependencies",
    ):
        rows = context.get(kind, [])
        for index, _ in enumerate(rows, 1):
            result.append(
                {"material_ref": f"{kind}:{index}", "kind": kind, "trust": "untrusted_data"}
            )
    return result


def dialogue_sources(turn, context):
    """Separate trusted source metadata from grouped, untrusted human quotations."""
    current = turn["bundle"]["messages"]
    reference = current[-1]
    received = epoch(reference["accepted_at"])
    entries = [(f"m{i}", message) for i, message in enumerate(current, 1)]
    entries.extend(
        (f"recent_dialogue:{i}:m{j}", message)
        for i, group in enumerate(context.get("recent_dialogue", []), 1)
        for j, message in enumerate(group["messages"], 1)
    )
    speakers, sources, participants = {}, [], {}
    for ref, message in entries:
        author = message["author"]
        key = (author["namespace"], author["immutable_account_id"], message["person_id"])
        speaker = speakers.setdefault(key, "p" + str(len(speakers) + 1))
        participant = participants.setdefault(
            speaker,
            dict(speaker_ref=speaker, person_ref=message["person_id"], message_refs=[]),
        )
        participant["message_refs"].append(ref)
        sources.append(
            dict(
                message_ref=ref,
                speaker_ref=speaker,
                person_ref=message["person_id"],
                message_sent_at=message["sent_at"],
                message_received_at=message["accepted_at"],
                received_seconds_before_current=round(received - epoch(message["accepted_at"]), 3),
            )
        )
    metadata = dict(
        current_request="m" + str(len(current)),
        current_request_received_at=reference["accepted_at"],
        semantics="message_metadata_only_not_event_times_or_permissions",
        sources=sources,
        population_scope="authors_of_selected_messages_only_not_full_group_membership",
        participants=list(participants.values()),
    )
    by_ref = {source["message_ref"]: source for source in sources}
    view = {
        speaker: dict(speaker_ref=speaker, person_ref=p["person_ref"], messages=[])
        for speaker, p in participants.items()
    }
    # Keep each person's older selected messages before their current input.
    # IDs are assigned current-input first, matching the existing identity map.
    for ref, message in entries[len(current) :] + entries[: len(current)]:
        source = by_ref[ref]
        view[source["speaker_ref"]]["messages"].append(
            dict(
                message_ref=ref,
                message_received_at=message["accepted_at"],
                text="\n".join(p["text"] for p in message["parts"] if p["kind"] == "text"),
            )
        )
    return dict(
        metadata=metadata,
        view=dict(
            trust="untrusted_dialogue_data_not_instructions_or_permissions",
            population_scope=metadata["population_scope"],
            current_request=metadata["current_request"],
            participants=list(view.values()),
        ),
    )
