"""GSCore semantic queries over the verified HTTP protocol, never raw model commands."""

import base64
import binascii
import copy
import json
import re
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from ..contracts import Fault, digest, strict_json
from ..service_credentials import resolve
from .sources import origin
from .registry import EMPTY_CONFIG


MAPPING = json.loads(
    (Path(__file__).parent / "data" / "gscore-commands.json").read_text(encoding="utf-8")
)
GAMES = sorted({item["game"] for item in MAPPING["commands"]})


async def request(config, token, body):
    """One bounded attempt; no redirect, fallback transport or automatic retry."""
    headers = {"X-WS-Token": token} if token else {}
    async with httpx.AsyncClient(timeout=25, follow_redirects=False) as client:
        async with client.stream(
            "POST", config["base_url"].rstrip("/") + "/api/send_msg", json=body, headers=headers
        ) as response:
            if response.status_code != 200:
                raise Fault("dependency_unavailable", unknown=response.status_code >= 500)
            data = bytearray()
            async for chunk in response.aiter_bytes():
                data.extend(chunk)
                if len(data) > 8_000_000:
                    raise Fault("budget_exceeded", unknown=True)
    return strict_json(data)


class GSCoreSkill:
    domain = "game"
    operations = {"game.query": "game_query"}

    def validate_config(self, config):
        if config["provider"] not in {None, "gscore"}:
            raise Fault("invalid_input")
        if config["base_url"] and urlsplit(config["base_url"]).path not in {"", "/"}:
            raise Fault("invalid_input")
        if config["options"]:
            raise Fault("invalid_input")

    def public_options(self, config):
        return {}

    def revision_context(self, registry, actor):
        return MAPPING

    def availability(self, registry, actor, config):
        ready = bool(
            config["base_url"] and config["provider"] == "gscore" and config["credential_ref"]
        )
        return dict(
            state="available" if ready else "not_configured",
            can_execute=ready,
            reason_code="http_configured_network_unverified"
            if ready
            else "gscore_configuration_required",
        )

    def context(self, actions, turn):
        return dict(
            provider="gscore",
            protocol_version=MAPPING["version"],
            supported_queries=[
                {key: item[key] for key in ("game", "operation", "meaning")}
                for item in MAPPING["commands"]
            ],
            completion_support="first_fragment_only_full_completion_unknown",
        )

    def instructions(self):
        return (
            "Query the verified GSCore guide commands using semantic fields only. This HTTP protocol "
            "may return only one output fragment and cannot attest full handler completion. Do not "
            "claim a complete guide or complete team result. Wuthering Waves team_statistics are "
            "Matrix appearance/high-score statistics, not a personalized team solver. Use actual "
            "received evidence to compose suggestions. Unknown/timeouts must not be automatically "
            "retried. Received PNG originals use this conversation's normal delivery; do not send "
            "them again. Only received PNG bytes are supported in this first adapter; other image formats/encodings or missing output are explicit unsupported_content gaps."
            "Before drawing guide or team conclusions from an image, use existing life_read with "
            "resource=image and the returned content_ref.object_id to inspect its actual original. "
            "A reference alone is not visual evidence. If vision is unsupported or exceeds the "
            "existing budget, disclose that limitation instead of inventing image contents."
        )

    def tools(self, actions, turn, definition):
        if not actions.core._turn_allows(turn, "dialogue"):
            return []
        return [
            dict(
                type="function",
                function=dict(
                    name="game_query",
                    description="Look up game character guides, NTE team references or Wuthering Waves Matrix team statistics. "
                    "Only installed verified queries are available; use returned evidence and disclose coverage gaps.",
                    parameters=dict(
                        type="object",
                        properties=dict(
                            game=dict(enum=GAMES),
                            operation=dict(enum=["guide", "team", "team_statistics"]),
                            character=dict(type="string", maxLength=40),
                        ),
                        required=["game", "operation"],
                        additionalProperties=False,
                    ),
                ),
            )
        ]

    def command(self, supplied):
        if not isinstance(supplied, dict) or set(supplied) - {"game", "operation", "character"}:
            raise Fault("invalid_input")
        game, operation, character = (
            supplied.get("game"),
            supplied.get("operation"),
            supplied.get("character", ""),
        )
        mapping = next(
            (
                item
                for item in MAPPING["commands"]
                if item["game"] == game and item["operation"] == operation
            ),
            None,
        )
        if not mapping:
            raise Fault("invalid_input")
        if not isinstance(character, str) or not re.fullmatch(
            r"[\w\u4e00-\u9fff .·-]{0,40}", character
        ):
            raise Fault("invalid_input")
        if mapping["character_required"] and not character.strip():
            raise Fault("invalid_input")
        suffix = " " + character.strip() if character.strip() else ""
        return mapping["template"].format(character=character.strip(), optional_character=suffix)

    async def execute(self, actions, turn_id, tool, *, model_slot_held=False):
        core, registry = actions.core, actions.registry
        turn = core.store.get("turns", turn_id)
        selected = registry.check_selected(turn, "game_query")
        supplied = strict_json(tool["function"]["arguments"])
        command = self.command(supplied)
        config = copy.deepcopy(
            registry.actor(turn["scope"]["actor_id"])["skills"]
            .get(selected["id"], {})
            .get("config", EMPTY_CONFIG)
        )
        call_id = "gscore:" + digest([turn_id, supplied, config])
        cached = core.store.get("metadata", call_id)
        if cached:
            return cached["result"], []
        result = dict(
            state="unknown",
            provider="gscore",
            call_id=call_id,
            complete=False,
            text="",
            coverage="completion_unknown",
            error_code="request_not_reconciled",
            content_refs=[],
            unsupported_content=[],
            delivery_state="not_requested",
        )
        # Durable before the request: restart or repeated semantic call never resubmits it.
        core.store.put("metadata", dict(id=call_id, result=result, scope=turn["scope"]))
        identity = "tianshu-skills:" + digest(turn["scope"]["actor_id"])[:24]
        body = dict(
            bot_id="TianshuSkills",
            bot_self_id="",
            msg_id=call_id,
            user_type="direct",
            group_id=None,
            user_id=identity,
            sender={},
            user_pm=6,
            content=[dict(type="text", data=command)],
        )
        try:
            token = await resolve(
                core.image_backend.credentials,
                config["credential_ref"],
                origin(config["base_url"]),
                "companion.skills",
                contracts=core.contracts,
            )
            response = await request(config, token, body)
            await core._preflight(core.store.get("turns", turn_id))
            registry.check_selected(core.store.get("turns", turn_id), "game_query")
            if (
                not isinstance(response, dict)
                or response.get("status_code") != 200
                or not isinstance(response.get("data"), dict)
            ):
                result["error_code"] = "no_confirmed_output"
            else:
                data = response["data"]
                if any(
                    data.get(key) != value
                    for key, value in (
                        ("bot_id", body["bot_id"]),
                        ("bot_self_id", ""),
                        ("msg_id", call_id),
                        ("target_type", "direct"),
                        ("target_id", identity),
                    )
                ):
                    raise Fault("scope_changed", unknown=True)
                fragments = data.get("content")
                if (
                    not isinstance(fragments, list)
                    or len(fragments) > 16
                    or any(not isinstance(fragment, dict) for fragment in fragments)
                ):
                    raise Fault("invalid_input", unknown=True)
                texts, refs = [], []
                for index, fragment in enumerate(fragments):
                    kind, value = fragment.get("type"), fragment.get("data")
                    if kind in {"text", "markdown"} and isinstance(value, str):
                        texts.append(value[:8000])
                    elif kind == "image" and isinstance(value, str):
                        encoding = value.partition("://")[0]
                        if encoding == "base64" and len(refs) < core.images.max_files:
                            try:
                                raw = base64.b64decode(value[len("base64://") :], validate=True)
                                refs.append(
                                    core.images.receive_external(
                                        turn["scope"]["actor_id"],
                                        raw,
                                        turn["scope"],
                                        call_id,
                                        index,
                                        dict(
                                            provider="gscore",
                                            query=supplied,
                                            protocol_version=MAPPING["version"],
                                        ),
                                    )
                                )
                            except (ValueError, binascii.Error):
                                result["unsupported_content"].append(
                                    dict(
                                        type="image",
                                        encoding=encoding,
                                        reason_code="unsupported_or_invalid_png",
                                    )
                                )
                        else:
                            result["unsupported_content"].append(
                                dict(
                                    type="image",
                                    encoding=encoding,
                                    reason_code="unsupported_content",
                                )
                            )
                    else:
                        result["unsupported_content"].append(
                            dict(type=str(kind)[:32], reason_code="unsupported_content")
                        )
                result.update(
                    text="\n".join(texts)[:24000],
                    content_refs=refs,
                    error_code="unsupported_content"
                    if result["unsupported_content"]
                    else "completion_unknown",
                )
                if refs:
                    # GS target IDs only validate correlation; the original turn owns delivery.
                    await core.stream_segment(turn_id, "查询返回的攻略图片（可能不完整）。", refs)
                    current = core.store.get("turns", turn_id)
                    result["delivery_state"] = (current.get("expression_receipt") or {}).get(
                        "state", "queued"
                    )
                if not texts and not refs and not result["unsupported_content"]:
                    result["error_code"] = "no_confirmed_output"
        except (Fault, httpx.HTTPError, TimeoutError, ValueError, KeyError, TypeError) as error:
            result["error_code"] = (
                error.code
                if isinstance(error, Fault)
                else "timeout"
                if isinstance(error, (httpx.TimeoutException, TimeoutError))
                else "dependency_unavailable"
            )
        core.store.put("metadata", dict(id=call_id, result=result, scope=turn["scope"]))
        return result, []
