"""Versioned group controls; native mentions remain separate from literal triggers."""

import re

MODES = {"disabled", "observe", "reply", "active"}
GROUP = re.compile(r"[1-9][0-9]*\Z")


def rule(value):
    if (
        not isinstance(value, dict)
        or set(value) != {"mode", "actor_id", "keywords"}
        or value["mode"] not in MODES
        or (value["actor_id"] is not None and not isinstance(value["actor_id"], str))
        or value["actor_id"] == ""
        or not isinstance(value["keywords"], list)
        or len(value["keywords"]) > 32
        or any(
            not isinstance(word, str) or not word.strip() or len(word) > 64
            for word in value["keywords"]
        )
        or len(set(value["keywords"])) != len(value["keywords"])
    ):
        raise ValueError("invalid group rule")
    return {**value, "keywords": list(value["keywords"])}


def normalize(value):
    """Read real legacy policies deterministically, without ever enabling active."""
    if not isinstance(value, dict):
        raise ValueError("invalid group policy")
    if value.get("version") == 2:
        if set(value) != {"version", "default", "groups"} or type(value["version"]) is not int:
            raise ValueError("invalid group policy")
        groups = value["groups"]
        if not isinstance(groups, dict) or len(groups) > 256:
            raise ValueError("invalid group policy")
        if any(not isinstance(key, str) or not GROUP.fullmatch(key) for key in groups):
            raise ValueError("invalid group id")
        return dict(
            version=2,
            default=rule(value["default"]),
            groups={key: rule(item) for key, item in groups.items()},
        )
    if (
        set(value) - {"actor_id"} != {"observe", "mode", "list"}
        or type(value["observe"]) is not bool
        or value["mode"] not in {"observe_only", "whitelist", "blacklist"}
        or not isinstance(value["list"], list)
        or len(value["list"]) > 256
        or any(not isinstance(key, str) or not GROUP.fullmatch(key) for key in value["list"])
        or len(set(value["list"])) != len(value["list"])
    ):
        raise ValueError("invalid legacy group policy")
    actor = value.get("actor_id")
    default = "observe"
    override = "observe"
    if not value["observe"]:
        default = override = "disabled"
    elif value["mode"] == "whitelist":
        override = "reply"
    elif value["mode"] == "blacklist":
        default = "reply"
    return dict(
        version=2,
        default=rule(dict(mode=default, actor_id=actor, keywords=[])),
        groups={
            key: rule(dict(mode=override, actor_id=actor, keywords=[])) for key in value["list"]
        },
    )


def effective(policy, group):
    # The owner validates the full document at apply/load. Message hot paths
    # inspect only their own rule, never every unrelated group's keywords.
    if policy.get("version") == 2:
        return policy["groups"].get(group, policy["default"])
    mode = "observe"
    if not policy["observe"]:
        mode = "disabled"
    elif policy["mode"] == "whitelist" and group in policy["list"]:
        mode = "reply"
    elif policy["mode"] == "blacklist" and group not in policy["list"]:
        mode = "reply"
    return dict(mode=mode, actor_id=policy.get("actor_id"), keywords=[])


def decision(policy, conversation, *, mentioned=False, text=""):
    kind, target = conversation.split(":", 1)
    if kind == "private":
        permitted = (policy["mode"] == "whitelist" and target in policy["list"]) or (
            policy["mode"] == "blacklist" and target not in policy["list"]
        )
        return dict(
            observe=policy["observe"],
            reply_permitted=bool(policy["observe"] and permitted),
            reply_triggered=bool(policy["observe"] and permitted),
            active=False,
            keyword_triggered=False,
        )
    item = effective(policy, target)
    # Legacy host policies did not contain actor identities. Platform remains
    # their owner. New v2 policies with no actor can only archive.
    bound = policy.get("version") != 2 or item["actor_id"] is not None
    permitted = bound and item["mode"] in {"reply", "active"}
    keyword = bool(permitted and text and any(word in text for word in item["keywords"]))
    return dict(
        observe=item["mode"] != "disabled",
        reply_permitted=permitted,
        reply_triggered=bool(permitted and (mentioned or keyword)),
        active=bool(bound and item["mode"] == "active"),
        keyword_triggered=keyword,
    )
