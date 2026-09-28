"""Authenticated, narrow runtime bot bindings owned by the Platform caller.

Rows live in Core's existing metadata table. They never contain a principal, credential,
origin or source assertion, and cannot replace a deployment binding.
"""

import json

from .contracts import Fault, digest


class BotBindings:
    PREFIX = "bot-binding:"

    def __init__(self, core, sender, static_bindings):
        self.core = core
        self.sender = sender
        self.static = frozenset(static_bindings)
        self.rows = {}
        for row in core.store.db.execute("SELECT body FROM metadata WHERE id LIKE 'bot-binding:%'"):
            item = json.loads(row[0])
            self._validate_saved(item)
            connection_id = item["connection_id"]
            if connection_id in self.rows:
                raise RuntimeError("Duplicate bot binding")
            self.rows[connection_id] = item
        for item in self.rows.values():
            self._install(item)

    def _validate_saved(self, item):
        if (
            not isinstance(item, dict)
            or set(item)
            != {
                "id",
                "connection_id",
                "revision",
                "binding_id",
                "actor_id",
                "conversation",
                "enabled",
                "semantic",
                "request_id",
            }
            or item["id"] != self.PREFIX + item["connection_id"]
            or not isinstance(item["revision"], int)
            or item["revision"] < 1
            or type(item["enabled"]) is not bool
            or not isinstance(item["conversation"], dict)
            or item["conversation"].get("kind") not in {"group", "private"}
        ):
            raise RuntimeError("Invalid persisted bot binding")

    def _install(self, item):
        binding_id = item["binding_id"]
        if binding_id in self.static:
            return
        self.sender.select_dynamic(binding_id)
        if item["enabled"] and item["actor_id"] in self.core.roles:
            self.core.bindings[binding_id] = {
                "service": "platform",
                "namespace": "qq",
                "audience": "group" if item["conversation"]["kind"] == "group" else "self_private",
                "actor_ids": [item["actor_id"]],
            }
        else:
            self.core.bindings.pop(binding_id, None)

    def status(self, service, body):
        if service != "platform" or not isinstance(body, dict) or set(body) != {"connection_id"}:
            raise Fault("forbidden" if service != "platform" else "invalid_input")
        item = self.rows.get(body["connection_id"])
        return {
            "found": item is not None,
            "binding": None
            if item is None
            else {
                "connection_id": item["connection_id"],
                "revision": item["revision"],
                "enabled": item["enabled"]
                and item["actor_id"] in self.core.roles
                and item["binding_id"] not in self.static,
            },
        }

    def apply(self, service, body):
        if service != "platform":
            raise Fault("forbidden")
        if not isinstance(body, dict) or set(body) != {
            "request_id",
            "connection_id",
            "revision",
            "binding_id",
            "actor_id",
            "conversation",
            "enabled",
        }:
            raise Fault("invalid_input")
        connection_id, binding_id = body["connection_id"], body["binding_id"]
        conversation = body["conversation"]
        if (
            not isinstance(connection_id, str)
            or not connection_id.startswith("bot:")
            or len(connection_id) > 80
            or not isinstance(binding_id, str)
            or binding_id != "binding:" + connection_id
            or binding_id in self.static
            or any(
                c["binding_id"] == binding_id and c["connection_id"] != connection_id
                for c in self.rows.values()
            )
            or (
                body["actor_id"] not in self.core.roles
                and (body["enabled"] or connection_id not in self.rows)
            )
            or not isinstance(conversation, dict)
            or set(conversation) != {"kind", "id"}
            or conversation["kind"] not in {"group", "private"}
            or not isinstance(conversation["id"], str)
            or not 1 <= len(conversation["id"]) <= 128
            or type(body["revision"]) is not int
            or body["revision"] < 1
            or type(body["enabled"]) is not bool
            or not isinstance(body["request_id"], str)
            or len(body["request_id"]) > 128
        ):
            raise Fault("invalid_input")
        semantic = digest({key: value for key, value in body.items() if key != "request_id"})
        previous = self.rows.get(connection_id)
        if previous is not None:
            if body["revision"] == previous["revision"]:
                if previous["semantic"] != semantic or previous["request_id"] != body["request_id"]:
                    raise Fault("idempotency_conflict")
                return self.status(service, {"connection_id": connection_id})["binding"]
            if body["revision"] != previous["revision"] + 1:
                raise Fault("version_conflict")
            if any(
                previous[key] != body[key] for key in ("binding_id", "actor_id", "conversation")
            ):
                raise Fault("scope_changed")
        elif body["revision"] != 1 or body["enabled"]:
            raise Fault("version_conflict")
        item = {
            "id": self.PREFIX + connection_id,
            "connection_id": connection_id,
            "revision": body["revision"],
            "binding_id": binding_id,
            "actor_id": body["actor_id"],
            "conversation": conversation,
            "enabled": body["enabled"],
            "semantic": semantic,
            "request_id": body["request_id"],
        }
        with self.core.store.transaction():
            self.core.store.put("metadata", item)
        self.rows[connection_id] = item
        self._install(item)
        return {
            "connection_id": connection_id,
            "revision": item["revision"],
            "enabled": item["enabled"],
        }
