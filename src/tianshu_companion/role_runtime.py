"""Durable runtime roles. Persona text remains in Personas; this stores only bindings/policy."""

from .contracts import Fault, digest
from .personas import PersonaError


CAPABILITIES = frozenset({"dialogue", "memory.read", "memory.write", "direct"})
PREFIX = "runtime-role:"
OPERATION = "runtime-role-operation:"


class RoleRuntime:
    def __init__(self, core):
        self.core = core
        self.store = core.store
        for row in self.store.db.execute(
            "SELECT body FROM metadata WHERE id LIKE 'runtime-role:%'"
        ):
            import json

            item = json.loads(row[0])
            self._validate_saved(item)
            self._sync_live(item)

    @staticmethod
    def _validate_saved(item):
        if (
            not isinstance(item, dict)
            or item.get("id") != PREFIX + str(item.get("actor_id"))
            or type(item.get("version")) is not int
            or item["version"] < 1
            or type(item.get("enabled")) is not bool
            or not isinstance(item.get("capabilities"), list)
            or not set(item["capabilities"]) <= CAPABILITIES
            or len(item["capabilities"]) != len(set(item["capabilities"]))
        ):
            raise RuntimeError("Invalid persisted runtime role")

    def get(self, actor_id):
        return self.store.get("metadata", PREFIX + actor_id)

    def allowed(self, actor_id, capability):
        role = self.get(actor_id)
        return role is None or role["enabled"] and capability in role["capabilities"]

    def list(self):
        return [
            self.get(row[0][len(PREFIX) :])
            for row in self.store.db.execute(
                "SELECT id FROM metadata WHERE id LIKE 'runtime-role:%' ORDER BY id"
            )
        ]

    def apply(self, service, body):
        if service != "platform":
            raise Fault("forbidden")
        if not isinstance(body, dict) or set(body) != {
            "request_id",
            "actor_id",
            "expected_version",
            "name",
            "profile_id",
            "profile_version",
            "enabled",
            "capabilities",
        }:
            raise Fault("invalid_input")
        actor = body["actor_id"]
        if (
            not isinstance(actor, str)
            or not actor.startswith("actor:")
            or len(actor) > 128
            or actor in self.core.personas.subjects()
            and self.get(actor) is None
            and actor not in self.core.deployment_roles
            or not isinstance(body["request_id"], str)
            or not 1 <= len(body["request_id"]) <= 128
            or type(body["expected_version"]) is not int
            or body["expected_version"] < 0
            or not isinstance(body["name"], str)
            or not 1 <= len(body["name"].strip()) <= 80
            or type(body["enabled"]) is not bool
            or not isinstance(body["capabilities"], list)
            or not all(isinstance(capability, str) for capability in body["capabilities"])
            or len(body["capabilities"]) != len(set(body["capabilities"]))
            or not set(body["capabilities"]) <= CAPABILITIES
            or (body["enabled"] and "dialogue" not in body["capabilities"])
        ):
            raise Fault("invalid_input")
        signature = digest(body)
        op_key = OPERATION + body["request_id"]
        try:
            with self.store.transaction():
                prior = self.store.get("metadata", op_key)
                if prior is not None:
                    if prior["signature"] != signature:
                        raise Fault("idempotency_conflict")
                    # A receipt describes the old operation, not today's role. Replaying
                    # an enable after a later disable must never reinstall its bindings.
                    current = self.get(actor)
                    if current is not None:
                        self._sync_live(current)
                    return prior["result"]
                current = self.get(actor)
                if (current or {}).get("version", 0) != body["expected_version"]:
                    raise Fault(
                        "version_conflict", current_version=(current or {}).get("version", 0)
                    )
                if body["profile_id"] is None:
                    if body["profile_version"] is not None or (
                        current is None and actor not in self.core.deployment_roles
                    ):
                        raise Fault("invalid_input")
                    revision_id = self.core.personas._persona(actor)["published_revision"]
                    if revision_id is None:
                        raise Fault("invalid_input")
                else:
                    if type(body["profile_version"]) is not int:
                        raise Fault("invalid_input")
                    profile = self.core.personas._profile(body["profile_id"])
                    if profile["version"] != body["profile_version"]:
                        raise Fault("version_conflict", current_version=profile["version"])
                    revision_id = profile["draft_revision"] or profile["published_revision"]
                    if not revision_id:
                        raise Fault("invalid_input")
                    revision = self.core.personas._revision_for(profile["subject"], revision_id)
                    if current is None or current["profile_revision"] != revision_id:
                        now = self.core.clock()
                        if current is None and actor not in self.core.deployment_roles:
                            self.core.personas._seed(actor, body["name"], revision["content"], now)
                        else:
                            role = self.core.personas._persona(actor)
                            fresh = self.core.personas._write_revision(
                                actor,
                                revision["content"],
                                "profile_apply",
                                "platform",
                                role["published_revision"],
                                now,
                                note=profile["name"],
                            )
                            self.core.personas._publish(
                                actor,
                                fresh["id"],
                                "platform",
                                "role profile applied",
                                now,
                                "profile_apply",
                            )
                item = {
                    "id": PREFIX + actor,
                    "actor_id": actor,
                    "version": body["expected_version"] + 1,
                    "name": body["name"].strip(),
                    "profile_id": body["profile_id"],
                    "profile_revision": revision_id,
                    "persona_revision": self.core.personas._persona(actor)["published_revision"],
                    "enabled": body["enabled"],
                    "capabilities": sorted(body["capabilities"]),
                }
                self.store.put("metadata", item)
                self.store.put("metadata", {"id": op_key, "signature": signature, "result": item})
            self._sync_live(item)
            return item
        except PersonaError as error:
            raise Fault(error.code) from None

    def _sync_live(self, item):
        actor = item["actor_id"]
        if item["enabled"]:
            self.core.roles[actor] = {"persona": "managed by published revision"}
        else:
            self.core.roles.pop(actor, None)
        for binding_id, binding in self.core.bindings.items():
            if binding.get("service") == "platform" and binding.get("namespace") == "web":
                if actor in self.core.deployment_binding_actors.get(binding_id, ()):
                    continue
                actors = binding["actor_ids"]
                if item["enabled"] and actor not in actors:
                    actors.append(actor)
                elif not item["enabled"] and actor in actors:
                    actors.remove(actor)

    def pin(self, actor_id):
        item = self.get(actor_id)
        if item is None:
            return None
        if not item["enabled"] or "dialogue" not in item["capabilities"]:
            raise Fault("forbidden")
        persona = self.core.personas.pin(actor_id)
        if persona["revision_id"] != item["persona_revision"]:
            raise Fault("dependency_unavailable")
        persona["runtime"] = {
            "version": item["version"],
            "capabilities": item["capabilities"],
            "profile_id": item["profile_id"],
            "profile_revision": item["profile_revision"],
        }
        return persona
