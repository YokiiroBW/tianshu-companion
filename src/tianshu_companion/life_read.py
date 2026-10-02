"""Authorized read-only projection of persisted life state and published diaries.

This module owns exactly five things and nothing else: request validation, the double
authorization, the domain projection, the capture consistency checks and the response
budget. It spells no SQL (that is `life_read_queries`), it creates no index (that is
`life_read_index`), it speaks no HTTP (that is `app`), it never calls `Life` - so no tick, no
schedule projection, no generation and no publication can happen behind a read - and it
writes nothing at all.

Five rules are load-bearing:

- **Authorization is two independent facts, and the reader is never the caller's word.**
  The service is established by the bearer credential at the adapter; this module maps that
  service to one fixed deployment `reader_id`, and an operation is allowed only when the
  requested actor is inside that entry's deployed `actor_ids` (or its explicitly deployed
  `runtime_roles` membership) *and* the persisted
  `life_access` row for that actor already names the reader. A revoked grant is gone on the
  next request because the row is read per request; an unknown actor and an ungranted actor
  answer the same 404, so no caller can probe which actors exist. `life_access` is existing
  life state, not a second ACL store.
- **Only persisted state is projected, and absence stays absent.** `activity` and
  `outfit_ref` are `null` until the life engine has actually written something - an actor
  that was configured but never ticked has no activity - so the projection reports `null`
  instead of guessing a default or projecting the current schedule. Nothing here calls
  `Life.snapshot`/`summary`, which tick the world before answering.
- **Only the published pointer is readable.** The diary list and the revision read never
  substitute the current draft, never expose `current_revision`, and a revision id that is
  not the published one is a 404 rather than a hint that drafts exist.
- **A capture mark is proved, never assumed.** The frozen recipe and material hash are read
  from the diary row itself (never from the mutable recipe table), and the configuration
  version comes from the nearest gateway receipt along the published revision's parent
  chain. A contradiction between that receipt and the live diary row is a refusal: reporting
  the newer draft's configuration as the published capture would be a false provenance
  claim. No field is invented to patch an old row into looking complete.
- **A page is bounded and complete.** Page size is bounded, the row count is bounded, and the
  finished response is measured in real UTF-8 bytes. A page that would exceed its budget is
  refused rather than shortened, because a shortened page that claims to be the page is
  exactly the silent loss these limits exist to prevent.
"""

import math
import time
from datetime import date

from .contracts import Fault, canonical, digest

SCHEMA_VERSION = 1
# The largest deployment this port will enumerate, one PK pair per identity.
MAX_DEPLOYED_ACTORS = 64
MAX_IDENTITY = 128
DEFAULT_PAGE = 20
MAX_PAGE = 50
# Whole responses, in real UTF-8 JSON bytes.
LIST_BUDGET = 262144
REVISION_BUDGET = 1048576
# Parent links followed while proving one published capture. Beyond this the answer is a
# budget refusal; a repeated revision is corruption, not a long chain.
PARENT_LIMIT = 64
# Raw request body ceiling, enforced by the HTTP adapter before anything is parsed.
MAX_REQUEST_BYTES = 16384


def identity(value, what):
    """One bounded opaque identity. Never a default, never an empty string."""
    if not isinstance(value, str) or not value or len(value) > MAX_IDENTITY:
        raise ValueError(
            what + " must be a non-empty string of at most " + str(MAX_IDENTITY) + " characters"
        )
    return value


def readers(document, services=None):
    """The optional deployment mapping of read services to their fixed reader identity.

    Keys are existing caller service names, so a credential that was never registered as a
    caller can never be given a reader by this document, and `services` (when the caller
    knows the registry) turns an unknown name into a startup failure instead of an entry
    that silently never authenticates. A malformed entry raises `ValueError`: a deployment
    that means to grant reading must not start half-configured. `None` means the section is
    absent and the four routes answer 503; an empty mapping means the port exists and nobody
    is registered, which answers 403 rather than pretending the port is missing.
    """
    if document is None:
        return None
    if not isinstance(document, dict):
        raise ValueError("life_readers must be an object keyed by caller service")
    mapping, seen = {}, set()
    for service, entry in document.items():
        if not isinstance(service, str) or not service:
            raise ValueError("life_readers keys must be caller service names")
        if services is not None and service not in services:
            raise ValueError("life_readers names a service that is not a configured caller")
        if (
            not isinstance(entry, dict)
            or set(entry) - {"reader_id", "actor_ids", "runtime_roles"}
            or not {"reader_id", "actor_ids"} <= set(entry)
        ):
            raise ValueError("Each life_readers entry needs reader_id and actor_ids")
        runtime_roles = entry.get("runtime_roles", False)
        if type(runtime_roles) is not bool:
            raise ValueError("runtime_roles must be boolean")
        reader = identity(entry["reader_id"], "reader_id")
        actors = entry["actor_ids"]
        if (
            not isinstance(actors, list)
            or not (0 if runtime_roles else 1) <= len(actors) <= MAX_DEPLOYED_ACTORS
        ):
            raise ValueError("actor_ids must list between 1 and " + str(MAX_DEPLOYED_ACTORS))
        deployed = [identity(actor, "actor_id") for actor in actors]
        if len(set(deployed)) != len(deployed):
            raise ValueError("actor_ids must be unique")
        if reader in seen:
            # One reader identity behind two service credentials would let either credential
            # inherit the other's grants; a deployment names each reader exactly once.
            raise ValueError("reader_id must identify exactly one service")
        seen.add(reader)
        mapping[service] = {"reader_id": reader, "actor_ids": tuple(deployed)}
        if runtime_roles:
            mapping[service]["runtime_roles"] = True
    return mapping


def _corrupt():
    raise Fault("dependency_unavailable")


def _stored_text(body, key):
    value = body.get(key)
    if not isinstance(value, str) or not value:
        _corrupt()
    return value


def _stored_optional_text(body, key):
    value = body.get(key)
    if value is not None and not isinstance(value, str):
        _corrupt()
    return value


def _stored_version(body, key):
    value = body.get(key)
    if type(value) is not int or value < 1:
        _corrupt()
    return value


def _stored_time(body, key):
    value = body.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        _corrupt()
    return value


def _civil_day(value):
    """A real `YYYY-MM-DD` civil date, not merely a string that could be one."""
    try:
        return str(date.fromisoformat(value)) == value
    except (ValueError, TypeError):
        return False


def _frozen_recipe(diary):
    """The recipe and material hash frozen on the diary row when it was requested.

    Read from the row itself: the diary's recipe is immutable history, while the recipe table
    holds whatever the actor is configured with now, so using it here would relabel an old
    diary with today's constraints.
    """
    recipe = diary.get("recipe")
    if not isinstance(recipe, dict):
        _corrupt()
    recipe_id = recipe.get("id")
    if not isinstance(recipe_id, str) or not recipe_id:
        _corrupt()
    version = recipe.get("version")
    if type(version) is not int or version < 1:
        _corrupt()
    material = diary.get("material_version")
    if not isinstance(material, str) or not material:
        _corrupt()
    return recipe, material


def _positive(value):
    if type(value) is not int or value < 1:
        raise Fault("invalid_input")
    return value


def _request_identity(request, key):
    try:
        return identity(request.get(key), key)
    except ValueError:
        raise Fault("invalid_input") from None


def _page_limit(request, maximum=MAX_PAGE):
    value = request.get("limit", DEFAULT_PAGE)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise Fault("invalid_input")
    return value


def _cursor(request):
    """The `after` position: a sort key, never a grant, and optionally a deleted row.

    Like `limit`, an explicitly present cursor must have the documented shape: absent means
    "first page", while `null` is a caller error rather than a silently ignored default.
    """
    if "after" not in request:
        return None
    value = request["after"]
    if not isinstance(value, dict) or set(value) != {"day", "diary_id"}:
        raise Fault("invalid_input")
    day = value["day"]
    if not isinstance(day, str) or not _civil_day(day):
        raise Fault("invalid_input")
    return day, _request_identity(value, "diary_id")


class LifeRead:
    """Persisted life reads over one narrow query port and one deployment mapping."""

    def __init__(self, queries, *, readers, contracts, clock=time.time):
        self.queries, self.readers = queries, readers
        self.contracts, self.clock = contracts, clock

    def handle(self, service, operation, request):
        """One authenticated operation. The service came from the bearer credential."""
        entry = self.readers.get(service)
        if entry is None:
            raise Fault("forbidden")
        handler = {
            "actors": self._actors,
            "snapshot": self._snapshot,
            "diaries": self._diaries,
            "revision": self._revision,
            "today": self._today,
            "timeline": self._timeline,
        }.get(operation)
        if handler is None:
            raise Fault("not_found")
        return handler(entry, request)

    # ---------------------------------------------------------------- authorization

    def _granted(self, entry, actor_id):
        """Deployed static/runtime membership and an existing grant to this fixed reader."""
        if actor_id not in entry["actor_ids"] and not (
            entry.get("runtime_roles") and self.queries.runtime_role(actor_id) is not None
        ):
            return False
        access = self.queries.access(actor_id)
        if access is None:
            return False
        granted = access.get("readers")
        if not isinstance(granted, list) or any(not isinstance(name, str) for name in granted):
            _corrupt()
        return entry["reader_id"] in granted

    def _require_grant(self, entry, actor_id):
        if not self._granted(entry, actor_id):
            raise Fault("not_found")

    def _now(self):
        value = self.clock()
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            _corrupt()
        return value

    # ---------------------------------------------------------------- request boundary

    def _envelope(self, request, allowed):
        """One strict request: exactly the documented keys, and `schema_version` == 1.

        A boolean is not the integer 1, a missing version is not a default, and an unknown
        key is a refusal rather than something quietly ignored - a caller that misspells a
        field must not be told its request was understood.
        """
        if not isinstance(request, dict):
            raise Fault("invalid_input")
        if set(request) - allowed:
            raise Fault("invalid_input")
        if type(request.get("schema_version")) is not int or request["schema_version"] != 1:
            raise Fault("invalid_input")
        return request

    def _emit(self, document, budget):
        """One exact response, measured over the bytes the caller actually receives."""
        if len(canonical(document).encode("utf-8")) > budget:
            raise Fault("budget_exceeded")
        return document

    # ---------------------------------------------------------------- operations

    def _actors(self, entry, request):
        """Deployed and granted actors, in Python Unicode order, one bounded page."""
        request = self._envelope(request, {"schema_version", "limit", "after_actor_id"})
        limit = _page_limit(request)
        after = None
        if "after_actor_id" in request:
            after = _request_identity(request, "after_actor_id")
        items = []
        with self.queries.reading():
            candidates = set(entry["actor_ids"])
            if entry.get("runtime_roles"):
                candidates.update(self.queries.runtime_actor_ids(after, MAX_DEPLOYED_ACTORS + 1))
                if len(candidates) > MAX_DEPLOYED_ACTORS:
                    raise Fault("budget_exceeded")
            candidates = sorted(actor for actor in candidates if after is None or actor > after)
            for actor_id in candidates:
                if len(items) > limit:
                    break  # One row past the page only decides whether a next page exists.
                if not self._granted(entry, actor_id):
                    continue
                actor = self.queries.actor(actor_id)
                if actor is None:
                    continue
                items.append(
                    dict(
                        actor_id=actor_id,
                        actor_version=_stored_version(actor, "version"),
                        world_id=_stored_text(actor, "world_id"),
                        room_id=_stored_text(actor, "room_id"),
                    )
                )
        more = len(items) > limit
        items = items[:limit]
        return self._emit(
            dict(
                schema_version=SCHEMA_VERSION,
                fictional=True,
                items=items,
                next_after_actor_id=items[-1]["actor_id"] if more and items else None,
            ),
            LIST_BUDGET,
        )

    def _snapshot(self, entry, request):
        """The last persisted state of one actor, with the room and world it references."""
        request = self._envelope(request, {"schema_version", "actor_id"})
        actor_id = _request_identity(request, "actor_id")
        with self.queries.reading():
            self._require_grant(entry, actor_id)
            actor = self.queries.actor(actor_id)
            if actor is None:
                raise Fault("not_found")
            room = self.queries.room(_stored_text(actor, "room_id"))
            world = self.queries.world(_stored_text(actor, "world_id"))
            if room is None or world is None or room.get("world_id") != actor["world_id"]:
                # A missing or crossed reference is corrupt state, not an empty answer: the
                # projection is only meaningful while actor, room and world agree.
                _corrupt()
            document = dict(
                schema_version=SCHEMA_VERSION,
                fictional=True,
                actor_id=actor_id,
                actor_version=_stored_version(actor, "version"),
                world_id=actor["world_id"],
                world_version=_stored_version(world, "version"),
                room_id=actor["room_id"],
                room_version=_stored_version(room, "version"),
                timezone=_stored_text(world, "timezone"),
                activity=_stored_optional_text(actor, "activity"),
                mood=_stored_text(actor, "mood"),
                outfit_ref=_stored_optional_text(actor, "outfit_ref"),
                changed_at=_stored_time(actor, "changed_at"),
                observed_at=self._now(),
                state_basis="last_persisted",
            )
        return self._emit(document, LIST_BUDGET)

    def _diaries(self, entry, request):
        """Published diaries of one actor, `(day DESC, id DESC)`, keyset paged."""
        request = self._envelope(request, {"schema_version", "actor_id", "limit", "after"})
        actor_id = _request_identity(request, "actor_id")
        limit = _page_limit(request)
        after = _cursor(request)
        with self.queries.reading():
            self._require_grant(entry, actor_id)
            rows = self.queries.diaries_page(actor_id, limit + 1, after)
            items = [self._diary_entry(actor_id, row) for row in rows[:limit]]
            more = len(rows) > limit
        return self._emit(
            dict(
                schema_version=SCHEMA_VERSION,
                fictional=True,
                actor_id=actor_id,
                items=items,
                next_after=(
                    dict(day=items[-1]["day"], diary_id=items[-1]["diary_id"])
                    if more and items
                    else None
                ),
            ),
            LIST_BUDGET,
        )

    def _today(self, entry, request):
        """The persisted plan is a set of intentions, never a read-time clock projection."""
        self._life_contract("today_request", request)
        request = self._envelope(request, {"schema_version", "actor_id"})
        actor_id = _request_identity(request, "actor_id")
        with self.queries.reading():
            self._require_grant(entry, actor_id)
            actor = self.queries.actor(actor_id)
            if actor is None or not actor.get("daily_plan_id"):
                raise Fault("not_found")
            plan = self.queries.plan(actor["daily_plan_id"])
            if plan is None or plan.get("actor_id") != actor_id or not _civil_day(plan.get("day")):
                _corrupt()
            if plan.get("state") not in {"active", "paused", "completed", "superseded"}:
                _corrupt()
            generation_states = {
                "queued",
                "generating",
                "completed",
                "unavailable",
                "failed",
                "interrupted",
                "superseded",
                "skipped",
            }
            if plan.get("generation_state") not in generation_states or plan.get(
                "generated_by"
            ) not in {"baseline", "gateway"}:
                _corrupt()
            phases = plan.get("entries")
            if not isinstance(phases, list) or not 1 <= len(phases) <= 24:
                _corrupt()
            items = []
            for phase in phases:
                if (
                    type(phase.get("minute")) is not int
                    or not 0 <= phase["minute"] < 1440
                    or phase.get("state") not in {"planned", "current", "elapsed", "skipped"}
                    or phase.get("generation_state") not in generation_states
                    or phase.get("phase_id") != "phase:" + digest([plan["id"], phase["minute"]])
                ):
                    _corrupt()
                items.append(
                    {
                        "phase_id": phase["phase_id"],
                        "minute": phase["minute"],
                        "activity": _stored_text(phase, "activity"),
                        "detail": _stored_optional_text(phase, "detail"),
                        "state": phase["state"],
                        "generation_state": phase["generation_state"],
                    }
                )
            document = {
                "schema_version": 1,
                "fictional": True,
                "actor_id": actor_id,
                "day": plan["day"],
                "timezone": _stored_text(plan, "timezone"),
                "enabled": actor.get("life_enabled", True),
                "state_basis": "last_persisted",
                "observed_at": self._now(),
                "plan": {
                    "plan_id": plan["id"],
                    "version": _stored_version(plan, "version"),
                    "state": plan["state"],
                    "generation_state": plan["generation_state"],
                    "generated_by": plan["generated_by"],
                    "entries": items,
                    "current_phase_id": _stored_optional_text(plan, "current_phase_id"),
                },
            }
        self._life_contract("today_response", document, response=True)
        return self._emit(document, LIST_BUDGET)

    def _timeline(self, entry, request):
        self._life_contract("timeline_request", request)
        request = self._envelope(request, {"schema_version", "actor_id", "day", "limit", "after"})
        actor_id = _request_identity(request, "actor_id")
        day = request.get("day")
        if not _civil_day(day):
            raise Fault("invalid_input")
        limit, after = _page_limit(request), None
        if "after" in request:
            cursor = request["after"]
            if (
                not isinstance(cursor, dict)
                or set(cursor) != {"position", "known_id"}
                or type(cursor["position"]) is not int
            ):
                raise Fault("invalid_input")
            after = cursor["position"], _request_identity(cursor, "known_id")
        with self.queries.reading():
            self._require_grant(entry, actor_id)
            if self.queries.actor(actor_id) is None:
                raise Fault("not_found")
            rows = self.queries.timeline_page(actor_id, day, limit + 1, after)
            items = []
            for known in rows[:limit]:
                if (
                    known.get("conversation_id") != actor_id
                    or known.get("state") != day
                    or type(known.get("sequence")) is not int
                ):
                    _corrupt()
                event = self.queries.event(_stored_text(known, "event_id"))
                if event is None or event.get("fictional") is not True:
                    _corrupt()
                generated_by = event.get(
                    "generated_by",
                    "simulation" if event.get("kind") == "simulation" else "baseline",
                )
                if generated_by not in {"baseline", "gateway", "simulation"}:
                    _corrupt()
                items.append(
                    {
                        "event_id": event["id"],
                        "known_id": _stored_text(known, "id"),
                        "position": known["sequence"],
                        "kind": _stored_text(event, "kind"),
                        "summary": _stored_text(event, "summary"),
                        "occurred_at": _stored_time(event, "occurred_at"),
                        "learned_at": _stored_time(known, "learned_at"),
                        "via": _stored_text(known, "via"),
                        "phase_id": _stored_optional_text(event, "phase_id"),
                        "plan_id": _stored_optional_text(event, "plan_id"),
                        "generated_by": generated_by,
                    }
                )
            more = len(rows) > limit
        document = {
            "schema_version": 1,
            "fictional": True,
            "actor_id": actor_id,
            "day": day,
            "state_basis": "last_persisted",
            "items": items,
            "next_after": {"position": items[-1]["position"], "known_id": items[-1]["known_id"]}
            if more
            else None,
        }
        self._life_contract("timeline_response", document, response=True)
        return self._emit(document, LIST_BUDGET)

    def _life_contract(self, definition, value, *, response=False):
        if "life-read" not in self.contracts.schemas:
            raise Fault("dependency_unavailable")
        try:
            self.contracts.check("life-read#" + definition, value)
        except Fault:
            raise Fault("dependency_unavailable" if response else "invalid_input") from None

    def _bound_capture(self, actor_id, diary, diary_id):
        """The frozen recipe and material of one diary, proved against its content address.

        A diary id *is* the digest of the capture it records - actor, day, the whole frozen
        recipe and the material hash - so recomputing it is the only way to know the stored
        row still describes the capture a reader is about to be told about. Both the list and
        the single revision go through this one check, because a damaged recipe marker must
        never be reported as a real capture in one place and refused in the other: the list
        names the recipe version it read, so an unproved recipe version is a false claim just
        as surely as unproved text would be. The comparison is against the identity the caller
        really addressed - the primary key the row was read by, or the id the page selected -
        and nothing here rewrites a fact or falls back to the current recipe table.
        """
        recipe, material = _frozen_recipe(diary)
        if digest([actor_id, diary.get("day"), recipe, material]) != diary_id:
            _corrupt()
        return recipe, material

    def _diary_entry(self, actor_id, diary):
        """One published diary, proved against the revision it points at.

        The row is only projected once its published pointer resolves to a revision of this
        very diary carrying this very material hash, and once the recipe it carries is proved
        to be the capture its id was derived from; a row that fails any of those checks is
        corrupt data, and skipping it would make the page claim to be complete when it is not.
        The live `state` is kept as stored - a diary that was revised after publication is a
        draft again - while `current_revision` is never part of the answer, so the reader
        cannot reach unpublished text through the list.
        """
        diary_id = diary.get("id")
        if not isinstance(diary_id, str) or not diary_id:
            _corrupt()
        if diary.get("conversation_id") != actor_id or diary.get("fictional") is not True:
            _corrupt()
        day = diary.get("day")
        if not isinstance(day, str) or not _civil_day(day):
            _corrupt()
        state = diary.get("state")
        if not isinstance(state, str) or not state:
            _corrupt()
        published = diary.get("published_revision")
        if not isinstance(published, str) or not published:
            _corrupt()
        recipe, material = self._bound_capture(actor_id, diary, diary_id)
        revision = self.queries.revision(published)
        if revision is None:
            _corrupt()
        if (
            revision.get("conversation_id") != diary_id
            or revision.get("material_version") != material
            or revision.get("fictional") is not True
        ):
            _corrupt()
        return dict(
            diary_id=diary_id,
            actor_id=actor_id,
            day=day,
            state=state,
            version=_stored_version(diary, "version"),
            published_revision_id=published,
            fictional=True,
            captured=dict(
                recipe_id=recipe["id"], recipe_version=recipe["version"], material_version=material
            ),
        )

    def _revision(self, entry, request):
        """One published revision, in the order that keeps authorization ahead of content."""
        request = self._envelope(
            request,
            {"schema_version", "actor_id", "diary_id", "revision_id", "expected_diary_version"},
        )
        actor_id = _request_identity(request, "actor_id")
        diary_id = _request_identity(request, "diary_id")
        revision_id = _request_identity(request, "revision_id")
        expected = _positive(request.get("expected_diary_version"))
        with self.queries.reading():
            self._require_grant(entry, actor_id)
            diary = self.queries.diary(diary_id)
            if diary is None or diary.get("conversation_id") != actor_id:
                raise Fault("not_found")
            version = _stored_version(diary, "version")
            # The version is settled before any text is read, so a stale caller learns
            # nothing about content it is not being given.
            if version != expected:
                raise Fault("version_conflict", current_version=version)
            published = diary.get("published_revision")
            if not isinstance(published, str) or published != revision_id:
                raise Fault("not_found")
            revision = self.queries.revision(revision_id)
            if revision is None:
                _corrupt()
            if revision.get("conversation_id") != diary_id or revision.get("fictional") is not True:
                _corrupt()
            recipe, material = self._bound_capture(actor_id, diary, diary_id)
            if revision.get("material_version") != material:
                _corrupt()
            config_version = self._capture(diary, diary_id, revision_id, revision)
            content = revision.get("content")
            if not isinstance(content, str):
                _corrupt()
            created_at = _stored_time(revision, "created_at")
            captured = dict(
                recipe_id=recipe["id"],
                recipe_version=recipe["version"],
                material_version=material,
                config_version=config_version,
            )
        return self._emit(
            dict(
                schema_version=SCHEMA_VERSION,
                fictional=True,
                actor_id=actor_id,
                diary_id=diary_id,
                diary_version=version,
                revision_id=revision_id,
                content=content,
                created_at=created_at,
                captured=captured,
            ),
            REVISION_BUDGET,
        )

    def _capture(self, diary, diary_id, revision_id, revision):
        """Walk to the nearest gateway capture along the published parent chain.

        A stored revision carries its material hash but not the writing configuration version
        it was generated under; that version survives only in the route receipt of the
        gateway-sourced ancestor, so the chain is followed - bounded, acyclic, and strictly
        inside this diary - until that receipt is found. A human revision inherits whatever
        its parent proved; the editor and reason are internal review facts and are never part
        of the answer. Each step is one primary-key read, so the whole walk is at most
        `PARENT_LIMIT` reads and anything longer is a budget refusal.
        """
        material = diary["material_version"]
        seen = {revision_id}
        node = revision
        for _ in range(PARENT_LIMIT):
            if node.get("conversation_id") != diary_id or node.get("material_version") != material:
                _corrupt()
            source = node.get("source")
            if not isinstance(source, dict):
                _corrupt()
            kind = source.get("kind")
            if kind == "gateway":
                return self._receipt_version(diary, source.get("receipt"))
            if kind != "human":
                _corrupt()
            parent = node.get("parent")
            if not isinstance(parent, str) or not parent:
                _corrupt()
            if parent in seen:
                _corrupt()  # A cycle is corrupt history, not a chain that is merely long.
            seen.add(parent)
            node = self.queries.revision(parent)
            if node is None:
                _corrupt()
        raise Fault("budget_exceeded")

    def _receipt_version(self, diary, receipt):
        """The one number this read takes from a gateway receipt, after checking all of it.

        The receipt is validated against the published model contract exactly as the gateway
        client validates it, and the request id it carries is deliberately not compared with
        anything: it is a random model id, not the diary id. What is compared is the
        configuration version against the diary row, because a retry rewrites that row while
        the published revision keeps the configuration it was really generated under - a
        disagreement is corrupt evidence and is refused rather than answered with the newer
        draft's configuration. The rest of the receipt stays internal.
        """
        try:
            self.contracts.check("model#route_receipt", receipt)
        except Fault:
            _corrupt()
        if (
            receipt.get("caller_service") != "companion"
            or receipt.get("outcome") != "succeeded"
            or type(receipt.get("config_version")) is not int
            or receipt["config_version"] < 1
        ):
            _corrupt()
        if receipt["config_version"] != diary.get("config_version"):
            _corrupt()
        return receipt["config_version"]
