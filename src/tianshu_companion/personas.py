"""Registered character personas: drafts, immutable revisions, explicit publication.

The companion already deploys a `roles` mapping and pins one role copy while a turn is
being prepared. What was missing is authorship of that mapping: there is no way to edit a
persona, no immutable history, and no explicit approval step before a revision may reach a
model. This module adds exactly that, on the existing fields, so no second character store
is created and no other subsystem has to change.

Rules that hold everywhere in here:

- A revision is immutable. Content is written once under a content-addressed id and the
  row is never updated afterwards; publishing, rejecting and rolling back only move
  pointers and append history rows of their own.
- A revision reaches a model only after an explicit approval by a named operator. Nothing
  in the chat path, the model output, a source document, or a persona field can approve.
- Persona content is character text. It is never an authority: it cannot change role
  permissions, source registration, model binding, send eligibility or Memory relations,
  and it is read by `Core` only as the system prompt of a conversation turn.
- Rolling back publishes a *new* revision carrying an earlier revision's content, so the
  history stays append-only and an old approval can never be revived as current.
"""

import json
import re

from .contracts import canonical, digest

REVISION_SUFFIX = "persona-revision/v1"
REVISION_SCALARS = (str, int, float, bool, type(None))
REVISION_TEXT_FIELDS = {"persona": 20000, "tone": 20000, "style": 20000, "address": 4000}
REVISION_MAX_FIELDS = 16
IDENTITY_MAX = 128
MAX_REVISIONS_PER_ACTOR = 512
MAX_PERSONAS = 256

# Explicit operator identities only. A persona field, a chat message, a recalled memory or a
# model answer is never an operator identity, and only these two entry points may call
# `draft`/`approve`/`reject`/`publish`/`rollback`/`retire`:
#   * the local maintenance CLI (`persona_cli`), which takes the SQLite owner lock and
#     therefore cannot run while a service is serving the same database;
#   * the in-process/HTTP management port, which in deployment is reachable only with the
#     dedicated management credential (`Core`/`persona_cli --url`).
IDENTITY = re.compile(r"[^\x00-\x1f\x7f]{1,%d}\Z" % IDENTITY_MAX)
ACTOR = re.compile(r"[^\x00-\x1f\x7f]{1,128}\Z")

# Adapter-facing domain errors. `invalid_input` means the request was refused before any
# state changed; `version_conflict` means the caller's `expected` version is stale. Both are
# transport-neutral: an adapter (HTTP route, local CLI) maps them. This module never speaks
# HTTP, a status code or a CLI exit code.
INVALID = "invalid_input"
CONFLICT = "version_conflict"
MISSING = "not_found"


class PersonaError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


def _invalid(message):
    raise PersonaError(INVALID, message)


def _conflict(message="Stale version"):
    raise PersonaError(CONFLICT, message)


def identity(value):
    """A bounded printable operator identity; never parsed for privileges."""
    if not isinstance(value, str) or not IDENTITY.match(value) or not value.strip():
        _invalid("Operator identity required")
    return value


def actor_id(value):
    if not isinstance(value, str) or not ACTOR.match(value) or not value.strip():
        _invalid("Invalid actor id")
    return value


def _reason(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 2000:
        _invalid("A bounded reason is required")
    return value


def _expected(value):
    if type(value) is not int or value < 1:
        _invalid("Expected positive version required")
    return value


def _required(request, key):
    if key not in request:
        _invalid("Missing " + key)
    return request[key]


def normalize(content):
    """One bounded, canonical persona document. Only scalars, never nested authority."""
    if not isinstance(content, dict) or not content:
        _invalid("Persona content must be a non-empty object")
    if len(content) > REVISION_MAX_FIELDS:
        _invalid("Too many persona fields")
    normalized = {}
    for key, value in content.items():
        if not isinstance(key, str) or not key or len(key) > 64:
            _invalid("Invalid persona field name")
        if isinstance(value, str):
            limit = REVISION_TEXT_FIELDS.get(key, 4000)
            if not value.strip() or len(value) > limit:
                _invalid("Invalid persona text")
        elif not isinstance(value, REVISION_SCALARS):
            _invalid("Persona fields must be scalars")
        elif isinstance(value, float) and value != value:
            _invalid("Non-finite persona value")
        normalized[key] = value
    if not isinstance(normalized.get("persona"), str):
        _invalid("A persona string is required")
    return {k: normalized[k] for k in sorted(normalized)}


def fingerprint(content):
    return digest(dict(suffix=REVISION_SUFFIX, content=content))


def _revision_id(subject, content, parent):
    # A revision is identified by its whole provenance, not only its bytes: replaying old
    # content through a rollback is a new revision that supersedes the live one, so the
    # earlier revision keeps its own id and the chain stays linear and traceable.
    return digest(dict(suffix=REVISION_SUFFIX, subject=subject, content=content, parent=parent))


def _publication_id(subject, revision, generation):
    return digest(dict(suffix=REVISION_SUFFIX, subject=subject, revision=revision, seq=generation))


def resolve_seed(actor, entry):
    """One `roles` deployment entry -> normalized persona content.

    `{"persona": "..."}` and the current `{"version": n, "persona": "..."}` shape both
    resolve; anything else is refused instead of guessed.
    """
    actor_id(actor)
    if isinstance(entry, str):
        entry = dict(persona=entry)
    if not isinstance(entry, dict):
        _invalid("Invalid personas deployment entry")
    declared = entry.get("version")
    if declared is not None and (type(declared) is not int or declared < 1):
        _invalid("Invalid personas deployment version")
    content = {k: v for k, v in entry.items() if k != "version"}
    return declared, normalize(content)


def deployment(config):
    """Deployment configuration -> (source ref, `roles` mapping).

    One rule, one owner: the CLI's `import`/`draft --from-config` and the service's startup
    import all read the deployment shape through this function, so the two entry points can
    never disagree about where a persona is declared.
    """
    if not isinstance(config, dict):
        _invalid("Invalid deployment configuration")
    registered = config.get("personas")
    if isinstance(registered, dict) and isinstance(registered.get("roles"), dict):
        return config.get("config_version"), registered["roles"]
    roles = config.get("roles")
    if not isinstance(roles, dict):
        _invalid("Deployment configuration has no roles mapping")
    return config.get("config_version"), roles


class Personas:
    """Draft/revision/publication store. The host authenticates the operator first.

    No method here accepts a permission, a role claim or a model statement, and none of
    them is read from persona content.
    """

    def __init__(self, store, clock):
        self.store, self.clock = store, clock

    # ------------------------------------------------------------------ internals

    def _get(self, table, key):
        item = self.store.get("persona_" + table, key)
        if item is None:
            raise PersonaError(MISSING, f"Unknown {table}")
        return item

    def _persona(self, subject):
        row = self.store.get("persona_personas", subject)
        if row is None:
            raise PersonaError(MISSING, "Character is not registered")
        return row

    def _revision(self, revision_id):
        row = self.store.get("persona_revisions", revision_id)
        if row is None:
            raise PersonaError(MISSING, "Unknown revision")
        return row

    def _revision_for(self, subject, revision_id):
        row = self._revision(revision_id)
        if row["subject"] != subject:
            # Cross-character isolation: another character's history is not readable or
            # publishable through this subject, even by its own operator.
            _invalid("Revision belongs to another character")
        return row

    def _save(self, table, item, expected=None):
        old = self.store.get("persona_" + table, item["id"])
        if expected is not None:
            _expected(expected)
            if (old or {}).get("version") != expected:
                _conflict()
        item = dict(item)
        item["version"] = (old or {}).get("version", 0) + 1
        self.store.put("persona_" + table, item)
        return item

    def _write_revision(self, subject, content, source, operator, parent, created_at, note=None):
        """Write-once. An existing row with the same id is verified, never overwritten."""
        revision_id = _revision_id(subject, content, parent)
        existing = self.store.get("persona_revisions", revision_id)
        if existing is not None:
            if (
                existing["fingerprint"] != fingerprint(content)
                or existing["subject"] != subject
                or existing["parent"] != parent
            ):
                _invalid("Revision content is immutable")
            return existing
        return self._save(
            "revisions",
            dict(
                id=revision_id,
                conversation_id=subject,
                sequence=len(self.store.list("persona_revisions", subject)) + 1,
                state="draft",
                subject=subject,
                content=content,
                fingerprint=fingerprint(content),
                source=source,
                operator=operator,
                parent=parent,
                note=note,
                created_at=created_at,
            ),
        )

    def _generation(self, subject):
        return len(self.store.list("persona_publications", subject))

    def _publish(self, subject, revision_id, operator, reason, created_at, kind):
        """Lands one immutable publication row and moves the live pointer in one step."""
        persona = self._persona(subject)
        generation = self._generation(subject) + 1
        row = self._save(
            "publications",
            dict(
                id=_publication_id(subject, revision_id, generation),
                conversation_id=subject,
                sequence=generation,
                state="published",
                subject=subject,
                revision_id=revision_id,
                supersedes=persona["published_revision"],
                operator=operator,
                reason=reason,
                kind=kind,
                created_at=created_at,
            ),
        )
        persona = self._save(
            "personas",
            dict(
                persona,
                state="published",
                published_revision=revision_id,
                published_at=created_at,
                publisher=operator,
                draft_revision=None,
                updated_at=self.clock(),
            ),
            expected=persona["version"],
        )
        return row, persona

    def _seed(self, subject, declared, content, created_at):
        """Seed one character. Returns the persona row this import left behind.

        Three cases, each answered exactly once: a character with no persona row yet is
        registered and published outright; a character that already has a live revision is
        never overwritten (the declared seed is recorded as pending identity only); and a
        character that exists but was never published yet can have its unpublished draft
        moved forward by a new deployment document.
        """
        persona = self.store.get("persona_personas", subject)
        revision = self._write_revision(
            subject, content, "initial_config", "deployment", None, created_at
        )
        if persona is None:
            self._save(
                "personas",
                dict(
                    id=subject,
                    conversation_id=subject,
                    sequence=1,
                    state="published",
                    subject=subject,
                    published_revision=None,
                    published_at=None,
                    publisher=None,
                    draft_revision=None,
                    retired=False,
                    retired_at=None,
                    imported=declared,
                    created_at=created_at,
                    updated_at=created_at,
                ),
            )
            _, published = self._publish(
                subject, revision["id"], "deployment", "initial_config import", created_at, "seed"
            )
            return published
        if persona["published_revision"] is not None:
            # Never overwrite a published version on restart. The declared seed is
            # recorded as pending identity only; an operator must draft and publish it.
            if persona["imported"] != declared:
                return self._save(
                    "personas",
                    dict(persona, imported=declared, updated_at=self.clock()),
                    expected=persona["version"],
                )
            return persona
        current = (
            self.store.get("persona_revisions", persona["draft_revision"])
            if persona["draft_revision"] is not None
            else None
        )
        if current is None or current["fingerprint"] != revision["fingerprint"]:
            persona = self._save(
                "personas",
                dict(persona, draft_revision=revision["id"], imported=declared),
                expected=persona["version"],
            )
        _, persona = self._publish(
            subject, revision["id"], "deployment", "initial_config import", created_at, "seed"
        )
        return persona

    # --------------------------------------------------------------------- entry

    def import_config(self, config, *, created_at=None):
        """Idempotent deployment import from one configuration document.

        `config` is the deployment configuration itself; the shape rule lives in
        `deployment`, so the service startup import and the CLI's `import` read it
        identically. The deployed `config_version` is the only key that may re-import:
        identical entries are a no-op, and a new source may add characters and move an
        unpublished draft forward - it may never replace a published version or approve one.
        """
        source_ref, roles = deployment(config)
        if len(roles) > MAX_PERSONAS:
            _invalid("Invalid personas deployment mapping")
        resolved = {actor: resolve_seed(actor, entry) for actor, entry in roles.items()}
        key = digest(dict(suffix=REVISION_SUFFIX, source=source_ref, roles=resolved))
        now = self.clock() if created_at is None else created_at
        if created_at is not None and (
            isinstance(created_at, bool) or not isinstance(created_at, (int, float))
        ):
            _invalid("Invalid import timestamp")
        if self.store.get("persona_imports", key):
            return dict(
                source_ref=source_ref, imported=[], unchanged=sorted(resolved), skipped=True
            )
        imported = []
        with self.store.transaction():
            for actor in sorted(resolved):
                declared, content = resolved[actor]
                imported.append(self._seed(actor, declared, content, now)["subject"])
            self._save(
                "imports",
                dict(
                    id=key,
                    conversation_id=None,
                    sequence=1,
                    state="applied",
                    source_ref=source_ref,
                    actors=sorted(resolved),
                    created_at=now,
                ),
            )
        return dict(
            source_ref=source_ref,
            imported=imported,
            unchanged=[],
            skipped=False,
        )

    def recover(self):
        """Validate pointers after acquiring the owner lock; invent nothing.

        A persona whose published pointer does not resolve keeps its pointer and is
        reported as needing an explicit repair, never silently reset, cleared or
        re-approved.
        """
        problems = []
        with self.store.transaction():
            for persona in self.store.list("persona_personas"):
                for key in ("published_revision", "draft_revision"):
                    revision_id = persona.get(key)
                    if revision_id is None:
                        continue
                    row = self.store.get("persona_revisions", revision_id)
                    if row is None or row["subject"] != persona["subject"]:
                        problems.append(dict(subject=persona["subject"], pointer=key))
                    elif row["fingerprint"] != fingerprint(row["content"]):
                        problems.append(dict(subject=persona["subject"], pointer=key, corrupt=True))
        return problems

    # ------------------------------------------------------------------- reading

    def get(self, subject):
        actor_id(subject)
        persona = self._persona(subject)
        published = (
            self._revision(persona["published_revision"])
            if persona["published_revision"] is not None
            else None
        )
        draft = (
            self._revision(persona["draft_revision"])
            if persona["draft_revision"] is not None
            else None
        )
        return {
            "subject": persona["subject"],
            "version": persona["version"],
            "state": self._state(persona, published, draft),
            "published_revision": persona["published_revision"],
            "draft_revision": persona["draft_revision"],
            "published": self._view(persona["published_revision"]),
            "published_at": persona["published_at"],
            "publisher": persona["publisher"],
            "draft": self._view(persona["draft_revision"]),
            "pending_approval": bool(
                draft
                and persona["draft_revision"] != persona["published_revision"]
                and self._approval(persona["draft_revision"]) is not None
            ),
            "retired": persona["retired"],
            "retired_at": persona["retired_at"],
            "imported": persona["imported"],
            "updated_at": persona["updated_at"],
        }

    def subjects(self):
        return [row["subject"] for row in self.store.list("persona_personas")]

    def _state(self, persona, published, draft):
        if persona["retired"]:
            return "retired"
        if persona["published_revision"] is None:
            return "unpublished"
        if draft is None or persona["draft_revision"] == persona["published_revision"]:
            return "published"
        if self._approval(draft["id"]) is not None:
            return "approved"
        return "draft"

    def _view(self, revision_id):
        if revision_id is None:
            return None
        row = self.store.get("persona_revisions", revision_id)
        if row is None:
            return None
        approval = self._approval(revision_id)
        return {
            "revision_id": row["id"],
            "fingerprint": row["fingerprint"],
            "content": row["content"],
            "parent": row["parent"],
            "source": row["source"],
            "operator": row["operator"],
            "note": row["note"],
            "created_at": row["created_at"],
            "approved_by": approval["operator"] if approval else None,
            "approved_at": approval["created_at"] if approval else None,
            "approval_reason": approval["reason"] if approval else None,
        }

    def _approval(self, revision_id):
        # Approval rows are owned by the character (`conversation_id`), so the revision is
        # matched on its own field. A later rejection withdraws an earlier approval: the
        # last decision on this revision is the one that stands.
        rows = [
            row for row in self.store.list("persona_approvals") if row["revision_id"] == revision_id
        ]
        return rows[-1] if rows and rows[-1]["decision"] == "approved" else None

    def revisions(self, subject):
        actor_id(subject)
        self._persona(subject)
        return [self._view(row["id"]) for row in self.store.list("persona_revisions", subject)]

    def publications(self, subject):
        actor_id(subject)
        self._persona(subject)
        return self.store.list("persona_publications", subject)

    def rollbacks(self, subject):
        actor_id(subject)
        self._persona(subject)
        return self.store.list("persona_rollbacks", subject)

    def approvals(self, subject):
        actor_id(subject)
        self._persona(subject)
        return [row for row in self.store.list("persona_approvals") if row["subject"] == subject]

    def history(self, subject):
        """Full append-only history plus the live pointer; never rewritten."""
        actor_id(subject)
        persona = self.get(subject)
        return {
            "persona": persona,
            "revisions": self.revisions(subject),
            "publications": self.publications(subject),
            "approvals": self.approvals(subject),
            "rollbacks": self.rollbacks(subject),
        }

    def revision(self, revision_id):
        row = self._revision(revision_id)
        return self._view(row["id"])

    def capabilities(self, subject):
        """Read-only persona view plus the authority facts it deliberately cannot reach."""
        actor_id(subject)
        persona = self.get(subject)
        return {
            "subject": subject,
            "state": persona["state"],
            "published_revision": persona["published"]["revision_id"]
            if persona["published"]
            else None,
            "draft_revision": persona["draft"]["revision_id"] if persona["draft"] else None,
            "retired": persona["retired"],
            "grants_permissions": False,
            "changes_sources": False,
            "changes_model_binding": False,
            "changes_send_eligibility": False,
            "changes_memory_relations": False,
        }

    # ------------------------------------------------------------------ pinning

    def pin(self, subject):
        """The published revision a turn must snapshot, or an explicit refusal.

        A character without a published, non-retired revision cannot prepare a new turn.
        Turns already prepared keep the revision they recorded; nothing here rewrites them.
        """
        actor_id(subject)
        persona = self.store.get("persona_personas", subject)
        if persona is None or persona["published_revision"] is None:
            _invalid("Persona is not published")
        if persona["retired"]:
            _invalid("Persona is retired")
        row = self.store.get("persona_revisions", persona["published_revision"])
        if row is None or row["subject"] != subject:
            _invalid("Persona publication pointer is unresolved")
        if row["fingerprint"] != fingerprint(row["content"]):
            _invalid("Persona revision content does not match its fingerprint")
        content = json.loads(canonical(row["content"]))
        # The snapshot keeps the existing `roles` shape - `persona` is the character text a
        # turn already reads - and adds the version facts around it. There is no second
        # character store and no second field name for the same thing.
        return {
            "actor_id": subject,
            "persona": content["persona"],
            "content": content,
            "revision_id": row["id"],
            "revision_version": row["version"],
            "fingerprint": row["fingerprint"],
            "pinned_at": self.clock(),
        }

    def verify(self, snapshot):
        """Re-check one pinned snapshot against the revision it names.

        `Core` calls this before every model call: a snapshot whose bytes no longer match
        its recorded fingerprint is a corrupt persona and must not reach a model. The
        revision is not required to be the live one any more - an already prepared turn
        keeps the version it pinned, which is the point of pinning it.
        """
        if not isinstance(snapshot, dict):
            _invalid("Invalid persona snapshot")
        row = self.store.get("persona_revisions", snapshot.get("revision_id"))
        if row is None:
            _invalid("Persona snapshot revision is unresolved")
        if row["fingerprint"] != snapshot.get("fingerprint") or row["subject"] != snapshot.get(
            "actor_id"
        ):
            _invalid("Persona snapshot does not match its revision")
        if row["fingerprint"] != fingerprint(row["content"]) or canonical(
            row["content"]
        ) != canonical(snapshot.get("content")):
            _invalid("Persona snapshot content does not match its fingerprint")
        if snapshot.get("persona") != row["content"]["persona"]:
            _invalid("Persona snapshot text does not match its revision")
        return row["id"]

    # --------------------------------------------------------------------- manage

    def manage(self, request):
        """The one application entry point for every persona use case.

        Both adapters - the local CLI and the authenticated management port - submit the
        same operation document here, so no rule is implemented twice and no entry point
        touches the persona tables directly. The host authenticates the caller before this
        runs; this method only sees an already-authorised operator identity.
        """
        if not isinstance(request, dict):
            _invalid("Invalid persona operation")
        operation = request.get("operation")
        if not isinstance(operation, str):
            _invalid("Missing operation")
        if operation == "list":
            subjects = self.subjects()
            return dict(
                schema_version=1,
                operation=operation,
                subjects=subjects,
                personas=[self.get(actor) for actor in subjects],
            )
        if operation == "import":
            if "config" in request:
                result = self.import_config(request["config"])
            else:
                result = self.import_config(
                    dict(
                        config_version=request.get("source_ref"), roles=_required(request, "roles")
                    )
                )
            return dict(schema_version=1, operation=operation, **result)
        subject = actor_id(_required(request, "subject"))
        if operation == "get":
            return dict(schema_version=1, operation=operation, persona=self.get(subject))
        if operation == "history":
            return dict(schema_version=1, operation=operation, **self.history(subject))
        if operation == "capabilities":
            return dict(schema_version=1, operation=operation, **self.capabilities(subject))
        if operation == "draft":
            result = self.draft(
                subject,
                request.get("content"),
                operator=_required(request, "operator"),
                expected=_required(request, "expected"),
                reason=_required(request, "reason"),
                note=request.get("note"),
                from_config=request.get("from_config"),
            )
            return dict(schema_version=1, operation=operation, **result)
        if operation in {"approve", "reject"}:
            method = self.approve if operation == "approve" else self.reject
            persona = method(
                subject,
                _required(request, "revision_id"),
                operator=_required(request, "operator"),
                expected=_required(request, "expected"),
                reason=_required(request, "reason"),
            )
            return dict(schema_version=1, operation=operation, persona=persona)
        if operation in {"publish", "rollback"}:
            method = self.publish if operation == "publish" else self.rollback
            result = method(
                subject,
                _required(request, "revision_id"),
                operator=_required(request, "operator"),
                expected=_required(request, "expected"),
                reason=_required(request, "reason"),
            )
            return dict(schema_version=1, operation=operation, **result)
        if operation in {"retire", "restore"}:
            method = self.retire if operation == "retire" else self.restore
            persona = method(
                subject,
                operator=_required(request, "operator"),
                expected=_required(request, "expected"),
                reason=_required(request, "reason"),
            )
            return dict(schema_version=1, operation=operation, persona=persona)
        _invalid("Unknown persona operation")

    # ------------------------------------------------------------------- writing

    def draft(
        self, subject, content=None, *, operator, expected, reason, note=None, from_config=None
    ):
        """Create a new draft revision. Publication is never implied.

        `content` is the new character document; `from_config` re-reads one deployment
        entry instead (drafting a configuration change). Exactly one of them is given.
        """
        actor_id(subject)
        operator = identity(operator)
        _expected(expected)
        reason = _reason(reason)
        if note is not None:
            note = _reason(note)
        if (content is None) == (from_config is None):
            _invalid("Exactly one persona source is required")
        normalized = (
            normalize(content) if content is not None else resolve_seed(subject, from_config)[1]
        )
        with self.store.transaction():
            persona = self._persona(subject)
            if persona["version"] != expected:
                _conflict()
            if persona["retired"]:
                _invalid("A retired character cannot be drafted; restore it first")
            revision = self._write_revision(
                subject,
                normalized,
                "editor",
                operator,
                persona["published_revision"],
                self.clock(),
                note=note,
            )
            persona = self._save(
                "personas",
                dict(persona, draft_revision=revision["id"]),
                expected=persona["version"],
            )
        return dict(persona=self.get(subject), revision=self._view(revision["id"]))

    def approve(self, subject, revision_id, *, operator, expected, reason):
        """Explicit approval of exactly one revision. Model and chat text never call this."""
        actor_id(subject)
        operator = identity(operator)
        _expected(expected)
        reason = _reason(reason)
        with self.store.transaction():
            persona = self._persona(subject)
            if persona["version"] != expected:
                _conflict()
            revision = self._revision_for(subject, revision_id)
            if (
                persona["draft_revision"] != revision["id"]
                or persona["published_revision"] == revision["id"]
            ):
                _invalid("Only the pending draft of this character can be approved")
            self._save(
                "approvals",
                dict(
                    id=digest(
                        [
                            REVISION_SUFFIX,
                            subject,
                            revision["id"],
                            operator,
                            "approved",
                            persona["version"],
                            self.clock(),
                        ]
                    ),
                    conversation_id=subject,
                    sequence=len(self.store.list("persona_approvals", subject)) + 1,
                    state="approved",
                    subject=subject,
                    revision_id=revision["id"],
                    fingerprint=revision["fingerprint"],
                    operator=operator,
                    decision="approved",
                    reason=reason,
                    created_at=self.clock(),
                ),
            )
        return self.get(subject)

    def reject(self, subject, revision_id, *, operator, expected, reason):
        """Withdraw a draft. History keeps the revision; no approval may survive it."""
        actor_id(subject)
        operator = identity(operator)
        _expected(expected)
        reason = _reason(reason)
        with self.store.transaction():
            persona = self._persona(subject)
            if persona["version"] != expected:
                _conflict()
            revision = self._revision_for(subject, revision_id)
            if persona["published_revision"] == revision["id"]:
                _invalid("A published revision cannot be rejected; roll forward instead")
            if persona["draft_revision"] != revision["id"]:
                _invalid("Only the pending draft of this character can be rejected")
            self._save(
                "approvals",
                dict(
                    id=digest(
                        [
                            REVISION_SUFFIX,
                            subject,
                            revision["id"],
                            operator,
                            "rejected",
                            persona["version"],
                            self.clock(),
                        ]
                    ),
                    conversation_id=subject,
                    sequence=len(self.store.list("persona_approvals", subject)) + 1,
                    state="rejected",
                    subject=subject,
                    revision_id=revision["id"],
                    fingerprint=revision["fingerprint"],
                    operator=operator,
                    decision="rejected",
                    reason=reason,
                    created_at=self.clock(),
                ),
            )
            persona = self._save(
                "personas",
                dict(persona, draft_revision=None),
                expected=persona["version"],
            )
        return self.get(subject)

    def publish(self, subject, revision_id, *, operator, expected, reason):
        """Publish one approved revision. Approval must cover exactly these bytes."""
        actor_id(subject)
        operator = identity(operator)
        _expected(expected)
        reason = _reason(reason)
        with self.store.transaction():
            persona = self._persona(subject)
            if persona["version"] != expected:
                _conflict()
            if persona["retired"]:
                _invalid("A retired character cannot be published")
            revision = self._revision_for(subject, revision_id)
            if revision["fingerprint"] != fingerprint(revision["content"]):
                _invalid("Revision content does not match its fingerprint")
            if persona["published_revision"] == revision["id"]:
                return {
                    "publication": self.publications(subject)[-1],
                    "persona": self.get(subject),
                }
            if persona["draft_revision"] != revision["id"]:
                _invalid("Only the pending draft of this character can be published")
            approval = self._approval(revision["id"])
            if approval is None or approval["fingerprint"] != revision["fingerprint"]:
                _invalid("An explicit approval of this revision is required")
            publication, _ = self._publish(
                subject, revision["id"], operator, reason, self.clock(), "publish"
            )
        return dict(publication=publication, persona=self.get(subject))

    def rollback(self, subject, revision_id, *, operator, expected, reason):
        """Traceable rollback: a new revision replays old content and is published.

        The earlier revision is untouched, so the history stays append-only and the
        approval that authorised the rolled-back revision is not revived by going back.
        """
        actor_id(subject)
        operator = identity(operator)
        _expected(expected)
        reason = _reason(reason)
        with self.store.transaction():
            persona = self._persona(subject)
            if persona["version"] != expected:
                _conflict()
            if persona["retired"]:
                _invalid("A retired character cannot be restored")
            target = self._revision_for(subject, revision_id)
            if persona["published_revision"] == target["id"]:
                _invalid("This revision is already live")
            published = [row for row in self.store.list("persona_publications", subject)]
            if not any(row["revision_id"] == target["id"] for row in published):
                _invalid("Only a previously published revision can be restored")
            created_at = self.clock()
            restored = self._write_revision(
                subject,
                json.loads(canonical(target["content"])),
                "rollback",
                operator,
                persona["published_revision"],
                created_at,
                note=reason,
            )
            self._save(
                "rollbacks",
                dict(
                    id=digest(
                        [REVISION_SUFFIX, subject, target["id"], persona["version"], created_at]
                    ),
                    conversation_id=subject,
                    sequence=len(self.store.list("persona_rollbacks", subject)) + 1,
                    state="applied",
                    subject=subject,
                    restored_revision=restored["id"],
                    target_revision=target["id"],
                    superseded_revision=persona["published_revision"],
                    operator=operator,
                    reason=reason,
                    created_at=created_at,
                ),
            )
            persona = self._save(
                "personas",
                dict(persona, draft_revision=restored["id"]),
                expected=persona["version"],
            )
            self._save(
                "approvals",
                dict(
                    id=digest(
                        [REVISION_SUFFIX, subject, restored["id"], operator, "rollback", created_at]
                    ),
                    conversation_id=subject,
                    sequence=len(self.store.list("persona_approvals", subject)) + 1,
                    state="approved",
                    subject=subject,
                    revision_id=restored["id"],
                    fingerprint=restored["fingerprint"],
                    operator=operator,
                    decision="approved",
                    reason="rollback to " + target["id"],
                    created_at=created_at,
                ),
            )
            publication, _ = self._publish(
                subject, restored["id"], operator, reason, created_at, "rollback"
            )
        return dict(
            publication=publication,
            restored_revision=restored["id"],
            target_revision=target["id"],
            persona=self.get(subject),
        )

    def retire(self, subject, *, operator, expected, reason):
        """Stop this character from preparing new turns. Prepared turns keep their snapshot."""
        actor_id(subject)
        operator = identity(operator)
        _expected(expected)
        reason = _reason(reason)
        with self.store.transaction():
            persona = self._persona(subject)
            if persona["version"] != expected:
                _conflict()
            if persona["retired"]:
                return self.get(subject)
            self._save(
                "personas",
                dict(
                    persona,
                    retired=True,
                    retired_at=self.clock(),
                    retired_by=operator,
                    retire_reason=reason,
                ),
                expected=persona["version"],
            )
        return self.get(subject)

    def restore(self, subject, *, operator, expected, reason):
        """Re-enable a retired character without changing which revision is live."""
        actor_id(subject)
        operator = identity(operator)
        _expected(expected)
        reason = _reason(reason)
        with self.store.transaction():
            persona = self._persona(subject)
            if persona["version"] != expected:
                _conflict()
            if not persona["retired"]:
                _invalid("Character is not retired")
            if persona["published_revision"] is None:
                _invalid("A character without a published revision cannot be restored")
            self._save(
                "personas",
                dict(
                    persona,
                    retired=False,
                    retired_at=None,
                    retired_by=operator,
                    retire_reason=reason,
                ),
                expected=persona["version"],
            )
        return self.get(subject)

    # --------------------------------------------------------------- grant check

    def read_access(self, subject, *, reader):
        """Read-only grant store; it confers no admin right and no persona privilege."""
        actor_id(subject)
        reader = identity(reader)
        return any(
            row["reader"] == reader and row["subject"] == subject
            for row in self.store.list("persona_access")
        )

    def grant(self, subject, readers, *, operator, expected=None):
        actor_id(subject)
        operator = identity(operator)
        self._persona(subject)
        if not isinstance(readers, list) or len(readers) > 64:
            _invalid("Too many readers")
        readers = [identity(reader) for reader in readers]
        old = self.store.get("persona_access", subject)
        if old and expected is None:
            _invalid("Expected version required")
        return self._save(
            "access",
            dict(
                id=subject,
                conversation_id=subject,
                sequence=1,
                state="granted",
                subject=subject,
                readers=sorted(set(readers)),
                operator=operator,
                granted_at=self.clock(),
            ),
            expected,
        )
