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
- **A write is identified by its operation, not only by its bytes.** Every write carries a
  request id, and the whole execution - the business writes and the result - is committed
  in one transaction under that identity. Replaying the same request returns the recorded
  result without touching anything; reusing the same request id for a different request is
  refused. Content addressing deduplicates revisions; it does not deduplicate operations.
- **Reading a character is bounded and separate from writing it.** Browsing (catalog and one
  history kind at a time), reading one immutable revision and comparing two of them are read
  operations: they carry no request id, write no fact and no ledger row, and move no pointer.
  They page with SQL `LIMIT` instead of holding a whole history in memory, and their cursors,
  byte budgets and comparison rules live in `persona_queries`, which knows no table.
"""

import json
import re
import uuid

from .contracts import canonical, digest
from .persona_queries import (
    CURSOR_RESERVE,
    DOCUMENT_MAX_BYTES,
    HISTORY_KINDS,
    PAGE_MAX_BYTES,
    QueryError,
    comparison,
    fit,
    issue_cursor,
    new_secret,
    page_limit,
    read_cursor,
    within_budget,
)

REVISION_SUFFIX = "persona-revision/v1"
OPERATION_SUFFIX = "persona-operation/v1"
REVISION_SCALARS = (str, int, float, bool, type(None))
REVISION_TEXT_FIELDS = {"persona": 20000, "tone": 20000, "style": 20000, "address": 4000}
REVISION_MAX_FIELDS = 16
IDENTITY_MAX = 128
MAX_REVISIONS_PER_ACTOR = 512
MAX_PERSONAS = 256
# The result document a replayed write returns. Bounded like every other stored field.
MAX_RESULT_BYTES = 65536
REQUEST_MAX = 128

# The fields that identify a request. Anything a caller could change and still expect the
# same operation - content, revision, expected version, operator, reason, note, the
# deployment entry being drafted from - belongs here; nothing volatile does.
REQUEST_FIELDS = (
    "operation",
    "subject",
    "content",
    "from_config",
    "revision_id",
    "operator",
    "expected",
    "reason",
    "note",
    "profile_id",
    "name",
    "description",
    "target",
    "target_expected",
    "source",
    "source_expected",
)

PROFILE_PREFIX = "persona-profile:"
PROFILE_NAME_LIMIT = 80
PROFILE_DESCRIPTION_LIMIT = 400


def _profile_text(value, limit, required=False):
    if not isinstance(value, str) or len(value) > limit or any(ord(c) < 32 for c in value):
        _invalid("Invalid profile metadata")
    if required and not value.strip():
        _invalid("Profile name required")
    return value.strip()


# One history, one table. The kind vocabulary is `persona_queries`'; the table names are the
# domain's, so no adapter can name a table and no pure rule has to know one.
HISTORY_TABLES = {
    "revisions": "persona_revisions",
    "publications": "persona_publications",
    "approvals": "persona_approvals",
    "rollbacks": "persona_rollbacks",
}

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


def _record_key(value):
    """One `(position, id)` continuation key, exactly the shape a page cursor hands back."""
    if (
        not isinstance(value, list)
        or len(value) != 2
        or isinstance(value[0], bool)
        or not isinstance(value[0], int)
        or not isinstance(value[1], str)
        or not value[1]
    ):
        _invalid("Malformed cursor")
    return value


def request_identity(value):
    """A bounded request id. It names one operation, and it is not a privilege."""
    if not isinstance(value, str) or not value.strip() or len(value) > REQUEST_MAX:
        _invalid("A bounded request_id is required for every write")
    return value


def operation_key(scope, request_id, operation):
    """Identity of one write: authorization scope + request id + operation.

    The scope keeps two differently authenticated callers apart, so a request id minted on
    one surface can never replay or block an operation on another.
    """
    return digest(
        dict(suffix=OPERATION_SUFFIX, scope=scope, request=request_id, operation=operation)
    )


def operation_digest(operation, document):
    """The normalized request this identity is bound to.

    Same key with a different digest is a different request reusing one identity, and that
    is refused rather than silently applied. `canonical` makes the comparison independent of
    key order, so an identical request always digests identically.
    """
    payload = {key: document[key] for key in REQUEST_FIELDS if key in document}
    payload["operation"] = operation
    return digest(dict(suffix=OPERATION_SUFFIX, request=payload))


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
    if actor.startswith(PROFILE_PREFIX):
        _invalid("A deployment role cannot use the profile namespace")
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
        # One process-local cursor signing key. It is never persisted, so cursors handed out
        # by an earlier process are refused rather than silently reinterpreted.
        self.cursor_secret = new_secret()

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

    # ------------------------------------------------------- operation identities

    def _stored_operation(self, key):
        return self.store.get("persona_operations", key)

    def _record_operation(self, key, scope, request_id, operation, request_digest, result):
        """Append one operation outcome. Called inside the write's own transaction."""
        document = canonical(result)
        if len(document.encode("utf-8")) > MAX_RESULT_BYTES:
            _invalid("Operation result is too large to record")
        return self._save(
            "operations",
            dict(
                id=key,
                conversation_id=None,
                sequence=1,
                state="applied",
                scope=scope,
                request_id=request_id,
                operation=operation,
                request_digest=request_digest,
                result=json.loads(document),
                applied_at=self.clock(),
            ),
        )

    def _perform(self, operation, document, execute):
        """Run one write under its operation identity, atomically with its result.

        The ledger row and the business writes share one transaction, so a response that is
        lost in transit can be retried: the retry finds the recorded outcome and returns it
        instead of running the write a second time. A different request reusing the same
        identity is refused; a *different* request with a stale `expected` version still
        fails with `version_conflict`, because that check is not replaced by this one.
        """
        request_id = request_identity(_required(document, "request_id"))
        scope = document.get("scope") or "local"
        if not isinstance(scope, str) or not scope.strip() or len(scope) > IDENTITY_MAX:
            _invalid("Invalid operation scope")
        key = operation_key(scope, request_id, operation)
        request_digest = operation_digest(operation, document)
        with self.store.transaction():
            stored = self._stored_operation(key)
            if stored is not None:
                if stored["request_digest"] != request_digest:
                    _invalid(
                        "This request_id is already bound to a different request; "
                        "use a new request_id"
                    )
                return json.loads(canonical(stored["result"]))
            result = execute()
            self._record_operation(key, scope, request_id, operation, request_digest, result)
        return result

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
        if persona is not None and persona.get("kind") == "profile":
            _invalid("A profile cannot be imported as a role")
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
        return [
            row["subject"]
            for row in self.store.list("persona_personas")
            if row.get("kind") != "profile"
        ]

    def _state(self, persona, published, draft):
        if persona["retired"]:
            return "retired"
        if persona["published_revision"] is None:
            return "draft" if draft is not None else "unpublished"
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
        row = self._last_decision(revision_id)
        return row if row is not None and row["decision"] == "approved" else None

    def _last_decision(self, revision_id):
        """The last decision on one revision, read through its own index and one row.

        The answer is the same one `_approval` has always given - the last decision in
        `(position, id)` order wins - but it no longer requires the whole decision table to
        be loaded first, so a read stays bounded by the page rather than by the history.
        """
        row = self.store.db.execute(
            "SELECT body FROM persona_approvals WHERE json_extract(body,'$.revision_id')=? "
            "ORDER BY position DESC,id DESC LIMIT 1",
            (revision_id,),
        ).fetchone()
        return json.loads(row[0]) if row else None

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

    # ------------------------------------------------------------- bounded reading

    def _bounded(self, call, *arguments):
        """Run one bounded read, mapping a pure rule's refusal into the persona vocabulary."""
        try:
            return call(*arguments)
        except QueryError as error:
            raise PersonaError(error.code, error.message) from None

    def _revision_row(self, revision_id):
        return None if revision_id is None else self.store.get("persona_revisions", revision_id)

    def _location(self, persona):
        """Where a character's pointers stand, as one small document every read shares.

        Full persona text is deliberately absent: a list or a page says which revision is
        live, and the caller reads the text through the single-revision read.
        """
        return dict(
            persona_version=persona["version"],
            state=self._state(
                persona,
                self._revision_row(persona["published_revision"]),
                self._revision_row(persona["draft_revision"]),
            ),
            published_revision=persona["published_revision"],
            draft_revision=persona["draft_revision"],
        )

    def _page_rows(self, table, subject, after, limit):
        """One keyset page straight from SQL: a range seek, never a scan of the history.

        `LIMIT` alone does not make a read bounded: without a matching index `ORDER BY
        position,id` needs a temporary b-tree, and SQLite then has to visit every record of
        the character before it can answer - a short page would still walk the whole history.
        The query therefore matches `persona_<kind>_page(conversation_id,position,id)` and
        seeks with one row value, so the page starts at the record the cursor ended on:
        `(position,id) > (?,?)` covers both the later-position case and the equal-position
        case with a larger id in a single index range. The plan is
        `SEARCH ... USING INDEX persona_<kind>_page (conversation_id=? AND (position,id)>(?,?))`
        with no temporary b-tree, so a deep page costs the same as the first one.
        """
        sql = "SELECT position,id,body FROM " + table + " WHERE conversation_id=?"
        arguments = [subject]
        if after is not None:
            sql += " AND (position,id)>(?,?)"
            arguments += [after[0], after[1]]
        sql += " ORDER BY position,id LIMIT ?"
        arguments.append(limit)
        return [
            (row[0], row[1], json.loads(row[2])) for row in self.store.db.execute(sql, arguments)
        ]

    def _page(
        self,
        operation,
        entries,
        keys,
        limit,
        *,
        more_probe=False,
        subject=None,
        kind=None,
        version=None,
        consistency=None,
    ):
        """Assemble one bounded page: whole records, an exact continuation cursor.

        The budget covers the finished response, cursor included, so a caller can always
        parse what it received and can always continue at the record the page ended on. A
        page that is short because of the budget still says `has_more`, so nothing is lost
        silently and nothing incomplete is sent.

        A page of one character's history always states the basis it was read at - the
        subject, the kind and the persona version the records were projected against - so a
        caller never has to decode the opaque cursor to learn which version it is looking at,
        and the last page states it just as explicitly as the first one.
        """
        kept, kept_keys = fit(entries, keys, PAGE_MAX_BYTES - CURSOR_RESERVE)
        more = more_probe or len(kept) < len(entries)
        document = dict(schema_version=1, operation=operation)
        if subject is not None:
            document.update(
                subject=subject,
                kind=kind,
                persona_version=version,
                consistency="version_bound",
            )
        elif consistency is not None:
            document["consistency"] = consistency
        document.update(
            limit=limit,
            count=len(kept),
            entries=kept,
            has_more=more,
            next_cursor=(
                issue_cursor(
                    self.cursor_secret,
                    dict(
                        operation=operation,
                        subject=subject,
                        kind=kind,
                        limit=limit,
                        version=version,
                        last_key=kept_keys[-1],
                    ),
                )
                if more
                else None
            ),
        )
        return within_budget(document, PAGE_MAX_BYTES)

    def _catalog_entry(self, persona):
        published = self._revision_row(persona["published_revision"])
        draft = self._revision_row(persona["draft_revision"])
        return dict(
            subject=persona["subject"],
            state=self._state(persona, published, draft),
            version=persona["version"],
            published_revision=persona["published_revision"],
            draft_revision=persona["draft_revision"],
            retired=persona["retired"],
            updated_at=persona["updated_at"],
        )

    def _read_catalog(self, request):
        """A live keyset directory: characters in ascending id, one bounded page at a time.

        No snapshot is promised across pages - the pointer each entry reports is the one it
        had when that page was read - so a character registered behind the cursor is simply
        not repeated, and one registered ahead of it appears on the next page. Nothing here
        loads the directory into memory: the page is `LIMIT`ed in SQL, ordered by the primary
        key so it is an index seek rather than a sort, and the cursor is the last subject that
        was returned. Inside one page the records and their pointers come from the same short
        read transaction.
        """
        limit = page_limit(request.get("limit"))
        cursor = request.get("cursor")
        after = None
        if cursor is not None:
            binding = read_cursor(self.cursor_secret, cursor, operation="catalog", limit=limit)
            last_key = binding["last_key"]
            if len(last_key) != 1 or not isinstance(last_key[0], str):
                _invalid("Malformed cursor")
            after = last_key[0]
        sql = "SELECT body FROM persona_personas"
        arguments = []
        if after is not None:
            sql += " WHERE id>?"
            arguments.append(after)
        sql += " ORDER BY id LIMIT ?"
        arguments.append(limit + 1)
        with self.store.transaction():
            rows = [json.loads(row[0]) for row in self.store.db.execute(sql, arguments)]
            entries = [self._catalog_entry(row) for row in rows[:limit]]
        return self._page(
            "catalog",
            entries,
            [[row["subject"]] for row in rows[:limit]],
            limit,
            more_probe=len(rows) > limit,
            consistency="live_keyset",
        )

    def _history_entry(self, kind, row):
        if kind == "revisions":
            decision = self._last_decision(row["id"])
            return dict(
                revision_id=row["id"],
                fingerprint=row["fingerprint"],
                parent=row["parent"],
                source=row["source"],
                operator=row["operator"],
                note=row["note"],
                created_at=row["created_at"],
                decision=decision["decision"] if decision else None,
                decided_by=decision["operator"] if decision else None,
                decided_at=decision["created_at"] if decision else None,
            )
        if kind == "publications":
            return dict(
                publication_id=row["id"],
                revision_id=row["revision_id"],
                supersedes=row["supersedes"],
                generation=row["sequence"],
                kind=row["kind"],
                state=row["state"],
                operator=row["operator"],
                reason=row["reason"],
                created_at=row["created_at"],
            )
        if kind == "approvals":
            return dict(
                approval_id=row["id"],
                revision_id=row["revision_id"],
                fingerprint=row["fingerprint"],
                decision=row["decision"],
                state=row["state"],
                operator=row["operator"],
                reason=row["reason"],
                created_at=row["created_at"],
            )
        return dict(
            rollback_id=row["id"],
            restored_revision=row["restored_revision"],
            target_revision=row["target_revision"],
            superseded_revision=row["superseded_revision"],
            state=row["state"],
            operator=row["operator"],
            reason=row["reason"],
            created_at=row["created_at"],
        )

    def _read_history_page(self, subject, request):
        """One history kind of one character, page by page, bound to the version it read.

        The pointer and the page come from the same short transaction, and the cursor carries
        the version they were taken at. Any later draft, approval, rejection, publication,
        rollback, retirement or restore advances that version, so an old cursor is refused
        with `version_conflict` instead of quietly stitching a newer page onto an older
        pointer. The only way forward is to reopen the first page.

        The response metadata, the page records and the revision/approval projection are all
        formed inside that one read transaction; only serialization, the byte budget and the
        cursor encoding happen after it. The page therefore states its own basis - subject,
        kind and `persona_version`, with `consistency="version_bound"` - on every page
        including the last and the empty one, so a caller reads the version it is looking at
        from the response instead of decoding the opaque cursor.
        """
        kind = request.get("kind")
        if kind not in HISTORY_KINDS:
            _invalid("kind must be one of " + ", ".join(HISTORY_KINDS))
        limit = page_limit(request.get("limit"))
        cursor = request.get("cursor")
        with self.store.transaction():
            persona = self._persona(subject)
            version = persona["version"]
            after = None
            if cursor is not None:
                binding = read_cursor(
                    self.cursor_secret,
                    cursor,
                    operation="history_page",
                    subject=subject,
                    kind=kind,
                    limit=limit,
                )
                if binding["version"] != version:
                    _conflict("This history moved since the page was read; reopen the first page")
                after = _record_key(binding["last_key"])
            rows = self._page_rows(HISTORY_TABLES[kind], subject, after, limit + 1)
            entries = [self._history_entry(kind, row) for _, _, row in rows[:limit]]
            keys = [[position, record_id] for position, record_id, _ in rows[:limit]]
        return self._page(
            "history_page",
            entries,
            keys,
            limit,
            more_probe=len(rows) > limit,
            subject=subject,
            kind=kind,
            version=version,
        )

    def _read_revision(self, subject, request):
        """One immutable revision of one character, with where it stands right now.

        Membership is checked before any content is returned, so another character's history
        is not readable through this subject. The pointers and the text come from the same
        read transaction, so the answer never pairs today's pointer with yesterday's bytes.
        """
        revision_id = _required(request, "revision_id")
        if not isinstance(revision_id, str) or not revision_id:
            _invalid("A revision id is required")
        with self.store.transaction():
            row = self._revision_for(subject, revision_id)
            if row["fingerprint"] != fingerprint(row["content"]):
                _invalid("Revision content does not match its fingerprint")
            persona = self._persona(subject)
            document = dict(
                schema_version=1,
                operation="revision",
                subject=subject,
                revision=self._view(row["id"]),
                is_published=persona["published_revision"] == row["id"],
                is_draft=persona["draft_revision"] == row["id"],
                **self._location(persona),
            )
        return within_budget(document, DOCUMENT_MAX_BYTES)

    def _read_compare(self, subject, request):
        """Two immutable revisions of one character, compared field by field.

        The answer is only about the four persona text fields plus a digest-level statement
        about any extension field, so it can never dress an extension change up as persona
        text - nor report four unchanged fields as "the persona is identical". No model is
        called, no quality is scored and nothing is approved: the reviewers' own operations
        stay the only way a revision reaches a model. Both revisions must belong to the
        request's character.
        """
        left_id = _required(request, "left")
        right_id = _required(request, "right")
        for name, value in (("left", left_id), ("right", right_id)):
            if not isinstance(value, str) or not value:
                _invalid("A revision id is required for " + name)
        with self.store.transaction():
            left = self._revision_for(subject, left_id)
            right = self._revision_for(subject, right_id)
            left_fingerprint, right_fingerprint = (
                fingerprint(left["content"]),
                fingerprint(right["content"]),
            )
            if left["fingerprint"] != left_fingerprint or right["fingerprint"] != right_fingerprint:
                _invalid("Revision content does not match its fingerprint")
            persona = self._persona(subject)
            document = dict(
                schema_version=1,
                operation="compare",
                subject=subject,
                left=self._revision_ref(left),
                right=self._revision_ref(right),
                **comparison(
                    left["content"],
                    right["content"],
                    left_id=left["id"],
                    right_id=right["id"],
                    left_fingerprint=left_fingerprint,
                    right_fingerprint=right_fingerprint,
                ),
                **self._location(persona),
            )
        # Over budget the comparison is refused, never shortened: a truncated diff would read
        # as if the missing fields were equal, and both revisions stay readable on their own.
        return within_budget(
            document, DOCUMENT_MAX_BYTES, "Comparison exceeds the response byte budget"
        )

    def _revision_ref(self, row):
        """One revision without its text; the values live in the comparison itself."""
        reference = self._view(row["id"])
        reference.pop("content")
        return reference

    # ------------------------------------------------------------------ pinning

    def pin(self, subject):
        """The published revision a turn must snapshot, or an explicit refusal.

        A character without a published, non-retired revision cannot prepare a new turn.
        Turns already prepared keep the revision they recorded; nothing here rewrites them.
        """
        actor_id(subject)
        persona = self.store.get("persona_personas", subject)
        if persona is not None and persona.get("kind") == "profile":
            _invalid("A reusable profile is not a registered character")
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

    def _profile(self, profile_id):
        if not isinstance(profile_id, str) or not profile_id.startswith(PROFILE_PREFIX):
            _invalid("Invalid profile id")
        row = self._persona(profile_id)
        if row.get("kind") != "profile":
            _invalid("Not a reusable profile")
        return row

    def _author_content(self, fields, base=None):
        if not isinstance(fields, dict) or set(fields) - set(REVISION_TEXT_FIELDS):
            _invalid("Only the four persona text fields may be edited")
        content = dict(base or {})
        for field in REVISION_TEXT_FIELDS:
            if field not in fields:
                continue
            value = fields[field]
            if not isinstance(value, str) or len(value) > REVISION_TEXT_FIELDS[field]:
                _invalid("Invalid persona text")
            if value.strip():
                content[field] = value
            elif field == "persona":
                _invalid("A persona string is required")
            else:
                content.pop(field, None)
        return normalize(content)

    def _author_base(self, row):
        revision_id = row["draft_revision"] or row["published_revision"]
        return self._revision_for(row["subject"], revision_id)["content"] if revision_id else {}

    def _author_summary(self, row):
        return {
            "id": row["subject"],
            "kind": row.get("kind", "role"),
            "name": row.get("name") or row["subject"],
            "description": row.get("description", ""),
            "version": row["version"],
            "state": self._state(
                row,
                self._revision(row["published_revision"]) if row["published_revision"] else None,
                self._revision(row["draft_revision"]) if row["draft_revision"] else None,
            ),
            "published_revision": row["published_revision"],
            "draft_revision": row["draft_revision"],
            "updated_at": row["updated_at"],
            "last_applied_target": row.get("last_applied_target"),
            "last_applied_profile_revision": row.get("last_applied_profile_revision"),
            "last_applied_target_revision": row.get("last_applied_target_revision"),
        }

    def author_catalog(self):
        # The deployment already caps all personas at 256. Profiles use the same rows and
        # revisions, but never become Core roles merely by existing in this directory.
        with self.store.transaction():
            rows = self.store.list("persona_personas")
            profiles = [self._author_summary(row) for row in rows if row.get("kind") == "profile"]
            roles = [self._author_summary(row) for row in rows if row.get("kind") != "profile"]
        return (
            sorted(profiles, key=lambda row: (row["name"].casefold(), row["id"])),
            sorted(roles, key=lambda row: row["id"]),
        )

    def author_view(self, subject):
        row = self._persona(subject)
        summary = self._author_summary(row)
        summary["content"] = self._author_base(row)
        return summary

    def register_runtime_role(self, subject, name, operator):
        """Create an unpublished target for the normal approve/publish apply path.

        The caller must apply a profile in its enclosing transaction before the
        role is usable. A deployment seed would publish without an approval row.
        """
        actor_id(subject)
        identity(operator)
        name = _profile_text(name, PROFILE_NAME_LIMIT, True)
        if self.store.get("persona_personas", subject) is not None:
            _invalid("Role already exists")
        if len(self.store.list("persona_personas")) >= MAX_PERSONAS:
            _invalid("Persona capacity reached")
        now = self.clock()
        return self._save(
            "personas",
            dict(
                id=subject, conversation_id=subject, sequence=1, state="unpublished",
                subject=subject, kind="role", name=name, description="",
                published_revision=None, published_at=None, publisher=None,
                draft_revision=None, retired=False, retired_at=None, imported=None,
                created_at=now, updated_at=now,
            ),
        )

    def create_profile(
        self, *, name, description, content, operator, reason, source=None, source_expected=None
    ):
        identity(operator)
        _reason(reason)
        name = _profile_text(name, PROFILE_NAME_LIMIT, True)
        description = _profile_text(description, PROFILE_DESCRIPTION_LIMIT)
        with self.store.transaction():
            base = None
            if source is not None:
                actor_id(source)
                _expected(source_expected)
                source_row = self._persona(source)
                if source_row["version"] != source_expected:
                    _conflict("Stale copy source")
                base = self._author_base(source_row)
            elif source_expected is not None:
                _invalid("Copy source required")
            normalized = self._author_content(content, base)
            if len(self.store.list("persona_personas")) >= MAX_PERSONAS:
                _invalid("Persona capacity reached")
            subject = PROFILE_PREFIX + uuid.uuid4().hex
            now = self.clock()
            row = self._save(
                "personas",
                dict(
                    id=subject,
                    conversation_id=subject,
                    sequence=1,
                    state="unpublished",
                    subject=subject,
                    kind="profile",
                    name=name,
                    description=description,
                    published_revision=None,
                    published_at=None,
                    publisher=None,
                    draft_revision=None,
                    retired=False,
                    retired_at=None,
                    imported=None,
                    created_at=now,
                    updated_at=now,
                ),
            )
            revision = self._write_revision(subject, normalized, "editor", operator, None, now)
            row = self._save(
                "personas", dict(row, draft_revision=revision["id"]), expected=row["version"]
            )
        return self._author_summary(row)

    def save_profile(self, profile_id, *, name, description, content, operator, expected, reason):
        identity(operator)
        _reason(reason)
        _expected(expected)
        name = _profile_text(name, PROFILE_NAME_LIMIT, True)
        description = _profile_text(description, PROFILE_DESCRIPTION_LIMIT)
        with self.store.transaction():
            row = self._profile(profile_id)
            if row["version"] != expected:
                _conflict()
            normalized = self._author_content(content, self._author_base(row))
            revision = self._write_revision(
                profile_id,
                normalized,
                "editor",
                operator,
                row["draft_revision"] or row["published_revision"],
                self.clock(),
            )
            row = self._save(
                "personas",
                dict(
                    row,
                    name=name,
                    description=description,
                    draft_revision=revision["id"],
                    updated_at=self.clock(),
                ),
                expected=row["version"],
            )
        return self._author_summary(row)

    def save_role(self, subject, *, name, description, content, operator, expected, reason):
        identity(operator)
        _reason(reason)
        _expected(expected)
        name = _profile_text(name, PROFILE_NAME_LIMIT, True)
        description = _profile_text(description, PROFILE_DESCRIPTION_LIMIT)
        with self.store.transaction():
            row = self._persona(subject)
            if row.get("kind") == "profile":
                _invalid("A profile is not a role")
            if row["version"] != expected:
                _conflict()
            if row["retired"]:
                _invalid("A retired role cannot be edited")
            normalized = self._author_content(content, self._author_base(row))
            revision = self._write_revision(
                subject,
                normalized,
                "editor",
                operator,
                row["published_revision"],
                self.clock(),
            )
            row = self._save(
                "personas",
                dict(
                    row,
                    name=name,
                    description=description,
                    draft_revision=revision["id"],
                    updated_at=self.clock(),
                ),
                expected=row["version"],
            )
        return self._author_summary(row)

    def apply_role(self, subject, *, name, description, content, operator, expected, reason):
        # One SQLite transaction contains metadata, immutable draft, explicit approval,
        # publication pointer and the outer operation ledger. No adapter chains requests.
        with self.store.transaction():
            saved = self.save_role(
                subject,
                name=name,
                description=description,
                content=content,
                operator=operator,
                expected=expected,
                reason=reason,
            )
            revision_id = saved["draft_revision"]
            approved = self.approve(
                subject,
                revision_id,
                operator=operator,
                expected=saved["version"],
                reason=reason,
            )
            self.publish(
                subject,
                revision_id,
                operator=operator,
                expected=approved["version"],
                reason=reason,
            )
            return self._author_summary(self._persona(subject))

    def apply_profile(
        self,
        profile_id,
        *,
        target,
        target_expected,
        expected,
        operator,
        reason,
        name=None,
        description=None,
        content=None,
    ):
        identity(operator)
        _reason(reason)
        _expected(expected)
        _expected(target_expected)
        actor_id(target)
        with self.store.transaction():
            profile = self._profile(profile_id)
            if profile["version"] != expected:
                _conflict("Stale profile version")
            if content is not None:
                profile = self._profile(
                    self.save_profile(
                        profile_id,
                        name=name,
                        description=description,
                        content=content,
                        operator=operator,
                        expected=expected,
                        reason=reason,
                    )["id"]
                )
            role = self._persona(target)
            if role.get("kind") == "profile":
                _invalid("A profile cannot be an application target")
            if role["version"] != target_expected:
                _conflict("Stale target role version")
            if role["retired"]:
                _invalid("A retired role cannot be applied")
            source_revision = profile["draft_revision"] or profile["published_revision"]
            source = self._revision_for(profile_id, source_revision)
            # Keep target extensions, then take all four editor fields solely from the profile.
            # An omitted optional field in the profile means cleared, not inherited from role.
            extensions = {
                key: value
                for key, value in self._author_base(role).items()
                if key not in REVISION_TEXT_FIELDS
            }
            normalized = normalize({**extensions, **source["content"]})
            revision = self._write_revision(
                target,
                normalized,
                "profile_apply",
                operator,
                role["published_revision"],
                self.clock(),
                note=profile["name"],
            )
            role = self._save(
                "personas",
                dict(role, draft_revision=revision["id"], updated_at=self.clock()),
                expected=role["version"],
            )
            approved = self.approve(
                target,
                revision["id"],
                operator=operator,
                expected=role["version"],
                reason=reason,
            )
            self.publish(
                target,
                revision["id"],
                operator=operator,
                expected=approved["version"],
                reason=reason,
            )
            profile = self._save(
                "personas",
                dict(
                    profile,
                    last_applied_target=target,
                    last_applied_profile_revision=source_revision,
                    last_applied_target_revision=revision["id"],
                    last_applied_at=self.clock(),
                    updated_at=self.clock(),
                ),
                expected=profile["version"],
            )
        return {
            "profile": self._author_summary(profile),
            "target": self._author_summary(self._persona(target)),
        }

    def manage(self, request):
        """The one application entry point for every persona use case.

        Both adapters - the local CLI and the authenticated management port - submit the
        same operation document here, so no rule is implemented twice and no entry point
        touches the persona tables directly. The host authenticates the caller before this
        runs; this method only sees an already-authorised operator identity.

        A write must carry `request_id`. `scope` optionally names the authorization surface
        that issued it and defaults to the local one; it partitions operation identities and
        is never read as a permission.

        The reads that browse a character (`catalog`/`history_page`/`revision`/`compare`) are
        dispatched here as well, so both adapters get them without a second entry point and
        without either one owning a rule. They carry no `request_id`, because they are not
        writes: they record nothing, move no pointer and are safe to repeat.
        """
        if not isinstance(request, dict):
            _invalid("Invalid persona operation")
        operation = request.get("operation")
        if not isinstance(operation, str):
            _invalid("Missing operation")
        if operation == "author_catalog":
            profiles, roles = self.author_catalog()
            return dict(schema_version=1, operation=operation, profiles=profiles, roles=roles)
        if operation == "author_view":
            subject = actor_id(_required(request, "subject"))
            return dict(schema_version=1, operation=operation, item=self.author_view(subject))
        if operation == "create_profile":
            result = self._perform(
                operation,
                request,
                lambda: self.create_profile(
                    name=_required(request, "name"),
                    description=request.get("description", ""),
                    content=_required(request, "content"),
                    operator=_required(request, "operator"),
                    reason=_required(request, "reason"),
                    source=request.get("source"),
                    source_expected=request.get("source_expected"),
                ),
            )
            return dict(schema_version=1, operation=operation, item=result)
        if operation in {"save_profile", "apply_profile", "save_role", "apply_role"}:
            subject = actor_id(_required(request, "subject"))
            common = dict(
                operator=_required(request, "operator"),
                expected=_required(request, "expected"),
                reason=_required(request, "reason"),
            )
            if operation == "apply_profile":
                result = self._perform(
                    operation,
                    request,
                    lambda: self.apply_profile(
                        subject,
                        target=_required(request, "target"),
                        target_expected=_required(request, "target_expected"),
                        name=request.get("name"),
                        description=request.get("description"),
                        content=request.get("content"),
                        **common,
                    ),
                )
            else:
                result = self._perform(
                    operation,
                    request,
                    lambda: getattr(self, operation)(
                        subject,
                        name=_required(request, "name"),
                        description=request.get("description", ""),
                        content=_required(request, "content"),
                        **common,
                    ),
                )
            return dict(schema_version=1, operation=operation, item=result)
        if operation == "list":
            subjects = self.subjects()
            return dict(
                schema_version=1,
                operation=operation,
                subjects=subjects,
                personas=[self.get(actor) for actor in subjects],
            )
        if operation == "catalog":
            return self._bounded(self._read_catalog, request)
        if operation == "import":
            # The deployment import keeps its own idempotence: the document digest is the
            # key, so replaying one deployment is already a no-op that reports itself as
            # skipped, and a changed document is a new import rather than a reuse.
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
        if operation == "history_page":
            return self._bounded(self._read_history_page, subject, request)
        if operation == "revision":
            return self._bounded(self._read_revision, subject, request)
        if operation == "compare":
            return self._bounded(self._read_compare, subject, request)
        if operation == "get":
            return dict(schema_version=1, operation=operation, persona=self.get(subject))
        if operation == "history":
            return dict(schema_version=1, operation=operation, **self.history(subject))
        if operation == "capabilities":
            return dict(schema_version=1, operation=operation, **self.capabilities(subject))
        if operation == "draft":
            result = self._submit(
                operation,
                request,
                subject,
                dict(
                    content=request.get("content"),
                    operator=_required(request, "operator"),
                    expected=_required(request, "expected"),
                    reason=_required(request, "reason"),
                    note=request.get("note"),
                    from_config=request.get("from_config"),
                ),
            )
            return dict(schema_version=1, operation=operation, **result)
        if operation in {"approve", "reject"}:
            persona = self._submit(
                operation,
                request,
                subject,
                dict(
                    revision_id=_required(request, "revision_id"),
                    operator=_required(request, "operator"),
                    expected=_required(request, "expected"),
                    reason=_required(request, "reason"),
                ),
            )
            return dict(schema_version=1, operation=operation, persona=persona)
        if operation in {"publish", "rollback"}:
            result = self._submit(
                operation,
                request,
                subject,
                dict(
                    revision_id=_required(request, "revision_id"),
                    operator=_required(request, "operator"),
                    expected=_required(request, "expected"),
                    reason=_required(request, "reason"),
                ),
            )
            return dict(schema_version=1, operation=operation, **result)
        if operation in {"retire", "restore"}:
            persona = self._submit(
                operation,
                request,
                subject,
                dict(
                    operator=_required(request, "operator"),
                    expected=_required(request, "expected"),
                    reason=_required(request, "reason"),
                ),
            )
            return dict(schema_version=1, operation=operation, persona=persona)
        _invalid("Unknown persona operation")

    def _submit(self, operation, request, subject, options):
        """One write, wrapped in its operation identity before any rule runs."""
        method = getattr(self, operation)
        return self._perform(operation, request, lambda: method(subject, **options))

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
        """Explicit approval of exactly one revision. Model and chat text never call this.

        Approval is a visible state change, so it advances the persona version like every
        other write: `expected` covers the approval state too, and two operators cannot both
        approve the same pending draft without one of them seeing a stale version. The
        decision row is still append-only, so nothing here rewrites history.
        """
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
            if persona["retired"]:
                _invalid("A retired character cannot be approved; restore it first")
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
            self._save(
                "personas",
                dict(
                    persona,
                    state="approved",
                    approved_at=self.clock(),
                    approved_by=operator,
                    updated_at=self.clock(),
                ),
                expected=persona["version"],
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
