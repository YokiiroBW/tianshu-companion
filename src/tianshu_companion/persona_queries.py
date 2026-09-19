"""Bounded page, cursor and revision-comparison rules for the persona read operations.

Pure rules and nothing else. This module holds no table name, opens no store, writes no SQL
and speaks neither HTTP nor a CLI: it owns the arithmetic that must not be re-decided per
adapter - how large a page may be, what an opaque cursor is bound to, and what "these two
revisions differ" means. `personas` owns the character facts and the queries and calls in
here, so the domain and every adapter share one answer.

Three rules are load-bearing:

- **A page is bounded twice.** By a record count (`limit`, default 20, maximum 100) and by
  the real UTF-8 JSON size of the page. Only whole records are returned and the continuation
  cursor always names the record the page actually ended on, so a client can never mistake a
  shortened page for the end of the history. A record that cannot fit the budget on its own
  is refused instead of trimmed: the client can read it through the single-revision read.
- **A cursor is opaque, verified and bound.** It carries no text and no credential, it is
  signed with a process-local secret so a restart invalidates it, and it is accepted only by
  the same operation, subject, kind and page size that issued it.
- **A comparison states differences, never quality.** Only the four persona text fields are
  interpreted as persona semantics; any other field of the document is reported as present
  and changed, by digest only, so an extension field can never be dressed up as persona text
  and four unchanged fields are never reported as "the persona is identical".
"""

import base64
import hashlib
import hmac
import json
import os

from .contracts import canonical

INVALID = "invalid_input"

# Page size. `None` means "the caller did not ask", which is the documented default; a boolean
# is not a number and is refused like any other out-of-range value.
PAGE_DEFAULT_LIMIT = 20
PAGE_MAX_LIMIT = 100
# The whole page response, in real UTF-8 JSON bytes, cursor included.
PAGE_MAX_BYTES = 256 * 1024
# One single revision or one comparison, in real UTF-8 JSON bytes.
DOCUMENT_MAX_BYTES = 1024 * 1024
# Room reserved inside the page budget for the envelope and for the continuation cursor, so
# the budget can be applied to the records before the cursor exists and still hold for the
# finished response.
CURSOR_RESERVE = 4096
CURSOR_MAX = 2048
CURSOR_FIELDS = ("operation", "subject", "kind", "limit", "version", "last_key")

# One history, one kind at a time. The vocabulary lives here so no adapter invents a fourth.
HISTORY_KINDS = ("revisions", "publications", "approvals", "rollbacks")
# The only fields a comparison interprets as persona text. Everything else in a revision
# document is reported as an extension, never as persona semantics.
COMPARE_FIELDS = ("persona", "tone", "style", "address")


class QueryError(Exception):
    """A transport-neutral refusal from a pure read rule.

    The domain maps it into its own error vocabulary, so this module never has to know how a
    refusal is spelled on the wire.
    """

    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


def invalid(message):
    raise QueryError(INVALID, message)


def page_limit(value):
    """One bounded page size. Absent means the default; a bool is never a page size."""
    if value is None:
        return PAGE_DEFAULT_LIMIT
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > PAGE_MAX_LIMIT:
        invalid("limit must be a whole number between 1 and " + str(PAGE_MAX_LIMIT))
    return value


def byte_size(value):
    """The real UTF-8 JSON size of a document - what the caller actually receives."""
    return len(canonical(value).encode("utf-8"))


def within_budget(document, limit, message="Response exceeds its byte budget"):
    """One bounded document, or an explicit refusal. Never a shortened one."""
    if byte_size(document) > limit:
        invalid(message)
    return document


def fit(entries, keys, budget):
    """Keep whole records, in order, inside one byte budget.

    A record larger than the budget is refused rather than cut: a shortened record would be
    indistinguishable from a complete one, and that is exactly the silent loss this rule
    exists to prevent.
    """
    kept, kept_keys, used = [], [], 0
    for entry, key in zip(entries, keys):
        size = byte_size(entry)
        if size > budget:
            invalid("A single record exceeds the page byte budget")
        if used + size > budget:
            break
        kept.append(entry)
        kept_keys.append(key)
        used += size
    return kept, kept_keys


def new_secret():
    """One process-local cursor signing key. Nothing about it is persisted."""
    return os.urandom(32)


def _b64(raw):
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _unb64(text):
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _tag(secret, payload):
    return _b64(hmac.new(secret, payload.encode("ascii"), hashlib.sha256).digest())


def issue_cursor(secret, binding):
    """Bind one continuation cursor to this request. Opaque, signed, strictly bounded."""
    if set(binding) != set(CURSOR_FIELDS):
        invalid("Malformed cursor binding")
    payload = _b64(canonical(binding).encode("utf-8"))
    token = payload + "." + _tag(secret, payload)
    if len(token) > CURSOR_MAX:
        invalid("Cursor is too large")
    return token


def read_cursor(secret, token, *, operation, limit, subject=None, kind=None):
    """Verify one opaque cursor and prove it belongs to exactly this request.

    The signature makes a forged or truncated token a refusal rather than a guess, and the
    binding makes a cursor unusable on another character, another history kind, another
    operation or another page size. The caller compares the bound version itself, because a
    moved version is a conflict to report, not a malformed cursor.
    """
    if not isinstance(token, str) or not token or len(token) > CURSOR_MAX:
        invalid("A bounded cursor is required")
    payload, dot, tag = token.partition(".")
    if not dot or not payload or not tag:
        invalid("Malformed cursor")
    if not hmac.compare_digest(tag, _tag(secret, payload)):
        invalid("Cursor failed verification")
    try:
        binding = json.loads(_unb64(payload).decode("utf-8"))
    except (ValueError, UnicodeError):
        invalid("Malformed cursor")
    if not isinstance(binding, dict) or set(binding) != set(CURSOR_FIELDS):
        invalid("Malformed cursor")
    for key, wanted in (
        ("operation", operation),
        ("subject", subject),
        ("kind", kind),
        ("limit", limit),
    ):
        if binding.get(key) != wanted:
            invalid("Cursor does not belong to this request")
    last_key = binding["last_key"]
    if not isinstance(last_key, list) or not last_key:
        invalid("Malformed cursor")
    return binding


def comparison(left, right, *, left_id, right_id, left_fingerprint, right_fingerprint):
    """Two immutable revisions of one character, field by field.

    A missing field and an explicit `null` are different facts and are reported as such, and
    the four-field scope is stated in the answer so a caller can never read an extension
    field as persona text. Identity is not inferred from the four fields: `identical_revision`
    follows the revision id and `content_identical` follows the whole-document fingerprint.
    """
    extra_left = {k: v for k, v in left.items() if k not in COMPARE_FIELDS}
    extra_right = {k: v for k, v in right.items() if k not in COMPARE_FIELDS}
    present = bool(extra_left or extra_right)
    return dict(
        comparison_scope=list(COMPARE_FIELDS),
        fields={name: _field(left, right, name) for name in COMPARE_FIELDS},
        additional_fields_present=present,
        additional_fields_changed=present and canonical(extra_left) != canonical(extra_right),
        additional_field_names=sorted(set(extra_left) | set(extra_right)),
        identical_revision=left_id == right_id,
        content_identical=left_fingerprint == right_fingerprint,
    )


def _field(left, right, name):
    left_present, right_present = name in left, name in right
    left_value, right_value = left.get(name), right.get(name)
    if not left_present and not right_present:
        change = "unchanged"
    elif not left_present:
        change = "added"
    elif not right_present:
        change = "removed"
    else:
        change = "unchanged" if canonical(left_value) == canonical(right_value) else "modified"
    return dict(
        presence=dict(left=left_present, right=right_present),
        change=change,
        left=left_value,
        right=right_value,
    )
