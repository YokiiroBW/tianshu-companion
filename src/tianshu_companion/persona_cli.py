"""Local maintenance CLI for registered character personas.

An adapter and nothing else. It parses arguments, opens one target, and submits an
operation document to the single application entry point (`Personas.manage`); every rule -
what a draft is, when a revision may be published, what history must be kept - lives in
`personas`. Nothing here writes a persona table directly, so the CLI and the authenticated
management port cannot drift apart.

Two deployment shapes, one command surface:

* **Offline** (`--database`). This process opens the SQLite file and takes the single owner
  lock before touching anything. A service currently serving that database already holds
  the lock, so the command refuses with an explicit `service_running` result instead of
  writing behind a live process. Recovery guidance lives in `docs/personas.md`.
* **Online** (`--url`). A thin client of the management port on a running Core. It sends
  the dedicated management credential and nothing else; chat, ingest and bridge credentials
  are never accepted there.

Either way the command names an operator and every mutation is explicit. Nothing here can
be triggered by a chat message, a model answer, a source document or a persona field.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from .personas import PersonaError, Personas, deployment
from .store import Store

DEFAULT_TOKEN_ENV = "TIANSHU_PERSONA_ADMIN_TOKEN"
OPERATIONS = (
    "list",
    "get",
    "history",
    "capabilities",
    "draft",
    "approve",
    "reject",
    "publish",
    "rollback",
    "retire",
    "restore",
    "import",
)
MUTATIONS = ("draft", "approve", "reject", "publish", "rollback", "retire", "restore", "import")
VERSIONED = ("draft", "approve", "reject", "publish", "rollback", "retire", "restore")
SUBJECT_OPERATIONS = tuple(op for op in OPERATIONS if op not in ("list", "import"))


def build_parser():
    parser = argparse.ArgumentParser(
        prog="persona_cli",
        description="Manage registered character personas (draft, approve, publish, history).",
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--database", type=Path, help="Offline maintenance on this SQLite file.")
    target.add_argument("--url", help="Base URL of a running Core management port.")
    parser.add_argument("--config", type=Path, help="Deployment configuration JSON for `import`.")
    parser.add_argument("--token-env", default=DEFAULT_TOKEN_ENV)
    parser.add_argument("--subject", help="Character id, for example actor:companion.")
    parser.add_argument("--operator", help="Explicit operator identity; required for every write.")
    parser.add_argument("--reason", help="Bounded reason recorded with the change.")
    parser.add_argument("--expected", type=int, help="Optimistic persona version the writer saw.")
    parser.add_argument("--revision", help="Target revision id.")
    parser.add_argument("--content", type=Path, help="Persona document JSON for `draft`.")
    parser.add_argument(
        "--from-config", action="store_true", help="Draft this character's config entry."
    )
    parser.add_argument("--note", help="Optional editor note stored with the revision.")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("command", choices=OPERATIONS)
    return parser


def operation_document(args):
    """Arguments -> the application operation document, without applying persona rules.

    Only naming happens here: an argument that was not supplied stays absent, so the persona
    module remains the single place that decides whether it was required and why.
    """
    document = {"operation": args.command}
    if args.command in SUBJECT_OPERATIONS:
        document["subject"] = args.subject
    if args.command in MUTATIONS:
        document["operator"] = args.operator
        document["reason"] = args.reason
    if args.command in VERSIONED:
        document["expected"] = args.expected
    if args.command in ("approve", "reject", "publish", "rollback"):
        document["revision_id"] = args.revision
    if args.command == "draft":
        document["note"] = args.note
        if args.content is not None:
            document["content"] = json.loads(args.content.read_text(encoding="utf-8"))
        if args.from_config:
            document["from_config"] = config_entry(args)
    if args.command == "import":
        document["config"] = json.loads(args.config.read_text(encoding="utf-8"))
    return {key: value for key, value in document.items() if value is not None}


def config_entry(args):
    """This character's entry in the deployment document, read through the shared rule."""
    _, roles = deployment(json.loads(args.config.read_text(encoding="utf-8")))
    if args.subject not in roles:
        raise PersonaError("not_found", "Character is not in the deployment configuration")
    return roles[args.subject]


def _request(args, document):
    token = os.environ.get(args.token_env)
    if not token:
        return dict(
            ok=False,
            code="management_credential_missing",
            detail=f"Set {args.token_env}; online persona management is refused without it.",
        )
    request = urllib.request.Request(
        args.url.rstrip("/") + "/internal/v1/persona/manage",
        data=json.dumps(document).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + token},
    )
    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            return dict(ok=True, result=json.load(response))
    except urllib.error.HTTPError as error:
        try:
            body = json.load(error)
        except (ValueError, UnicodeError):
            body = None
        return dict(
            ok=False, code=(body or {}).get("code", "http_error"), status=error.code, detail=body
        )
    except (urllib.error.URLError, OSError, TimeoutError) as error:
        return dict(ok=False, code="management_unreachable", detail=type(error).__name__)


def run(args, *, store=None):
    """One command. `store` is injectable for tests; production opens the file itself."""
    document = operation_document(args)
    if args.command == "import":
        # One shared shape rule for the service startup import and this command.
        source_ref, roles = deployment(document["config"])
        document = dict(
            operation="import",
            config=dict(config_version=source_ref, roles=roles),
        )
    if args.url:
        return _request(args, document)
    owned = store is None
    target = Store(args.database) if owned else store
    try:
        return dict(ok=True, result=Personas(target, time.time).manage(document))
    finally:
        if owned:
            target.close()


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        result = run(args)
    except RuntimeError as error:
        # The single-owner lock is the guard: a live service owns this database.
        result = dict(
            ok=False,
            code="service_running",
            detail=str(error),
            remedy=(
                "Persona maintenance is refused while a service owns this database. Stop the "
                "service (or use --url against its authenticated management port), then retry."
            ),
        )
    except PersonaError as error:
        result = dict(ok=False, code=error.code, detail=error.message)
    except (KeyError, ValueError, OSError, UnicodeError) as error:
        result = dict(ok=False, code="invalid_input", detail=type(error).__name__)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
