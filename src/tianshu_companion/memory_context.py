"""Typed Memory evidence and exact batch checks; no independent memory authority."""

import copy
import hashlib

from .contracts import Fault, PROFILE_DOMAIN, canonical, digest

SELECTION = ["identity", "style", "relationship", "current_items", "evidence"]
FRESH_FIELDS = {"verified_at", "valid_until", "owner_heads", "source_revision"}


def bundle_identity(bundle):
    fresh = FRESH_FIELDS
    if bundle.get("retrieval_mode", "factual") == "context":
        fresh = fresh | {
            "scope_version",
            "association_version",
            "scope_checks",
            "history_snapshot",
            "input_history_snapshot",
        }
    return {key: value for key, value in bundle.items() if key not in fresh}


def group_context_owners(scope, checks):
    """A viewer and exact same-group owners, never a directory or association."""
    if not 1 <= len(checks) <= 17 or checks[0]["scope"] != scope:
        raise Fault("forbidden")
    people = set()
    for check in checks:
        owner = check["scope"]
        person = owner["person_id"]
        if (
            not isinstance(person, str)
            or person in people
            or any(owner[key] != scope[key] for key in ("actor_id", "audience", "conversation_id"))
            or check["association_id"] is not None
            or check["association_version"] != 1
        ):
            raise Fault("forbidden")
        people.add(person)
    return people


def _authorized_owner(scope, owner, allowed, include_associated, *, contextual=False):
    if canonical(owner) not in allowed or owner["actor_id"] != scope["actor_id"]:
        raise Fault("forbidden")
    if scope["audience"] == "group":
        if any(owner[key] != scope[key] for key in ("audience", "conversation_id")):
            raise Fault("forbidden")
    else:
        if not include_associated and owner != scope:
            raise Fault("forbidden")
        if contextual and owner != scope:
            if (
                scope["audience"] != "self_private"
                or owner["audience"] != "self_private"
                or allowed[canonical(owner)]["association_id"] is None
            ):
                raise Fault("forbidden")
        elif owner["person_id"] != scope["person_id"]:
            raise Fault("forbidden")


def query_request(
    header, scope, text, budget, *, queries=None, known_bundle=None, retrieval_mode="factual"
):
    if retrieval_mode not in {"context", "factual"}:
        raise Fault("invalid_input")
    request = dict(
        schema_version=2,
        query=header,
        requested_scope=scope,
        queries=queries
        if queries is not None
        else [
            dict(
                query_id="current",
                text=text,
                kinds=["facts", "episodes"],
                intent="current",
                time_filter=None,
            )
        ],
        selection=SELECTION,
        include_associated=scope["audience"] == "self_private",
        budget=budget,
        limits=dict(facts=8, episodes=8),
        known_bundle=known_bundle,
    )
    if retrieval_mode != "factual":
        request["retrieval_mode"] = retrieval_mode
    return request


def validate_response(request, response, now, epoch):
    scope, bundle, evidence = request["requested_scope"], response["bundle"], response["evidence"]
    if (
        response["request_id"] != request["query"]["request_id"]
        or response["effective_scope"] != scope
    ):
        raise Fault("forbidden")
    if bundle["effective_scope"] != scope or any(
        bundle[key] != request[key]
        for key in ("queries", "selection", "include_associated", "budget", "limits")
    ):
        raise Fault("scope_changed")
    if bundle.get("retrieval_mode", "factual") != request.get("retrieval_mode", "factual"):
        raise Fault("invalid_input")
    if epoch(bundle["valid_until"]) <= now:
        raise Fault("scope_changed")
    ids = [entry["evidence_id"] for entry in evidence]
    query_ids = [entry["query_id"] for entry in request["queries"]]
    if (
        len(set(ids)) != len(ids)
        or len(set(query_ids)) != len(query_ids)
        or bundle["evidence_ids"] != ids
        or bundle["evidence_digest"] != digest(evidence)
        or [row["query_id"] for row in response["query_results"]] != query_ids
        or [row["query_id"] for row in bundle["selection_constraints"]] != query_ids
        or any(not set(row["evidence_ids"]).issubset(ids) for row in response["query_results"])
    ):
        raise Fault("invalid_input")
    size = len(canonical(evidence).encode()) if evidence else 0
    if (
        any(response["budget_used"][key] > request["budget"][key] for key in request["budget"])
        or response["budget_used"]["bytes"] < size
    ):
        raise Fault("budget_exceeded")
    facts = [row for row in evidence if row["kind"] != "episode"]
    episodes = [row for row in evidence if row["kind"] == "episode"]
    if len(facts) > request["limits"]["facts"] or len(episodes) > request["limits"]["episodes"]:
        raise Fault("budget_exceeded")
    allowed = {canonical(row["scope"]): row for row in bundle["scope_checks"]}
    if (
        bundle["scope_checks"][0]["scope"] != scope
        or bundle["scope_checks"][0]["scope_version"] != bundle["scope_version"]
    ):
        raise Fault("scope_changed")
    if scope["audience"] == "group":
        group_context_owners(scope, bundle["scope_checks"])
    sources = {row["dependency_id"]: row for row in bundle["source_dependencies"]}
    if len(sources) != len(bundle["source_dependencies"]):
        raise Fault("invalid_input")
    for row in evidence:
        owner = row["owner_scope"]
        _authorized_owner(
            scope,
            owner,
            allowed,
            request["include_associated"],
            contextual=row["kind"] in {"summary", "portrait"},
        )
        if owner["person_id"] != row["subject_person_id"]:
            raise Fault("forbidden")
    by_record = {row["unit"]["record_id"]: row for row in facts}
    if len(by_record) != len(facts):
        raise Fault("invalid_input")
    covered = set()
    for dependency in bundle["group_dependencies"]:
        group = dependency["group"]
        members = set(group["record_ids"])
        if members != {r["record_id"] for r in dependency["records"]} or not members.issubset(
            by_record
        ):
            raise Fault("invalid_input")
        contextual = all(
            by_record[identity]["kind"] in {"summary", "portrait"} for identity in members
        )
        for key in dependency["source_dependency_ids"]:
            if key not in sources:
                raise Fault("invalid_input")
            owner = sources[key]["owner_scope"]
            if contextual:
                # Contextual records retain every actual author leaf. Their
                # owner is not the author of all the quoted conversation.
                _authorized_owner(
                    scope, owner, allowed, request["include_associated"], contextual=True
                )
            elif owner != dependency["owner_scope"]:
                raise Fault("invalid_input")
        for record in dependency["records"]:
            row = by_record[record["record_id"]]
            unit = row["unit"]
            if scope["audience"] == "group" and (
                unit["visibility"] != "shared_projection"
                or any(source["kind"] != "shareable_projection" for source in unit["sources"])
            ):
                raise Fault("forbidden")
            if (
                unit["record_version"] != record["record_version"]
                or unit["semantic_group_id"] != group["semantic_group_id"]
                or record["group_id"] != group["semantic_group_id"]
                or row["owner_scope"] != dependency["owner_scope"]
                or unit["subject_person_id"] != row["subject_person_id"]
                or unit["sources"] != row["source_refs"]
            ):
                raise Fault("invalid_input")
        covered.update(members)
    if covered != set(by_record):
        raise Fault("invalid_input")
    group_ids = {
        dependency["group"]["semantic_group_id"] for dependency in bundle["group_dependencies"]
    }
    if len(group_ids) != len(bundle["group_dependencies"]):
        raise Fault("invalid_input")
    for result, constraint in zip(response["query_results"], bundle["selection_constraints"]):
        if result["status"] != constraint["result_status"]:
            raise Fault("invalid_input")
        if request.get("retrieval_mode") == "context" and (
            result["status"] != "incomplete"
            or "context_only" not in response["omissions"]
            or "context_only" not in result["omissions"]
            or result["coverage"]["candidate_complete"]
            or constraint["candidate_domain_complete"]
            or constraint["proof_kinds"] != ["positive_sources"]
            or constraint["owner_resolution"] != dict(mode="unresolved", targets=[])
        ):
            raise Fault("invalid_input")
        if result["status"] == "no_match" and (
            result["evidence_ids"]
            or "no_match" not in constraint["proof_kinds"]
            or not constraint["candidate_domain_complete"]
            or not result["coverage"]["candidate_complete"]
            or not result["coverage"]["index_complete"]
        ):
            raise Fault("invalid_input")
        targets = constraint["owner_resolution"]["targets"]
        for target in targets:
            if not set(target["witness_group_ids"]).issubset(group_ids):
                raise Fault("invalid_input")
            if target["status"] == "resolved":
                if target["person_id"] is None or (
                    target["kind"] == "self" and target["person_id"] != scope["person_id"]
                ):
                    raise Fault("forbidden")
            elif target["person_id"] is not None or result["status"] not in {
                "incomplete",
                "unresolved",
            }:
                raise Fault("invalid_input")
    episode_versions = {
        (d["episode_id"], d["episode_version"]): d for d in bundle["episode_dependencies"]
    }
    if len(episode_versions) != len(episodes):
        raise Fault("invalid_input")
    for row in episodes:
        dependency = episode_versions.get((row["episode_id"], row["episode_version"]))
        if dependency is None:
            raise Fault("invalid_input")
        source = sources.get(dependency["source_dependency_id"])
        if source is None or source["owner_scope"] != row["owner_scope"]:
            raise Fault("invalid_input")
        span = row["span"]
        if (
            dependency["span"] != span
            or span["end_utf8"] - span["start_utf8"] != len(row["text"].encode())
            or dependency["text_hash"] != hashlib.sha256(row["text"].encode()).hexdigest()
        ):
            raise Fault("invalid_input")
        if row["source_refs"] != [
            dict(
                kind="source_episode",
                owner="memory",
                episode_ref=row["episode_id"],
                episode_version=row["episode_version"],
            )
        ]:
            raise Fault("invalid_input")
    return response


def selection(response):
    """Keep existing record/version consumers on the same canonical returned units."""
    bundle = response["bundle"]
    return dict(
        schema_version=1,
        version_domain="memory-context/v1",
        request_id=response["request_id"],
        effective_scope=response["effective_scope"],
        **{
            key: bundle[key]
            for key in (
                "scope_version",
                "association_version",
                "scope_checks",
                "verified_at",
                "valid_until",
            )
        },
        selected_units=[row["unit"] for row in response["evidence"] if row["kind"] != "episode"],
        dependency_groups=[row["group"] for row in bundle["group_dependencies"]],
        budget_used=response["budget_used"],
        omissions=response["omissions"],
    )


def prompt_material(response):
    """Canonical state supplements, rather than displaces, source episodes/short context."""
    states, episodes = [], []
    for row in response["evidence"]:
        if row["kind"] == "episode":
            episodes.append(
                {
                    key: row[key]
                    for key in (
                        "evidence_id",
                        "subject_person_id",
                        "text",
                        "reality",
                        "observed_at",
                        "event_time",
                        "source_refs",
                    )
                }
            )
        else:
            state = {
                key: row[key]
                for key in (
                    "evidence_id",
                    "kind",
                    "subject_person_id",
                    "observed_at",
                    "valid_from",
                    "valid_to",
                )
            }
            state["record_id"] = row["unit"]["record_id"]
            if row["kind"] in {"summary", "portrait"}:
                state.update(
                    interpretation=row["unit"]["statement"],
                    importance=row["importance"],
                    expires_at=row["expires_at"],
                )
            if "fact" in row:
                state["fact"] = row["fact"]
            states.append(state)
    return dict(memory_state=states, episodes=episodes, memory_coverage=response["query_results"])


def legacy_check(check, header):
    """Serialize the original check semantics; local source/binding checks remain in Core."""
    scope = check["scope"]
    domain = check["version_domain"]
    if domain == "memory-context/v2":
        kind = "context_bundle"
        request = dict(
            query=header, requested_scope=scope, bundle=copy.deepcopy(check["memory_bundle"])
        )
        expected = {}
    elif domain == "text-input/v1" and "input_sources" in check:
        kind = "input_sources"
        request = dict(
            query=header,
            requested_scope=scope,
            source_check=dict(
                schema_version=1,
                request_id=header["request_id"],
                turn_id=check["source_turn_id"],
                input_revision=check["input_revision"],
                scope=scope,
                sources=copy.deepcopy(check["input_sources"]),
            ),
        )
        # This checks current exact sources, not an old room/history version.
        expected = {}
    elif domain == PROFILE_DOMAIN:
        kind = "profile"
        request = dict(
            query=header,
            requester_scope=scope,
            target=check["target"],
            query_text=check["text"],
            selection=check["selection"],
            known_scope_version=check["scope_version"],
            budget=dict(tokens=0, bytes=0),
        )
        expected = dict(
            requester_scope=scope, target=check["target"], scope_version=check["scope_version"]
        )
    elif domain == "text-input/v1" or (
        domain == "text-dialogue/v1" and check.get("history_snapshot") is not None
    ):
        kind = "input_history" if domain == "text-input/v1" else "history"
        request = dict(
            query=header,
            requested_scope=scope,
            include_associated=scope["audience"] == "self_private",
            expected_snapshot=None,
            known_snapshot=check["history_snapshot"],
        )
        expected = dict(effective_scope=scope, snapshot=check["history_snapshot"])
    elif domain == "text-dialogue/v1":
        kind = "select"
        known = check.get("memory_snapshot") or {}
        request = dict(
            query=header,
            requested_scope=scope,
            query_text="context validity",
            selection=SELECTION,
            known_scope_version=check["scope_version"],
            budget=dict(tokens=0, bytes=0),
            include_associated=scope["audience"] == "self_private",
            time_range=None,
            limit=32,
            known_association_version=known.get("association_version"),
            known_scope_checks=known.get("scope_checks"),
        )
        expected = dict(
            effective_scope=scope,
            scope_version=check["scope_version"],
            association_version=known.get("association_version"),
            scope_checks=known.get("scope_checks"),
        )
    else:
        return None
    return dict(check_id="check:" + digest(check), kind=kind, request=request, expected=expected)
