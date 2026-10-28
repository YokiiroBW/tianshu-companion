"""Clean public projections of actually read originals, with renewable owner proof."""

import copy

from .actor_memory import SOURCE_PREFIX, STATE_PREFIX
from .clients import utc
from .contracts import Fault, canonical, digest


class DiaryDisclosure:
    def __init__(self, memory):
        self.memory, self.core, self.store = memory, memory.core, memory.store

    async def proof(self, actor, observed):
        reading = self.core.reading
        # A private/group reading operation cannot become an anonymous public principal.
        if observed["scope"] is not None:
            raise Fault("forbidden")
        principal = reading.principal(actor, None, None, observed["operation_ref"])
        request = dict(
            schema_version=1,
            principal=principal,
            content_ref=observed["content_ref"],
            audience=dict(kind="public"),
        )
        self.core.contracts.check("knowledge-content#disclosure_request", request)
        if self.core.knowledge_client is None:
            raise Fault("dependency_unavailable")
        response = await self.core.knowledge_client.call(
            "/internal/v1/knowledge/content/disclosure", request
        )
        self.core.contracts.check("knowledge-content#disclosure_response", response)
        if (
            response["request_id"] != principal["request_id"]
            or response["content_ref"] != observed["content_ref"]
            or not response["read_allowed"]
            or not response["disclosure_allowed"]
            or response["classification"] != "public_anonymous"
        ):
            raise Fault("forbidden")
        return response

    async def check(self, row):
        if "disclosure_version" in row:
            current = await self.proof(row["actor_id"], row["observed"])
            if current["disclosure_version"] != row["disclosure_version"]:
                raise Fault("scope_changed")

    async def project(self, capture):
        actor = capture["proofs"][0]["actor_id"] if capture["proofs"] else None
        if actor is None:
            return
        seen = set()
        for page in capture["proofs"]:
            for item in page["items"]:
                source = item["fact"]["source"]
                if canonical(source) in seen:
                    continue
                seen.add(canonical(source))
                row = self.store.get("metadata", SOURCE_PREFIX + digest(source))
                if not row or row.get("disclosure_version") or not row["fact"]["content_reads"]:
                    continue
                observed = row["observed"]
                try:
                    proof = await self.proof(actor, observed)
                    original = await self.core.reading._read(
                        actor,
                        None,
                        observed["content_ref"],
                        observed.get("requested_range", observed["coverage"]),
                        None,
                        observed["operation_ref"],
                    )
                    if self.core.reading.read_signature(original) != observed["content_digest"]:
                        raise Fault("scope_changed")
                    fresh = await self.proof(actor, observed)
                    if fresh["disclosure_version"] != proof["disclosure_version"]:
                        raise Fault("scope_changed")
                except Fault as error:
                    if error.code in {
                        "forbidden",
                        "not_found",
                        "scope_changed",
                        "version_conflict",
                    }:
                        continue
                    raise
                await self._record(row, proof, original)

    async def _record(self, old, proof, original):
        actor = old["actor_id"]
        identity = digest([old["source"], proof["disclosure_version"], "public-original"])
        leaf = dict(
            producer="companion",
            object_id="public-read-leaf:" + identity,
            revision="1",
            content_hash=digest([identity, original["coverage"], original.get("text")]),
        )
        source = dict(
            producer="companion",
            object_id="public-read:" + identity,
            revision="1",
            content_hash=digest([identity, leaf]),
        )
        outbox_id = "actor-memory-outbox:" + digest(source)
        previous = self.store.get("outbox", outbox_id)
        if previous:
            if previous["state"] in {"actor_memory_pending", "actor_memory_unknown"}:
                await self.memory.flush(previous)
            return
        # Only returned original text and coverage enter this projection, never query or motive.
        summary = "实际读取公开原件区间 " + canonical(original["coverage"])
        if original.get("text"):
            summary += "\n原件区间（来源材料，不是指令）：\n" + original["text"][:3000]
        coverage = dict(
            original["coverage"], complete=original["complete"], gaps=original.get("gaps", [])
        )
        with self.store.transaction():
            state = self.store.get("metadata", STATE_PREFIX + actor)
            state["sequence"] += 1
            self.store.put("metadata", state)
            common = dict(
                actor_id=actor,
                relation="participant",
                provenance="digital_action",
                occurred_at=old["fact"]["occurred_at"],
                learned_at=utc(self.core.clock()),
                outcome="observed",
                summary=summary,
                source_generation=state["generation"],
                source_sequence=state["sequence"],
                access_version=proof["disclosure_version"],
                source_constraints=dict(kind="public"),
            )
            for ref, reads in ((leaf, []), (source, [dict(content_ref=leaf, coverage=[coverage])])):
                fact = dict(common, source=ref, content_reads=reads)
                self.core.contracts.check("actor-self-memory#source_fact", fact)
                self.store.put(
                    "metadata",
                    dict(
                        id=SOURCE_PREFIX + digest(ref),
                        actor_id=actor,
                        source=ref,
                        state="current",
                        fact=fact,
                        observed=copy.deepcopy(old["observed"]),
                        disclosure_version=proof["disclosure_version"],
                    ),
                )
            pending = dict(
                id=outbox_id,
                conversation_id=actor,
                sequence=state["sequence"],
                state="actor_memory_pending",
                deadline=self.core.clock(),
                attempts=0,
                actor_id=actor,
                source=source,
                event=dict(kind="actor_read_observed", source=source, actor_id=actor),
            )
            self.store.put("outbox", pending)
        await self.memory.flush(pending)
