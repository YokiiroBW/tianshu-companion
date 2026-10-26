"""Archive reads bound to the current channel and both real source owners.

Chat owns stored text/currentness; Memory supplies only the known-forget gate.
Neither a successful archive check nor an unmapped import is Core-current proof.
"""

import copy
import asyncio

from .clients import JsonService, uid
from .context import CHAT_HISTORY_DOMAIN, merge_checks, scope_check
from .context_checks import dependency_changed
from .contracts import Fault, canonical


class ChatHistory:
    def __init__(self, core, client=None, mappings=()):
        self.core, self.client = core, client
        self.mappings = {}
        if not isinstance(mappings, (list, tuple)):
            raise ValueError("chat_history_mappings must be an array")
        for row in mappings:
            if (
                not isinstance(row, dict)
                or set(row) != {"channel", "mapping_id"}
                or not isinstance(row["mapping_id"], str)
                or not row["mapping_id"].strip()
                or len(row["mapping_id"]) > 128
            ):
                raise ValueError("Invalid Chat history mapping")
            core.contracts.check("chat-history#channel", row["channel"])
            key = canonical(row["channel"])
            if key in self.mappings:
                raise ValueError("Duplicate Chat history channel")
            self.mappings[key] = row["mapping_id"]
        if client is not None and not all(
            name in core.contracts.schemas for name in ("chat-history", "archive-memory-check")
        ):
            raise ValueError("Chat history requires the frozen archive contracts")

    @property
    def available(self):
        memory = getattr(self.core.memory, "client", None)
        return bool(
            self.client is not None
            and self.client.url
            and self.client.token
            and memory is not None
            and memory.url
            and memory.token
            and self.mappings
        )

    def mapping(self, channel):
        value = self.mappings.get(canonical(channel))
        if not self.available or value is None:
            raise Fault("dependency_unavailable")
        return value

    async def _request(self, operation, body):
        self.core.contracts.check("chat-history#" + operation + "_request", body)
        path = "read-locator" if operation == "read_locator" else operation
        value = await self.client.call("/internal/evidence/" + path, body)
        self.core.contracts.check("chat-history#" + operation + "_response", value)
        if value["channel"] != body["channel"]:
            raise Fault("invalid_input")
        if operation == "search" and (
            value["coverage"]["returned"] != len(value["records"])
            or len(value["records"]) > body["limit"]
        ):
            raise Fault("invalid_input")
        if (
            operation == "read_locator"
            and value["status"] == "archive_current"
            and (
                value["target"]["locator"] != body["locator"]
                or value["target"]["stored_content_sha256"]
                != body["expected_stored_content_sha256"]
                or len(value["context"]["before"]) > body["before"]
                or len(value["context"]["after"]) > body["after"]
            )
        ):
            raise Fault("invalid_input")
        return value

    async def _current(self, scope, origin, channel, mapping_id, records):
        """No text is released until every alias has a corresponding owner verdict."""
        try:
            # All pages share one existing authority-read budget. A record with
            # many aliases cannot mint a fresh full budget for every batch.
            async with asyncio.timeout(JsonService.QUERY_BUDGET_SECONDS):
                return await self._current_once(scope, origin, channel, mapping_id, records)
        except TimeoutError:
            error = Fault("timeout", retryable_read=True)
            error.origin_read_unavailable = True
            raise error from None

    async def _current_once(self, scope, origin, channel, mapping_id, records):
        if self.mapping(channel) != mapping_id:
            raise dependency_changed()
        verdicts = {}
        for start in range(0, len(records), 100):
            rows = records[start : start + 100]
            response = await self._request(
                "check",
                dict(
                    schema_version=1,
                    channel=channel,
                    items=[
                        dict(
                            locator=row["locator"],
                            expected_stored_content_sha256=row["stored_content_sha256"],
                        )
                        for row in rows
                    ],
                ),
            )
            results = response["results"]
            if len(results) != len(rows) or any(
                item["locator"] != row["locator"] for row, item in zip(rows, results)
            ):
                raise Fault("invalid_input")
            for row, item in zip(rows, results):
                verdicts[row["locator"]] = (
                    item["status"] == "archive_current"
                    and item["current_stored_content_sha256"] == row["stored_content_sha256"]
                )
        aliases = [
            dict(
                evidence_id=row["locator"],
                content_hash=row["stored_content_sha256"],
                sender_id=row["sender_id"],
                alias=alias,
            )
            for row in records
            for alias in row["native_aliases"]
        ]
        supported = set()
        withdrawn = set()
        for start in range(0, len(aliases), 100):
            items = aliases[start : start + 100]
            body = dict(
                schema_version=1,
                request_id=uid("archive-check"),
                scope=scope,
                origin=origin,
                mapping_id=mapping_id,
                items=items,
            )
            self.core.contracts.check("archive-memory-check#request", body)
            response = await self.core.memory.client.call(
                "/internal/v1/memory/browser/archive-check", body
            )
            self.core.contracts.check("archive-memory-check#response", response)
            if (
                any(response[key] != body[key] for key in ("request_id", "scope", "mapping_id"))
                or len(response["items"]) != len(items)
                or any(
                    any(result[key] != item[key] for key in ("evidence_id", "content_hash"))
                    for item, result in zip(items, response["items"])
                )
            ):
                raise Fault("invalid_input")
            for item, result in zip(items, response["items"]):
                if result["forget_state"] == "withdrawn":
                    withdrawn.add(item["evidence_id"])
                elif result["forget_state"] == "no_known_forget":
                    supported.add(item["evidence_id"])
        return {
            key for key, current in verdicts.items() if current and key in supported - withdrawn
        }

    async def verify(self, check):
        records = check["archive_records"]
        accepted = await self._current(
            check["scope"], check["origin"], check["channel"], check["mapping_id"], records
        )
        if accepted != {row["locator"] for row in records}:
            raise dependency_changed()

    async def execute(self, turn_id, tool, supplied):
        core = self.core
        turn = core.store.get("turns", turn_id)
        name = tool["function"]["name"]
        core.skills.check_selected(turn, name)
        if not core._turn_allows(turn, "memory.read"):
            raise Fault("forbidden")
        channel = turn["bundle"]["collection_key"]["channel"]
        mapping_id = self.mapping(channel)
        if not isinstance(supplied, dict):
            raise Fault("invalid_input")
        if name == "chat_history_search":
            if set(supplied) - {
                "keyword",
                "sender_id",
                "start_timestamp",
                "end_timestamp",
                "cursor",
                "limit",
            }:
                raise Fault("invalid_input")
            operation = "search"
            body = dict(
                schema_version=1,
                channel=channel,
                filters={
                    key: supplied.get(key)
                    for key in ("keyword", "sender_id", "start_timestamp", "end_timestamp")
                },
                cursor=supplied.get("cursor"),
                limit=supplied.get("limit", 20),
            )
            filters = body["filters"]
            if (
                filters["start_timestamp"] is not None
                and filters["end_timestamp"] is not None
                and filters["start_timestamp"] > filters["end_timestamp"]
            ):
                raise Fault("invalid_input")
        elif name == "chat_history_read":
            if set(supplied) - {"locator", "expected_stored_content_sha256", "before", "after"}:
                raise Fault("invalid_input")
            operation = "read_locator"
            body = dict(schema_version=1, channel=channel, **supplied)
            body.setdefault("before", 2)
            body.setdefault("after", 2)
        else:
            raise Fault("invalid_input")
        core.contracts.check("chat-history#" + operation + "_request", body)
        await core._preflight(turn)
        core.skills.check_selected(core.store.get("turns", turn_id), name)
        response = await self._request(operation, body)
        rows = (
            response.get("records", [])
            if operation == "search"
            else (
                [response["target"], *response["context"]["before"], *response["context"]["after"]]
                if response["status"] == "archive_current"
                else []
            )
        )
        # Overlapping neighbour records remain one exact proof, never conflicting text.
        unique = {}
        for row in rows:
            if row["locator"] in unique and unique[row["locator"]] != row:
                raise Fault("invalid_input")
            unique[row["locator"]] = row
        rows = list(unique.values())
        accepted = await self._current(turn["scope"], turn["origin"], channel, mapping_id, rows)
        await core._preflight(core.store.get("turns", turn_id))
        core.skills.check_selected(core.store.get("turns", turn_id), name)
        kept = [row for row in rows if row["locator"] in accepted]
        if operation == "read_locator" and body["locator"] not in accepted:
            kept = []
        if kept:
            check = scope_check(turn)
            for key in ("scope_version", "history_snapshot", "memory_snapshot"):
                del check[key]
            check.update(
                version_domain=CHAT_HISTORY_DOMAIN,
                mapping_id=mapping_id,
                archive_records=[
                    {
                        key: copy.deepcopy(row[key])
                        for key in (
                            "locator",
                            "stored_content_sha256",
                            "sender_id",
                            "native_aliases",
                        )
                    }
                    for row in kept
                ],
            )
            with core.store.transaction():
                fresh = core.store.get("turns", turn_id)
                core._check_preparation_origin(turn, fresh)
                core.skills.check_selected(fresh, name)
                fresh["context_checks"] = merge_checks(fresh.get("context_checks", []), [check])
                core._save_turn(fresh)
        result = dict(
            state="completed",
            source="chat_archive",
            current_source_state="archive_checked_and_no_known_forget",
            core_source_state="not_attested",
            records=[
                {key: value for key, value in row.items() if key != "native_aliases"}
                for row in kept
            ],
            omitted_records=len(rows) - len(kept),
        )
        if operation == "search":
            result.update(next_cursor=response["next_cursor"], coverage=response["coverage"])
        else:
            result.update(
                target_locator=body["locator"],
                status=response["status"] if kept else "unavailable",
                context_complete=response["context"]["complete"] if kept else False,
            )
        return result, []
