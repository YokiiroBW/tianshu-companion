"""Real registered archive tools with synthetic HTTP source owners only."""

import asyncio
import copy
import json

import httpx
import pytest

from support import Harness
from test_delivery_v2 import source_turn
from tianshu_companion.chat_history import ChatHistory
from tianshu_companion.clients import JsonService, utc
from tianshu_companion.context import CHAT_HISTORY_DOMAIN, inherited_checks, model_input_checks
from tianshu_companion.context_checks import VerificationCheckpoint
from tianshu_companion.contracts import Fault, canonical, digest
from tianshu_companion.memory_context import legacy_check


def record(label="one", aliases=1):
    text = "原话🍋：还没约好。" + label
    return dict(
        locator="audit-evidence:0:" + "a" * 64 + ":message_" + label + ":" + digest(text),
        archive_observation_id="audit-observation:" + digest([label, "archive"]),
        stored_content_sha256=digest(text),
        text=text,
        text_representation="stored_text",
        sender_id="actual-author:" + label,
        timestamp=1_790_000_000,
        source_sequence=None,
        external_message_id=label,
        message_id=label,
        media_state="unavailable",
        native_aliases=[
            dict(
                platform="qq",
                account_id="actual-bot-self",
                room_id="private:a",
                message_type="private",
                id_kind="external",
                message_id=f"{label}-{index}",
                import_source_id=None,
            )
            for index in range(aliases)
        ],
    )


class ArchiveOwners:
    def __init__(self, h, rows):
        self.h, self.rows = h, rows
        self.calls, self.gates = [], []
        self.withdrawn, self.unsupported, self.changed = set(), set(), set()
        self.corrupt = None

    def archive(self, request):
        value = json.loads(request.content)
        self.calls.append((request.url.path, copy.deepcopy(value)))
        channel = value["channel"]
        if request.url.path.endswith("/search"):
            response = dict(
                schema_version=1,
                channel=channel,
                status="archive_observed",
                current_source_state="not_checked",
                id_kind="external",
                records=self.rows,
                next_cursor=None,
                coverage=dict(consistency="live_keyset", returned=len(self.rows), has_more=False),
            )
        elif request.url.path.endswith("/read-locator"):
            response = dict(
                schema_version=1,
                channel=channel,
                core_source_state="not_observed",
                id_kind="external",
                status="archive_current",
                target=self.rows[0],
                context=dict(before=self.rows[1:2], after=self.rows[2:3], complete=False),
            )
        else:
            response = dict(
                schema_version=1,
                channel=channel,
                core_source_state="not_observed",
                observed_at=1_790_000_000,
                results=[
                    dict(
                        locator=item["locator"],
                        status="changed" if item["locator"] in self.changed else "archive_current",
                        current_stored_content_sha256=None
                        if item["locator"] in self.changed
                        else item["expected_stored_content_sha256"],
                    )
                    for item in value["items"]
                ],
            )
            if self.corrupt == "chat_order":
                response["results"].reverse()
            elif self.corrupt == "chat_missing":
                response["results"].pop()
        return httpx.Response(200, json=response)

    def memory(self, request):
        assert request.url.path == "/internal/v1/memory/browser/archive-check"
        value = json.loads(request.content)
        self.gates.append(copy.deepcopy(value))
        assert value["scope"]["actor_id"] == "actor:a"
        response = dict(
            schema_version=1,
            request_id=value["request_id"],
            scope=value["scope"],
            mapping_id=value["mapping_id"],
            checked_at=utc(self.h.clock()),
            items=[
                dict(
                    evidence_id=item["evidence_id"],
                    content_hash=item["content_hash"],
                    forget_state="withdrawn"
                    if item["alias"]["message_id"] in self.withdrawn
                    else "unsupported"
                    if item["evidence_id"] in self.unsupported
                    else "no_known_forget",
                    mapping="unmapped",
                    checked_at=utc(self.h.clock()),
                )
                for item in value["items"]
            ],
        )
        if self.corrupt == "gate_order":
            response["items"].reverse()
        elif self.corrupt == "gate_missing":
            response["items"].pop()
        elif self.corrupt == "gate_hash":
            response["items"][0]["content_hash"] = "f" * 64
        return httpx.Response(200, json=response)


async def configured(rows):
    h = Harness(silence_ms=0)
    turn = await source_turn(h)
    owners = ArchiveOwners(h, rows)
    chat = JsonService(
        "https://chat.fixture.invalid", "fixture", transport=httpx.MockTransport(owners.archive)
    )
    memory = JsonService(
        "https://memory.fixture.invalid",
        "fixture",
        transport=httpx.MockTransport(owners.memory),
        memory_dependency=True,
    )
    h.memory.client = memory
    h.core.chat_history = ChatHistory(
        h.core,
        chat,
        [dict(channel=turn["bundle"]["collection_key"]["channel"], mapping_id="trusted-map")],
    )
    h.core.role_actions.tools(turn)
    return h, turn, owners, chat, memory


def tool(name="chat_history_search", **values):
    return dict(
        id="call:archive", type="function", function=dict(name=name, arguments=canonical(values))
    )


async def cleanup(h, chat, memory):
    await h.core.close()
    await chat.close()
    await memory.close()


def test_search_uses_current_channel_actual_author_and_all_aliases_with_negative_priority():
    async def run():
        rows = [record("one", aliases=2), record("two")]
        h, turn, owners, chat, memory = await configured(rows)
        owners.withdrawn.add("one-1")
        try:
            result, attachments = await h.core.role_actions.execute(
                turn["id"], tool(keyword="原话", limit=2)
            )
            assert attachments == []
            assert [row["locator"] for row in result["records"]] == [rows[1]["locator"]]
            assert result["records"][0]["sender_id"] == "actual-author:two"
            assert "native_aliases" not in result["records"][0]
            assert result["omitted_records"] == 1 and result["core_source_state"] == "not_attested"
            assert [item["alias"]["message_id"] for item in owners.gates[0]["items"]] == [
                "one-0",
                "one-1",
                "two-0",
            ]
            assert owners.gates[0]["mapping_id"] == "trusted-map"
            assert owners.gates[0]["origin"] == turn["origin"]
            check = h.core.store.get("turns", turn["id"])["context_checks"][-1]
            assert check["version_domain"] == CHAT_HISTORY_DOMAIN
            assert (
                check["archive_records"][0]["stored_content_sha256"]
                == rows[1]["stored_content_sha256"]
            )
            assert "text" not in check["archive_records"][0]
            assert legacy_check(check, {}) is None  # No invented Memory legacy kind.
        finally:
            await cleanup(h, chat, memory)

    asyncio.run(run())


@pytest.mark.parametrize(
    "change", ["chat_order", "chat_missing", "gate_order", "gate_missing", "gate_hash"]
)
def test_owner_responses_must_correspond_in_full_original_order(change):
    async def run():
        h, turn, owners, chat, memory = await configured([record("one"), record("two")])
        owners.corrupt = change
        try:
            with pytest.raises(Fault, match="invalid_input"):
                await h.core.role_actions.execute(turn["id"], tool())
            assert any(path.endswith("/check") for path, _ in owners.calls)
            assert bool(owners.gates) == change.startswith("gate_")
            assert not h.core.store.get("turns", turn["id"]).get("context_checks")
        finally:
            await cleanup(h, chat, memory)

    asyncio.run(run())


def test_changed_and_unsupported_records_are_not_consumed_and_every_alias_is_batched():
    async def run():
        rows = [record("one", aliases=101), record("two"), record("three")]
        h, turn, owners, chat, memory = await configured(rows)
        owners.unsupported.add(rows[0]["locator"])
        owners.changed.add(rows[1]["locator"])
        try:
            result, _ = await h.core.role_actions.execute(turn["id"], tool())
            assert [row["locator"] for row in result["records"]] == [rows[2]["locator"]]
            assert [len(body["items"]) for body in owners.gates] == [100, 3]
            assert sum(len(body["items"]) for body in owners.gates) == 103
            assert result["omitted_records"] == 2
        finally:
            await cleanup(h, chat, memory)

    asyncio.run(run())


def test_many_alias_batches_share_one_authority_read_budget(monkeypatch):
    async def run():
        h, turn, owners, chat, memory = await configured([record("one", aliases=201)])
        entered = []
        original = owners.memory

        async def slow(request):
            entered.append(request)
            if len(entered) == 2:
                await asyncio.Event().wait()
            return original(request)

        await memory.client.aclose()
        memory.client = httpx.AsyncClient(transport=httpx.MockTransport(slow))
        monkeypatch.setattr(JsonService, "QUERY_BUDGET_SECONDS", 0.02)
        try:
            with pytest.raises(Fault) as caught:
                await h.core.role_actions.execute(turn["id"], tool())
            assert caught.value.code == "timeout" and not caught.value.unknown
            assert len(entered) == 2  # No third batch starts with another full budget.
            assert not h.core.store.get("turns", turn["id"]).get("context_checks")
        finally:
            await cleanup(h, chat, memory)

    asyncio.run(run())


def test_model_cannot_replace_trusted_scope_mapping_or_service_and_read_preserves_neighbours():
    async def run():
        rows = [record("one"), record("two"), record("three")]
        h, turn, owners, chat, memory = await configured(rows)
        try:
            for values in (
                dict(channel={}),
                dict(mapping_id="other"),
                dict(actor_id="actor:b"),
                dict(base_url="https://other.invalid"),
                dict(limit=101),
                dict(start_timestamp=2, end_timestamp=1),
            ):
                with pytest.raises(Fault):
                    await h.core.role_actions.execute(turn["id"], tool(**values))
            assert owners.calls == []
            result, _ = await h.core.role_actions.execute(
                turn["id"],
                tool(
                    "chat_history_read",
                    locator=rows[0]["locator"],
                    expected_stored_content_sha256=rows[0]["stored_content_sha256"],
                    before=1,
                    after=1,
                ),
            )
            assert result["target_locator"] == rows[0]["locator"]
            assert [row["text"] for row in result["records"]] == [row["text"] for row in rows]
            assert result["context_complete"] is False
            original = h.core.store.get("turns", turn["id"])
            changed = copy.deepcopy(original)
            changed["bundle"]["collection_key"]["channel"]["channel_conversation_id"] = "other"
            assert (
                h.core.chat_history.mappings.get(
                    canonical(changed["bundle"]["collection_key"]["channel"])
                )
                is None
            )
        finally:
            await cleanup(h, chat, memory)

    asyncio.run(run())


def test_retained_archive_check_revalidates_both_owners_and_original_authority():
    async def run():
        row = record()
        h, turn, owners, chat, memory = await configured([row])
        try:
            await h.core.role_actions.execute(turn["id"], tool())
            fresh = h.core.store.get("turns", turn["id"])
            check = fresh["context_checks"][-1]
            async with VerificationCheckpoint() as checkpoint:
                await h.core._verify_context_check(fresh, check, checkpoint, memory_checks={})
            assert len(owners.gates) == 2
            owners.withdrawn.add("one-0")
            async with VerificationCheckpoint() as checkpoint:
                with pytest.raises(Fault) as caught:
                    await h.core._verify_context_check(fresh, check, checkpoint, memory_checks={})
            assert caught.value.context_dependency_invalid
            owners.withdrawn.clear()
            owners.changed.add(row["locator"])
            with pytest.raises(Fault):
                await h.core.chat_history.verify(check)
            h.origins.values[turn["origin"]["assertion_ref"]]["revoked"] = True
            async with VerificationCheckpoint() as checkpoint:
                with pytest.raises(Fault):
                    await h.core._verify_context_check(fresh, check, checkpoint)
        finally:
            await cleanup(h, chat, memory)

    asyncio.run(run())


@pytest.mark.parametrize("withdraw_before_second_model", [False, True])
def test_actual_native_dispatch_records_archive_lineage_and_checks_it_before_reuse(
    withdraw_before_second_model,
):
    async def run():
        row = record()
        h, turn, owners, chat, memory = await configured([row])
        calls = []

        class Model:
            async def complete(self, current, messages, *, tools, on_delta):
                calls.append(copy.deepcopy(messages))
                if len(calls) == 1:
                    return dict(role="assistant", content=None, tool_calls=[tool()]), {
                        "fixture": True
                    }
                assert row["text"] in messages[-1]["content"]
                return dict(role="assistant", content="引用原话而非宣布已完成。", tool_calls=[]), {
                    "fixture": True
                }

        original = owners.memory

        def gate(request):
            if withdraw_before_second_model and owners.gates:
                owners.withdrawn.add("one-0")
            return original(request)

        await memory.client.aclose()
        memory.client = httpx.AsyncClient(transport=httpx.MockTransport(gate))
        h.core.gateway = Model()
        try:
            messages = [
                dict(role="system", content="Original instruction"),
                dict(role="user", content="{}"),
            ]
            if withdraw_before_second_model:
                with pytest.raises(Fault):
                    await h.core._respond_native(turn, messages)
                assert len(calls) == 1
            else:
                await h.core._respond_native(turn, messages)
                assert len(calls) == 2
                fresh = h.core.store.get("turns", turn["id"])
                assert any(
                    check["version_domain"] == CHAT_HISTORY_DOMAIN
                    for check in inherited_checks(fresh)
                )
                assert any(
                    check["version_domain"] == CHAT_HISTORY_DOMAIN
                    for check in model_input_checks(fresh)["checks"]
                )
                # Schema6 has no honest Chat proof representation: the optional
                # derived BOT is omitted; independent human inputs remain intact.
                assert (
                    h.core.summary_windows._reply_dependencies(fresh, {fresh["id"]: fresh}) is None
                )
                assert h.core.summary_windows._input_members(fresh, "new")[0][1] == "你好"
                owners.withdrawn.add("one-0")
                with pytest.raises(Fault):
                    await h.core.chat_history.verify(
                        next(
                            check
                            for check in inherited_checks(fresh)
                            if check["version_domain"] == CHAT_HISTORY_DOMAIN
                        )
                    )
        finally:
            await cleanup(h, chat, memory)

    asyncio.run(run())
