import asyncio

from test_nonebot_sdk import FakeBot, FakePort, group_event
from tianshu_nonebot.runtime import BotRuntime


def test_explicit_retryable_nonexecution_survives_restart_and_replays_identical_event(tmp_path):
    class Busy(FakePort):
        async def event(self, body):
            self.events.append(body)
            return (
                dict(event_id=body["event_id"], state="not_started", retryable=True)
                if len(self.events) == 1
                else dict(event_id=body["event_id"], state="accepted")
            )

    async def run():
        port, bot = Busy(), FakeBot()
        options = dict(
            connection_id="conn",
            platform_id="host",
            bot_self_id="42",
            allowed_conversations=["group:99"],
            journal_path=tmp_path / "journal.db",
            platform_url="http://127.0.0.1:8000",
            token="synthetic",
            get_bot=lambda _: bot,
            port=port,
        )
        runtime = BotRuntime(**options)
        await runtime.capture(bot, group_event())
        await runtime.flush_events()
        assert len(runtime.journal.pending_events()) == 1
        await runtime.close()
        runtime = BotRuntime(**options)
        await runtime.flush_events()
        assert not runtime.journal.pending_events()
        assert port.events[0] == port.events[1]
        assert not bot.calls
        await runtime.close()

    asyncio.run(run())
