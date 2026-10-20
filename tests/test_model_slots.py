import asyncio

import pytest

from tianshu_companion.model_slots import ModelSlots


def test_dialogue_preference_preserves_fifo_and_background_progress():
    async def run():
        slots = ModelSlots(1)
        await slots.acquire()
        order = []

        async def job(label, pool):
            async with pool:
                order.append(label)
                await asyncio.sleep(0)

        background = asyncio.create_task(job("background", slots.background))
        foreground = [asyncio.create_task(job(f"chat-{i}", slots)) for i in range(4)]
        await asyncio.sleep(0)
        slots.release()
        await asyncio.gather(background, *foreground)
        assert order == ["chat-0", "chat-1", "chat-2", "background", "chat-3"]

    asyncio.run(run())


def test_background_does_not_fill_all_model_capacity():
    async def run():
        slots = ModelSlots(2)
        first_entered, release_first, second_entered = (asyncio.Event() for _ in range(3))

        async def background(first):
            async with slots.background:
                (first_entered if first else second_entered).set()
                if first:
                    await release_first.wait()

        first = asyncio.create_task(background(True))
        await first_entered.wait()
        second = asyncio.create_task(background(False))
        await asyncio.sleep(0)
        assert not second_entered.is_set()
        await asyncio.wait_for(slots.acquire(), 0.5)
        assert not second_entered.is_set()
        slots.release()
        release_first.set()
        await asyncio.gather(first, second)
        assert second_entered.is_set()

    asyncio.run(run())


@pytest.mark.parametrize("capacity", [1, 2, 3, 4, 8])
def test_background_ceiling_keeps_configured_foreground_reserve(capacity):
    async def run():
        slots = ModelSlots(capacity)
        ceiling = max(1, capacity // 2)
        entered = []
        release = asyncio.Event()

        async def work(number):
            async with slots.background:
                entered.append(number)
                await release.wait()

        jobs = [asyncio.create_task(work(i)) for i in range(capacity + 1)]
        for _ in range(20):
            await asyncio.sleep(0)
        assert len(entered) == ceiling
        if capacity > 1:
            async with asyncio.timeout(0.5):
                async with slots:
                    pass
        release.set()
        await asyncio.gather(*jobs)
        for _ in range(capacity):
            await slots.acquire()
        for _ in range(capacity):
            slots.release()

    asyncio.run(run())


def test_queued_dialogue_gets_capacity_before_a_second_background_generation():
    async def run():
        slots = ModelSlots(4)
        release_background, release_dialogue = asyncio.Event(), asyncio.Event()
        entered = []

        async def work(label, pool, gate):
            async with pool:
                entered.append(label)
                await gate.wait()

        first = asyncio.create_task(work("background-1", slots.background, release_background))
        for _ in range(10):
            await asyncio.sleep(0)
        for _ in range(3):
            await slots.acquire()
        second = asyncio.create_task(work("background-2", slots.background, release_background))
        dialogue = [
            asyncio.create_task(work(f"chat-{i}", slots, release_dialogue)) for i in range(4)
        ]
        await asyncio.sleep(0)
        for _ in range(3):
            slots.release()
        for _ in range(10):
            await asyncio.sleep(0)
        assert entered == ["background-1", "chat-0", "chat-1", "chat-2"]
        release_dialogue.set()
        for _ in range(10):
            await asyncio.sleep(0)
        assert entered.index("chat-3") < entered.index("background-2")
        release_background.set()
        await asyncio.gather(first, second, *dialogue)
        async with asyncio.timeout(0.5):
            for _ in range(4):
                await slots.acquire()
        for _ in range(4):
            slots.release()

    asyncio.run(run())


@pytest.mark.parametrize("background", [False, True])
@pytest.mark.parametrize("grant_before_cancel", [False, True])
def test_cancellation_does_not_lose_capacity(background, grant_before_cancel):
    async def run():
        slots = ModelSlots(1)
        await slots.acquire()
        entered = False

        async def waiting():
            nonlocal entered
            async with slots.background if background else slots:
                entered = True

        task = asyncio.create_task(waiting())
        await asyncio.sleep(0)
        if grant_before_cancel:
            slots.release()
        task.cancel()
        if not grant_before_cancel:
            # Wake may remove the cancelled Future before its task handles cancellation.
            slots.release()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not entered
        async with asyncio.timeout(0.5):
            async with slots:
                pass
            async with slots.background:
                pass

    asyncio.run(run())
