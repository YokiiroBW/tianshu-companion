"""Circuit admission uses simulated time and real synthetic HTTP transport."""

import asyncio

import httpx
import pytest

from tianshu_companion.background_peers import BackgroundPeers, background_calls
from tianshu_companion.clients import JsonService
from tianshu_companion.contracts import Fault


def test_persistent_rejection_is_bounded_and_success_resets_without_foreground_delay():
    async def run():
        now, calls, sleeps = [0.0], [], []
        status = [403]

        async def sleep(delay):
            sleeps.append(delay)
            now[0] += delay

        def transport(request):
            calls.append(now[0])
            return httpx.Response(status[0], json={"code": "forbidden"})

        circuits = BackgroundPeers(clock=lambda: now[0], sleep=sleep, jitter=lambda: 1.0)
        client = JsonService(
            "https://fixture.invalid",
            "fixture",
            target="platform",
            transport=httpx.MockTransport(transport),
            background_peers=circuits,
        )
        try:
            with background_calls():
                while now[0] < 600:
                    with pytest.raises(Fault):
                        await client.call("/internal/v1/actor-self-memory/authority", {})
            assert len([value for value in calls if value < 600]) <= 20
            assert circuits.snapshot()[0]["consecutive_failures"] >= 5
            delayed = len(sleeps)
            # Foreground uses the existing transport immediately and does not mutate
            # background failure history or inherit its open gate.
            status[0] = 200
            await client.call("/internal/v1/actor-self-memory/authority", {})
            assert len(sleeps) == delayed
            with background_calls():
                await client.call("/internal/v1/actor-self-memory/authority", {})
                assert circuits.snapshot() == []
                delayed = len(sleeps)
                for _ in range(20):
                    await client.call("/internal/v1/actor-self-memory/authority", {})
                assert len(sleeps) == delayed
        finally:
            await client.close()

    asyncio.run(run())


def test_background_read_never_retries_in_the_same_pass_but_foreground_recovery_stays():
    async def run():
        calls = []

        def transport(request):
            calls.append(request)
            raise httpx.RemoteProtocolError("synthetic transport close")

        client = JsonService(
            "https://fixture.invalid",
            "fixture",
            target="memory",
            transport=httpx.MockTransport(transport),
        )
        try:
            with background_calls(), pytest.raises(Fault):
                await client.call("/internal/v1/identity/resolve", {})
            assert len(calls) == 1
            with pytest.raises(Fault):
                await client.call("/internal/v1/identity/resolve", {})
            assert len(calls) == 3
        finally:
            await client.close()

    asyncio.run(run())
