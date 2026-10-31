"""Проверяем поведение запросов без зависимости от публичных серверов."""

import asyncio

import httpx
import pytest

import gather_nuance
import task_0_fix_async as task_0
import task_1_sync_vs_async as task_1
import task_2_robust_fetch as task_2


@pytest.mark.parametrize("fn", [task_0.fast_main, task_0.fast_main_inline, task_1.fast_main])
def test_gather_overlaps_requests_and_preserves_order(monkeypatch, fn):
    async def scenario():
        active = 0
        peak = 0
        all_started = asyncio.Event()

        async def fake_fetch(client, url):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if active == 3:
                all_started.set()
            await asyncio.wait_for(all_started.wait(), timeout=1)
            active -= 1
            return url

        monkeypatch.setattr(
            task_0 if fn.__module__ == task_0.__name__ else task_1, "fetch", fake_fetch
        )
        assert await fn([3, 1, 2]) == [3, 1, 2]
        assert peak == 3

    asyncio.run(scenario())


def test_sequential_version_has_only_one_active_request(monkeypatch):
    seen = []

    async def fake_fetch(client, url):
        seen.append((url, "start"))
        await asyncio.sleep(0)
        seen.append((url, "end"))
        return url

    monkeypatch.setattr(task_0, "fetch", fake_fetch)
    assert asyncio.run(task_0.slow_main([1, 2])) == [1, 2]
    assert seen == [(1, "start"), (1, "end"), (2, "start"), (2, "end")]


def test_retry_recovers_and_waits_exponentially(monkeypatch):
    attempts, delays = [], []

    async def handler(request):
        attempts.append(request.url)
        return httpx.Response(503 if len(attempts) < 3 else 200)

    async def fake_sleep(delay):
        delays.append(delay)

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await task_2.robust_fetch(client, "https://test.invalid")

    monkeypatch.setattr(task_2.asyncio, "sleep", fake_sleep)
    assert asyncio.run(scenario()) == 200
    assert len(attempts) == 3
    assert delays == [1, 2]


def test_http_404_is_not_retried():
    attempts = []

    def handler(request):
        attempts.append(request.url)
        return httpx.Response(404)

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await task_2.robust_fetch(client, "https://test.invalid", backoff=0)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(scenario())
    assert len(attempts) == 1


def test_timeout_has_finite_attempts():
    attempts = []

    async def handler(request):
        attempts.append(request.url)
        await asyncio.Event().wait()

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await task_2.robust_fetch(client, "https://test.invalid", timeout=0.01, backoff=0)

    with pytest.raises(TimeoutError):
        asyncio.run(scenario())
    assert len(attempts) == 3


def test_cancellation_is_propagated():
    async def handler(request):
        raise asyncio.CancelledError

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await task_2.robust_fetch(client, "https://test.invalid")

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(scenario())


def test_one_failed_url_does_not_discard_other_results(monkeypatch):
    async def fake_fetch(client, url):
        if url == "bad":
            raise ValueError("broken URL")
        await asyncio.sleep(0)
        return 200

    monkeypatch.setattr(task_2, "robust_fetch", fake_fetch)
    results, errors = asyncio.run(task_2.robust_main(["good", "bad", "another"]))
    assert [item["url"] for item in results] == ["good", "another"]
    assert errors == [{"url": "bad", "type": "ValueError", "message": "broken URL"}]


@pytest.mark.parametrize(
    "fn", [gather_nuance.with_list, gather_nuance.inline, gather_nuance.generator]
)
def test_gather_variants_schedule_every_child(fn):
    async def scenario():
        gate = asyncio.Event()
        parent = asyncio.create_task(fn(gate, range(7)))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert len(asyncio.all_tasks()) - 2 == 7
        gate.set()
        assert await parent == list(range(7))

    asyncio.run(scenario())
