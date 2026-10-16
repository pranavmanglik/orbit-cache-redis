# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Redis integration tests for shared circuit admission and stale-completion fencing."""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

pytest.importorskip("orbit_resilience", reason="install orbit-cache-redis[resilience]")
redis_async = pytest.importorskip("redis.asyncio", reason="install orbit-cache-redis")

from orbit_resilience import (  # noqa: E402
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    CircuitStateStoreError,
)

from orbit_cache_redis.resilience import RedisCircuitStateStore  # noqa: E402


class BrokenRedisClient:
    """Raise a credential-like provider message to verify adapter error sanitization."""

    async def eval(self, script: str, numkeys: int, *keys_and_args: str | bytes) -> object:
        raise RuntimeError("redis://user:secret@private-host/0")


@pytest.mark.asyncio
async def test_backend_errors_are_sanitized() -> None:
    store = RedisCircuitStateStore(BrokenRedisClient())

    with pytest.raises(CircuitStateStoreError, match="Redis circuit-state command failed") as error:
        await store.snapshot("checkout.primary")

    assert "secret" not in str(error.value)


@pytest.mark.asyncio
async def test_policy_bounds_reject_unrepresentable_integers_before_redis_io() -> None:
    client = BrokenRedisClient()
    store = RedisCircuitStateStore(client)

    with pytest.raises(ValueError, match="recovery_timeout"):
        await store.acquire(
            "checkout.primary",
            failure_threshold=3,
            recovery_timeout=10**400,
            probe_lease_timeout=30,
        )
    with pytest.raises(ValueError, match="probe_lease_timeout"):
        await store.acquire(
            "checkout.primary",
            failure_threshold=3,
            recovery_timeout=30,
            probe_lease_timeout=10**400,
        )


@pytest.fixture
def redis_url() -> str:
    """Require an explicit disposable Redis URL; never guess a developer's local database."""
    value = os.environ.get("ORBIT_TEST_REDIS_URL")
    if not value:
        pytest.skip("set ORBIT_TEST_REDIS_URL to a disposable Redis database")
    return value


@pytest.mark.asyncio
async def test_breaker_state_and_single_probe_are_shared_between_instances(redis_url: str) -> None:
    client = redis_async.Redis.from_url(redis_url, decode_responses=False)
    peer_client = redis_async.Redis.from_url(redis_url, decode_responses=False)
    name = f"integration-{uuid.uuid4().hex}"
    store = RedisCircuitStateStore(client)
    peer_store = RedisCircuitStateStore(peer_client)
    state_key, probe_key = store._keys(name)
    first = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=0.1,
        state_store=store,
        name=name,
        probe_lease_timeout=2,
    )
    second = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=0.1,
        state_store=peer_store,
        name=name,
        probe_lease_timeout=2,
    )
    try:

        async def fail() -> None:
            raise ConnectionError("simulated provider outage")

        with pytest.raises(ConnectionError):
            await first.call(fail)
        assert (await second.refresh_state()).state is CircuitState.OPEN

        invoked_while_open = False

        async def forbidden_call() -> None:
            nonlocal invoked_while_open
            invoked_while_open = True

        with pytest.raises(CircuitOpenError):
            await second.call(forbidden_call)
        assert not invoked_while_open

        await asyncio.sleep(0.12)
        probes = 0

        async def probe() -> str:
            nonlocal probes
            probes += 1
            await asyncio.sleep(0.02)
            return "recovered"

        results = await asyncio.gather(
            first.call(probe), second.call(probe), return_exceptions=True
        )
        assert results.count("recovered") == 1
        assert sum(isinstance(result, CircuitOpenError) for result in results) == 1
        assert probes == 1
        assert (await first.refresh_state()).state is CircuitState.CLOSED
    finally:
        await client.delete(state_key, probe_key)
        await client.aclose()
        await peer_client.aclose()


@pytest.mark.asyncio
async def test_old_inflight_completion_cannot_close_newer_open_epoch(redis_url: str) -> None:
    client = redis_async.Redis.from_url(redis_url, decode_responses=False)
    name = f"epoch-{uuid.uuid4().hex}"
    store = RedisCircuitStateStore(client)
    state_key, probe_key = store._keys(name)
    old = CircuitBreaker(failure_threshold=1, state_store=store, name=name)
    current = CircuitBreaker(failure_threshold=1, state_store=store, name=name)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_success() -> str:
        entered.set()
        await release.wait()
        return "stale success"

    async def fail() -> None:
        raise ConnectionError("simulated provider outage")

    try:
        in_flight = asyncio.create_task(old.call(slow_success))
        await entered.wait()
        with pytest.raises(ConnectionError):
            await current.call(fail)
        release.set()
        assert await in_flight == "stale success"
        assert (await old.refresh_state()).state is CircuitState.OPEN
    finally:
        release.set()
        await client.delete(state_key, probe_key)
        await client.aclose()
