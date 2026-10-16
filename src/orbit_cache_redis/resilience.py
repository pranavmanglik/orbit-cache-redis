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
"""Redis-backed atomic state for Orbit's optional distributed circuit breakers."""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
import secrets
from typing import Protocol

from orbit_resilience import (
    CircuitPermit,
    CircuitSnapshot,
    CircuitState,
    CircuitStateStoreError,
)

_PREFIX = re.compile(r"[A-Za-z0-9][A-Za-z0-9:_-]{0,63}\Z")
_CIRCUIT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_MAX_FAILURE_THRESHOLD = 1_000_000
_MAX_RECOVERY_SECONDS = 86_400
_MIN_PROBE_LEASE_MS = 100
_MAX_PROBE_LEASE_MS = 600_000
_MAX_STATE_TTL_SECONDS = 31_536_000

_ACQUIRE_SCRIPT = """
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local state = redis.call('HGET', KEYS[1], 'state') or 'closed'
local failures = tonumber(redis.call('HGET', KEYS[1], 'failures') or '0')
local epoch = tonumber(redis.call('HGET', KEYS[1], 'epoch') or '0')
local open_until = tonumber(redis.call('HGET', KEYS[1], 'open_until') or '0')
local now = now_ms()
if state == 'open' then
  if now < open_until then
    redis.call('PEXPIRE', KEYS[1], ARGV[4])
    return {0, state, failures, epoch}
  end
  state = 'half-open'
  epoch = redis.call('HINCRBY', KEYS[1], 'epoch', 1)
  redis.call('HSET', KEYS[1], 'state', state)
elseif state ~= 'closed' and state ~= 'half-open' then
  return redis.error_reply('invalid circuit state')
end
if state == 'half-open' then
  local acquired = redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[2], 'NX')
  if not acquired then
    redis.call('PEXPIRE', KEYS[1], ARGV[4])
    return {0, state, failures, epoch}
  end
end
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return {1, state, failures, epoch}
"""

_RECORD_SCRIPT = """
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local state = redis.call('HGET', KEYS[1], 'state') or 'closed'
local failures = tonumber(redis.call('HGET', KEYS[1], 'failures') or '0')
local epoch = tonumber(redis.call('HGET', KEYS[1], 'epoch') or '0')
local open_until = tonumber(redis.call('HGET', KEYS[1], 'open_until') or '0')
local permit_epoch = tonumber(ARGV[1])
local probe_token = ARGV[2]
if epoch ~= permit_epoch then
  return {state, failures, epoch}
end
if probe_token ~= '' then
  if state ~= 'half-open' or redis.call('GET', KEYS[2]) ~= probe_token then
    return {state, failures, epoch}
  end
elseif state ~= 'closed' then
  return {state, failures, epoch}
end
if ARGV[3] == '1' then
  if probe_token ~= '' then
    redis.call('HSET', KEYS[1], 'state', 'closed', 'failures', 0, 'open_until', 0)
    epoch = redis.call('HINCRBY', KEYS[1], 'epoch', 1)
    redis.call('DEL', KEYS[2])
    state = 'closed'
    failures = 0
  elseif state == 'closed' then
    redis.call('HSET', KEYS[1], 'failures', 0)
    failures = 0
  end
else
  failures = redis.call('HINCRBY', KEYS[1], 'failures', 1)
  if probe_token ~= '' or failures >= tonumber(ARGV[4]) then
    state = 'open'
    open_until = now_ms() + tonumber(ARGV[5])
    redis.call('HSET', KEYS[1], 'state', state, 'open_until', open_until)
    epoch = redis.call('HINCRBY', KEYS[1], 'epoch', 1)
    redis.call('DEL', KEYS[2])
  end
end
redis.call('PEXPIRE', KEYS[1], ARGV[6])
return {state, failures, epoch}
"""

_ABANDON_SCRIPT = """
if redis.call('GET', KEYS[2]) == ARGV[1] then
  return redis.call('DEL', KEYS[2])
end
return 0
"""

_SNAPSHOT_SCRIPT = """
local state = redis.call('HGET', KEYS[1], 'state') or 'closed'
local failures = tonumber(redis.call('HGET', KEYS[1], 'failures') or '0')
local epoch = tonumber(redis.call('HGET', KEYS[1], 'epoch') or '0')
return {state, failures, epoch}
"""


class _RedisEvalClient(Protocol):
    """Narrow async Redis client surface; client lifecycle remains with its creator."""

    async def eval(self, script: str, numkeys: int, *keys_and_args: str | bytes) -> object:
        """Execute a Redis script atomically against the configured server."""


class RedisCircuitStateStore:
    """Atomic Redis adapter for ``orbit_resilience.CircuitStateStore``.

    The adapter uses short Lua scripts so admission, epoch checks, failure counts, and half-open
    probe ownership change atomically across processes. The Redis client is borrowed and is not
    closed by this object; its creator must keep it alive for the application's lifetime and close
    it during shutdown. Circuit identifiers are hashed before becoming key material.

    State entries use a sliding TTL. Configure Redis with an eviction policy appropriate for
    coordination state (commonly ``noeviction``), persistent authentication, and TLS on untrusted
    networks. Redis replication/failover can lose recent writes, so this adapter is not a consensus
    system and cannot promise breaker state survives a primary failover.
    """

    def __init__(
        self,
        client: _RedisEvalClient,
        *,
        prefix: str = "orbit:resilience:circuit",
        state_ttl_seconds: int = 86_400,
    ) -> None:
        """Wrap a caller-owned Redis client and validate bounded key/retention settings."""
        if not callable(getattr(client, "eval", None)):
            raise TypeError("client must provide an async eval method.")
        if not isinstance(prefix, str) or not _PREFIX.fullmatch(prefix):
            raise ValueError("prefix must be bounded Redis-safe text.")
        if (
            isinstance(state_ttl_seconds, bool)
            or not isinstance(state_ttl_seconds, int)
            or not 1 <= state_ttl_seconds <= _MAX_STATE_TTL_SECONDS
        ):
            raise ValueError("state_ttl_seconds is outside the supported range.")
        self._client = client
        self._prefix = prefix
        self._state_ttl_ms = state_ttl_seconds * 1_000

    async def acquire(
        self,
        name: str,
        *,
        failure_threshold: int,
        recovery_timeout: float,
        probe_lease_timeout: float,
    ) -> CircuitPermit | None:
        """Atomically admit closed-state work or lease one half-open recovery probe."""
        self._validate_policy(name, failure_threshold, recovery_timeout)
        probe_ms = _seconds_to_milliseconds(probe_lease_timeout)
        if not _MIN_PROBE_LEASE_MS <= probe_ms <= _MAX_PROBE_LEASE_MS:
            raise ValueError("probe_lease_timeout is outside the supported range.")
        state_ttl_ms = max(
            self._state_ttl_ms,
            math.ceil(recovery_timeout * 1_000) + probe_ms,
        )
        state_key, probe_key = self._keys(name)
        probe_token = secrets.token_urlsafe(32)
        result = await self._eval(
            _ACQUIRE_SCRIPT,
            state_key,
            probe_key,
            str(failure_threshold),
            str(probe_ms),
            probe_token,
            str(state_ttl_ms),
        )
        values = _array(result, 4)
        if _integer(values[0]) == 0:
            return None
        state = _text(values[1])
        if state not in (CircuitState.CLOSED.value, CircuitState.HALF_OPEN.value):
            raise CircuitStateStoreError("Redis returned an invalid circuit admission state.")
        epoch = _integer(values[3])
        return CircuitPermit(epoch=epoch, probe_token=probe_token if state == "half-open" else None)

    async def record(
        self,
        name: str,
        permit: CircuitPermit,
        *,
        succeeded: bool,
        failure_threshold: int,
        recovery_timeout: float,
    ) -> CircuitSnapshot:
        """Record a call outcome unless a newer epoch or probe lease superseded it."""
        self._validate_policy(name, failure_threshold, recovery_timeout)
        if not isinstance(permit, CircuitPermit) or not isinstance(succeeded, bool):
            raise ValueError("permit and succeeded arguments are invalid.")
        probe_token = permit.probe_token or ""
        retention = max(
            self._state_ttl_ms,
            math.ceil(recovery_timeout * 1_000) + _MAX_PROBE_LEASE_MS,
        )
        state_key, probe_key = self._keys(name)
        result = await self._eval(
            _RECORD_SCRIPT,
            state_key,
            probe_key,
            str(permit.epoch),
            probe_token,
            "1" if succeeded else "0",
            str(failure_threshold),
            str(math.ceil(recovery_timeout * 1_000)),
            str(retention),
        )
        return _snapshot(result)

    async def abandon_probe(self, name: str, permit: CircuitPermit) -> None:
        """Release only the matching probe token; a stale worker cannot delete a newer lease."""
        if not isinstance(permit, CircuitPermit):
            raise ValueError("permit is invalid.")
        if permit.probe_token is None:
            return
        state_key, probe_key = self._keys(name)
        await self._eval(_ABANDON_SCRIPT, state_key, probe_key, permit.probe_token)

    async def snapshot(self, name: str) -> CircuitSnapshot:
        """Read shared state without refreshing admission or changing its retention deadline."""
        state_key, _ = self._keys(name)
        result = await self._eval(_SNAPSHOT_SCRIPT, state_key)
        return _snapshot(result)

    def _keys(self, name: str) -> tuple[str, str]:
        """Return same-slot Redis Cluster keys without exposing the supplied circuit name."""
        if not isinstance(name, str) or not _CIRCUIT_NAME.fullmatch(name):
            raise ValueError("circuit name must be bounded Redis-safe text.")
        digest = hashlib.sha256(name.encode("ascii")).hexdigest()[:32]
        slot = f"{{{digest}}}"
        base = f"{self._prefix}:{slot}"
        return f"{base}:state", f"{base}:probe"

    @staticmethod
    def _validate_policy(name: str, failure_threshold: int, recovery_timeout: float) -> None:
        if not isinstance(name, str) or not _CIRCUIT_NAME.fullmatch(name):
            raise ValueError("circuit name must be bounded Redis-safe text.")
        if (
            isinstance(failure_threshold, bool)
            or not isinstance(failure_threshold, int)
            or not 1 <= failure_threshold <= _MAX_FAILURE_THRESHOLD
        ):
            raise ValueError("failure_threshold is outside the supported range.")
        if (
            isinstance(recovery_timeout, bool)
            or not isinstance(recovery_timeout, (int, float))
            or not 0 < recovery_timeout <= _MAX_RECOVERY_SECONDS
        ):
            raise ValueError("recovery_timeout is outside the supported range.")

    async def _eval(self, script: str, *keys_and_args: str) -> object:
        """Run one script while removing Redis messages and connection details from failures."""
        try:
            key_count = 2 if script in (_ACQUIRE_SCRIPT, _RECORD_SCRIPT, _ABANDON_SCRIPT) else 1
            return await self._client.eval(script, key_count, *keys_and_args)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise CircuitStateStoreError("Redis circuit-state command failed.") from None


def _seconds_to_milliseconds(value: float) -> int:
    """Validate a finite positive duration before converting it for Redis expiry commands."""
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not 0 < value <= _MAX_PROBE_LEASE_MS / 1_000
    ):
        raise ValueError("probe_lease_timeout is outside the supported range.")
    return math.ceil(value * 1_000)


def _array(value: object, expected_length: int) -> list[object]:
    """Validate the top-level shape of a Redis script response."""
    if not isinstance(value, (list, tuple)) or len(value) != expected_length:
        raise CircuitStateStoreError("Redis returned an invalid circuit-state response.")
    return list(value)


def _text(value: object) -> str:
    """Decode one Redis status field without exposing malformed provider data."""
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError:
            raise CircuitStateStoreError(
                "Redis returned an invalid circuit-state response."
            ) from None
    if not isinstance(value, str):
        raise CircuitStateStoreError("Redis returned an invalid circuit-state response.")
    return value


def _integer(value: object) -> int:
    """Parse a bounded non-negative counter from Redis's integer/byte response form."""
    if isinstance(value, bool):
        raise CircuitStateStoreError("Redis returned an invalid circuit-state response.")
    if isinstance(value, int):
        result = value
    elif isinstance(value, bytes):
        try:
            result = int(value.decode("ascii"))
        except (UnicodeDecodeError, ValueError):
            raise CircuitStateStoreError(
                "Redis returned an invalid circuit-state response."
            ) from None
    elif isinstance(value, str):
        try:
            result = int(value)
        except ValueError:
            raise CircuitStateStoreError(
                "Redis returned an invalid circuit-state response."
            ) from None
    else:
        raise CircuitStateStoreError("Redis returned an invalid circuit-state response.")
    if result < 0:
        raise CircuitStateStoreError("Redis returned an invalid circuit-state response.")
    return result


def _snapshot(value: object) -> CircuitSnapshot:
    """Convert and validate the adapter-neutral state returned by a Lua transition."""
    fields = _array(value, 3)
    try:
        state = CircuitState(_text(fields[0]))
    except ValueError:
        raise CircuitStateStoreError("Redis returned an invalid circuit-state response.") from None
    try:
        return CircuitSnapshot(state, _integer(fields[1]), _integer(fields[2]))
    except ValueError:
        raise CircuitStateStoreError("Redis returned an invalid circuit-state response.") from None


__all__ = ["RedisCircuitStateStore"]
