# Orbit Cache Redis

`orbit-cache-redis` is the optional Redis adapter for the provider-neutral `orbit-cache` capability.
Applications can depend on `AsyncCache` without Redis. The direct `RedisCache` API does not require
Core; the optional `RedisCachePlugin` integrates the adapter with Core's application lifecycle.

## Installation

Install the capability and Redis adapter for direct use:

```bash
python -m pip install orbit-cache orbit-cache-redis
```

For Core lifecycle management, install the optional extra:

```bash
python -m pip install 'orbit-cache-redis[core]'
```

OAuth2 transaction persistence is a separate optional integration. Install `orbit-auth-oauth2` and the
Redis extra only in applications that use that capability:

```bash
python -m pip install 'orbit-cache-redis[oauth2]'
```

The adapter currently supports Python 3.11–3.14 and is alpha; its public API is not stable.

## Use the cache directly

`RedisCache.from_url()` creates a lazy redis-py client. Connection attempts happen on the first
operation, not while constructing the cache. Values are bytes; this adapter does not serialize
Python objects for the caller.

```python
from os import environ

from orbit_cache_redis import RedisCache

cache = RedisCache.from_url(environ["ORBIT_REDIS_URL"])


async def use_cache() -> None:
    try:
        await cache.set("profile:42", b"serialized profile", ttl=60)
        profile = await cache.get("profile:42")
        removed = await cache.delete("profile:42")
    finally:
        await cache.aclose()
```

Supply `ORBIT_REDIS_URL` through a secret-aware deployment configuration source; never commit or
log credential-bearing URLs. Use a `rediss://` URL for TLS. `RedisCacheConfig` accepts a URL, a
maximum connection count (default 100), and socket/connect timeouts (default five seconds each).
Limits are validated before the client is created, and the URL is excluded from the configuration
representation and normalized errors.

Keys must be non-empty strings. Values must be `bytes`; Orbit does not choose an encoding or
serialization format. TTLs are omitted or positive whole seconds up to `2^31 - 1`. A cache miss
returns `None`; `delete` returns whether a key existed.

## Register with an Orbit application

Use the plugin when Core should resolve the cache through its shared capability key and manage its
shutdown:

```python
from os import environ

from orbit import Application, ApplicationConfig
from orbit_cache_redis import RedisCache
from orbit_cache_redis.plugin import RedisCachePlugin

app = Application(ApplicationConfig(name="orders"))
cache = RedisCache.from_url(environ["ORBIT_REDIS_URL"])
app.register_plugin(RedisCachePlugin(cache))
```

The plugin takes ownership of the cache, registers it as `AsyncCache`, and closes it during normal
shutdown or plugin-startup rollback. After application configuration has composed the plugin, the
container exposes the capability under `CACHE_DEPENDENCY_KEY`; resolve it in a service or request
scope rather than during application construction. If constructing and managing `RedisCache` directly instead,
the application code that creates it must call `await cache.aclose()` during shutdown. Closing is
idempotent, prevents new commands, waits for in-flight commands, and then releases the pool; using
a closed adapter raises `CacheOperationError`.

The plugin contributes a read-only **Check Redis connection** action to Orbit CLI. It sends PING
through the cache adapter without inspecting or changing key names or values.

## Failure and deployment behavior

Redis client setup errors become `CacheConfigurationError`; operation failures become
`CacheOperationError` without exposing redis-py messages or credential-bearing URLs. Task
cancellation propagates unchanged. Connections are lazy, so an unavailable Redis server is
reported when the first operation is awaited. Configure socket timeouts and deployment-level
health/retry behavior deliberately; this adapter does not silently retry operations.

This implementation targets standalone Redis URLs supported by redis-py: `redis://`, `rediss://`,
and `unix://`. Redis Sentinel and cluster topologies, cache serialization, eviction policy, and
application-level cache-aside behavior are not provided by the cache adapter. The adapter's client
pool and cache state are external to Orbit Core; Core does not require Redis for its own operation.

## Optional distributed leases

`orbit-cache-redis[lock]` installs the separate `orbit-lock` capability and adds a Redis-backed
`RedisLockProvider`. Acquisition is non-blocking, lease keys use a digest of the resource name,
and Lua compare-and-set operations protect renewal and release from stale owners. Fencing numbers
are allocated from one persistent counter per key prefix and increase across resources on the same
Redis history. Callers must pass the token to protected storage and have that storage reject stale
tokens. Redis rollback or failover can roll back the counter, so this adapter does not provide
failover-safe fencing or consensus guarantees. The counter must also remain present: manual deletion
or Redis eviction can reset it and repeat fencing tokens. Protect the counter from eviction and
monitor memory pressure; with a `noeviction` policy, memory exhaustion makes acquisition fail rather
than silently granting a reused token. Choose a backend with stronger consistency when stale writes
across failover would be unsafe.

The provider owns its lazy Redis client and must be closed after active application leases are
released. It waits for in-flight Redis commands during shutdown, but it does not renew leases in
the background. Cancellation or a transport failure after the acquire script commits can leave a
lease active until its TTL because the caller may not receive its owner handle. Use TLS, Redis ACLs,
network isolation, and a prefix isolated to the application.

```python
from os import environ

from orbit_cache_redis.lock import RedisLockProvider

provider = RedisLockProvider.from_url(environ["ORBIT_REDIS_URL"])


async def update() -> None:
    lease = await provider.try_acquire("invoice:42", ttl_ms=30_000)
    if lease is None:
        return
    async with lease:
        # The storage operation must atomically reject tokens older than its latest accepted token.
        await write_invoice(fencing_token=lease.fencing_token)


async def shutdown() -> None:
    await provider.aclose()
```

The opt-in cache integration test exercises actual Redis bytes, TTL, delete, and connection
shutdown when `ORBIT_REDIS_URL` is set. The package does not claim a Redis failover test.

## Optional shared circuit breakers

Install `orbit-cache-redis[resilience]` to use Redis as the shared state store for
`orbit-resilience.CircuitBreaker`. Each worker must use the same stable circuit name and policy.
The Redis client is borrowed by `RedisCircuitStateStore`; the application owns its pool and must
close it during shutdown. A half-open probe uses a bounded lease, so set the lease longer than the
maximum operation deadline plus the expected completion-write latency. State-store failures fail
closed, and a failed outcome write after an operation ran is reported with
`CircuitStateStoreError.operation_attempted=True` to make possible side-effect ambiguity explicit.

```python
from os import environ

from redis.asyncio import Redis
from orbit_resilience import CircuitBreaker
from orbit_cache_redis.resilience import RedisCircuitStateStore

redis = Redis.from_url(environ["ORBIT_REDIS_URL"])
store = RedisCircuitStateStore(redis)
payments_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=30,
    state_store=store,
    name="payments.primary",
    probe_lease_timeout=45,
)

async def shutdown() -> None:
    await redis.aclose()
```

Use TLS, Redis ACLs, network isolation, and a dedicated bounded key prefix. The adapter hashes
circuit names and uses same-slot keys for Redis Cluster script execution. Choose an eviction policy
that protects coordination state (commonly `noeviction`) and monitor memory pressure. Redis
replication/failover can lose recent state; this adapter is not a consensus system and does not
guarantee breaker state survives primary failover. `ORBIT_TEST_REDIS_URL` enables disposable Redis
integration tests in the package test suite; CI provisions an isolated Redis service for them.

## Optional OAuth2 transaction store

`orbit-cache-redis[oauth2]` adds `RedisOAuthTransactionStore` in `orbit_cache_redis.oauth2`. It implements the
browser-bound, one-use transaction contract from `orbit-auth-oauth2`. State is hashed before it becomes a
Redis key, records have a bounded TTL and size, and a Lua script checks the browser-session digest
and consumes the record atomically. The store owns its async Redis client and must be closed during
application shutdown:

```python
from os import environ

from orbit_auth_oauth2 import OAuth2Flow
from orbit_cache_redis.oauth2 import RedisOAuthTransactionStore

transactions = RedisOAuthTransactionStore.from_url(environ["ORBIT_REDIS_URL"])
flow = OAuth2Flow(transactions)


async def shutdown() -> None:
    await transactions.aclose()
```

The Redis store provides transaction persistence only. Applications must provide an `OAuth2Provider`
implementation, securely generate and retain the opaque browser session binding, and validate OIDC
ID tokens with a trusted verifier. Use TLS (`rediss://`), Redis ACLs restricted to the required key
namespace and commands, network isolation, monitored capacity, and an appropriate backup and
encryption-at-rest policy. Redis persistence and backups can retain sensitive authorization
transactions; scope access and retention accordingly. The package does not include a Redis server
integration test, provider adapter, OIDC verifier, or production deployment configuration.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

The adapter tests use a fake async Redis client and do not require a Redis server. To include the
live cache integration check, set `ORBIT_REDIS_URL` to a disposable Redis database before running
the suite:

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

The lock and OAuth adapter tests are skipped when their separate capability packages are absent. To include them from a workspace checkout, install the sibling packages `orbit-lock`, `orbit-auth`, and `orbit-auth-oauth2`, then install this package with `[lock,oauth2,dev]`. They use fakes and do not require a live Redis server.

Licensed under Apache-2.0.

