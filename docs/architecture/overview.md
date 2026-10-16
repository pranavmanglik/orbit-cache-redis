# Orbit Cache Redis: architecture and boundaries

## Responsibility

`orbit-cache-redis` is the optional Redis adapter for the provider-neutral `orbit-cache` capability.
The direct cache API depends on `orbit-cache` and redis-py, not Core. `RedisCachePlugin` is a
separate optional Core lifecycle integration installed through the `core` extra.

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `orbit-cache>=0.1.0a1,<0.2`
- `redis>=5,<9`
- Optional `core` extra: `orbit-core>=0.1.0a1,<0.2` for `RedisCachePlugin`.
- Optional `oauth2` group: `orbit-auth-oauth2>=0.1.0a1,<0.2`.
- Optional `lock` group: `orbit-lock>=0.1.0a1,<0.2`.
- Optional `resilience` group: `orbit-resilience>=0.1.0a1,<0.2` for Redis-backed shared circuit state.
- Development dependencies: Core for lifecycle tests, plus pytest, pytest-asyncio, Ruff, and mypy.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_cache_redis/__init__.py`
- `src/orbit_cache_redis/cache.py`
- `src/orbit_cache_redis/config.py`
- `src/orbit_cache_redis/lock.py`
- `src/orbit_cache_redis/resilience.py`
- `src/orbit_cache_redis/oauth2.py`
- `src/orbit_cache_redis/plugin.py`

## Public contract and scope

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

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.

The optional circuit-state adapter borrows the caller's Redis client and implements the
`orbit-resilience.CircuitStateStore` contract. Its Lua transitions coordinate workers on the
configured Redis history, but Redis replication and failover do not provide consensus guarantees.
The Redis cluster hash tag keeps each script's state and probe keys in one slot; this does not add
Redis Cluster client support to the cache adapter itself.
