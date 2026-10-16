# Orbit Cache Redis: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

- `ORBIT_REDIS_URL`

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-cache-redis` is the optional Redis adapter for the provider-neutral `orbit-cache` capability.
The direct `RedisCache` API works without Core. `RedisCachePlugin` adds Core-owned resource
registration and shutdown behavior and is available through the `core` extra.

## Installation

Install the capability and adapter for direct use. Add the optional Core extra only when using the
lifecycle plugin:

```bash
python -m pip install orbit-cache orbit-cache-redis
# Add the Core lifecycle plugin only when using it:
python -m pip install 'orbit-cache-redis[core]'
```

OAuth2 transaction persistence is a separate optional integration. Install `orbit-auth-oauth2` and the
Redis extra only in applications that use that capability:

```bash
python -m pip install 'orbit-cache-redis[oauth2]'
```

The adapter currently supports Python 3.11–3.14 and is alpha; its public API is not stable.

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

For distributed leases, protect the persistent fencing counter from deletion and eviction. A
`noeviction` policy preserves it, but Redis memory exhaustion will then reject writes, including
lease acquisition; monitor memory headroom and write errors. Eviction or loss of the counter can
repeat fencing tokens and invalidate stale-writer protection.

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
