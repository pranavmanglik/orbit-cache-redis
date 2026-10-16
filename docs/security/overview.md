# Orbit Cache Redis: security

This page describes security-relevant behavior implemented by `orbit-cache-redis` and the controls
that remain the responsibility of the application and Redis operator. It is implementation guidance,
not a security certification or a substitute for reviewing the exact version and deployment.

## Connection security

- Use `rediss://` for TLS-protected TCP connections to remote Redis deployments. The adapter passes
  URLs to redis-py and does not disable certificate verification.
- Keep Redis URLs in a secret manager or protected environment configuration. `RedisCacheConfig`
  excludes the URL from its representation, and adapter errors do not include raw redis-py error
  messages. Application logs, traces, crash reports, and command-line arguments still need review
  for accidental secret exposure.
- Use a dedicated Redis ACL identity with only the commands and key patterns required by the
  enabled integrations. The cache needs ordinary key reads, writes, and deletes; lock, OAuth
  transaction, and shared circuit-state integrations also evaluate bounded Lua scripts. Do not
  grant administrative commands to an application identity unless the deployment has a separate,
  justified need.
- Restrict network access to application workloads and operational systems that need Redis. Apply
  the provider's patching, persistence, backup, encryption-at-rest, and secret-rotation controls.

## Cache data

`RedisCache` stores bytes without choosing a serialization format. Treat both keys and values as
untrusted and potentially sensitive. Avoid credentials, access tokens, password material, and
personal data in cache entries unless the application has a documented need and suitable retention
and access controls. Do not log cached values. Hash or otherwise avoid exposing user identifiers in
keys when they could appear in diagnostics or Redis tooling.

The capability intentionally leaves key/value size limits and TTL policy provider-specific. Set
application-level bounds appropriate to the workload, configure Redis memory and eviction policy,
and use explicit TTLs for data that must not remain indefinitely. Redis eviction may remove entries
before their TTL; cache contents are not a durable source of truth.

## Optional integrations

### Distributed leases

`RedisLockProvider` uses owner-checked Lua scripts for acquisition, renewal, and release. It hashes
resource names before placing them in Redis keys. Its monotonically increasing fencing counter is
atomic only within the active Redis history: failover, restore, or replication rollback may repeat
or decrease fencing tokens. Manual deletion or eviction of the counter key also resets the sequence
and can repeat tokens. Configure memory policy and ACLs so the counter cannot be evicted or deleted;
with `noeviction`, memory exhaustion causes acquisition to fail and must be monitored. Protected
storage must reject stale tokens, and workloads requiring failover-safe fencing need an allocator
with stronger consistency guarantees. A lease is not proof that a process still owns a resource after
its TTL expires; do not treat it as a substitute for fencing at the protected write boundary.

### OAuth2 transaction storage

`RedisOAuthTransactionStore` stores short-lived authorization transactions using an opaque state
digest as the Redis key. It uses `SET NX` to avoid overwriting a pending transaction and an atomic
Lua operation to compare the browser-session binding before consuming a transaction. The stored
record includes temporary authorization data such as a PKCE verifier. Keep Redis access tightly
restricted, use TLS remotely, and set backups/retention to match the sensitivity and short lifetime
of this data. The adapter's TTL bounds are not a replacement for validating the OAuth callback,
issuer, redirect URI, nonce, and token response in the OAuth capability and application.

### Shared circuit-breaker state

`RedisCircuitStateStore` hashes the stable circuit name before forming Redis keys. Its two keys use
the same Redis Cluster hash tag so each Lua script can operate atomically on one slot. Apply a
dedicated key prefix and ACL pattern, protect coordination keys from eviction, and monitor write
errors. Redis replication and failover can lose recent transitions; do not use this adapter where
breaker state requires consensus-level durability.

## Failures, cancellation, and lifecycle

The adapters normalize provider exceptions and allow task cancellation to propagate. They do not
retry commands, because retry safety depends on the operation and application semantics. A network
failure can leave the caller uncertain whether a command was applied; design writes and surrounding
workflows for that uncertainty.

`RedisCache`, `RedisLockProvider`, and `RedisOAuthTransactionStore` own their internally created
clients and close them during `aclose()`. `RedisCircuitStateStore` instead borrows its client's pool;
the application that created that client must close it. Owned adapters reject new operations and
wait for in-flight commands before closing the pool. Applications should call
`aclose()` from their lifespan shutdown path, or install the optional Core plugin that takes
lifecycle ownership. If shutdown is cancelled or the backend close fails, retry cleanup during
application teardown and report the failure through the application's operational logging.

## Verification before deployment

Test the selected Redis version and topology with the application's real ACL, TLS, timeout, memory,
eviction, persistence, and failover settings. Exercise credential rotation, connection loss,
cancellation, shutdown during active requests, backup/restore, and (when enabled) stale-lock and
OAuth transaction replay scenarios. Unit tests with fake clients verify adapter behavior; they do
not establish interoperability with a live Redis server or production topology.
