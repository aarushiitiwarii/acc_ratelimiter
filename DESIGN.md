# RateGuard — Design, Evidence and Limitations

## 1. Request flow

```
request ─► [1] authenticate (HMAC / JWT, local CPU, L1 claims LRU) ──fail──► 401   (no Redis touched)
        ─► [2] client IP (X-Forwarded-For only if TRUST_XFF)
        ─► [3] hierarchical check: tenant ∧ API key ∧ client IP
                 each level = one local bucket (L1) holding a reservation ("lease") of tokens
                 allowed iff ALL levels have a token; then all are decremented together
                 denied  ─► 429 + Retry-After + X-RateLimit-Level (which level said no)
        ─► [4] reverse-proxy to UPSTREAM_URL
```

Redis is off the hot path. A bucket talks to Redis only to **lease** a batch (`lease.lua`), to
**return** unused tokens (`give.lua`), or to **probe** for recovery.

## 2. Components

| Component | File | What it does |
|---|---|---|
| Atomic token bucket + sliding window | `lease.lua` | `Tokens_new = min(Cap, Tokens_old + Δt·Rate)` and a weighted two-window sliding counter; grants `min(tokens, window room, requested)` in one atomic step. Touches exactly one key, so it is Redis-Cluster safe. |
| Drift compensation (return) | `give.lua`, `Limiter.queueGive` | Unused tokens of an expired lease (or evicted bucket) are returned, capped at capacity. |
| Drift compensation (recovery) | `Limiter.refill` | After an outage the replica charges the shared bucket for what it admitted locally (`reconciled` counter). |
| L1 bucket cache | `lru.go`, `Limiter.buckets` | Bounded sharded LRU. Eviction can only *discard* local tokens (handed back), never mint them → cannot bypass limits (tested). |
| L1 claims cache | `auth.go` | Validated credentials cached ≤ `AUTH_CACHE_TTL_S` and never past the token's own expiry. |
| Background sync daemon | `Limiter.daemon/sweep` | Every `LEASE_TTL/2`: returns expired leases, prefetches for hot buckets, probes Redis for buckets in fallback (works with zero traffic). |
| Adaptive leases | `nextLease` | Lease starts at 1 token, doubles while a bucket drains it within one TTL, halves when it expires unused, capped at `min(BATCH_SIZE, ⌈cap/replicas⌉)`. Sparse buckets (most IPs) never hoard budget or need a give-back. |
| Wait-on-in-flight | `Limiter.prepare/Allow` | If a lease runs dry while a refill is in flight, the request waits for it (≤ one Redis timeout) instead of being denied. Denial therefore means the *shared* bucket is empty, not that a prefetch was slow. |
| Fail-open w/ local approximation | `canTake`, `refill` | Redis error → bucket enters fallback and enforces `cap/REPLICAS`, `rate/REPLICAS` locally. Cold start during an outage starts with the full share (fail open); a previously leased bucket carries over what it had left. |
| Local-only IP level | `LevelCfg.LocalOnly` (`IP_LOCAL`) | IP bucket is a per-replica share, zero Redis traffic. |

## 3. PS requirement → implementation → evidence

| PS requirement | Implementation | Evidence |
|---|---|---|
| Hierarchical bucket per tenant / API key / IP | 3 levels, one Redis key each | `TestHierarchy`, `TestKeysIndependent`, `TestLocalOnlyIP` |
| Sliding-window token bucket | `lease.lua` bucket + window counter | `TestSlidingWindow` |
| Local L1 LRU cache (keys + counters) | `lru.go`; claims + bucket caches | `TestEvictionNoBypass`, `TestAuth` |
| Atomic Redis Lua scripts | `lease.lua`, `give.lua` | `TestMultiReplicaAccuracy` (3 pods, 24 goroutines, **300/300**, never over) |
| Background batch sync daemon | `daemon/sweep` + give workers | `TestGiveBack`, `TestFallbackAndRecovery` (recovery with no traffic) |
| Drift compensation | give-back + outage reconciliation | `TestGiveBack`; outage run below |
| Reject unauthenticated traffic before Redis | auth is pure local HMAC | e2e: 200 bad-key requests → 200×401, `redis_calls` 0 → 0 |
| Fail-open with local approximation | fallback mode | `TestFallbackAndRecovery`; outage run below |
| Edge latency penalty < 5 ms | in-process path ~0.4–0.6 µs | benchmark + latency table below |
| Accuracy under bursts from distributed IPs | `-ips` pool in loadgen | accuracy table below |
| 100k QPS benchmark script | `bench/loadgen.go` (`-qps`, `-c`) | tool delivered; **100k not demonstrated** (see §5) |
| Edge deployment | Go binary: container / EC2 / any edge VM | local kind cluster only (see §5) |

## 4. Measured results

Environment for **all numbers below: a 1-vCPU Linux sandbox** with Redis 7, 3 gateway processes, the load
generators and a Python upstream all sharing that one core. Absolute throughput is therefore *not* representative;
ratios, enforcement accuracy and behaviour are. Re-run on your hardware (README → Benchmark) and replace this table.

**Enforcement accuracy** — 3 replicas, 16 keys, key limit cap 20 / 10 per s, 10 s, signed keys, spoofed IPs:

| Scenario | Requests | Allowed vs max | Redis calls / request |
|---|---|---|---|
| 20 IPs (dense), per-IP leases | 106,958 | 1871 vs 1920 (**−2.6 %**) | 0.082 |
| 2000 IPs (sparse), per-IP leases | 67,434 | 1901 vs 1920 (−1.0 %) | 0.981 |
| 2000 IPs (sparse), `IP_LOCAL=true` | 108,225 | 1886 vs 1920 (−1.8 %) | **0.058** |
| Baseline design (Redis on every request) | — | exact | 1.000 |

No run ever *over*-admitted. Under-admission (≤ 2.6 %) is the price of tokens sitting in short leases.
The sparse row shows the honest limit of local reservation: a bucket that sees ≲ 2 req/s cannot amortise a lease, so
every request costs a Redis call; `IP_LOCAL` removes that cost for the IP level.

**Redis outage under load** (3 replicas, 16 s, Redis killed at t = 4 s, restarted *empty* at t = 10 s):
all replicas entered fallback within 1 s, returned to normal within 2 s of restart; total admitted 29,258 vs 28,800
for a healthy run (**+1.6 %**, bounded). The earlier no-fallback design denied almost everything during an outage
(see `results.txt`: 82,539 allowed / 688,809 denied).

**Latency penalty** (1 connection, 6 s, same box; gateway path includes auth + 3-level limit + a full reverse-proxy hop):

| | p50 | p95 | p99 |
|---|---|---|---|
| Direct to upstream | 0.68 ms | 1.02 ms | 1.38 ms |
| Via gateway | 1.07 ms | 1.89 ms | 3.05 ms |
| **Penalty** | **+0.4 ms** | +0.9 ms | **+1.7 ms** (< 5 ms) |

In-process `Allow()` microbenchmark: 433 ns/op (1 core), 24 B/op, 1 alloc.

## 5. Known limitations (state these in the report)

1. **Not run on a real edge platform.** The gateway is a Go reverse proxy; it runs on EC2/containers/kind. CloudFront Functions / Cloudflare Workers were not used (they can't hold a Redis connection or local state the same way).
2. **100k QPS is not demonstrated.** The load generator supports it but one shared core cannot; it needs a multi-core generator host and gateways with real CPU.
3. **IP level assumes even load balancing when `IP_LOCAL=true`**, and fallback shares assume `REPLICAS` is correct. With autoscaling, set it from the real replica count or the effective limit scales with it.
4. **Under-admission up to a few %** from stranded leases, and a request that arrives while a lease is refilling may wait one Redis round trip (≤ 50 ms timeout; typically < 1 ms).
5. **Fail-open is a trade-off:** during an outage the limit is approximated, not exact (+1.6 % measured), and a Redis that restarts *empty* gives every bucket a fresh burst of `cap`.
6. **Auth is intentionally simple:** shared-secret HMAC keys and HS256 JWTs. No JWKS/rotation, and a single key cannot be revoked before `AUTH_CACHE_TTL_S` / secret rotation. `TRUST_XFF` must only be enabled behind a trusted proxy.
7. **Sliding window** is the standard weighted two-window counter (an approximation, not a per-request log).
8. Limits are env-configured per level, not per tenant; `/admin/limits` changes one replica at a time.
