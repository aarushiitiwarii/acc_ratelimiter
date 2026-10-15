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
| Local-only IP level | `LevelCfg.LocalOnly` (`IP_LOCAL`, on by default in `gateway.yaml`) | IP bucket is a per-replica share, zero Redis traffic. Can be toggled live (`/admin/limits?local=`). |
| Operations console | `console/`, `console.yaml` | In-cluster service + dashboard: fleet health, live policies on every node, verification checks per PS requirement (load generator, Redis kill/restore through the Kubernetes API, latency comparison). |

## 3. PS requirement → implementation → evidence

| PS requirement | Implementation | Evidence (test / console page) |
|---|---|---|
| Hierarchical bucket per tenant / API key / IP | 3 levels, one Redis key each | `TestHierarchy`, `TestKeysIndependent`, `TestLocalOnlyIP`; console check **Hierarchical limits** (each case stopped by its own level) |
| Sliding-window token bucket | `lease.lua` bucket + window counter | `TestSlidingWindow`; window limit editable on the console |
| Local L1 LRU cache (keys + counters) | `lru.go`; claims + bucket caches | `TestEvictionNoBypass`, `TestAuth`; L1 bucket count per node on the console |
| Atomic Redis Lua scripts | `lease.lua`, `give.lua` | `TestMultiReplicaAccuracy` (3 replicas, 24 goroutines, **300/300**, never over) |
| Background batch sync daemon | `daemon/sweep` + give workers | `TestGiveBack`, `TestFallbackAndRecovery` (recovery with no traffic) |
| Drift compensation | give-back + outage reconciliation | `TestGiveBack`; console check **Redis failure** (reconciled count) |
| Reject unauthenticated traffic before Redis | auth is pure local HMAC/JWT | console check **Unauthenticated traffic dropped**: 30,000 forged requests → 30,000 × 401, **0** Redis calls |
| Fail-open with local approximation | fallback mode | `TestFallbackAndRecovery`; console check **Redis failure**: 3/3 nodes fell back in 0.8 s, recovered 1.8 s after restart |
| Live limit changes | limits passed to Lua on every lease; `/admin/limits` | `TestLiveLimitChange`; console policy editor (50 → 200 → 10/s followed within ~1 s) |
| Edge latency penalty < 5 ms | in-process decision ~0.7 µs | console check **Edge latency penalty**: +0.3 … +0.5 ms p50, +0.6 … +2.0 ms p99 |
| Accuracy under bursts from distributed IPs | spoofed `X-Forwarded-For` pool (up to 20,000 IPs) | console check **Burst accuracy**: −1.1 % … −1.9 %, never over |
| 100k QPS benchmark script | `bench/loadgen.go`; console check **Throughput benchmark** | delivered; **100k over HTTP not reached on one laptop** (§5.2) |
| Edge deployment | Go binary on kind; maps to CloudFront → ECS/EC2 + ElastiCache (§6) | kind cluster only (§5.1) |

## 4. Measured results

**Environment: one laptop, Docker Desktop VM with 8 vCPU / 8 GB, kind cluster `edge`.** The load generator (console pod),
3 gateway replicas, 3 upstream (`hello`) replicas, 3 baseline replicas and Redis 7 all share those 8 vCPUs. Every number was
produced from the demo console and can be reproduced from the console steps (full numbers under each step's Details). Absolute throughput is a floor, not a ceiling.
Default limits (`gateway.yaml`): tenant 400 + 200/s, key 100 + 50/s, IP 60 + 30/s (local-only), batch ≤ 50, lease TTL 500 ms.

**Enforcement accuracy** (fresh buckets, 10 s; "bound" = most requests the configured limits permit):

| Scenario | Requests | Allowed vs bound | Redis calls / request | req/s |
|---|---|---|---|---|
| Burst: 4 tenants × 4 keys, 5,000 IPs, 256 workers, per-IP Redis leases | 138,462 | 9,432 / 9,612 (**−1.9 %**) | 0.90 | 13.8k |
| Same, `IP_LOCAL=true` | 211,671 | 9,501 / 9,642 (**−1.5 %**) | **0.048** | 21.1k |
| Tenant binds: 1 tenant × 8 keys, 3,000 IPs, 4,000 req/s offered | 40,054 | 2,390 / 2,402 (−0.5 %) | 0.04 | 4.0k |
| Key binds: 1 key, 3,000 IPs | 40,044 | 600 / 600 (0.0 %) | 0.02 | 4.0k |
| IP binds: 32 keys from 2 IPs | 40,061 | 720 / 720 (0.0 %) | 0.11 | 4.0k |
| Baseline design (Redis on every request) | — | exact | 1.000 | — |

No run ever *over*-admitted. Under-admission (≤ 2 %) is the cost of tokens stranded in short leases.

**Latency penalty** (console check "Edge latency penalty": 800 req/s paced, 16 workers, 8 s per phase, 4 tenants × 5 keys, 200 IPs;
latency of 200 responses measured at the in-cluster client):

| Path | p50 | p95 | p99 | Redis calls / request |
|---|---|---|---|---|
| Direct to upstream | 0.63 ms | 1.15 ms | 1.55 ms | 0 |
| Baseline (sync Redis per request) | 1.15 ms | 1.95 ms | 2.85 ms | 1.000 |
| RateGuard gateway (auth + 3 levels + proxy hop) | 1.15 ms | 2.15 ms | 3.55 ms | **0.25–0.33** |
| **Gateway penalty vs direct** | **+0.5 ms** | +1.0 ms | **+1.6 … +2.0 ms** (< 5 ms) | |

The gateway's penalty is mostly the extra HTTP proxy hop; the decision itself costs microseconds. With Redis inside the same
cluster the baseline is not slower in latency. Its cost is Redis load (1 call per request), which grows with traffic and with
the distance between edge and Redis.

**Redis outage under load** (2 tenants × 4 keys, 2,000 req/s offered, binding limit 2 × 200/s): allowed stayed at
~400/s before, during and after the outage. All 3 nodes entered fallback within ~1 s of `deployment/redis` scaling to 0
and returned to normal within ~2 s of the (empty) Redis becoming ready; each node reconciled ~4.2k locally admitted tokens.
Fallback shares assume an even load balancer: when one node receives no traffic, its share goes unused and the outage under-admits.

**Live limit change**: steady 1,500 req/s on one key, key rate changed 50 → 200 → 10/s from the console: the allowed rate
followed (≈50 → ≈200 → ≈10/s) within about a second on all nodes, with no restart.

**Throughput** (closed loop, all limits raised to 1M):
- 256 workers: **10.0k req/s** through the full path (auth, 3 levels, proxy to `hello`), p50 18.5 ms of CPU queueing, because the
  generator, gateways and upstreams share 8 vCPUs. 1,024 workers: 8.1k req/s, so the box is saturated.
- Best 1-second rate observed: **25.7k req/s** (mostly 429s, which skip the proxy hop).
- In-process decision path (`BenchmarkAllowParallel`, Redis in Docker): 650–775 ns/op, **~1.3–1.5M decisions/s per process**.
  It stays flat from 1 to 8 cores because the benchmark drives one hot tenant bucket (one mutex).
  The limiter is not the bottleneck here; HTTP handling and proxying on shared CPUs are.

## 5. Known limitations (state these in the report)

1. **Not run on a real edge platform.** The gateway is a Go reverse proxy on kind. CloudFront Functions / Cloudflare Workers
   cannot hold a Redis connection or local state the same way. §6 maps the deployment to AWS.
2. **100k QPS not demonstrated over HTTP.** One 8-vCPU laptop runs the generator, 3 gateways, 3 upstreams, 3 baselines and
   Redis together and saturates at ~10k full-path / ~26k decision-only req/s. The in-process limiter does ~1.3M decisions/s.
   Reaching 100k QPS needs a separate multi-core generator host and gateway replicas with dedicated CPU. `bench/loadgen -qps 100000`
   is ready for that run.
3. **IP level assumes even load balancing when `IP_LOCAL=true`**, and fallback shares assume `REPLICAS` is correct. A client
   pinned to one node (one keep-alive connection) gets 1/REPLICAS of its IP budget. With autoscaling, set `REPLICAS` from the
   real replica count.
4. **Under-admission up to ~2 %** from stranded leases. A request that arrives while a lease is refilling may wait one Redis
   round trip (≤ 50 ms timeout, typically < 1 ms).
5. **Fail-open is a trade-off:** during an outage the limit is approximated, not exact. A Redis that restarts *empty* gives
   every bucket a fresh burst of `cap`; reconciliation then charges the outage's admissions back.
6. **Auth is intentionally simple:** shared-secret HMAC keys and HS256 JWTs. There is no JWKS or rotation, and a key cannot be
   revoked before `AUTH_CACHE_TTL_S` expires or the secret rotates. `gateway.yaml` sets `TRUST_XFF=true` only so the demo can
   simulate distributed clients; enable it only behind a trusted CDN or load balancer.
7. **Sliding window** is the standard weighted two-window counter (an approximation, not a per-request log).
8. Limits are configured per level, not per tenant. The console applies a change to every node; a single
   `/admin/limits` call changes one node, and a restarted pod returns to the manifest values.
9. The console holds the admin token and auth secret and can scale Redis (its Role is limited to `deployment/redis`). It is a
   demo tool: do not expose it outside the cluster without authentication.

## 6. Mapping to AWS (target deployment, not executed)

| This repo (kind) | AWS |
|---|---|
| `gateway` Deployment, 3 replicas | Gateway containers on ECS/EKS or EC2 per region behind an ALB; CloudFront in front for TLS and a trusted `X-Forwarded-For` (enables `TRUST_XFF`) |
| `redis` Deployment | ElastiCache for Redis (cluster mode). Each script touches one key, so it is slot-safe |
| `hello` upstream | The protected microservices |
| `REPLICAS` env | Set from the ECS desired count / HPA |
| `rateguard` Secret | Secrets Manager / SSM Parameter Store |
| Console "Kill Redis" | ElastiCache failover test or a security-group block |
