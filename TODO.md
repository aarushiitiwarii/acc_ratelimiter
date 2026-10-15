# RateGuard — PS Compliance Checklist (updated)

`[x]` implemented **and** demonstrated (test name / result in `DESIGN.md`) · `[ ]` still to do on your side

## 1. Edge gateway
- [x] API-key / JWT validation (HMAC, HS256), not just header presence — `TestAuth`
- [x] 401 before any Redis access — e2e: 200 bad keys → 200×401, redis_calls 0 → 0
- [x] 429 with `Retry-After` and `X-RateLimit-Level`; `/healthz`, `/stats` (old fields preserved)
- [x] Proxies allowed requests to `UPSTREAM_URL` (verified end-to-end through a real upstream)
- [ ] Verify the proxy path against your actual backend service inside kind
- [ ] Document kind/EC2 deployment vs a real edge platform (see DESIGN §5.1)

## 2. Distributed limiting
- [x] Shared enforcement across replicas — `TestMultiReplicaAccuracy` (300/300), 3-process e2e (never over-admits)
- [x] Independent buckets per key — `TestKeysIndependent`
- [x] Concurrent burst from many source IPs — loadgen e2e, 2000 spoofed IPs, ~108k requests
- [x] Accuracy measured: −1.0 % … −2.6 % (under-admission only)
- [ ] Repeat the accuracy run on your cluster and paste numbers into DESIGN §4

## 3. Policies
- [x] Token bucket (capacity, refill)
- [x] Sliding-window cap — `TestSlidingWindow`
- [x] Hierarchical tenant / key / IP — `TestHierarchy`, `TestLocalOnlyIP`
- [x] Identity sources defined: tenant+key from credential claims, IP from socket (or XFF if trusted)
- [x] Live limit change (`/admin/limits`) — Lua takes limits per call, so changes apply on the next lease
- [ ] Add a test/demo of a live limit change (curl before/after) for the presentation

## 4. L1 cache and reservations
- [x] Bounded LRU for buckets and claims; eviction cannot bypass — `TestEvictionNoBypass`
- [x] Background sync daemon (return, prefetch, recovery probe)
- [x] Drift compensation: give-back — `TestGiveBack`; recovery reconciliation — outage run
- [x] Redis calls per request measured: 0.058–0.082 (dense / IP_LOCAL) vs 1.0 baseline

## 5. Failure and recovery
- [x] Fallback on every replica; recovery with no traffic — `TestFallbackAndRecovery`, 3-pod outage run
- [x] Bounded error during outage: +1.6 % (Redis restarted empty)
- [x] Trade-offs documented — DESIGN §5.5
- [ ] Repeat the outage test in kind with `kubectl scale deploy/redis --replicas=0` then back to 1

## 6. Performance
- [x] Benchmark tool: distributed IPs, multi-tenant keys, percentiles, accuracy, `-qps` pacing
- [x] Latency penalty measured: +0.4 ms p50, +1.7 ms p99 (1-core box)
- [ ] **Run on multi-core hardware**: baseline vs gateway at equal load; aim at 100k QPS with an external generator host
- [ ] Save exact commands + output in `results.txt`

## 7. Deployment
- [ ] Update `Dockerfile.gateway` so it copies all of `gateway/` (`*.go`, `lease.lua`, `give.lua`)
- [ ] Add new env vars to `gateway.yaml` (`AUTH_SECRET` as a Secret, `IP_LOCAL`, `UPSTREAM_URL`, `REPLICAS`)
- [ ] Fix `rebuild.bat` (pin image tag, don't recreate cluster, don't reset limits)
- [ ] Fresh-build verification

## 8. Dashboard
- [ ] Optionally show the new `/stats` fields (`denied_tenant/key/ip`, `auth_hits`, `tokens_returned`, `reconciled`)
- [ ] Label live vs simulated panels

## 9. Documentation
- [x] README, DESIGN (flow, PS mapping, results, limitations)
- [ ] Final presentation / demo script
