# RateGuard — PS Compliance Checklist (updated)

`[x]` implemented **and** demonstrated (test name / console page / numbers in `DESIGN.md`) · `[ ]` still open

## 1. Edge gateway
- [x] API-key / JWT validation (HMAC, HS256), not just header presence — `TestAuth`
- [x] 401 before any Redis access — console check: 30,000 forged → 30,000 × 401, 0 Redis calls
- [x] 429 with `Retry-After`, `X-RateLimit-Level`, `X-Served-By`; `/healthz`, `/stats` (old fields preserved)
- [x] Proxies allowed requests to the real `hello` upstream inside kind (responses read `hello from …`)
- [x] Real edge platform not used; difference and AWS mapping documented — DESIGN §5.1, §6

## 2. Distributed limiting
- [x] Shared enforcement across replicas — `TestMultiReplicaAccuracy`, 3-pod kind runs (never over-admits)
- [x] Independent buckets per key — `TestKeysIndependent`
- [x] Concurrent burst from many source IPs — console *Burst accuracy* check, 5,000 spoofed IPs
- [x] Accuracy measured on the cluster: −1.5 % … −1.9 % (under-admission only) — DESIGN §4

## 3. Policies
- [x] Token bucket (capacity, refill) and sliding-window cap — `TestSlidingWindow`
- [x] Hierarchical tenant / key / IP — `TestHierarchy`, `TestLocalOnlyIP`, console *Hierarchical limits* check
- [x] Live limit change — `TestLiveLimitChange`, console policy editor (applied to every node)

## 4. L1 cache and reservations
- [x] Bounded LRU for buckets and claims; eviction cannot bypass — `TestEvictionNoBypass`
- [x] Background sync daemon (return, prefetch, recovery probe)
- [x] Drift compensation: give-back — `TestGiveBack`; recovery reconciliation — console *Redis failure* check
- [x] Redis calls per request: 0.02–0.33 (gateway) vs 1.0 (baseline)

## 5. Failure and recovery
- [x] Outage test inside kind (deployment/redis → 0 → 1 under load) — console *Redis failure* check (automated): fallback in 0.8 s, rate held, recovery in 1.8 s
- [x] Trade-offs documented — DESIGN §5.3, §5.5

## 6. Performance
- [x] Benchmark tooling: `bench/loadgen.go` + console *Latency* and *Throughput* checks
- [x] Latency penalty on the 8-vCPU box: +0.5 ms p50, +1.6–2.0 ms p99 (< 5 ms)
- [x] Baseline vs gateway at equal load — console *Latency* check
- [ ] **100k QPS over HTTP**: needs a separate multi-core generator host (laptop saturates at ~10k full-path / ~26k decision-only;
      in-process limiter ~1.3M decisions/s) — DESIGN §5.2

## 7. Deployment
- [x] Dockerfiles build the whole module (Lua embedded)
- [x] Manifests carry all env vars; secrets in the `rateguard` Secret; headless services for per-pod stats
- [x] `rebuild.bat`: creates the cluster only if missing (pinned API port via `kind-config.yaml`), fixed `:demo` tags, no ad-hoc `set env`; fresh-cluster build verified
- [x] `demo.bat`: auto-reconnecting port-forward + browser

## 8. Dashboard
- [x] Operations console (`console/`): fleet health, live policies, verification report with one-click checks; all data live

## 9. Documentation
- [x] README (run + presentation order), DESIGN (flow, PS mapping, results, limitations, AWS mapping)
