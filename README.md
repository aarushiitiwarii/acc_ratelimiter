# RateGuard — Edge-Based Distributed Rate Limiting Middleware

Go gateway that enforces **hierarchical (tenant → API key → client IP)** rate limits
at the edge. Decisions are made from local token reservations (L1); a shared Redis
holds the authoritative state and is only contacted in batches.

See `DESIGN.md` for architecture, PS-requirement mapping, measured results and limitations.

## Layout

| Path | Purpose |
|---|---|
| `gateway/main.go` | HTTP server, auth → limit → proxy pipeline, `/stats`, `/admin/limits` |
| `gateway/limiter.go` | L1 token reservations, hierarchy, sync daemon, drift compensation, fallback |
| `gateway/auth.go` | HMAC API keys + HS256 JWT validation, LRU claims cache |
| `gateway/lru.go` | Bounded sharded LRU (bucket cache + claims cache) |
| `gateway/lease.lua` | Atomic Redis script: token bucket + sliding-window cap |
| `gateway/give.lua` | Atomic Redis script: return unused leased tokens |
| `gateway/limiter_test.go` | Integration tests (need a Redis) + microbenchmark |
| `bench/loadgen.go` | Load generator with distributed IPs, multi-tenant keys, accuracy report |
| `console/` | Operations console: dashboard (`web/index.html`) + in-cluster API (fleet stats, policies, verification checks, test traffic) |
| `baseline/` | Comparison limiter: one synchronous Redis call per request |
| `main.go` | `hello` upstream service the gateway protects |
| `*.yaml`, `rebuild.bat`, `demo.bat` | kind deployment and demo launcher |

## Run the demo (Windows, Docker Desktop + kind)

```bat
rebuild.bat      :: builds all images, creates the kind cluster if missing, deploys everything
demo.bat         :: port-forwards the console and opens http://localhost:9000
```

`rebuild.bat` is safe to re-run. It reuses the cluster, takes limits only from the manifests, tags images `:demo`
(`imagePullPolicy: Never`) and runs `rollout restart` so the new build is picked up. Secrets come from `AUTH_SECRET` /
`ADMIN_TOKEN` environment variables (defaults `demo123` / `admin123`) and are stored in the `rateguard` Secret.

`rebuild.bat` creates the cluster from `kind-config.yaml`, which pins the API server to port 6443. Kind's default random
port can later be reserved by Windows (Hyper-V/WinNAT), which leaves the cluster unreachable.

### The console

One operations dashboard for the gateway fleet:

| Area | Shows |
|---|---|
| Header | Edge-node and Redis health, running activity, **Test traffic**, **Run all checks** |
| KPI tiles | Requests/s, allowed/s, throttled (429)/s, blocked (401)/s, Redis calls per request, added latency p99 |
| Traffic + Edge nodes | Live chart (last 2 min) next to per-node state (normal / local fallback), Redis status, activity feed |
| Rate-limit policies | Tenant, API key and client IP limits; **Edit** applies a change to every node immediately |
| Verification | One row per problem-statement requirement, with pass/fail, the measured numbers and a **Run** button |

**Run all checks** (about 3 minutes) runs every verification in the console backend against the live cluster:

| Check | What it does |
|---|---|
| Unauthenticated traffic dropped before Redis | 30,000 forged-key requests; passes if all get 401 and the gateways make 0 Redis calls |
| Burst accuracy from distributed IPs | 256 workers, 5,000 IPs, 10 s at max rate; passes if allowed ≤ the limits' bound (never over) |
| Hierarchical limits | Three runs, each designed so the tenant, key or IP limit binds; passes if each is stopped by its own level |
| Edge latency penalty < 5 ms | Same paced traffic direct, through the Redis-per-request baseline and through RateGuard; passes if the added p99 < 5 ms |
| Redis failure | Steady traffic, stops Redis, measures fallback and allowed rate, restarts Redis, measures recovery; always restores Redis |
| Throughput benchmark | 15 s at max rate with limits raised (then restored); reported as a measurement |

**Test traffic** (drawer) generates manual traffic (rate, duration, tenants, keys, IPs, % forged keys) and holds the failure
controls (stop/start/flush Redis) for live demonstrations. Each run first waits up to ~2 s for the previous traffic's background
sync to settle, so its Redis-call count is its own. Results live in memory and reset when the console pod restarts.

## Run the tests

```bash
redis-server --port 6390 --save "" --daemonize yes
go test ./gateway -race -count=1 -v          # REDIS_TEST_ADDR overrides 127.0.0.1:6390
go test ./gateway -run xxx -bench Allow -benchtime=3s
```
Tests skip (not fail) if Redis is unreachable. `TestFallbackAndRecovery` also needs the `redis-server` binary.

## Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_ADDR` | `redis:6379` | shared state store |
| `PORT` | `8080` | listen port |
| `UPSTREAM_URL` | *(empty)* | reverse-proxy target; empty = reply `allowed by <pod>` (old demo behaviour) |
| `REPLICAS` | `3` | number of gateway replicas (sizes fallback shares and max lease) |
| `KEY_CAP` / `KEY_RATE` | `LIMIT_CAPACITY` / `LIMIT_RATE`, else 100 / 10 | per-API-key burst / tokens per second |
| `TENANT_CAP` / `TENANT_RATE` | 10× key | per-tenant limits |
| `IP_CAP` / `IP_RATE` | 10× key | per-client-IP limits |
| `IP_LOCAL` | `false` | enforce the IP level locally (per-replica share, no Redis) — recommended for many sparse IPs |
| `WINDOW_MS`, `{KEY,TENANT,IP}_WINDOW_LIMIT` | `1000`, `0` | sliding-window cap (requests per window); `0` disables |
| `BATCH_SIZE` | `20` | **maximum** lease size (leases adapt between 1 and this) |
| `LEASE_TTL_MS` | `500` | how long a local reservation lives before unused tokens are returned |
| `BUCKET_CACHE_SIZE` | `100000` | L1 bucket LRU capacity |
| `AUTH_SECRET` | *(empty)* | HMAC secret. **Empty = legacy demo mode** (any non-empty `X-API-Key`, tenant `default`) |
| `AUTH_CACHE_SIZE`, `AUTH_CACHE_TTL_S` | `50000`, `300` | validated-claims LRU |
| `TRUST_XFF` | `false` | take client IP from `X-Forwarded-For` (only behind a trusted CDN/LB; spoofable otherwise) |
| `ADMIN_TOKEN` | *(empty)* | enables `/admin/limits` when set |

Old deployments keep working: `LIMIT_CAPACITY`/`LIMIT_RATE` map to the key level and
`/stats` keeps its original leading fields (new fields are appended).

### Credentials
```bash
AUTH_SECRET=s3cret ./gateway mint acme key1        # -> acme.key1.<sig>   (send as X-API-Key)
# or Authorization: Bearer <HS256 JWT with claims: tenant, sub, exp>
```
Missing/invalid credentials → `401` **before any Redis access**.

### Live limit change
```bash
curl -XPOST -H "X-Admin-Token: $ADMIN_TOKEN" \
  'http://pod:8080/admin/limits?level=key&cap=50&rate=5&window_ms=1000&window_limit=40'
```
Applies to that replica immediately (call each replica, or use the console, which applies to all of them). Redis state is
clamped to the new capacity on the next lease. `local=true|false` switches a level between Redis-backed and local-only.

### Kubernetes
`gateway.yaml` sets every variable above for the demo (`AUTH_SECRET` and `ADMIN_TOKEN` from the `rateguard` Secret,
`IP_LOCAL=true`, `UPSTREAM_URL=http://hello`, `REPLICAS=3`, `TRUST_XFF=true` for the spoofed-IP load generator). Keep
`REPLICAS` equal to `spec.replicas`. The Lua scripts are compiled into the binary (`go:embed`), so the image only needs the Go build.

## Benchmark

```bash
go build -o loadgen ./bench
# gateways need TRUST_XFF=true so the generator can spoof distributed client IPs
./loadgen -url http://GATEWAY/ -c 256 -d 30s -tenants 8 -keys 8 -ips 5000 \
          -secret $AUTH_SECRET -key-cap 5000 -key-rate 1000          # throughput, p50/p95/p99, accuracy
./loadgen -url http://BACKEND/  -c 256 -d 30s -tenants 8 -keys 8 -ips 5000   # same load, no gateway => latency penalty
./loadgen ... -qps 100000                                            # paced / open-loop target rate
```
Accuracy line: `allowed` vs `expected` = Σkeys (cap + rate × duration); negative deviation = under-admission,
positive = over-admission. Pass `-expected N` if a tenant/IP level binds instead of the key level.
Redis calls per request: diff the `redis_calls` counter on every replica's `/stats` before/after, divided by requests.

**To reach 100k QPS** run the generator on a separate multi-core machine (or several in parallel), point it at the
gateway replicas through the Service/load balancer, and give the gateways real CPU requests/limits. A single laptop
core cannot generate 100k req/s of HTTP traffic *and* serve it.
