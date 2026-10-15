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
Applies to that replica immediately (call each replica). Redis state is clamped to the new capacity on the next lease.

### Kubernetes
Add the new variables to `gateway.yaml`'s container `env`, make sure the image build copies **every** file in `gateway/`
(`*.go` and both `*.lua`), and rebuild the image under a new tag. Example additions:
```yaml
- {name: AUTH_SECRET, valueFrom: {secretKeyRef: {name: rateguard, key: auth-secret}}}
- {name: IP_LOCAL, value: "true"}
- {name: UPSTREAM_URL, value: "http://backend:8080"}
```

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
