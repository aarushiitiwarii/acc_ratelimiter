# RateGuard - Problem Statement Compliance Checklist

**Project:** RateGuard: An Edge-Based Distributed Rate Limiting Middleware

**Goal:** Build and evaluate a distributed rate-limiting gateway that enforces shared API limits with low latency, local token reservations, Redis coordination, and resilience to Redis failures.

**Status legend**
- `[x]` Implemented or demonstrated by existing evidence
- `[~]` Partially implemented or requires verification
- `[ ]` Pending implementation or testing

**Important:** Implementation and successful verification are tracked separately. A feature is not considered complete until its behavior is demonstrated.

---

## 1. Edge Gateway and Request Processing

- [x] Gateway accepts HTTP requests containing an API key.
- [x] Requests without an API key return HTTP 401.
- [x] Rate-limited requests return HTTP 429.
- [x] Gateway exposes health and statistics endpoints.
- [~] Verify that unauthenticated requests are rejected before any Redis access.
- [~] Verify that the gateway proxies permitted requests to backend services.
- [ ] Validate API keys rather than checking only whether the header is present.
- [ ] Evaluate deployment on an actual edge platform or document the local Kubernetes implementation and its limitations.

## 2. Distributed Rate Limiting

- [x] Deploy multiple gateway replicas in Kubernetes.
- [x] Use Redis as the shared rate-limit state store.
- [x] Implement an atomic Redis Lua token-bucket script.
- [x] Reserve tokens locally to reduce Redis calls.
- [~] Verify strict shared rate-limit enforcement across all gateway replicas.
- [ ] Test the same API key across all replicas using repeatable distributed traffic tests.
- [ ] Verify that separate API keys maintain independent rate-limit buckets.
- [ ] Test concurrent requests and burst traffic from multiple clients.
- [ ] Measure enforcement accuracy and identify any excess or missing allowances caused by distributed reservations.

## 3. Required Rate-Limiting Policies

- [x] Implement token-bucket capacity and refill-rate controls.
- [~] Determine whether the implementation satisfies the sliding-window requirement in the PS; implement the required algorithm if necessary.
- [ ] Implement hierarchical limits per tenant, API key, and client IP.
- [ ] Define how tenant identity, API-key identity, and client IP are obtained and validated.
- [ ] Test that each policy is enforced independently and that combined limits work correctly.
- [ ] Verify behavior when configured limits are changed.

## 4. Local L1 Cache and Token Reservations

- [x] Maintain per-key in-memory token state.
- [x] Reserve token batches from Redis.
- [~] Verify local reservation expiry and token-refill behavior.
- [ ] Implement a bounded LRU cache for local entries if absent.
- [ ] Implement and verify caching of validated API-key claims.
- [ ] Verify that cache eviction does not permit rate-limit bypasses.
- [ ] Implement or verify background batch synchronization and refill scheduling.
- [ ] Implement and test drift compensation between local reservations and shared Redis state.
- [ ] Measure Redis calls per request and confirm that local reservations reduce Redis traffic.

## 5. Redis Failure and Recovery

- [x] Detect Redis errors during rate-limit operations.
- [x] Implement a local fallback approximation.
- [x] Demonstrate fallback activation and recovery on an individual gateway.
- [~] Verify that requests encountering a Redis error follow the intended fail-open policy.
- [ ] Test Redis failure independently across all gateway replicas.
- [ ] Verify local fallback limits during a prolonged Redis outage.
- [ ] Test recovery when Redis becomes available again.
- [ ] Verify that recovery does not create excessive allowances or unexpected denials.
- [ ] Document the consistency and security trade-offs of fail-open behavior.

## 6. High-Throughput Performance Evaluation

- [x] Maintain a baseline gateway for comparison.
- [x] Record preliminary baseline and optimized-gateway benchmark results in `results.txt`.
- [ ] Review the benchmark methodology, traffic generator, request counts, and test conditions.
- [ ] Re-run baseline and optimized-gateway tests under equivalent conditions.
- [ ] Measure throughput in requests per second.
- [ ] Measure average, p50, p95, and p99 latency.
- [ ] Evaluate the PS target of an edge latency penalty below 5 ms.
- [ ] Implement or verify a benchmark capable of evaluating the 100,000-QPS target.
- [ ] Test high-volume bursts from distributed clients or source IPs.
- [ ] Record rate-limit accuracy, Redis calls, errors, and resource usage during benchmarks.
- [ ] Save reproducible benchmark commands and verified results.

## 7. Docker and Kubernetes Deployment

- [x] Build Docker images for the gateway and baseline.
- [x] Deploy Redis in Kubernetes.
- [x] Deploy three gateway replicas and three baseline replicas.
- [x] Expose the gateway through a Kubernetes service.
- [~] Review `rebuild.bat` for cluster deletion, image versions, configuration resets, and deployment assumptions.
- [ ] Make rebuilds repeatable without unintentionally destroying existing demo settings.
- [ ] Ensure the intended image and configuration are deployed after every rebuild.
- [ ] Verify deployment from a fresh build.
- [ ] Document the local kind deployment and any cloud-edge deployment limitations.

## 8. Live Dashboard and Demonstration

- [x] Create the RateGuard Console dashboard.
- [x] Display live gateway replica statistics through the gateway stats endpoints.
- [x] Display per-replica allowed and denied request counts.
- [~] Verify that all three replicas remain reachable during testing.
- [~] Verify the displayed Redis and replica health indicators against actual backend state.
- [ ] Clearly distinguish live backend data from simulated dashboard visualizations.
- [ ] Demonstrate rate limiting, distributed enforcement, and Redis failure/recovery through repeatable tests.

## 9. Final Validation and Documentation

- [ ] Map every original PS requirement to its implementation and verification evidence.
- [ ] Document the architecture and request-processing flow.
- [ ] Explain Redis Lua atomic operations and shared rate-limit state.
- [ ] Explain L1 caching, token reservations, background synchronization, and drift compensation.
- [ ] Explain fallback behavior and consistency trade-offs.
- [ ] Document benchmark methodology and measured results.
- [ ] Record known limitations and unimplemented requirements.
- [ ] Update the README with setup, deployment, test, and benchmark instructions.
- [ ] Prepare the final demonstration and project presentation.

---

## Current Demonstration Configuration

Last recorded gateway settings:

- Gateway replicas: `3`
- Redis address: `redis:6379`
- `LIMIT_CAPACITY=5`
- `LIMIT_RATE=1`
- `BATCH_SIZE=1`
- `LEASE_TTL_MS=500`
- `REPLICAS=3`

The previously reported deployed image was `gateway:v5`. Verify the current deployment before relying on these values.

**Warning:** Review `rebuild.bat` before running it. The previous version was reported to recreate the kind cluster and deploy `gateway:v2` with higher rate-limit settings.

## Priority Order

1. Audit every PS requirement against the existing source code.
2. Verify and correct distributed rate-limit enforcement.
3. Complete missing L1 caching, hierarchical policies, synchronization, and drift compensation.
4. Verify Redis outage and recovery behavior across replicas.
5. Review and make the rebuild workflow safe.
6. Run reproducible performance and accuracy benchmarks.
7. Complete deployment validation, documentation, and final demonstration.

**Completion criterion:** RateGuard is ready for final submission only when every mandatory PS requirement has either been implemented and verified or explicitly documented as an outstanding limitation.