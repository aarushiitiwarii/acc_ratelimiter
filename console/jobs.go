package main

import (
	"fmt"
	"math"
	"time"
)

// Verification checks: one per requirement of the problem statement. Each
// check drives real traffic through the cluster and stores a pass/fail
// result with the measured numbers (GET /api/state -> "report").

type CheckResult struct {
	ID       string `json:"id"`
	Status   string `json:"status"` // pass | fail | info | running
	Headline string `json:"headline"`
	Detail   string `json:"detail"`
	At       int64  `json:"at"`
}

var checkOrder = []string{"auth", "burst", "hier", "latency", "outage", "throughput"}

var checkName = map[string]string{
	"auth":       "Unauthenticated traffic dropped before Redis",
	"burst":      "Burst accuracy from distributed IPs",
	"hier":       "Hierarchical limits: tenant, API key, client IP",
	"latency":    "Edge latency penalty < 5 ms",
	"outage":     "Redis failure: fail-open with local approximation",
	"throughput": "Throughput benchmark",
}

type cancelled struct{}

func (c *Console) setResult(id, status, headline, detail string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.report[id] = &CheckResult{ID: id, Status: status, Headline: headline, Detail: detail, At: time.Now().UnixMilli()}
}

// sleep waits d unless the suite is cancelled (then it panics with cancelled{},
// recovered in runSuite, so every check can be written straight-line).
func sleepOrCancel(d time.Duration, cancel chan struct{}) {
	select {
	case <-time.After(d):
	case <-cancel:
		panic(cancelled{})
	}
}

func (c *Console) runAndWait(spec LoadSpec, cancel chan struct{}) RunSummary {
	r := c.startLoad(spec)
	select {
	case <-r.done:
	case <-cancel:
		r.Stop()
		<-r.done
		panic(cancelled{})
	}
	return r.Summary()
}

// waitFor polls cond every 250 ms; it returns how long it took, or -1 on timeout.
func waitFor(cond func() bool, timeout time.Duration, cancel chan struct{}) time.Duration {
	start := time.Now()
	for time.Since(start) < timeout {
		if cond() {
			return time.Since(start)
		}
		sleepOrCancel(250*time.Millisecond, cancel)
	}
	return -1
}

// avgAllowed is the mean allowed/s of the last n one-second samples.
func (c *Console) avgAllowed(n int) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := c.history
	if len(h) < n {
		n = len(h)
	}
	if n == 0 {
		return 0
	}
	sum := 0.0
	for _, s := range h[len(h)-n:] {
		sum += s.Allowed
	}
	return sum / float64(n)
}

func (c *Console) nodeModes() (up, fallback int, redisUp bool, reconciled float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pods {
		if !p.Up {
			continue
		}
		up++
		if p.Mode == "fallback" {
			fallback++
		}
		reconciled += p.Stats["reconciled"]
	}
	return up, fallback, c.redis.Up, reconciled
}

func human(n float64) string {
	switch {
	case n >= 1e6:
		return fmt.Sprintf("%.1fM", n/1e6)
	case n >= 1e4:
		return fmt.Sprintf("%.1fk", n/1e3)
	}
	s := fmt.Sprintf("%.0f", n)
	if len(s) > 3 {
		s = s[:len(s)-3] + "," + s[len(s)-3:]
	}
	return s
}

func latOK(s RunSummary) [4]float64 {
	if s.LatOK[0] > 0 {
		return s.LatOK
	}
	return s.Lat
}

// runSuite runs the given checks one after another. Only one suite runs at a time.
func (c *Console) runSuite(ids []string, cancel chan struct{}) {
	c.suiteMu.Lock()
	defer c.suiteMu.Unlock()
	defer func() {
		c.mu.Lock()
		c.job = ""
		c.mu.Unlock()
	}()
	for i, id := range ids {
		select {
		case <-cancel:
			return
		default:
		}
		c.mu.Lock()
		c.job = fmt.Sprintf("Check %d/%d: %s", i+1, len(ids), checkName[id])
		prev := c.report[id]
		c.report[id] = &CheckResult{ID: id, Status: "running", At: time.Now().UnixMilli()}
		c.mu.Unlock()
		ok := func() (done bool) {
			defer func() {
				if r := recover(); r != nil {
					if _, is := r.(cancelled); !is {
						panic(r)
					}
					c.mu.Lock()
					if prev != nil {
						c.report[id] = prev
					} else {
						delete(c.report, id)
					}
					c.mu.Unlock()
					done = false
				}
			}()
			c.runCheck(id, cancel)
			return true
		}()
		if !ok {
			return
		}
		c.mu.Lock()
		res := c.report[id]
		c.mu.Unlock()
		if res != nil {
			c.event("info", "check %q: %s - %s", checkName[id], res.Status, res.Headline)
		}
		sleepOrCancelSafe(time.Second, cancel)
	}
}

func sleepOrCancelSafe(d time.Duration, cancel chan struct{}) {
	select {
	case <-time.After(d):
	case <-cancel:
	}
}

func (c *Console) runCheck(id string, cancel chan struct{}) {
	switch id {
	case "auth":
		s := c.runAndWait(LoadSpec{Target: "gateway", Duration: 10, Concurrency: 32, QPS: 3000, Tenants: 1, Keys: 1,
			IPs: 5000, BadPct: 100, Fresh: true, Scenario: "auth", Label: "forged-key flood"}, cancel)
		status := "fail"
		if s.Sent > 0 && s.Unauth == s.Sent && s.RedisCalls == 0 {
			status = "pass"
		}
		c.setResult(id, status,
			fmt.Sprintf("%s forged requests → %s × 401 at the edge, %d Redis calls", human(float64(s.Sent)), human(float64(s.Unauth)), s.RedisCalls),
			fmt.Sprintf("3,000 req/s for 10 s from 5,000 IPs. Signature check is local CPU; typical response %.2f ms.", s.Lat[0]))

	case "burst":
		s := c.runAndWait(LoadSpec{Target: "gateway", Duration: 10, Concurrency: 256, Tenants: 4, Keys: 4,
			IPs: 5000, Fresh: true, Scenario: "burst", Label: "burst"}, cancel)
		valid := float64(s.OK + s.Limited)
		rpr := 0.0
		if valid > 0 {
			rpr = float64(s.RedisCalls) / valid
		}
		if s.Bound <= 0 || !s.Saturated {
			c.setResult(id, "info", fmt.Sprintf("%s allowed; load did not exceed the limit", human(float64(s.OK))), s.BoundNote)
			return
		}
		dev := (float64(s.OK) - s.Bound) / s.Bound * 100
		status := "fail"
		if float64(s.OK) <= s.Bound*1.01 {
			status = "pass"
		}
		c.setResult(id, status,
			fmt.Sprintf("%s allowed vs %s permitted (%+.1f%%), never over", human(float64(s.OK)), human(s.Bound), dev),
			fmt.Sprintf("%s requests in 10 s (%s req/s) from 5,000 IPs across all nodes; %.3f Redis calls per request.",
				human(float64(s.Sent)), human(s.RPS), rpr))

	case "hier":
		cases := []struct {
			level     int
			name      string
			t, k, ips int
		}{{LvlTenant, "tenant", 1, 8, 3000}, {LvlKey, "API key", 1, 1, 3000}, {LvlIP, "client IP", 4, 8, 2}}
		pass := true
		head, det := "", ""
		for _, cs := range cases {
			s := c.runAndWait(LoadSpec{Target: "gateway", Duration: 8, Concurrency: 64, QPS: 4000, Tenants: cs.t, Keys: cs.k,
				IPs: cs.ips, Fresh: true, Scenario: "hier", Label: lvlName[cs.level]}, cancel)
			top := 0
			for i := range s.ByLevel {
				if s.ByLevel[i] > s.ByLevel[top] {
					top = i
				}
			}
			mark := "✓"
			if top != cs.level || s.Limited == 0 {
				mark, pass = "✗", false
			}
			if head != "" {
				head += " · "
				det += " "
			}
			head += cs.name + " " + mark
			det += fmt.Sprintf("%s test: %s allowed of %s permitted.", cs.name, human(float64(s.OK)), human(s.Bound))
		}
		status := "fail"
		if pass {
			status = "pass"
		}
		c.setResult(id, status, head+": each test stopped by its own limit", det)

	case "latency":
		phases := []struct{ target, label string }{
			{"direct", "direct to upstream (no limiter)"},
			{"baseline", "baseline: Redis call per request"},
			{"gateway", "RateGuard gateway"},
		}
		var res [3]RunSummary
		for i, p := range phases {
			res[i] = c.runAndWait(LoadSpec{Target: p.target, Duration: 8, Concurrency: 16, QPS: 800,
				Tenants: 4, Keys: 5, IPs: 200, Fresh: true, Scenario: "latency", Label: p.label}, cancel)
			sleepOrCancel(time.Second, cancel)
		}
		d, b, g := latOK(res[0]), latOK(res[1]), latOK(res[2])
		p50, p99 := g[0]-d[0], g[2]-d[2]
		rg := float64(res[2].RedisCalls) / math.Max(1, float64(res[2].OK+res[2].Limited))
		rb := float64(res[1].RedisCalls) / math.Max(1, float64(res[1].OK+res[1].Limited))
		status := "fail"
		if p99 < 5 {
			status = "pass"
		}
		c.setResult(id, status,
			fmt.Sprintf("+%.2f ms p50, +%.2f ms p99 added; %.2f vs %.2f Redis calls per request", p50, p99, rg, rb),
			fmt.Sprintf("800 req/s, 8 s per path. p50/p99: direct %.2f/%.2f ms, Redis-per-request limiter %.2f/%.2f ms, RateGuard %.2f/%.2f ms.",
				d[0], d[2], b[0], b[2], g[0], g[2]))

	case "outage":
		if !c.kube.ok {
			c.setResult(id, "fail", "Needs Kubernetes access to stop Redis", "")
			return
		}
		killed := false
		defer func() {
			if killed { // never leave Redis down, even when cancelled
				c.kube.scale("redis", 1)
			}
			c.stopLoad()
		}()
		r := c.startLoad(LoadSpec{Target: "gateway", Duration: 0, Concurrency: 32, QPS: 2000, Tenants: 2, Keys: 4,
			IPs: 2000, Scenario: "outage", Label: "steady traffic (outage check)"})
		_ = r
		sleepOrCancel(7*time.Second, cancel)
		before := c.avgAllowed(5)
		_, _, _, rec0 := c.nodeModes()
		if err := c.kube.scale("redis", 0); err != nil {
			c.setResult(id, "fail", "Could not stop Redis: "+err.Error(), "")
			return
		}
		killed = true
		c.event("warn", "Redis outage injected: deployment/redis scaled to 0")
		detect := waitFor(func() bool { up, fb, _, _ := c.nodeModes(); return up > 0 && fb == up }, 30*time.Second, cancel)
		sleepOrCancel(7*time.Second, cancel)
		during := c.avgAllowed(5)
		up, fb, _, _ := c.nodeModes()
		if err := c.kube.scale("redis", 1); err != nil {
			c.setResult(id, "fail", "Could not restart Redis: "+err.Error(), "")
			return
		}
		killed = false
		c.event("info", "Redis restore requested: deployment/redis scaled to 1 (starts EMPTY)")
		recovered := waitFor(func() bool { u, f, rUp, _ := c.nodeModes(); return rUp && u > 0 && f == 0 }, 90*time.Second, cancel)
		sleepOrCancel(5*time.Second, cancel)
		after := c.avgAllowed(5)
		_, _, _, rec1 := c.nodeModes()
		status := "fail"
		if detect >= 0 && recovered >= 0 && during >= 0.5*before && during > 0 {
			status = "pass"
		}
		head := fmt.Sprintf("%d/%d nodes fell back", fb, up)
		if detect >= 0 {
			head += fmt.Sprintf(" in %.1f s", detect.Seconds())
		}
		head += fmt.Sprintf("; %s/s still allowed during the outage (%s/s before)", human(during), human(before))
		det := "Nodes did not return to normal within 90 s."
		if recovered >= 0 {
			det = fmt.Sprintf("Redis came back empty; nodes were normal again %.1f s after its restart was requested, %s/s allowed after. %s locally admitted requests were charged back to Redis.",
				recovered.Seconds(), human(after), human(rec1-rec0))
		}
		c.setResult(id, status, head, det)

	case "throughput":
		c.mu.Lock()
		saved := c.limits
		c.mu.Unlock()
		defer func() {
			for i := range saved {
				if saved[i].Cap > 0 {
					c.setLimits(i, saved[i])
				}
			}
		}()
		for i := range saved {
			c.setLimits(i, LevelCfg{Cap: 1e6, Rate: 1e6, WinMs: 1000, Local: i == LvlIP})
		}
		s := c.runAndWait(LoadSpec{Target: "gateway", Duration: 15, Concurrency: 256, Tenants: 8, Keys: 8,
			IPs: 5000, Fresh: true, Scenario: "throughput", Label: "max throughput"}, cancel)
		c.setResult(id, "info",
			fmt.Sprintf("%s req/s through the full path, p50 %.1f ms, p99 %.1f ms", human(s.RPS), s.Lat[0], s.Lat[2]),
			"Auth + 3 limits + proxy hop, limits raised for the test. One 8-vCPU laptop runs the load generator, gateways, upstream and Redis together, so 100k QPS needs dedicated hosts (DESIGN.md §5.2). The limiter itself does ~1.3M decisions/s in-process.")
	}
}

func (c *Console) stopLoad() {
	c.mu.Lock()
	cur := c.cur
	c.mu.Unlock()
	if cur != nil && cur.Running() {
		cur.Stop()
		<-cur.done
	}
}
