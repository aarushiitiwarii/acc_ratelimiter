package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ---------- credentials ----------

var tenantNames = []string{"acme", "globex", "initech", "umbrella", "hooli", "stark", "wayne", "wonka",
	"tyrell", "cyberdyne", "soylent", "aperture", "oscorp", "wernham", "dunder", "vandelay"}

func tenantName(i int) string {
	if i < len(tenantNames) {
		return tenantNames[i]
	}
	return fmt.Sprintf("tenant%d", i)
}

func mintKey(secret, tenant, keyID string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(tenant + "." + keyID))
	return tenant + "." + keyID + "." + hex.EncodeToString(m.Sum(nil))[:32]
}

func mintJWT(secret, tenant, sub string, ttl time.Duration) string {
	enc := base64.RawURLEncoding
	h := enc.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	p, _ := json.Marshal(map[string]any{"tenant": tenant, "sub": sub, "exp": time.Now().Add(ttl).Unix()})
	msg := h + "." + enc.EncodeToString(p)
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(msg))
	return msg + "." + enc.EncodeToString(m.Sum(nil))
}

func ipOf(i int) string {
	return fmt.Sprintf("%d.%d.%d.%d", 11+i/62500%200, i/250%250, i%250, 1+i%7)
}

// ---------- latency histogram (lock-free, ~1-10% resolution, µs) ----------

const histN = 380

type Hist struct{ b [histN]atomic.Int64 }

func histIdx(us int64) int {
	switch {
	case us < 1000:
		return int(us / 10)
	case us < 10000:
		return 100 + int((us-1000)/100)
	case us < 100000:
		return 190 + int((us-10000)/1000)
	case us < 5000000:
		return 280 + int((us-100000)/50000)
	}
	return histN - 1
}

// histVal returns the midpoint of bucket i in milliseconds.
func histVal(i int) float64 {
	switch {
	case i < 100:
		return (float64(i)*10 + 5) / 1000
	case i < 190:
		return (1000 + float64(i-100)*100 + 50) / 1000
	case i < 280:
		return (10000 + float64(i-190)*1000 + 500) / 1000
	case i < histN-1:
		return (100000 + float64(i-280)*50000 + 25000) / 1000
	}
	return 5000
}

func (h *Hist) Add(us int64) { h.b[histIdx(us)].Add(1) }

func (h *Hist) Count() int64 {
	var n int64
	for i := range h.b {
		n += h.b[i].Load()
	}
	return n
}

// Pcts returns the requested percentiles (0..1) in ms.
func (h *Hist) Pcts(ps ...float64) []float64 {
	var c [histN]int64
	var n int64
	for i := range h.b {
		c[i] = h.b[i].Load()
		n += c[i]
	}
	out := make([]float64, len(ps))
	if n == 0 {
		return out
	}
	for j, p := range ps {
		target := int64(math.Ceil(p * float64(n)))
		if target < 1 {
			target = 1
		}
		var acc int64
		for i := range c {
			acc += c[i]
			if acc >= target {
				out[j] = histVal(i)
				break
			}
		}
	}
	return out
}

// ---------- load runs ----------

type LoadSpec struct {
	Target      string  `json:"target"`     // gateway | baseline | direct
	Duration    float64 `json:"duration_s"` // 0 = until stopped (max 15 min)
	Concurrency int     `json:"concurrency"`
	QPS         int     `json:"qps"` // 0 = closed loop (as fast as possible)
	Tenants     int     `json:"tenants"`
	Keys        int     `json:"keys"` // per tenant
	IPs         int     `json:"ips"`
	Skew        bool    `json:"skew"`    // ~80% of traffic on the first key
	BadPct      int     `json:"bad_pct"` // % of requests with a forged key
	Fresh       bool    `json:"fresh"`   // unique tenant names => buckets start full
	Scenario    string  `json:"scenario"`
	Label       string  `json:"label"`
}

type RunSummary struct {
	ID          int        `json:"id"`
	Scenario    string     `json:"scenario"`
	Label       string     `json:"label"`
	Spec        LoadSpec   `json:"spec"`
	Running     bool       `json:"running"`
	Started     int64      `json:"started"`
	Elapsed     float64    `json:"elapsed_s"`
	Sent        int64      `json:"sent"`
	OK          int64      `json:"ok"`
	Limited     int64      `json:"limited"`
	Unauth      int64      `json:"unauth"`
	Other       int64      `json:"other"`
	Errors      int64      `json:"errors"`
	ByLevel     [3]int64   `json:"by_level"`
	RPS         float64    `json:"rps"`
	Lat         [4]float64 `json:"lat_ms"`    // p50 p95 p99 p99.9, all responses
	LatOK       [4]float64 `json:"lat_ok_ms"` // same, 200 responses only
	Bound       float64    `json:"bound"`     // max requests the limits allow (gateway, fresh runs)
	BoundNote   string     `json:"bound_note"`
	Saturated   bool       `json:"saturated"` // offered load exceeded the bound
	LimitsMoved bool       `json:"limits_changed"`
	RedisCalls  int64      `json:"redis_calls"` // during the run (gateway: from pod stats)
	Creds       []string   `json:"creds,omitempty"`
}

type LoadRun struct {
	id      int
	spec    LoadSpec
	start   time.Time
	end     atomic.Int64 // unix nanos when the last worker stopped
	final   atomic.Bool  // set after onDone: every summary field is final
	stop    chan struct{}
	stopped atomic.Bool
	done    chan struct{}

	ok, limited, unauth, other, errs atomic.Int64
	byLevel                          [3]atomic.Int64
	all, okH                         Hist
	win                              atomic.Pointer[Hist] // current 1 s interval

	cfgAtStart  Config
	limitsMoved atomic.Bool
	redisStart  int64
	redisEnd    atomic.Int64
	creds       []string
	tenantsUsed int
}

func (r *LoadRun) Stop() {
	if r.stopped.CompareAndSwap(false, true) {
		close(r.stop)
	}
}

// Running stays true until onDone has recorded the end-of-run Redis counters,
// so a summary that says "finished" is complete.
func (r *LoadRun) Running() bool { return !r.final.Load() }

func (r *LoadRun) elapsed() float64 {
	if e := r.end.Load(); e != 0 {
		return time.Unix(0, e).Sub(r.start).Seconds()
	}
	return time.Since(r.start).Seconds()
}

func (r *LoadRun) Summary() RunSummary {
	s := RunSummary{ID: r.id, Scenario: r.spec.Scenario, Label: r.spec.Label, Spec: r.spec,
		Running: r.Running(), Started: r.start.UnixMilli(), Elapsed: r.elapsed(),
		OK: r.ok.Load(), Limited: r.limited.Load(), Unauth: r.unauth.Load(),
		Other: r.other.Load(), Errors: r.errs.Load(), LimitsMoved: r.limitsMoved.Load()}
	for i := range s.ByLevel {
		s.ByLevel[i] = r.byLevel[i].Load()
	}
	s.Sent = s.OK + s.Limited + s.Unauth + s.Other
	if s.Elapsed > 0 {
		s.RPS = float64(s.Sent) / s.Elapsed
	}
	copy(s.Lat[:], r.all.Pcts(.5, .95, .99, .999))
	copy(s.LatOK[:], r.okH.Pcts(.5, .95, .99, .999))
	if r.spec.Target == "gateway" {
		s.Bound, s.BoundNote = r.bound(s.Elapsed)
		s.Saturated = s.Bound > 0 && float64(s.OK+s.Limited) > s.Bound*1.05
		if e := r.redisEnd.Load(); e != 0 {
			s.RedisCalls = e - r.redisStart
		}
	} else if r.spec.Target == "baseline" {
		s.RedisCalls = s.OK + s.Limited // one synchronous Redis call per authenticated request
	}
	if len(r.creds) <= 4 {
		s.Creds = r.creds
	}
	return s
}

// bound is the most requests the configured hierarchy may admit during the
// run, assuming fresh (full) buckets and evenly spread IPs:
// min( Σ_tenants min(tenant budget, keys × key budget), IPs × IP budget ).
func (r *LoadRun) bound(d float64) (float64, string) {
	if !r.spec.Fresh {
		return 0, "re-used buckets (not fresh): no exact bound"
	}
	if r.limitsMoved.Load() {
		return 0, "limits changed during the run: no exact bound"
	}
	c := r.cfgAtStart
	if c[LvlKey].Cap == 0 {
		return 0, "limits unknown"
	}
	budget := func(l LevelCfg) float64 {
		b := l.Cap + l.Rate*d
		if l.WinMs > 0 && l.WinLimit > 0 {
			w := float64(l.WinLimit) * (d*1000/float64(l.WinMs) + 1)
			b = math.Min(b, w)
		}
		return b
	}
	keyB, tenB := budget(c[LvlKey]), budget(c[LvlTenant])
	total := 0.0
	binding := "key"
	for t := 0; t < r.tenantsUsed; t++ {
		k := float64(r.spec.Keys) * keyB
		if tenB < k {
			k = tenB
			binding = "tenant"
		}
		total += k
	}
	ipTotal := float64(r.spec.IPs) * budget(c[LvlIP])
	if ipTotal < total {
		total = ipTotal
		binding = "ip"
	}
	return math.Floor(total), binding + " level binds"
}

type loadEngine struct {
	secret                         string
	gatewayURL, baseURL, directURL string
}

func (e *loadEngine) targetURL(t string) string {
	switch t {
	case "baseline":
		return e.baseURL
	case "direct":
		return e.directURL
	}
	return e.gatewayURL
}

func normalize(s *LoadSpec) {
	clamp := func(v *int, lo, hi, def int) {
		if *v == 0 {
			*v = def
		}
		if *v < lo {
			*v = lo
		}
		if *v > hi {
			*v = hi
		}
	}
	if s.Target != "baseline" && s.Target != "direct" {
		s.Target = "gateway"
	}
	clamp(&s.Concurrency, 1, 4096, 64)
	clamp(&s.Tenants, 1, 1000, 1)
	clamp(&s.Keys, 1, 1000, 1)
	clamp(&s.IPs, 1, 1000000, 1)
	if s.QPS < 0 {
		s.QPS = 0
	}
	if s.BadPct < 0 {
		s.BadPct = 0
	}
	if s.BadPct > 100 {
		s.BadPct = 100
	}
	if s.Duration < 0 || s.Duration > 900 {
		s.Duration = 900
	}
}

// start launches a load run; it finishes after spec.Duration (or Stop()).
func (e *loadEngine) start(id int, spec LoadSpec, cfg Config, redisStart int64,
	onDone func(*LoadRun)) *LoadRun {
	normalize(&spec)
	r := &LoadRun{id: id, spec: spec, start: time.Now(), stop: make(chan struct{}),
		done: make(chan struct{}), cfgAtStart: cfg, redisStart: redisStart, tenantsUsed: spec.Tenants}
	r.win.Store(&Hist{})

	suffix := ""
	if spec.Fresh {
		suffix = fmt.Sprintf("-r%d", id)
	}
	for t := 0; t < spec.Tenants; t++ {
		for k := 0; k < spec.Keys; k++ {
			r.creds = append(r.creds, mintKey(e.secret, tenantName(t)+suffix, fmt.Sprintf("key%d", k)))
		}
	}
	bad := "forged.key0." + "00000000000000000000000000000000"
	ips := make([]string, spec.IPs)
	for i := range ips {
		ips[i] = ipOf(i)
	}
	url := e.targetURL(spec.Target)

	tr := &http.Transport{MaxIdleConns: spec.Concurrency * 2, MaxIdleConnsPerHost: spec.Concurrency * 2,
		IdleConnTimeout: 30 * time.Second, DisableCompression: true}
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	var interval time.Duration
	if spec.QPS > 0 {
		interval = time.Duration(float64(time.Second) * float64(spec.Concurrency) / float64(spec.QPS))
	}
	var deadline time.Time
	if spec.Duration > 0 {
		deadline = r.start.Add(time.Duration(spec.Duration * float64(time.Second)))
	} else {
		deadline = r.start.Add(15 * time.Minute)
	}

	var wg sync.WaitGroup
	for w := 0; w < spec.Concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)*7919 + time.Now().UnixNano()))
			next := time.Now().Add(time.Duration(rng.Int63n(int64(interval) + 1)))
			for {
				select {
				case <-r.stop:
					return
				default:
				}
				now := time.Now()
				if now.After(deadline) {
					return
				}
				if interval > 0 {
					if d := next.Sub(now); d > 0 {
						time.Sleep(d)
					}
					next = next.Add(interval)
					if behind := time.Since(next); behind > time.Second { // don't burst to catch up
						next = time.Now()
					}
				}
				ci := rng.Intn(len(r.creds))
				if spec.Skew && rng.Intn(100) < 80 {
					ci = 0
				}
				key := r.creds[ci]
				if spec.BadPct > 0 && rng.Intn(100) < spec.BadPct {
					key = bad
				}
				req, _ := http.NewRequest("GET", url, nil)
				req.Header.Set("X-API-Key", key)
				req.Header.Set("X-Forwarded-For", ips[rng.Intn(len(ips))])
				t0 := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					r.errs.Add(1)
					select {
					case <-r.stop:
					case <-time.After(20 * time.Millisecond):
					}
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				us := time.Since(t0).Microseconds()
				r.all.Add(us)
				r.win.Load().Add(us)
				switch resp.StatusCode {
				case 200:
					r.ok.Add(1)
					r.okH.Add(us)
				case 429:
					r.limited.Add(1)
					switch resp.Header.Get("X-RateLimit-Level") {
					case "tenant":
						r.byLevel[LvlTenant].Add(1)
					case "key":
						r.byLevel[LvlKey].Add(1)
					case "ip":
						r.byLevel[LvlIP].Add(1)
					}
				case 401:
					r.unauth.Add(1)
				default:
					r.other.Add(1)
				}
			}
		}(w)
	}
	go func() {
		wg.Wait()
		r.end.Store(time.Now().UnixNano())
		tr.CloseIdleConnections()
		if onDone != nil {
			onDone(r)
		}
		r.final.Store(true)
		close(r.done)
	}()
	return r
}
