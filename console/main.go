// console: RateGuard demo console. Runs inside the cluster next to the
// gateways, serves the dashboard (web/index.html) and exposes a small API to
// drive every part of the demo from the browser:
//
//	GET  /api/state           live pods, Redis, limits, 1 s history, runs, events
//	POST /api/load            start a load run          POST /api/load/stop
//	POST /api/verify          run verification checks (one per PS requirement)
//	POST /api/limits          change limits on every gateway pod (+ /reset)
//	POST /api/redis           {"action":"kill"|"restore"|"flush"}
//	POST /api/try             send a few hand-made requests through the gateway
package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed web
var webFS embed.FS

const (
	LvlTenant = 0
	LvlKey    = 1
	LvlIP     = 2
)

var lvlName = [3]string{"tenant", "key", "ip"}

type LevelCfg struct {
	Cap      float64 `json:"cap"`
	Rate     float64 `json:"rate"`
	WinMs    int64   `json:"window_ms"`
	WinLimit int64   `json:"window_limit"`
	Local    bool    `json:"local"`
}

type Config [3]LevelCfg

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ---------- live state ----------

type Pod struct {
	Name  string             `json:"name"`
	IP    string             `json:"ip"`
	Up    bool               `json:"up"`
	Mode  string             `json:"mode"`
	Stats map[string]float64 `json:"stats"`
	Rates map[string]float64 `json:"rates"` // per second over the last poll interval
	seen  time.Time
}

type Sample struct {
	T          int64              `json:"t"`
	Allowed    float64            `json:"allowed"`
	Denied     float64            `json:"denied"`
	DenTenant  float64            `json:"den_tenant"`
	DenKey     float64            `json:"den_key"`
	DenIP      float64            `json:"den_ip"`
	Unauth     float64            `json:"unauth"`
	RedisCalls float64            `json:"redis_calls"`
	RedisOps   float64            `json:"redis_ops"`
	BaseOK     float64            `json:"base_allowed"`
	BaseDenied float64            `json:"base_denied"`
	Fallback   int                `json:"fallback_pods"`
	PodsUp     int                `json:"pods_up"`
	RedisUp    bool               `json:"redis_up"`
	PerPod     map[string]float64 `json:"per_pod"`
	LoadRPS    *float64           `json:"load_rps"` // nil while no load runs
	LoadP50    *float64           `json:"load_p50"`
	LoadP99    *float64           `json:"load_p99"`
}

type Event struct {
	T    int64  `json:"t"`
	Kind string `json:"kind"` // info | warn | ok | limit | load
	Msg  string `json:"msg"`
}

type RedisInfo struct {
	Up       bool    `json:"up"`
	Desired  int     `json:"desired"`
	Ready    int     `json:"ready"`
	Keys     int64   `json:"keys"`
	OpsSec   float64 `json:"ops_sec"`
	Commands float64 `json:"commands"`
	Error    string  `json:"error,omitempty"`
}

type Console struct {
	secret, adminToken string
	gwPods, basePods   string
	eng                *loadEngine
	rdb                *redis.Client
	kube               *kube
	hc                 *http.Client

	mu        sync.Mutex
	pods      map[string]*Pod // gateway pods by IP
	base      map[string]*Pod // baseline pods by IP
	redis     RedisInfo
	deploys   map[string]deployStatus
	limits    Config
	defaults  *Config
	history   []Sample
	events    []Event
	runs      []*LoadRun
	cur       *LoadRun
	nextID    int
	job       string // running verification check, shown in the UI
	jobCancel chan struct{}
	suiteMu   sync.Mutex // one verification suite at a time
	report    map[string]*CheckResult
	lastCmds  float64
	lastPoll  time.Time
	lastEnd   time.Time // when the previous load run finished
}

func (c *Console) event(kind, f string, a ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventLocked(kind, f, a...)
}

func (c *Console) eventLocked(kind, f string, a ...any) {
	c.events = append(c.events, Event{time.Now().UnixMilli(), kind, fmt.Sprintf(f, a...)})
	if len(c.events) > 200 {
		c.events = c.events[len(c.events)-200:]
	}
}

func parseStats(s string) map[string]string {
	m := map[string]string{}
	for _, f := range strings.Fields(s) {
		if k, v, ok := strings.Cut(f, "="); ok {
			m[k] = v
		}
	}
	return m
}

func (c *Console) get(u string) (string, error) {
	resp, err := c.hc.Get(u)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("%s", resp.Status)
	}
	return string(b), nil
}

// pollPods fetches /stats from every pod behind a headless service.
func (c *Console) pollPods(svc string) map[string]map[string]string {
	ips, _ := net.LookupHost(svc)
	out := map[string]map[string]string{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ip := range ips {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			s, err := c.get("http://" + net.JoinHostPort(ip, "8080") + "/stats")
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				out[ip] = nil
				return
			}
			out[ip] = parseStats(s)
		}(ip)
	}
	wg.Wait()
	return out
}

func (c *Console) mergePods(dst map[string]*Pod, got map[string]map[string]string, now time.Time,
	gateway bool) {
	for ip, st := range got {
		p := dst[ip]
		if p == nil {
			p = &Pod{IP: ip, Stats: map[string]float64{}, Rates: map[string]float64{}}
			dst[ip] = p
		}
		p.seen = now
		if st == nil {
			if p.Up && gateway {
				c.eventLocked("warn", "edge node %s stopped answering", p.Name)
			}
			p.Up = false
			p.Rates = map[string]float64{}
			continue
		}
		if !p.Up && p.Name != "" && gateway {
			c.eventLocked("ok", "edge node %s is answering", st["pod"])
		}
		p.Up = true
		p.Name = st["pod"]
		mode := st["mode"]
		if mode == "" {
			mode = "normal"
		}
		if gateway && p.Mode != "" && p.Mode != mode {
			if mode == "fallback" {
				c.eventLocked("warn", "%s switched to FALLBACK (local approximation)", p.Name)
			} else {
				c.eventLocked("ok", "%s back to NORMAL (Redis reachable, debt reconciled)", p.Name)
			}
		}
		p.Mode = mode
		dt := now.Sub(c.lastPoll).Seconds()
		rates := map[string]float64{}
		for k, v := range st {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				continue
			}
			if old, ok := p.Stats[k]; ok && dt > 0 && f >= old {
				rates[k] = (f - old) / dt
			}
			p.Stats[k] = f
		}
		p.Rates = rates
	}
	for ip, p := range dst {
		if _, ok := got[ip]; !ok && now.Sub(p.seen) > 5*time.Second {
			delete(dst, ip) // pod is gone (rollout / scale-down)
		}
	}
}

func (c *Console) poll() {
	now := time.Now()
	gw := c.pollPods(c.gwPods)
	base := c.pollPods(c.basePods)

	ri := RedisInfo{}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	info, err := c.rdb.Info(ctx, "stats", "keyspace").Result()
	cancel()
	if err == nil {
		ri.Up = true
		for _, line := range strings.Split(info, "\n") {
			line = strings.TrimSpace(line)
			if v, ok := strings.CutPrefix(line, "total_commands_processed:"); ok {
				ri.Commands, _ = strconv.ParseFloat(v, 64)
			}
			if v, ok := strings.CutPrefix(line, "db0:keys="); ok {
				n, _, _ := strings.Cut(v, ",")
				ri.Keys, _ = strconv.ParseInt(n, 10, 64)
			}
		}
	} else {
		ri.Error = err.Error()
	}
	deps := map[string]deployStatus{}
	if c.kube.ok {
		for _, d := range []string{"redis", "gateway", "baseline", "hello"} {
			if s, err := c.kube.deployment(d); err == nil {
				deps[d] = s
			}
		}
	}
	ri.Desired, ri.Ready = deps["redis"].Desired, deps["redis"].Ready

	// current limits (from the first answering gateway pod)
	var lim *Config
	for ip, st := range gw {
		if st == nil {
			continue
		}
		if l, err := c.fetchLimits(ip); err == nil {
			lim = &l
			break
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	dt := now.Sub(c.lastPoll).Seconds()
	if ri.Up && c.lastCmds > 0 && ri.Commands >= c.lastCmds && dt > 0 {
		ri.OpsSec = (ri.Commands - c.lastCmds) / dt
	}
	if ri.Up {
		c.lastCmds = ri.Commands
	} else {
		c.lastCmds = 0
	}
	if c.redis.Up != ri.Up && !c.lastPoll.IsZero() {
		if ri.Up {
			c.eventLocked("ok", "Redis is reachable again")
		} else {
			c.eventLocked("warn", "Redis unreachable")
		}
	}
	c.redis = ri
	c.deploys = deps
	if lim != nil {
		c.limits = *lim
		if c.defaults == nil {
			d := *lim
			c.defaults = &d
		}
	}
	c.mergePods(c.pods, gw, now, true)
	c.mergePods(c.base, base, now, false)
	c.lastPoll = now

	s := Sample{T: now.UnixMilli(), RedisOps: ri.OpsSec, RedisUp: ri.Up, PerPod: map[string]float64{}}
	for _, p := range c.pods {
		if !p.Up {
			continue
		}
		s.PodsUp++
		if p.Mode == "fallback" {
			s.Fallback++
		}
		s.Allowed += p.Rates["allowed"]
		s.Denied += p.Rates["denied"]
		s.DenTenant += p.Rates["denied_tenant"]
		s.DenKey += p.Rates["denied_key"]
		s.DenIP += p.Rates["denied_ip"]
		s.Unauth += p.Rates["unauthorized"]
		s.RedisCalls += p.Rates["redis_calls"]
		s.PerPod[p.Name] = p.Rates["allowed"]
	}
	for _, p := range c.base {
		s.BaseOK += p.Rates["allowed"]
		s.BaseDenied += p.Rates["denied"]
	}
	if r := c.cur; r != nil && r.Running() {
		h := r.win.Swap(&Hist{})
		n := h.Count()
		rps := 0.0
		if dt > 0 {
			rps = float64(n) / dt
		}
		s.LoadRPS = &rps
		if n > 0 {
			p := h.Pcts(.5, .99)
			s.LoadP50, s.LoadP99 = &p[0], &p[1]
		}
	}
	c.history = append(c.history, s)
	if len(c.history) > 300 {
		c.history = c.history[len(c.history)-300:]
	}
}

func (c *Console) fetchLimits(ip string) (Config, error) {
	req, _ := http.NewRequest("GET", "http://"+net.JoinHostPort(ip, "8080")+"/admin/limits", nil)
	req.Header.Set("X-Admin-Token", c.adminToken)
	resp, err := c.hc.Do(req)
	if err != nil {
		return Config{}, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return Config{}, fmt.Errorf("%s", resp.Status)
	}
	var cfg Config
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		lvl := -1
		for i, n := range lvlName {
			if n == f[0] {
				lvl = i
			}
		}
		if lvl < 0 {
			continue
		}
		m := parseStats(strings.Join(f[1:], " "))
		l := &cfg[lvl]
		l.Cap, _ = strconv.ParseFloat(m["cap"], 64)
		l.Rate, _ = strconv.ParseFloat(m["rate"], 64)
		l.WinMs, _ = strconv.ParseInt(m["window_ms"], 10, 64)
		l.WinLimit, _ = strconv.ParseInt(m["window_limit"], 10, 64)
		l.Local = m["local"] == "true"
	}
	return cfg, nil
}

func (c *Console) gatewayRedisCalls() int64 {
	var n int64
	for _, st := range c.pollPods(c.gwPods) {
		if st != nil {
			v, _ := strconv.ParseInt(st["redis_calls"], 10, 64)
			n += v
		}
	}
	return n
}

// ---------- actions ----------

func (c *Console) startLoad(spec LoadSpec) *LoadRun {
	c.mu.Lock()
	prev := c.cur
	c.mu.Unlock()
	if prev != nil && prev.Running() {
		prev.Stop()
		<-prev.done
	}
	// Let the previous traffic's leases expire and be handed back first, so
	// the new run's Redis-call count only contains its own calls.
	c.mu.Lock()
	settle := time.Until(c.lastEnd.Add(1500 * time.Millisecond))
	c.mu.Unlock()
	if settle > 0 {
		time.Sleep(settle)
	}
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	cfg := c.limits
	c.mu.Unlock()

	var redisStart int64
	if spec.Target == "gateway" {
		redisStart = c.gatewayRedisCalls()
	}
	r := c.eng.start(id, spec, cfg, redisStart, func(r *LoadRun) {
		if r.spec.Target == "gateway" {
			time.Sleep(300 * time.Millisecond) // let background give-backs land
			r.redisEnd.Store(c.gatewayRedisCalls())
		}
		c.mu.Lock()
		c.lastEnd = time.Now()
		c.mu.Unlock()
		s := r.Summary()
		s.Running = false
		c.event("load", "run #%d finished (%s): %d sent, %d ok, %d limited, %d unauthorized, %.0f req/s, p99 %.2f ms",
			s.ID, s.Spec.Target, s.Sent, s.OK, s.Limited, s.Unauth, s.RPS, s.Lat[2])
	})
	c.mu.Lock()
	c.cur = r
	c.runs = append(c.runs, r)
	if len(c.runs) > 40 {
		c.runs = c.runs[len(c.runs)-40:]
	}
	dur := "until stopped"
	if r.spec.Duration > 0 {
		dur = fmt.Sprintf("%.0fs", r.spec.Duration)
	}
	qps := "max"
	if r.spec.QPS > 0 {
		qps = strconv.Itoa(r.spec.QPS)
	}
	c.eventLocked("load", "run #%d started -> %s: %s, %d workers, %s req/s, %d tenants x %d keys, %d IPs",
		id, r.spec.Target, dur, r.spec.Concurrency, qps, r.spec.Tenants, r.spec.Keys, r.spec.IPs)
	c.mu.Unlock()
	return r
}

func (c *Console) setLimits(lvl int, l LevelCfg) (int, []string) {
	ips, _ := net.LookupHost(c.gwPods)
	q := url.Values{}
	q.Set("level", lvlName[lvl])
	q.Set("cap", strconv.FormatFloat(l.Cap, 'f', -1, 64))
	q.Set("rate", strconv.FormatFloat(l.Rate, 'f', -1, 64))
	q.Set("window_ms", strconv.FormatInt(l.WinMs, 10))
	q.Set("window_limit", strconv.FormatInt(l.WinLimit, 10))
	q.Set("local", strconv.FormatBool(l.Local))
	okN := 0
	var errs []string
	for _, ip := range ips {
		req, _ := http.NewRequest("POST", "http://"+net.JoinHostPort(ip, "8080")+"/admin/limits?"+q.Encode(), nil)
		req.Header.Set("X-Admin-Token", c.adminToken)
		resp, err := c.hc.Do(req)
		if err != nil {
			errs = append(errs, ip+": "+err.Error())
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			errs = append(errs, ip+": "+resp.Status)
			continue
		}
		okN++
	}
	c.mu.Lock()
	c.limits[lvl] = l
	if c.cur != nil && c.cur.Running() {
		c.cur.limitsMoved.Store(true)
	}
	loc := ""
	if l.Local {
		loc = ", local-only"
	}
	win := ""
	if l.WinLimit > 0 {
		win = fmt.Sprintf(", window %d/%dms", l.WinLimit, l.WinMs)
	}
	c.eventLocked("limit", "%s limit -> cap %g, %g/s%s%s (applied on %d/%d edge nodes)",
		lvlName[lvl], l.Cap, l.Rate, win, loc, okN, len(ips))
	c.mu.Unlock()
	return okN, errs
}

type tryReq struct {
	Tenant string `json:"tenant"`
	Key    string `json:"key"`
	IP     string `json:"ip"`
	Cred   string `json:"cred"` // apikey | jwt | forged | none
	Count  int    `json:"count"`
	Target string `json:"target"`
}

type tryResp struct {
	Status int     `json:"status"`
	Level  string  `json:"level,omitempty"`
	Pod    string  `json:"pod,omitempty"`
	Ms     float64 `json:"ms"`
	Body   string  `json:"body"`
}

func (c *Console) try(t tryReq) (string, []tryResp) {
	if t.Count < 1 {
		t.Count = 1
	}
	if t.Count > 200 {
		t.Count = 200
	}
	if t.Tenant == "" {
		t.Tenant = "acme"
	}
	if t.Key == "" {
		t.Key = "key0"
	}
	if t.IP == "" {
		t.IP = "203.0.113.7"
	}
	var cred, header string
	switch t.Cred {
	case "jwt":
		cred, header = mintJWT(c.secret, t.Tenant, t.Key, time.Hour), "Authorization"
	case "forged":
		cred, header = t.Tenant+"."+t.Key+".deadbeefdeadbeefdeadbeefdeadbeef", "X-API-Key"
	case "none":
	default:
		cred, header = mintKey(c.secret, t.Tenant, t.Key), "X-API-Key"
	}
	u := c.eng.targetURL(t.Target)
	// New connection per request: the Service then spreads requests over the
	// edge nodes like independent clients, instead of pinning one keep-alive
	// connection (and so one node's local IP share) for the whole batch.
	hc := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	out := make([]tryResp, 0, t.Count)
	for i := 0; i < t.Count; i++ {
		req, _ := http.NewRequest("GET", u, nil)
		if header == "Authorization" {
			req.Header.Set(header, "Bearer "+cred)
		} else if header != "" {
			req.Header.Set(header, cred)
		}
		req.Header.Set("X-Forwarded-For", t.IP)
		t0 := time.Now()
		resp, err := hc.Do(req)
		if err != nil {
			out = append(out, tryResp{Status: 0, Body: err.Error()})
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		resp.Body.Close()
		out = append(out, tryResp{Status: resp.StatusCode, Level: resp.Header.Get("X-RateLimit-Level"),
			Pod: resp.Header.Get("X-Served-By"), Ms: float64(time.Since(t0).Microseconds()) / 1000,
			Body: strings.TrimSpace(string(b))})
	}
	return cred, out
}

// ---------- HTTP ----------

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	return json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(v)
}

func (c *Console) routes() http.Handler {
	mux := http.NewServeMux()
	sub, _ := fs.Sub(webFS, "web")
	files := http.FileServer(http.FS(sub))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// embedded files carry no modification time: stop browsers from
		// keeping a stale dashboard after a redeploy
		w.Header().Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})

	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		pods := make([]*Pod, 0, len(c.pods))
		for _, p := range c.pods {
			pods = append(pods, p)
		}
		sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
		base := make([]*Pod, 0, len(c.base))
		for _, p := range c.base {
			base = append(base, p)
		}
		sort.Slice(base, func(i, j int) bool { return base[i].Name < base[j].Name })
		runs := make([]RunSummary, 0, len(c.runs))
		for _, rr := range c.runs {
			runs = append(runs, rr.Summary())
		}
		var cur *RunSummary
		if c.cur != nil {
			s := c.cur.Summary()
			cur = &s
		}
		st := map[string]any{
			"now": time.Now().UnixMilli(), "pods": pods, "baseline": base, "redis": c.redis,
			"deployments": c.deploys, "limits": c.limits, "defaults": c.defaults,
			"history": c.history, "events": c.events, "runs": runs, "current": cur,
			"job": c.job, "kube": c.kube.ok, "report": c.report, "checks": checkOrder, "check_names": checkName,
		}
		data, err := json.Marshal(st)
		c.mu.Unlock()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Write(data)
	})

	mux.HandleFunc("POST /api/load", func(w http.ResponseWriter, r *http.Request) {
		var s LoadSpec
		if err := readJSON(r, &s); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		writeJSON(w, c.startLoad(s).Summary())
	})

	mux.HandleFunc("POST /api/load/stop", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		if c.jobCancel != nil {
			close(c.jobCancel)
			c.jobCancel = nil
		}
		cur := c.cur
		c.mu.Unlock()
		if cur != nil && cur.Running() {
			cur.Stop()
			c.event("load", "run #%d stopped by user", cur.id)
		}
		writeJSON(w, map[string]bool{"ok": true})
	})

	// Run verification checks: {"tests":["burst",...]} or {"tests":["all"]}.
	mux.HandleFunc("POST /api/verify", func(w http.ResponseWriter, r *http.Request) {
		var p struct {
			Tests []string `json:"tests"`
		}
		readJSON(r, &p)
		var ids []string
		for _, t := range p.Tests {
			if t == "all" {
				ids = checkOrder
				break
			}
			if _, ok := checkName[t]; ok {
				ids = append(ids, t)
			}
		}
		if len(ids) == 0 {
			http.Error(w, "tests must name checks: "+strings.Join(checkOrder, ", ")+" or all", 400)
			return
		}
		c.mu.Lock()
		if c.jobCancel != nil {
			close(c.jobCancel)
		}
		ch := make(chan struct{})
		c.jobCancel = ch
		c.mu.Unlock()
		go c.runSuite(ids, ch)
		writeJSON(w, map[string]bool{"ok": true})
	})

	mux.HandleFunc("POST /api/limits", func(w http.ResponseWriter, r *http.Request) {
		var p struct {
			Level string `json:"level"`
			LevelCfg
		}
		if err := readJSON(r, &p); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		lvl := -1
		for i, n := range lvlName {
			if n == p.Level {
				lvl = i
			}
		}
		if lvl < 0 || p.Cap <= 0 || p.Rate <= 0 {
			http.Error(w, "level must be tenant|key|ip and cap, rate > 0", 400)
			return
		}
		if p.WinMs <= 0 {
			p.WinMs = 1000
		}
		n, errs := c.setLimits(lvl, p.LevelCfg)
		writeJSON(w, map[string]any{"applied": n, "errors": errs})
	})

	mux.HandleFunc("POST /api/limits/reset", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		d := c.defaults
		c.mu.Unlock()
		if d == nil {
			http.Error(w, "defaults not known yet", 409)
			return
		}
		for i := range d {
			c.setLimits(i, d[i])
		}
		writeJSON(w, map[string]bool{"ok": true})
	})

	mux.HandleFunc("POST /api/redis", func(w http.ResponseWriter, r *http.Request) {
		var p struct {
			Action string `json:"action"`
		}
		readJSON(r, &p)
		var err error
		switch p.Action {
		case "kill":
			if err = c.kube.scale("redis", 0); err == nil {
				c.event("warn", "Redis outage injected: deployment/redis scaled to 0")
			}
		case "restore":
			if err = c.kube.scale("redis", 1); err == nil {
				c.event("info", "Redis restore requested: deployment/redis scaled to 1 (starts EMPTY)")
			}
		case "flush":
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err = c.rdb.FlushAll(ctx).Err()
			cancel()
			if err == nil {
				c.event("info", "Redis flushed: every shared bucket starts full again")
			}
		default:
			err = fmt.Errorf("action must be kill|restore|flush")
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, map[string]bool{"ok": true})
	})

	mux.HandleFunc("POST /api/try", func(w http.ResponseWriter, r *http.Request) {
		var t tryReq
		if err := readJSON(r, &t); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		cred, res := c.try(t)
		writeJSON(w, map[string]any{"credential": cred, "results": res})
	})

	mux.HandleFunc("POST /api/events/clear", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		c.events = nil
		c.runs = nil
		c.mu.Unlock()
		writeJSON(w, map[string]bool{"ok": true})
	})
	return mux
}

func main() {
	c := &Console{
		secret:     getenv("AUTH_SECRET", "demo123"),
		adminToken: os.Getenv("ADMIN_TOKEN"),
		gwPods:     getenv("GATEWAY_PODS", "gateway-pods"),
		basePods:   getenv("BASELINE_PODS", "baseline-pods"),
		rdb: redis.NewClient(&redis.Options{Addr: getenv("REDIS_ADDR", "redis:6379"),
			DialTimeout: 300 * time.Millisecond, ReadTimeout: 300 * time.Millisecond, MaxRetries: -1}),
		kube:    newKube(getenv("NAMESPACE", "default")),
		hc:      &http.Client{Timeout: 2 * time.Second},
		pods:    map[string]*Pod{},
		base:    map[string]*Pod{},
		deploys: map[string]deployStatus{},
		report:  map[string]*CheckResult{},
	}
	c.eng = &loadEngine{secret: c.secret,
		gatewayURL: getenv("GATEWAY_URL", "http://gateway/"),
		baseURL:    getenv("BASELINE_URL", "http://baseline/"),
		directURL:  getenv("DIRECT_URL", "http://hello/")}

	c.event("info", "console started")
	go func() {
		for {
			t0 := time.Now()
			c.poll()
			time.Sleep(time.Until(t0.Add(time.Second)))
		}
	}()
	addr := ":" + getenv("PORT", "9000")
	fmt.Println("rateguard console on", addr)
	srv := &http.Server{Addr: addr, Handler: c.routes(), ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
