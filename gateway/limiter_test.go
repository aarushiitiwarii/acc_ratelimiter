package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Tests need a Redis: set REDIS_TEST_ADDR (default 127.0.0.1:6390). They skip if unreachable.
func testAddr() string { return getenv("REDIS_TEST_ADDR", "127.0.0.1:6390") }

func rdbOrSkip(t testing.TB) *redis.Client {
	rdb := redis.NewClient(&redis.Options{Addr: testAddr()})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", testAddr(), err)
	}
	return rdb
}

func uniq(p string) string { return fmt.Sprintf("%s-%d", p, time.Now().UnixNano()) }

func cfgOf(keyCap, keyRate, tenCap, ipCap float64) Config {
	return Config{
		LvlTenant: {Cap: tenCap, Rate: 0.01, WinMs: 1000},
		LvlKey:    {Cap: keyCap, Rate: keyRate, WinMs: 1000},
		LvlIP:     {Cap: ipCap, Rate: 0.01, WinMs: 1000},
	}
}

func keys(tenant, key, ip string) [3]string {
	return [3]string{"rl:t:" + tenant, "rl:k:" + tenant + ":" + key, "rl:i:" + ip}
}

func count(l *Limiter, n int, k [3]string) (ok int) {
	for i := 0; i < n; i++ {
		if a, _ := l.Allow(k); a {
			ok++
		}
	}
	return
}

func TestKeysIndependent(t *testing.T) {
	l := NewLimiter(rdbOrSkip(t), cfgOf(5, 0.01, 1000, 1000), 1, 1, 500*time.Millisecond, 1000)
	defer l.Close()
	ten, ip := uniq("ten"), uniq("ip")
	a := count(l, 50, keys(ten, "A", ip))
	b := count(l, 50, keys(ten, "B", ip))
	if a != 5 || b != 5 {
		t.Fatalf("each key should get exactly its capacity: A=%d B=%d (want 5/5)", a, b)
	}
}

func TestHierarchy(t *testing.T) {
	// tenant cap 6 < 2 keys x 5: tenant level must bind.
	l := NewLimiter(rdbOrSkip(t), cfgOf(5, 0.01, 6, 1000), 1, 1, 500*time.Millisecond, 1000)
	defer l.Close()
	ten, ip := uniq("ten"), uniq("ip")
	total := count(l, 20, keys(ten, "A", ip)) + count(l, 20, keys(ten, "B", ip))
	if total != 6 {
		t.Fatalf("tenant cap should bind at 6, got %d", total)
	}
	// IP cap 3 < key cap 5: IP level must bind and be reported as the denier.
	l2 := NewLimiter(rdbOrSkip(t), cfgOf(5, 0.01, 1000, 3), 1, 1, 500*time.Millisecond, 1000)
	defer l2.Close()
	ten2, ip2 := uniq("ten"), uniq("ip")
	k := keys(ten2, "A", ip2)
	if got := count(l2, 20, k); got != 3 {
		t.Fatalf("ip cap should bind at 3, got %d", got)
	}
	if _, lvl := l2.Allow(k); lvl != LvlIP {
		t.Fatalf("denier should be ip level, got %d", lvl)
	}
}

func TestLocalOnlyIP(t *testing.T) {
	rdb := rdbOrSkip(t)
	cfg := cfgOf(1000, 1000, 1e6, 6)
	cfg[LvlIP].LocalOnly = true // this replica's share: 6/3 = 2
	l := NewLimiter(rdb, cfg, 3, 1, 500*time.Millisecond, 1000)
	defer l.Close()
	ip := uniq("ip")
	k := keys(uniq("t"), "A", ip)
	if got := count(l, 10, k); got != 2 {
		t.Fatalf("local-only IP level should admit this replica's share (2), got %d", got)
	}
	if n, _ := rdb.Exists(context.Background(), k[LvlIP]).Result(); n != 0 {
		t.Fatal("local-only level must not touch Redis")
	}
}

func TestSlidingWindow(t *testing.T) {
	cfg := cfgOf(100, 100, 1e6, 1e6)
	cfg[LvlKey].WinLimit = 10 // bucket allows a 100-burst, window caps it at 10/s
	l := NewLimiter(rdbOrSkip(t), cfg, 1, 5, 500*time.Millisecond, 1000)
	defer l.Close()
	got := count(l, 100, keys(uniq("t"), "A", uniq("ip")))
	if got < 9 || got > 10 {
		t.Fatalf("window should cap burst at ~10, got %d", got)
	}
}

// Same key hammered concurrently through 3 independent "pods" sharing Redis:
// the global limit must never be exceeded.
func TestMultiReplicaAccuracy(t *testing.T) {
	rdb := rdbOrSkip(t)
	var pods []*Limiter
	for i := 0; i < 3; i++ {
		pods = append(pods, NewLimiter(rdb, cfgOf(300, 0.01, 1e6, 1e6), 3, 20, 300*time.Millisecond, 1000))
	}
	defer func() {
		for _, p := range pods {
			p.Close()
		}
	}()
	k := keys(uniq("t"), "A", uniq("ip"))
	var ok atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 24; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			p := pods[w%3]
			for i := 0; i < 400; i++ {
				if a, _ := p.Allow(k); a {
					ok.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	t.Logf("allowed %d of 9600 (limit 300)", ok.Load())
	if ok.Load() > 300 {
		t.Fatalf("global limit exceeded: %d > 300", ok.Load())
	}
	if ok.Load() < 240 {
		t.Fatalf("too many tokens stranded in leases: %d < 240", ok.Load())
	}
}

// Unused reserved tokens must flow back to Redis (drift compensation).
func TestGiveBack(t *testing.T) {
	rdb := rdbOrSkip(t)
	l := NewLimiter(rdb, cfgOf(100, 0.01, 1e6, 1e6), 1, 50, 200*time.Millisecond, 1000)
	defer l.Close()
	k := keys(uniq("t"), "A", uniq("ip"))
	before := tokensReturned.Load()
	if a, _ := l.Allow(k); !a {
		t.Fatal("first request should pass")
	}
	// leased 50, used 1: after the lease expires the daemon returns ~49
	time.Sleep(900 * time.Millisecond)
	tok, _ := rdb.HGet(context.Background(), k[LvlKey], "tokens").Float64()
	t.Logf("redis tokens after give-back: %.2f (returned %d)", tok, tokensReturned.Load()-before)
	if tok < 98.5 {
		t.Fatalf("expected ~99 tokens back in Redis, got %.2f", tok)
	}
}

// Evicting a bucket from the L1 cache must not let a client bypass its limit.
func TestEvictionNoBypass(t *testing.T) {
	l := NewLimiter(rdbOrSkip(t), cfgOf(3, 0.01, 1e6, 1e6), 1, 1, 500*time.Millisecond, 16)
	defer l.Close()
	ten, ip := uniq("t"), uniq("ip")
	k := keys(ten, "victim", ip)
	if got := count(l, 10, k); got != 3 {
		t.Fatalf("want 3, got %d", got)
	}
	ev := evictions.Load()
	for i := 0; i < 500; i++ { // flood the tiny cache with other keys
		l.Allow(keys(ten, fmt.Sprint("noise", i), fmt.Sprint(ip, i)))
	}
	if evictions.Load() == ev {
		t.Fatal("expected evictions")
	}
	if got := count(l, 10, k); got != 0 {
		t.Fatalf("eviction let %d extra requests through", got)
	}
}

func TestFallbackAndRecovery(t *testing.T) {
	if _, err := exec.LookPath("redis-server"); err != nil {
		t.Skip("redis-server binary needed")
	}
	addr := "127.0.0.1:6391"
	l := NewLimiter(redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 20 * time.Millisecond}),
		cfgOf(6, 0.01, 6, 6), 3, 1, 300*time.Millisecond, 1000) // share = 6/3 = 2
	defer l.Close()
	k := keys(uniq("t"), "A", uniq("ip"))

	// Redis down: local approximation admits this replica's share, then denies.
	got := count(l, 20, k)
	if got != 2 {
		t.Fatalf("fallback should admit this replica's share (2), got %d", got)
	}
	if fallbackKeys.Load() == 0 {
		t.Fatal("expected fallback mode")
	}

	// Redis comes back: the daemon probes it and leaves fallback by itself.
	cmd := exec.Command("redis-server", "--port", "6391", "--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	deadline := time.Now().Add(5 * time.Second)
	for fallbackKeys.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if fallbackKeys.Load() != 0 {
		t.Fatal("did not recover from fallback")
	}
	t.Logf("recovered; reconciled=%d tokens", reconciled.Load())
}

func TestAuth(t *testing.T) {
	a := NewAuthenticator("s3cret", 100, time.Minute)
	req := func(h, v string) *http.Request {
		r, _ := http.NewRequest("GET", "/", nil)
		if v != "" {
			r.Header.Set(h, v)
		}
		return r
	}
	good := a.MintKey("acme", "k1")
	if c, ok := a.Authenticate(req("X-API-Key", good)); !ok || c.Tenant != "acme" || c.KeyID != "k1" {
		t.Fatalf("valid key rejected: %v %v", c, ok)
	}
	h0 := authHits.Load()
	a.Authenticate(req("X-API-Key", good))
	if authHits.Load() != h0+1 {
		t.Fatal("second validation should hit the claims cache")
	}
	for _, bad := range []string{"", "garbage", "acme.k1.deadbeef", "acme.k1", "evil.k1." + good[len("acme.k1."):]} {
		if _, ok := a.Authenticate(req("X-API-Key", bad)); ok {
			t.Fatalf("forged key accepted: %q", bad)
		}
	}
	mk := func(alg, payload, secret string) string {
		b := base64.RawURLEncoding
		h := b.EncodeToString([]byte(`{"alg":"` + alg + `","typ":"JWT"}`))
		p := b.EncodeToString([]byte(payload))
		m := hmac.New(sha256.New, []byte(secret))
		m.Write([]byte(h + "." + p))
		return h + "." + p + "." + b.EncodeToString(m.Sum(nil))
	}
	exp := fmt.Sprintf(`{"tenant":"acme","sub":"u1","exp":%d}`, time.Now().Add(time.Hour).Unix())
	if c, ok := a.Authenticate(req("Authorization", "Bearer "+mk("HS256", exp, "s3cret"))); !ok || c.Tenant != "acme" {
		t.Fatal("valid JWT rejected")
	}
	old := fmt.Sprintf(`{"tenant":"acme","sub":"u1","exp":%d}`, time.Now().Add(-time.Hour).Unix())
	if _, ok := a.Authenticate(req("Authorization", "Bearer "+mk("HS256", old, "s3cret"))); ok {
		t.Fatal("expired JWT accepted")
	}
	if _, ok := a.Authenticate(req("Authorization", "Bearer "+mk("HS256", exp, "wrong"))); ok {
		t.Fatal("JWT with wrong signature accepted")
	}
	if _, ok := a.Authenticate(req("Authorization", "Bearer "+mk("none", exp, "s3cret"))); ok {
		t.Fatal("alg=none accepted")
	}
}

// Live limit change: limits travel with every lease call, so a new config is
// enforced from the next lease without restarting pods or touching Redis.
func TestLiveLimitChange(t *testing.T) {
	rdb := rdbOrSkip(t)
	// Lower: a generous key limit is cut to cap 3; Redis clamps the bucket.
	l := NewLimiter(rdb, cfgOf(1000, 0.01, 1e6, 1e6), 1, 1, 500*time.Millisecond, 1000)
	defer l.Close()
	ten, ip := uniq("ten"), uniq("ip")
	k := keys(ten, "A", ip)
	if got := count(l, 10, k); got != 10 {
		t.Fatalf("before change: want 10 allowed, got %d", got)
	}
	c := l.Config()
	c[LvlKey].Cap, c[LvlKey].Rate = 3, 0.01
	l.SetConfig(c)
	// at most one token can still sit in the local lease (batch 1)
	if got := count(l, 20, k); got < 3 || got > 4 {
		t.Fatalf("after lowering to cap 3: want 3..4 allowed, got %d", got)
	}

	// Raise: an exhausted bucket refills at the new, higher rate.
	l2 := NewLimiter(rdb, cfgOf(5, 0.01, 1e6, 1e6), 1, 1, 500*time.Millisecond, 1000)
	defer l2.Close()
	k2 := keys(uniq("ten"), "A", uniq("ip"))
	if got := count(l2, 20, k2); got != 5 {
		t.Fatalf("before change: want 5, got %d", got)
	}
	c = l2.Config()
	c[LvlKey].Cap, c[LvlKey].Rate = 50, 100
	l2.SetConfig(c)
	time.Sleep(300 * time.Millisecond) // ~30 tokens at 100/s
	if got := count(l2, 100, k2); got < 20 || got > 40 {
		t.Fatalf("after raising rate to 100/s for 300ms: want ~30 allowed, got %d", got)
	}
}

// Latency added by the in-process decision path (cache-hit fast path).
func BenchmarkAllowParallel(b *testing.B) {
	rdb := rdbOrSkip(b)
	cfg := cfgOf(1e12, 1e9, 1e12, 1e12)
	l := NewLimiter(rdb, cfg, 1, 5000, 500*time.Millisecond, 100000)
	defer l.Close()
	ten := uniq("t")
	var n atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := n.Add(1)
		k := keys(ten, fmt.Sprint("k", id%64), fmt.Sprint("10.0.0.", id%250))
		for pb.Next() {
			l.Allow(k)
		}
	})
}

var _ = os.Getenv
