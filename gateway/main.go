package main

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lease.lua
var leaseLua string

var allowed, denied, redisCalls atomic.Int64
var fallbackKeys, fallbackEvents atomic.Int64

type bucket struct {
	mu         sync.Mutex
	tokens     int64
	expiresAt  time.Time
	refilling  bool
	nextRefill time.Time

	// fallback state (used only while Redis is unreachable)
	fallback    bool
	localTokens float64
	lastFill    time.Time
}

type Limiter struct {
	rdb      *redis.Client
	script   *redis.Script
	buckets  sync.Map
	capacity string
	rate     string
	capF     float64
	rateF    float64
	replicas float64
	batch    int64
	lowWater int64
	leaseTTL time.Duration
}

func (l *Limiter) get(key string) *bucket {
	if b, ok := l.buckets.Load(key); ok {
		return b.(*bucket)
	}
	b, _ := l.buckets.LoadOrStore(key, &bucket{})
	return b.(*bucket)
}

func (l *Limiter) Allow(key string) bool {
	b := l.get(key)
	b.mu.Lock()
	now := time.Now()

	// Fallback mode: Redis is unreachable, enforce our local share of the limit
	if b.fallback {
		elapsed := now.Sub(b.lastFill).Seconds()
		b.lastFill = now
		b.localTokens = math.Min(l.capF/l.replicas,
			b.localTokens+elapsed*l.rateF/l.replicas)
		ok := false
		if b.localTokens >= 1 {
			b.localTokens--
			ok = true
		}
		// Probe Redis in the background to detect recovery
		if !b.refilling && !now.Before(b.nextRefill) {
			b.refilling = true
			go l.refill(key, b)
		}
		b.mu.Unlock()
		return ok
	}

	if now.After(b.expiresAt) {
		b.tokens = 0 // lease expired: discard unused tokens (drift control)
	}
	// Empty and allowed to contact Redis: fetch a batch now
	if b.tokens == 0 && !b.refilling && !now.Before(b.nextRefill) {
		b.refilling = true
		b.mu.Unlock()
		l.refill(key, b)
		b.mu.Lock()
		if b.fallback { // Redis just failed: use the local bucket from now on
			b.mu.Unlock()
			return false
		}
	}
	ok := false
	if b.tokens > 0 {
		b.tokens--
		ok = true
	}
	// Running low: refill in the background so later requests never wait
	if b.tokens <= l.lowWater && !b.refilling && !time.Now().Before(b.nextRefill) {
		b.refilling = true
		go l.refill(key, b)
	}
	b.mu.Unlock()
	return ok
}

func (l *Limiter) refill(key string, b *bucket) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	redisCalls.Add(1)
	granted, err := l.script.Run(ctx, l.rdb, []string{"rl:" + key},
		l.capacity, l.rate, l.batch).Int64()

	b.mu.Lock()
	defer b.mu.Unlock()
	b.refilling = false

	if err != nil {
		// Redis unreachable: enter fallback mode
		b.nextRefill = time.Now().Add(100 * time.Millisecond)
		if !b.fallback {
			b.fallback = true
			b.tokens = 0
			b.localTokens = 0
			b.lastFill = time.Now()
			fallbackKeys.Add(1)
			fallbackEvents.Add(1)
		}
		return
	}

	// Redis answered: leave fallback mode if we were in it
	if b.fallback {
		b.fallback = false
		fallbackKeys.Add(-1)
	}
	if granted == 0 {
		b.nextRefill = time.Now().Add(100 * time.Millisecond) // bucket empty, back off
		return
	}
	b.tokens += granted
	b.expiresAt = time.Now().Add(l.leaseTTL)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int64) int64 {
	if v, err := strconv.ParseInt(os.Getenv(k), 10, 64); err == nil {
		return v
	}
	return def
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func main() {
	redisAddr := getenv("REDIS_ADDR", "redis:6379")
	batch := getenvInt("BATCH_SIZE", 20)
	capStr := getenv("LIMIT_CAPACITY", "100")
	rateStr := getenv("LIMIT_RATE", "10")
	capF, _ := strconv.ParseFloat(capStr, 64)
	rateF, _ := strconv.ParseFloat(rateStr, 64)
	replicas := float64(getenvInt("REPLICAS", 3))
	if replicas < 1 {
		replicas = 1
	}

	l := &Limiter{
		rdb:      redis.NewClient(&redis.Options{Addr: redisAddr}),
		script:   redis.NewScript(leaseLua),
		capacity: capStr,
		rate:     rateStr,
		capF:     capF,
		rateF:    rateF,
		replicas: replicas,
		batch:    batch,
		lowWater: batch / 4,
		leaseTTL: time.Duration(getenvInt("LEASE_TTL_MS", 500)) * time.Millisecond,
	}
	host, _ := os.Hostname()

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		mode := "normal"
		if fallbackKeys.Load() > 0 {
			mode = "fallback"
		}
		fmt.Fprintf(w, "pod=%s mode=%s allowed=%d denied=%d redis_calls=%d fallback_events=%d\n",
			host, mode, allowed.Load(), denied.Load(), redisCalls.Load(), fallbackEvents.Load())
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if key == "" {
			http.Error(w, "missing X-API-Key", http.StatusUnauthorized)
			return
		}
		if l.Allow(key) {
			allowed.Add(1)
			fmt.Fprintf(w, "allowed by %s\n", host)
		} else {
			denied.Add(1)
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many requests", http.StatusTooManyRequests)
		}
	})

	fmt.Printf("gateway listening on :8080 redis=%s batch=%d replicas=%.0f\n", redisAddr, batch, replicas)
	http.ListenAndServe(":8080", cors(http.DefaultServeMux))
}