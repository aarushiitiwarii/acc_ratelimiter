package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

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

func getenvF(k string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
		return v
	}
	return def
}

// loadConfig builds the 3-level hierarchy from the environment.
// Backwards compatible: LIMIT_CAPACITY / LIMIT_RATE configure the API-key level.
// Unset tenant and IP limits default to 10x the key limit so they only bind
// when you configure them (or many keys share one tenant / IP).
func loadConfig() Config {
	keyCap := getenvF("KEY_CAP", getenvF("LIMIT_CAPACITY", 100))
	keyRate := getenvF("KEY_RATE", getenvF("LIMIT_RATE", 10))
	win := getenvInt("WINDOW_MS", 1000)
	return Config{
		LvlTenant: {Cap: getenvF("TENANT_CAP", keyCap*10), Rate: getenvF("TENANT_RATE", keyRate*10),
			WinMs: win, WinLimit: getenvInt("TENANT_WINDOW_LIMIT", 0)},
		LvlKey: {Cap: keyCap, Rate: keyRate, WinMs: win, WinLimit: getenvInt("KEY_WINDOW_LIMIT", 0)},
		LvlIP: {Cap: getenvF("IP_CAP", keyCap*10), Rate: getenvF("IP_RATE", keyRate*10),
			WinMs: win, WinLimit: getenvInt("IP_WINDOW_LIMIT", 0), LocalOnly: getenv("IP_LOCAL", "false") == "true"},
	}
}

// clientIP: X-Forwarded-For is only honoured when TRUST_XFF=true (i.e. when
// the gateway really sits behind a trusted CDN/LB); otherwise it is spoofable.
func clientIP(r *http.Request, trustXFF bool) string {
	if trustXFF {
		if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
			if i := strings.IndexByte(xf, ','); i >= 0 {
				xf = xf[:i]
			}
			return strings.TrimSpace(xf)
		}
	}
	h, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return h
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, Authorization, X-Admin-Token")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	auth := NewAuthenticator(os.Getenv("AUTH_SECRET"), int(getenvInt("AUTH_CACHE_SIZE", 50000)),
		time.Duration(getenvInt("AUTH_CACHE_TTL_S", 300))*time.Second)

	// `gateway mint <tenant> <keyid>` prints a signed API key (needs AUTH_SECRET).
	if len(os.Args) == 4 && os.Args[1] == "mint" {
		if os.Getenv("AUTH_SECRET") == "" {
			fmt.Fprintln(os.Stderr, "set AUTH_SECRET first")
			os.Exit(1)
		}
		fmt.Println(auth.MintKey(os.Args[2], os.Args[3]))
		return
	}

	redisAddr := getenv("REDIS_ADDR", "redis:6379")
	replicas := getenvF("REPLICAS", 3)
	if replicas < 1 {
		replicas = 1
	}
	batch := getenvInt("BATCH_SIZE", 20)
	trustXFF := getenv("TRUST_XFF", "false") == "true"
	adminToken := os.Getenv("ADMIN_TOKEN")

	l := NewLimiter(redis.NewClient(&redis.Options{Addr: redisAddr, PoolSize: 64}), loadConfig(),
		replicas, batch, time.Duration(getenvInt("LEASE_TTL_MS", 500))*time.Millisecond,
		int(getenvInt("BUCKET_CACHE_SIZE", 100000)))
	host, _ := os.Hostname()

	var proxy *httputil.ReverseProxy
	if up := os.Getenv("UPSTREAM_URL"); up != "" {
		u, err := url.Parse(up)
		if err != nil {
			fmt.Fprintln(os.Stderr, "bad UPSTREAM_URL:", err)
			os.Exit(1)
		}
		proxy = httputil.NewSingleHostReverseProxy(u)
		proxy.Transport = &http.Transport{MaxIdleConns: 1024, MaxIdleConnsPerHost: 512, IdleConnTimeout: 90 * time.Second}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })

	// First fields keep the original format so the dashboard keeps parsing it.
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		mode := "normal"
		if fallbackKeys.Load() > 0 {
			mode = "fallback"
		}
		fmt.Fprintf(w, "pod=%s mode=%s allowed=%d denied=%d redis_calls=%d fallback_events=%d "+
			"denied_tenant=%d denied_key=%d denied_ip=%d auth_hits=%d auth_misses=%d "+
			"buckets=%d evictions=%d tokens_returned=%d reconciled=%d give_dropped=%d\n",
			host, mode, allowed.Load(), denied.Load(), redisCalls.Load(), fallbackEvents.Load(),
			deniedBy[LvlTenant].Load(), deniedBy[LvlKey].Load(), deniedBy[LvlIP].Load(),
			authHits.Load(), authMisses.Load(), l.Buckets(), evictions.Load(),
			tokensReturned.Load(), reconciled.Load(), giveDropped.Load())
	})

	// Live limit changes: POST /admin/limits?level=key&cap=50&rate=5[&window_ms=1000&window_limit=40]
	// applies to this replica immediately (apply to every replica; Redis keeps the shared state).
	mux.HandleFunc("/admin/limits", func(w http.ResponseWriter, r *http.Request) {
		if adminToken == "" || r.Header.Get("X-Admin-Token") != adminToken {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		cfg := l.Config()
		if r.Method == http.MethodPost {
			lvl := -1
			for i, n := range LvlName {
				if n == r.URL.Query().Get("level") {
					lvl = i
				}
			}
			if lvl < 0 {
				http.Error(w, "level must be tenant|key|ip", http.StatusBadRequest)
				return
			}
			q := r.URL.Query()
			c := cfg[lvl]
			if v, err := strconv.ParseFloat(q.Get("cap"), 64); err == nil && v > 0 {
				c.Cap = v
			}
			if v, err := strconv.ParseFloat(q.Get("rate"), 64); err == nil && v > 0 {
				c.Rate = v
			}
			if v, err := strconv.ParseInt(q.Get("window_ms"), 10, 64); err == nil && v >= 0 {
				c.WinMs = v
			}
			if v, err := strconv.ParseInt(q.Get("window_limit"), 10, 64); err == nil && v >= 0 {
				c.WinLimit = v
			}
			cfg[lvl] = c
			l.SetConfig(cfg)
		}
		for i, c := range cfg {
			fmt.Fprintf(w, "%s cap=%v rate=%v window_ms=%d window_limit=%d\n", LvlName[i], c.Cap, c.Rate, c.WinMs, c.WinLimit)
		}
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// 1. Authenticate with local CPU only. Bad/missing credentials are
		//    dropped here, before any Redis access.
		cl, ok := auth.Authenticate(r)
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// 2. Hierarchical check: tenant, API key, client IP.
		ip := clientIP(r, trustXFF)
		okAllow, lvl := l.Allow([3]string{
			"rl:t:" + cl.Tenant,
			"rl:k:" + cl.Tenant + ":" + cl.KeyID,
			"rl:i:" + ip,
		})
		if !okAllow {
			denied.Add(1)
			deniedBy[lvl].Add(1)
			w.Header().Set("Retry-After", "1")
			w.Header().Set("X-RateLimit-Level", LvlName[lvl])
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		allowed.Add(1)
		// 3. Proxy upstream (or answer directly when no upstream is configured).
		if proxy != nil {
			r.Header.Set("X-Tenant-ID", cl.Tenant)
			proxy.ServeHTTP(w, r)
			return
		}
		fmt.Fprintf(w, "allowed by %s\n", host)
	})

	fmt.Printf("gateway listening on :8080 redis=%s batch=%d replicas=%.0f auth=%v upstream=%v\n",
		redisAddr, batch, replicas, os.Getenv("AUTH_SECRET") != "", proxy != nil)
	srv := &http.Server{Addr: ":" + getenv("PORT", "8080"), Handler: cors(mux), ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
