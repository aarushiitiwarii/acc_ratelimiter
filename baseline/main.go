package main

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lease.lua
var leaseLua string

var allowed, denied atomic.Int64

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	redisAddr := getenv("REDIS_ADDR", "redis:6379")
	capacity := getenv("LIMIT_CAPACITY", "100")
	rate := getenv("LIMIT_RATE", "10")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	script := redis.NewScript(leaseLua)
	host, _ := os.Hostname()

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "pod=%s allowed=%d denied=%d\n", host, allowed.Load(), denied.Load())
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if key == "" {
			http.Error(w, "missing X-API-Key", http.StatusUnauthorized)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 50*time.Millisecond)
		defer cancel()

		// Ask Redis for exactly 1 token, on every request (this is the slow baseline)
		granted, err := script.Run(ctx, rdb, []string{"rl:" + key}, capacity, rate, 1).Int()
		if err != nil {
			http.Error(w, "redis error: "+err.Error(), http.StatusServiceUnavailable)
			return
		}

		if granted >= 1 {
			allowed.Add(1)
			fmt.Fprintf(w, "allowed by %s\n", host)
		} else {
			denied.Add(1)
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many requests", http.StatusTooManyRequests)
		}
	})

	fmt.Println("baseline gateway listening on :8080, redis =", redisAddr)
	http.ListenAndServe(":8080", nil)
}