// loadgen: high-throughput load generator + enforcement-accuracy checker for RateGuard.
//
//	go run ./bench -url http://localhost:8080/ -c 256 -d 20s -tenants 4 -keys 8 -ips 5000 \
//	    -secret $AUTH_SECRET -key-cap 5000 -key-rate 1000
//
// Each request carries a signed API key (tenant.keyN.sig) and a spoofed
// X-Forwarded-For from a pool of IPs (gateway must run with TRUST_XFF=true).
// Run it against the gateway AND against the bare backend to get the latency
// penalty, and against baseline.yaml's service to compare Redis-per-request.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func mint(secret, tenant, keyID string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(tenant + "." + keyID))
	return tenant + "." + keyID + "." + hex.EncodeToString(m.Sum(nil))[:32]
}

func main() {
	url := flag.String("url", "http://localhost:8080/", "target URL")
	conc := flag.Int("c", 128, "concurrent workers (connections)")
	dur := flag.Duration("d", 10*time.Second, "test duration")
	qps := flag.Int("qps", 0, "target total QPS (0 = closed loop, as fast as possible)")
	tenants := flag.Int("tenants", 1, "number of tenants")
	keysPer := flag.Int("keys", 1, "API keys per tenant")
	ips := flag.Int("ips", 1, "distinct spoofed client IPs")
	secret := flag.String("secret", "", "AUTH_SECRET to mint signed keys (empty = legacy raw keys)")
	zipf := flag.Bool("skew", false, "skew traffic: ~80% of requests hit the first key")
	keyCap := flag.Float64("key-cap", 0, "gateway KEY_CAP (enables accuracy report)")
	keyRate := flag.Float64("key-rate", 0, "gateway KEY_RATE (tokens/s)")
	expected := flag.Int64("expected", 0, "override expected #allowed (when a non-key level binds)")
	flag.Parse()

	type cred struct{ key string }
	var creds []cred
	for t := 0; t < *tenants; t++ {
		for k := 0; k < *keysPer; k++ {
			tn, kid := fmt.Sprintf("tenant%d", t), fmt.Sprintf("key%d", k)
			if *secret != "" {
				creds = append(creds, cred{mint(*secret, tn, kid)})
			} else {
				creds = append(creds, cred{tn + "-" + kid})
			}
		}
	}
	ipPool := make([]string, *ips)
	for i := range ipPool {
		ipPool[i] = fmt.Sprintf("%d.%d.%d.%d", 10+i/65025%200, i/255%255, i%255, 1+i%250)
	}

	tr := &http.Transport{MaxIdleConns: *conc * 2, MaxIdleConnsPerHost: *conc * 2, MaxConnsPerHost: *conc * 2,
		IdleConnTimeout: 90 * time.Second, DisableCompression: true}
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	var ok, limited, unauth, other, errs atomic.Int64
	perKeyOK := make([]atomic.Int64, len(creds))
	lats := make([][]int32, *conc) // microseconds, per worker
	start := time.Now()
	end := start.Add(*dur)
	var interval time.Duration
	if *qps > 0 {
		interval = time.Duration(float64(time.Second) * float64(*conc) / float64(*qps))
	}

	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + time.Now().UnixNano()))
			local := make([]int32, 0, 1<<16)
			next := start
			for time.Now().Before(end) {
				if interval > 0 {
					next = next.Add(interval)
					if d := time.Until(next); d > 200*time.Microsecond {
						time.Sleep(d)
					}
				}
				ci := rng.Intn(len(creds))
				if *zipf && rng.Intn(100) < 80 {
					ci = 0
				}
				req, _ := http.NewRequest("GET", *url, nil)
				req.Header.Set("X-API-Key", creds[ci].key)
				req.Header.Set("X-Forwarded-For", ipPool[rng.Intn(len(ipPool))])
				t0 := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					errs.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				local = append(local, int32(time.Since(t0)/time.Microsecond))
				switch resp.StatusCode {
				case 200:
					ok.Add(1)
					perKeyOK[ci].Add(1)
				case 429:
					limited.Add(1)
				case 401:
					unauth.Add(1)
				default:
					other.Add(1)
				}
			}
			lats[w] = local
		}(w)
	}
	wg.Wait()
	el := time.Since(start).Seconds()

	var all []int32
	for _, l := range lats {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[int(float64(len(all)-1)*p)]) / 1000
	}
	total := ok.Load() + limited.Load() + unauth.Load() + other.Load()
	fmt.Printf("target        %s\n", *url)
	fmt.Printf("duration      %.1fs   workers %d   keys %d   ips %d\n", el, *conc, len(creds), *ips)
	fmt.Printf("requests      %d  (%.0f req/s)   transport errors %d\n", total, float64(total)/el, errs.Load())
	fmt.Printf("status        200=%d  429=%d  401=%d  other=%d\n", ok.Load(), limited.Load(), unauth.Load(), other.Load())
	fmt.Printf("latency ms    p50=%.2f  p95=%.2f  p99=%.2f  p99.9=%.2f  max=%.2f\n",
		pct(.50), pct(.95), pct(.99), pct(.999), pct(1))
	exp := *expected
	if exp == 0 && *keyCap > 0 {
		// each key may admit at most: burst capacity + refill over the run
		exp = int64(float64(len(creds)) * (*keyCap + *keyRate*el))
	}
	if exp > 0 {
		dev := 100 * (float64(ok.Load()) - float64(exp)) / float64(exp)
		fmt.Printf("accuracy      allowed=%d expected<=%d  deviation=%+.2f%%  (positive = over-admission)\n", ok.Load(), exp, dev)
	}
	if len(os.Args) > 0 && errs.Load() > 0 {
		fmt.Println("note: transport errors present - lower -c or check the target")
	}
}
