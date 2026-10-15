package main

import (
	"context"
	_ "embed"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lease.lua
var leaseLua string

//go:embed give.lua
var giveLua string

// Hierarchy levels. A request must obtain a token at EVERY level.
const (
	LvlTenant = 0
	LvlKey    = 1
	LvlIP     = 2
)

var LvlName = [3]string{"tenant", "key", "ip"}

var (
	allowed, denied, redisCalls     atomic.Int64
	fallbackKeys, fallbackEvents    atomic.Int64
	deniedBy                        [3]atomic.Int64
	authHits, authMisses, evictions atomic.Int64
	tokensReturned, giveDropped     atomic.Int64
	reconciled                      atomic.Int64
)

// LevelCfg is the limit for one level of the hierarchy.
type LevelCfg struct {
	Cap      float64 // burst capacity (tokens)
	Rate     float64 // refill tokens / second
	WinMs    int64   // sliding window length (ms); 0 disables the window cap
	WinLimit int64   // max requests per sliding window
	// LocalOnly enforces this level purely in L1 as a per-replica share
	// (cap/replicas, rate/replicas) with no Redis traffic. Use for the IP level
	// when many sparse source IPs would otherwise cost one Redis call each.
	// Assumes the load balancer spreads an IP's traffic roughly evenly.
	LocalOnly bool
}

// Config holds the three levels. Replaced atomically on live updates.
type Config [3]LevelCfg

type bucket struct {
	mu         sync.Mutex
	key        string // redis key
	lvl        int
	tokens     int64 // locally reserved tokens (the lease)
	expiresAt  time.Time
	refilling  bool
	done       chan struct{} // closed when the in-flight refill finishes
	nextRefill time.Time
	lastUsed   time.Time

	// fallback state (used only while Redis is unreachable)
	fallback    bool
	localTokens float64
	lastFill    time.Time
	fbConsumed  int64 // tokens spent locally during the outage (reconciled on recovery)
	everLeased  bool  // true once Redis has answered for this bucket
	leaseN      int64 // adaptive lease size: grows while drained, halves when unused
	lastGrant   time.Time
}

// markRefilling / finishRefill: caller holds b.mu.
func (b *bucket) markRefilling() { b.refilling = true; b.done = make(chan struct{}) }
func (b *bucket) finishRefill()  { b.refilling = false; close(b.done) }

type giveReq struct {
	key string
	cfg LevelCfg
	n   int64
}

type Limiter struct {
	rdb          *redis.Client
	lease, give  *redis.Script
	cfg          atomic.Pointer[Config]
	buckets      *LRU[*bucket] // L1 cache of local token reservations (bounded)
	replicas     float64
	batch        int64
	leaseTTL     time.Duration
	redisTimeout time.Duration
	giveCh       chan giveReq
	sem          chan struct{} // bounds concurrent background Redis calls
	stop         chan struct{}
}

func NewLimiter(rdb *redis.Client, cfg Config, replicas float64, batch int64,
	leaseTTL time.Duration, cacheSize int) *Limiter {
	l := &Limiter{
		rdb: rdb, lease: redis.NewScript(leaseLua), give: redis.NewScript(giveLua),
		replicas: replicas, batch: batch, leaseTTL: leaseTTL,
		redisTimeout: 50 * time.Millisecond,
		giveCh:       make(chan giveReq, 4096),
		sem:          make(chan struct{}, 64),
		stop:         make(chan struct{}),
	}
	l.cfg.Store(&cfg)
	// Evicting a bucket never lets a client bypass limits: Redis stays
	// authoritative. We hand unused reserved tokens back to Redis.
	l.buckets = NewLRU[*bucket](cacheSize, 16, func(_ string, b *bucket) { l.releaseOnEvict(b) })
	for i := 0; i < 2; i++ {
		go l.giveWorker()
	}
	go l.daemon()
	return l
}

func (l *Limiter) Close() { close(l.stop) }

func (l *Limiter) Config() Config     { return *l.cfg.Load() }
func (l *Limiter) SetConfig(c Config) { l.cfg.Store(&c) }
func (l *Limiter) Buckets() int       { return l.buckets.Len() }

func (l *Limiter) getBucket(lvl int, key string) *bucket {
	return l.buckets.GetOrCreate(key, func() *bucket { return &bucket{key: key, lvl: lvl} }, false)
}

// maxLease caps a single reservation at this replica's fair share of the
// bucket, so one pod cannot hoard the shared budget.
func (l *Limiter) maxLease(c LevelCfg) int64 {
	n := l.batch
	if share := int64(math.Ceil(c.Cap / l.replicas)); n > share {
		n = share
	}
	if n < 1 {
		n = 1
	}
	return n
}

// nextLease is the size to request now. Cold / rarely-used buckets (most
// per-IP buckets) stay at a few tokens, so they neither hoard shared budget nor
// pay a give-back round trip; hot buckets ramp up to maxLease. Caller holds b.mu.
func (l *Limiter) nextLease(b *bucket, c LevelCfg) int64 {
	mx := l.maxLease(c)
	n := b.leaseN
	if n < 1 {
		n = 1
	}
	if n > mx {
		n = mx
	}
	return n
}

func (l *Limiter) lowWater(b *bucket, c LevelCfg) int64 { return l.nextLease(b, c) / 4 }

// wantsPrefetch: refill in the background only for buckets that have shown
// sustained demand (lease > 1). Sparse buckets refill on demand, one token at
// a time, so they never strand tokens or need a give-back round trip.
func (l *Limiter) wantsPrefetch(b *bucket, c LevelCfg) bool {
	return b.leaseN > 1 && b.tokens <= l.lowWater(b, c)
}

// Allow checks tenant, key and IP buckets. keys[i] is the Redis key for level i.
// It returns (true, -1) if allowed, else (false, level that denied).
func (l *Limiter) Allow(keys [3]string) (bool, int) {
	cfg := l.cfg.Load()
	var bs [3]*bucket
	for i := range bs {
		bs[i] = l.getBucket(i, keys[i])
	}
	// Phase 1: make sure every level has local tokens if Redis has any.
	//  - empty and free to refill  -> refill now (levels in parallel)
	//  - empty, refill in flight   -> wait for it (bounded by the Redis
	//    timeout) instead of denying while the shared bucket still has tokens
	//  - empty, shared bucket dry  -> (back-off window) deny immediately
	for attempt := 0; attempt < 3; attempt++ {
		var need [3]bool
		var waits [3]chan struct{}
		nNeed, nWait := 0, 0
		now := time.Now()
		for i := range bs {
			n, w := l.prepare(bs[i], cfg[i], now)
			need[i], waits[i] = n, w
			if n {
				nNeed++
			}
			if w != nil {
				nWait++
			}
		}
		if nNeed == 0 && nWait == 0 {
			break
		}
		if nNeed == 1 {
			for i := range bs {
				if need[i] {
					l.refill(bs[i], cfg[i])
				}
			}
		} else if nNeed > 1 { // cold start: lease the levels in parallel
			var wg sync.WaitGroup
			for i := range bs {
				if need[i] {
					wg.Add(1)
					go func(i int) { defer wg.Done(); l.refill(bs[i], cfg[i]) }(i)
				}
			}
			wg.Wait()
		}
		for i := range waits {
			if waits[i] != nil {
				<-waits[i]
			}
		}
	}

	// Lock in fixed level order (no deadlocks), check all, then consume all.
	for i := range bs {
		bs[i].mu.Lock()
	}
	now := time.Now()
	deny := -1
	for i := range bs {
		if !l.canTake(bs[i], cfg[i], now) {
			deny = i
			break
		}
	}
	if deny < 0 {
		for i := range bs {
			l.take(bs[i], cfg[i])
		}
	}
	var bg [3]bool
	for i := range bs {
		b := bs[i]
		b.lastUsed = now
		if !cfg[i].LocalOnly && !b.refilling && !now.Before(b.nextRefill) && (b.fallback || l.wantsPrefetch(b, cfg[i])) {
			b.markRefilling()
			bg[i] = true
		}
	}
	for i := range bs {
		bs[i].mu.Unlock()
	}
	for i := range bs {
		if bg[i] {
			l.goRefill(bs[i], cfg[i])
		}
	}
	return deny < 0, deny
}

// prepare expires stale leases (returning unused tokens to Redis) and tells
// the caller what to do about an empty bucket: refill synchronously (needSync),
// or wait on the refill that is already in flight (wait != nil).
func (l *Limiter) prepare(b *bucket, c LevelCfg, now time.Time) (needSync bool, wait chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.fallback && b.tokens > 0 && now.After(b.expiresAt) {
		l.queueGive(b.key, c, b.tokens) // drift compensation
		b.tokens = 0
		b.leaseN = maxI64(1, b.leaseN/2) // demand dropped: shrink the lease
	}
	if b.fallback || c.LocalOnly || b.tokens > 0 {
		return false, nil
	}
	if b.refilling {
		return false, b.done
	}
	if !now.Before(b.nextRefill) {
		b.markRefilling()
		return true, nil
	}
	return false, nil
}

// canTake: caller holds b.mu.
func (l *Limiter) canTake(b *bucket, c LevelCfg, now time.Time) bool {
	if b.fallback || c.LocalOnly {
		share := c.Cap / l.replicas
		if b.lastFill.IsZero() { // first sight of a local-only bucket: full share
			b.localTokens, b.lastFill = share, now
		}
		b.localTokens = math.Min(share, b.localTokens+now.Sub(b.lastFill).Seconds()*c.Rate/l.replicas)
		b.lastFill = now
		return b.localTokens >= 1
	}
	return b.tokens > 0
}

func (l *Limiter) take(b *bucket, c LevelCfg) {
	if b.fallback || c.LocalOnly {
		b.localTokens--
		b.fbConsumed++
	} else {
		b.tokens--
	}
}

// refill leases a batch of tokens from Redis. Caller must have set b.refilling.
func (l *Limiter) refill(b *bucket, c LevelCfg) {
	b.mu.Lock()
	n := l.nextLease(b, c)
	// Demand test: the previous lease was (nearly) drained AND was granted
	// less than one lease-TTL ago. A drained lease from long ago is just a
	// sparse client, not load.
	grow := !b.fallback && b.tokens <= n/4 && time.Since(b.lastGrant) < l.leaseTTL
	sparse := !b.fallback && time.Since(b.lastGrant) > 2*l.leaseTTL
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), l.redisTimeout)
	defer cancel()
	redisCalls.Add(1)
	granted, err := l.lease.Run(ctx, l.rdb, []string{b.key},
		c.Cap, c.Rate, n, c.WinMs, c.WinLimit).Int64()

	b.mu.Lock()
	b.finishRefill()
	now := time.Now()
	if err != nil {
		// Redis unreachable: fail-open-with-local-approximation.
		//  - never seen Redis for this key (cold start): start with this
		//    replica's full share, i.e. fail open up to cap/replicas;
		//  - already leased: carry over what is left, so a drained bucket
		//    does not get a free refill just because Redis went away.
		b.nextRefill = now.Add(100 * time.Millisecond)
		if !b.fallback {
			b.fallback = true
			if b.everLeased {
				b.localTokens = math.Min(float64(b.tokens), c.Cap/l.replicas)
			} else {
				b.localTokens = c.Cap / l.replicas
			}
			b.tokens = 0
			b.lastFill = now
			b.fbConsumed = 0
			fallbackKeys.Add(1)
			fallbackEvents.Add(1)
		}
		b.mu.Unlock()
		return
	}
	b.everLeased = true
	var debt int64
	if b.fallback { // recovery
		b.fallback = false
		fallbackKeys.Add(-1)
		debt, b.fbConsumed = b.fbConsumed, 0
	}
	if granted == 0 {
		b.nextRefill = now.Add(100 * time.Millisecond) // shared bucket empty: back off
	} else {
		b.tokens += granted
		b.expiresAt = now.Add(l.leaseTTL)
		b.lastGrant = now
		switch {
		case grow:
			b.leaseN = n * 2
		case sparse:
			b.leaseN = maxI64(1, n/2)
		default:
			b.leaseN = n
		}
	}
	b.mu.Unlock()

	// Drift compensation on recovery: charge the shared bucket for what this
	// replica admitted locally while Redis was away, so the outage does not
	// leave the global budget artificially full.
	if debt > 0 {
		ctx2, cancel2 := context.WithTimeout(context.Background(), l.redisTimeout)
		defer cancel2()
		redisCalls.Add(1)
		if _, err := l.lease.Run(ctx2, l.rdb, []string{b.key},
			c.Cap, c.Rate, debt, c.WinMs, c.WinLimit).Int64(); err == nil {
			reconciled.Add(debt)
		}
	}
}

// goRefill runs refill in the background, bounded by a semaphore so a Redis
// outage cannot spawn unbounded goroutines. b.refilling must already be true.
func (l *Limiter) goRefill(b *bucket, c LevelCfg) {
	select {
	case l.sem <- struct{}{}:
		go func() { defer func() { <-l.sem }(); l.refill(b, c) }()
	default:
		b.mu.Lock()
		b.finishRefill()
		b.mu.Unlock()
	}
}

func (l *Limiter) queueGive(key string, c LevelCfg, n int64) {
	if n <= 0 {
		return
	}
	select {
	case l.giveCh <- giveReq{key, c, n}:
	default:
		giveDropped.Add(n) // best effort: tokens just expire with the bucket
	}
}

func (l *Limiter) releaseOnEvict(b *bucket) {
	b.mu.Lock()
	n := b.tokens
	b.tokens = 0
	b.mu.Unlock()
	l.queueGive(b.key, l.cfg.Load()[b.lvl], n)
}

func (l *Limiter) giveWorker() {
	for {
		select {
		case <-l.stop:
			return
		case g := <-l.giveCh:
			ctx, cancel := context.WithTimeout(context.Background(), l.redisTimeout)
			redisCalls.Add(1)
			if err := l.give.Run(ctx, l.rdb, []string{g.key}, g.cfg.Cap, g.n, g.cfg.WinMs).Err(); err == nil {
				tokensReturned.Add(g.n)
			}
			cancel()
		}
	}
}

// daemon is the background batch-sync loop: it returns expired leases,
// pre-fetches batches for active buckets that are running low, and probes
// Redis for recovery on buckets in fallback mode - even with no traffic.
func (l *Limiter) daemon() {
	period := l.leaseTTL / 2
	if period < 10*time.Millisecond {
		period = 10 * time.Millisecond
	}
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.sweep()
		}
	}
}

func (l *Limiter) sweep() {
	cfg := l.cfg.Load()
	now := time.Now()
	var todo []*bucket
	l.buckets.Range(func(_ string, b *bucket) {
		b.mu.Lock()
		defer b.mu.Unlock()
		c := cfg[b.lvl]
		if !b.fallback && b.tokens > 0 && now.After(b.expiresAt) {
			l.queueGive(b.key, c, b.tokens)
			b.tokens = 0
			b.leaseN = maxI64(1, b.leaseN/2)
		}
		active := now.Sub(b.lastUsed) < 2*l.leaseTTL
		if !c.LocalOnly && !b.refilling && !now.Before(b.nextRefill) &&
			(b.fallback || (active && b.tokens <= l.lowWater(b, c))) {
			b.markRefilling()
			todo = append(todo, b)
		}
	})
	for _, b := range todo {
		l.goRefill(b, cfg[b.lvl])
	}
}

func maxI64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
