package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// Claims is the validated identity attached to a request.
type Claims struct {
	Tenant string
	KeyID  string
	Exp    time.Time // zero = no expiry
}

// Authenticator validates credentials with local CPU work only (HMAC), so an
// unauthenticated request is rejected before any Redis access. Validated
// claims are kept in a bounded LRU (the L1 claims cache).
//
// Accepted credentials:
//   - X-API-Key: <tenant>.<keyid>.<sig>   sig = hex(HMAC-SHA256(secret, tenant+"."+keyid))[:32]
//   - Authorization: Bearer <HS256 JWT>   claims: tenant, sub (key id), exp
//
// If no secret is configured the authenticator runs in legacy mode: any
// non-empty X-API-Key is accepted (tenant "default") - for demos only.
type Authenticator struct {
	secret []byte
	cache  *LRU[Claims]
	ttl    time.Duration
}

func NewAuthenticator(secret string, cacheSize int, ttl time.Duration) *Authenticator {
	return &Authenticator{secret: []byte(secret), cache: NewLRU[Claims](cacheSize, 16, nil), ttl: ttl}
}

func (a *Authenticator) sign(msg string) string {
	m := hmac.New(sha256.New, a.secret)
	m.Write([]byte(msg))
	return hex.EncodeToString(m.Sum(nil))[:32]
}

// MintKey creates an API key for (tenant, keyID).
func (a *Authenticator) MintKey(tenant, keyID string) string {
	return tenant + "." + keyID + "." + a.sign(tenant+"."+keyID)
}

func (a *Authenticator) Authenticate(r *http.Request) (Claims, bool) {
	cred := ""
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		cred = strings.TrimPrefix(h, "Bearer ")
	} else if k := r.Header.Get("X-API-Key"); k != "" {
		cred = k
	}
	if cred == "" {
		return Claims{}, false
	}
	if len(a.secret) == 0 { // legacy demo mode
		return Claims{Tenant: "default", KeyID: cred}, true
	}
	now := time.Now()
	if c, ok := a.cache.Get(cred); ok {
		if c.Exp.IsZero() || now.Before(c.Exp) {
			authHits.Add(1)
			return c, true
		}
	}
	authMisses.Add(1)
	var c Claims
	var ok bool
	if strings.Count(cred, ".") == 2 && strings.HasPrefix(cred, "ey") {
		c, ok = a.parseJWT(cred, now)
	} else {
		c, ok = a.parseAPIKey(cred)
	}
	if !ok {
		return Claims{}, false
	}
	// cache entry lives at most ttl (and never past the token's own expiry)
	exp := now.Add(a.ttl)
	if !c.Exp.IsZero() && c.Exp.Before(exp) {
		exp = c.Exp
	}
	cached := c
	cached.Exp = exp
	a.cache.Put(cred, cached)
	return c, true
}

func (a *Authenticator) parseAPIKey(k string) (Claims, bool) {
	p := strings.Split(k, ".")
	if len(p) != 3 || p[0] == "" || p[1] == "" {
		return Claims{}, false
	}
	want := a.sign(p[0] + "." + p[1])
	if !hmac.Equal([]byte(want), []byte(p[2])) {
		return Claims{}, false
	}
	return Claims{Tenant: p[0], KeyID: p[1]}, true
}

func (a *Authenticator) parseJWT(tok string, now time.Time) (Claims, bool) {
	p := strings.Split(tok, ".")
	if len(p) != 3 {
		return Claims{}, false
	}
	hb, err := base64.RawURLEncoding.DecodeString(p[0])
	if err != nil {
		return Claims{}, false
	}
	var hdr struct {
		Alg string `json:"alg"`
	}
	if json.Unmarshal(hb, &hdr) != nil || hdr.Alg != "HS256" { // reject "none" and other algs
		return Claims{}, false
	}
	m := hmac.New(sha256.New, a.secret)
	m.Write([]byte(p[0] + "." + p[1]))
	sig, err := base64.RawURLEncoding.DecodeString(p[2])
	if err != nil || !hmac.Equal(sig, m.Sum(nil)) {
		return Claims{}, false
	}
	pb, err := base64.RawURLEncoding.DecodeString(p[1])
	if err != nil {
		return Claims{}, false
	}
	var cl struct {
		Tenant string `json:"tenant"`
		Sub    string `json:"sub"`
		Exp    int64  `json:"exp"`
	}
	if json.Unmarshal(pb, &cl) != nil || cl.Tenant == "" || cl.Sub == "" {
		return Claims{}, false
	}
	c := Claims{Tenant: cl.Tenant, KeyID: cl.Sub}
	if cl.Exp != 0 {
		c.Exp = time.Unix(cl.Exp, 0)
		if !now.Before(c.Exp) {
			return Claims{}, false
		}
	}
	return c, true
}
