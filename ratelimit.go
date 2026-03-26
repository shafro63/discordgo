package discordgo

import (
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// customRateLimit holds information for defining a custom rate limit
type customRateLimit struct {
	suffix   string
	requests int
	reset    time.Duration
}

// globalRateLimitPerSecond is the maximum number of requests a bot can make
// per second across all routes, as defined by Discord's documentation.
const globalRateLimitPerSecond = 50

// RateLimitData holds all rate-limit information parsed from a Discord API
// response, aggregating both the X-RateLimit-* headers and, for HTTP 429
// responses, the JSON body fields into a single struct.
type RateLimitData struct {
	// Limit is the request cap for the current window (X-RateLimit-Limit).
	Limit int
	// Remaining is the requests still available in the current window.
	// -1 means the header was absent (e.g. error responses).
	Remaining int
	// BucketHash is the Discord bucket identifier (X-RateLimit-Bucket).
	BucketHash string
	// Global is true when the rate limit applies globally.
	// Set by X-RateLimit-Global header or when Scope is "global".
	Global bool
	// Scope is the rate limit scope: "user", "global", or "shared".
	// Only sent on 429 responses (X-RateLimit-Scope).
	Scope string
	// ResetAt is the pre-computed time at which the bucket resets.
	// Derived in priority order: 429 JSON body > Reset-After > Retry-After > Reset.
	ResetAt time.Time

	// TooManyRequests is non-nil on HTTP 429 responses and carries the JSON body.
	TooManyRequests *TooManyRequests

	Cloudflare bool
}

// ParseRateLimitResponse parses all rate-limit information from an HTTP
// response. It reads both the X-RateLimit-* headers and, for 429 responses,
// the JSON body. The raw response body is always returned for the caller to
// process (success payload, error message, etc.).
func ParseRateLimitResponse(resp *http.Response) (data *RateLimitData, body []byte, err error) {
	data = &RateLimitData{}
	data.Remaining = -1 // -1 signals "header absent"

	h := resp.Header

	if v := h.Get("X-RateLimit-Limit"); v != "" {
		data.Limit, _ = strconv.Atoi(v)
	}
	if v := h.Get("X-RateLimit-Remaining"); v != "" {
		if r, e := strconv.Atoi(v); e == nil {
			data.Remaining = r
		}
	}

	data.BucketHash = h.Get("X-RateLimit-Bucket")
	data.Global = h.Get("X-RateLimit-Global") == "true"
	data.Scope = h.Get("X-RateLimit-Scope")
	if data.Scope == "global" {
		data.Global = true
	}
	data.Cloudflare = h.Get("Via") == ""

	// Compute ResetAt from standard rate-limit headers (all responses).
	// X-RateLimit-Reset-After is preferred (sub-second, clock-skew safe).
	// X-RateLimit-Reset is the fallback (absolute Unix epoch).
	// Retry-After is 429-only and handled below.
	switch {
	case h.Get("X-RateLimit-Reset-After") != "":
		if secs, e := strconv.ParseFloat(h.Get("X-RateLimit-Reset-After"), 64); e == nil {
			data.ResetAt = time.Now().Add(time.Duration(secs * float64(time.Second)))
		}
	case h.Get("X-RateLimit-Reset") != "":
		if unix, e := strconv.ParseFloat(h.Get("X-RateLimit-Reset"), 64); e == nil {
			whole, frac := math.Modf(unix)
			data.ResetAt = time.Unix(int64(whole), int64(frac*float64(time.Second)))
		}
	}

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		// Retry-After is 429-only and takes highest priority for the retry delay.
		// It may be present even without a JSON body (e.g. Cloudflare or proxy).
		if v := h.Get("Retry-After"); v != "" {
			if secs, e := strconv.ParseInt(v, 10, 64); e == nil {
				data.ResetAt = time.Now().Add(time.Duration(secs) * time.Second)
			}
		}

		// Parse JSON body if present. retry_after overrides only when Retry-After
		// header was absent.
		if len(body) > 0 {
			tmr := &TooManyRequests{}
			if e := tmr.UnmarshalJSON(body); e != nil {
				err = e
				return
			}
			data.TooManyRequests = tmr
			if tmr.Global {
				data.Global = true
			}
			if h.Get("Retry-After") == "" && tmr.RetryAfter > 0 {
				data.ResetAt = time.Now().Add(tmr.RetryAfter)
			}
		}
	}

	return
}

// -----------------------------------------------------------------------
// RateLimiter
// -----------------------------------------------------------------------

// RateLimiter holds all rate-limit buckets and enforces both per-route and
// global rate limits according to Discord's API documentation.
//
// Buckets are keyed by "{discordHash}:{majorParam}" once the Discord bucket
// hash is known, or "~{routeTemplate}:{majorParam}" before the first response.
// This ensures that different major parameters (channel_id, guild_id,
// webhook_id+token) get independent rate-limit tracking even when they share
// the same Discord bucket hash.
type RateLimiter struct {
	sync.Mutex

	// global is the nanosecond timestamp until which ALL requests are blocked.
	// Set reactively when Discord returns a 429 with X-RateLimit-Global or
	// X-RateLimit-Scope: global.
	global *int64

	// globalTokens is a buffered channel of size 50 implementing the
	// proactive 50 req/s global rate limit. Acquiring a token is a simple
	// receive; a background goroutine refills it every second.
	globalTokens chan struct{}

	// buckets maps resolved bucket keys to their Bucket.
	buckets map[string]*Bucket

	// hashMap maps route templates to the Discord X-RateLimit-Bucket hash.
	// Once a hash is known for a route template, all future requests to that
	// route (regardless of major parameter) resolve via the hash.
	hashMap map[string]string

	customRateLimits []*customRateLimit

	// onGlobalRateLimit is called when a request must wait for
	// the proactive 50 req/s global rate limit. Optional.
	onGlobalRateLimit func(bucketID string)
}

// makeGlobalTokens creates a buffered channel pre-filled with 50 tokens.
func makeGlobalTokens() chan struct{} {
	ch := make(chan struct{}, globalRateLimitPerSecond)
	for i := 0; i < globalRateLimitPerSecond; i++ {
		ch <- struct{}{}
	}
	return ch
}

// startGlobalRefiller launches a background goroutine that refills the
// global token bucket every second back to 50.
func startGlobalRefiller(ch chan struct{}) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			for {
				select {
				case ch <- struct{}{}:
				default:
					goto full
				}
			}
		full:
		}
	}()
}

// NewRatelimiter returns a new RateLimiter.
func NewRatelimiter() *RateLimiter {
	tokens := makeGlobalTokens()
	startGlobalRefiller(tokens)
	return &RateLimiter{
		buckets:      make(map[string]*Bucket),
		hashMap:      make(map[string]string),
		global:       new(int64),
		globalTokens: tokens,
		customRateLimits: []*customRateLimit{
			{suffix: "//reactions//", requests: 1, reset: 200 * time.Millisecond},
		},
	}
}

// extractMajorParam parses a bucket ID (URL path) and extracts the Discord
// major parameter. Major parameters create independent rate limits within the
// same route: channel_id, guild_id, and webhook_id+webhook_token.
//
// Returns a route template (major param replaced by placeholder) and the
// extracted major parameter value. majorParam is "" when none is found.
func extractMajorParam(bucketID string) (routeTemplate, majorParam string) {
	path := bucketID
	if idx := strings.Index(path, "/api/v"); idx >= 0 {
		rest := path[idx:]
		if slashIdx := strings.Index(rest[1:], "/"); slashIdx >= 0 {
			afterVersion := rest[1+slashIdx:]
			if slashIdx2 := strings.Index(afterVersion[1:], "/"); slashIdx2 >= 0 {
				path = afterVersion[1+slashIdx2+1:]
			} else {
				path = afterVersion[1:]
			}
		}
	} else {
		path = strings.TrimPrefix(path, "/")
	}

	segments := strings.Split(path, "/")
	if len(segments) < 2 {
		return bucketID, ""
	}

	switch segments[0] {
	case "channels":
		majorParam = segments[1]
		segments[1] = "{channel_id}"
	case "guilds":
		majorParam = segments[1]
		segments[1] = "{guild_id}"
	case "webhooks":
		if len(segments) >= 3 {
			majorParam = segments[1] + ":" + segments[2]
			segments[1] = "{webhook_id}"
			segments[2] = "{webhook_token}"
		} else if len(segments) == 2 {
			majorParam = segments[1]
			segments[1] = "{webhook_id}"
		}
	}

	routeTemplate = strings.Join(segments, "/")
	return routeTemplate, majorParam
}

// resolveKey returns the map key for a bucket given its route template and
// major parameter. Uses the Discord hash when known, otherwise a synthetic key.
func (r *RateLimiter) resolveKey(routeTemplate, majorParam string) string {
	if hash, ok := r.hashMap[routeTemplate]; ok {
		return hash + ":" + majorParam
	}
	return "~" + routeTemplate + ":" + majorParam
}

// GetBucket retrieves or creates a bucket for the given bucket ID.
func (r *RateLimiter) GetBucket(key string) *Bucket {
	r.Lock()
	defer r.Unlock()

	routeTemplate, majorParam := extractMajorParam(key)
	resolvedKey := r.resolveKey(routeTemplate, majorParam)

	if b, ok := r.buckets[resolvedKey]; ok {
		return b
	}

	b := &Bucket{
		Remaining:      1,
		Key:            resolvedKey,
		global:         r.global,
		updated:        make(chan struct{}, 1),
		originalKey:    key,
		majorParam:     majorParam,
		syntheticRoute: routeTemplate,
	}

	for _, rl := range r.customRateLimits {
		if strings.HasSuffix(key, rl.suffix) {
			b.customRateLimit = rl
			break
		}
	}

	r.buckets[resolvedKey] = b
	return b
}

// RegisterBucketHash links a Discord rate-limit bucket hash to a route
// template. Migrates the bucket from its synthetic key to the hash-based key.
// No-op when the hash is already registered and unchanged for the route.
func (r *RateLimiter) RegisterBucketHash(urlKey, hash string) {
	r.Lock()
	defer r.Unlock()

	routeTemplate, majorParam := extractMajorParam(urlKey)

	oldHash, existed := r.hashMap[routeTemplate]
	if existed && oldHash == hash {
		return
	}

	r.hashMap[routeTemplate] = hash

	// If the hash changed, purge all orphaned buckets keyed by the old hash.
	// They'll never be resolved again since resolveKey now returns the new hash.
	if existed {
		prefix := oldHash + ":"
		for key := range r.buckets {
			if strings.HasPrefix(key, prefix) {
				delete(r.buckets, key)
			}
		}
	}

	syntheticKey := "~" + routeTemplate + ":" + majorParam
	hashKey := hash + ":" + majorParam

	if b, ok := r.buckets[syntheticKey]; ok {
		b.Key = hashKey
		r.buckets[hashKey] = b
		delete(r.buckets, syntheticKey)
	}
}

// GetWaitTime returns the duration to wait before a bucket can be used.
// Checks both per-bucket exhaustion and reactive global rate limits (429).
func (r *RateLimiter) GetWaitTime(b *Bucket, minRemaining int) time.Duration {
	if b.Remaining < minRemaining && b.reset.After(time.Now()) {
		return time.Until(b.reset)
	}
	sleepTo := time.Unix(0, atomic.LoadInt64(r.global))
	if time.Now().Before(sleepTo) {
		return time.Until(sleepTo)
	}
	return 0
}

// acquireGlobalToken blocks until a global token is available.
// Interaction endpoints are exempt per Discord documentation.
func (r *RateLimiter) acquireGlobalToken(bucketID string) {
	if strings.Contains(bucketID, "/interactions/") {
		return
	}
	// Try non-blocking first.
	select {
	case <-r.globalTokens:
		return
	default:
	}
	// No token available — warn and block.
	if r.onGlobalRateLimit != nil {
		r.onGlobalRateLimit(bucketID)
	}
	<-r.globalTokens
}

// LockBucket locks until a request can be made for the given bucket ID.
func (r *RateLimiter) LockBucket(bucketID string) *Bucket {
	return r.LockBucketObject(r.GetBucket(bucketID))
}

// LockBucketObject locks an already-resolved bucket until a request can be
// made, enforcing both per-bucket rate limits and the global 50 req/s cap.
func (r *RateLimiter) LockBucketObject(b *Bucket) *Bucket {
	b.waitForUpdate()

	if wait := r.GetWaitTime(b, 1); wait > 0 {
		time.Sleep(wait)
	}

	r.acquireGlobalToken(b.originalKey)

	b.Remaining--
	return b
}

// -----------------------------------------------------------------------
// Bucket
// -----------------------------------------------------------------------

// Bucket represents a rate-limit bucket. Each bucket is rate-limited
// independently from global limits. Buckets are identified by their Discord
// bucket hash combined with the major parameter value.
type Bucket struct {
	once sync.Once

	Key       string
	Remaining int

	reset  time.Time
	global *int64

	lastReset       time.Time
	customRateLimit *customRateLimit
	Userdata        interface{}
	discordBucketID string

	updated chan struct{}
	// Scope is the value of X-RateLimit-Scope from the last 429: "user", "global", or "shared".
	Scope string

	// originalKey is the raw bucketID URL passed by restapi.go.
	originalKey string
	// majorParam is the extracted major parameter value.
	majorParam string
	// syntheticRoute is the route template before Discord hash resolution.
	syntheticRoute string
}

func (b *Bucket) waitForUpdate() {
	b.once.Do(func() {
		b.updated <- struct{}{}
	})
	<-b.updated
}

func (b *Bucket) signal() {
	b.updated <- struct{}{}
}

// ReleaseEmpty signals the update channel without modifying bucket state.
// Use this when a request fails before a response is received.
func (b *Bucket) ReleaseEmpty() {
	b.signal()
}

// Release updates the bucket state from a parsed rate-limit response and
// unblocks any goroutines waiting on this bucket.
func (b *Bucket) Release(data *RateLimitData) {
	b.Update(data)
}

// Update applies rate-limit data to the bucket and signals waiting goroutines.
// Must be called exactly once per request, whether or not a response was received.
func (b *Bucket) Update(data *RateLimitData) {
	defer b.signal()

	if rl := b.customRateLimit; rl != nil {
		if time.Since(b.lastReset) >= rl.reset {
			b.Remaining = rl.requests - 1
			b.lastReset = time.Now()
		}
		if b.Remaining < 1 {
			b.reset = time.Now().Add(rl.reset)
		}
		return
	}

	if data.Scope != "" {
		b.Scope = data.Scope
	}
	if data.BucketHash != "" {
		b.discordBucketID = data.BucketHash
	}

	if !data.ResetAt.IsZero() {
		if data.Global {
			atomic.StoreInt64(b.global, data.ResetAt.UnixNano())
		} else {
			b.reset = data.ResetAt
		}
	}

	switch {
	case data.TooManyRequests != nil:
		// On 429, the bucket is exhausted regardless of what Remaining says.
		b.Remaining = 0
	case data.Remaining >= 0:
		b.Remaining = data.Remaining
	}
}
