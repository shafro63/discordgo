package discordgo

import (
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

// RateLimiter holds all ratelimit buckets
type RateLimiter struct {
	sync.Mutex
	global           *int64
	buckets          map[string]*Bucket
	bucketHashes     map[string]*Bucket // maps Discord X-RateLimit-Bucket hash -> shared Bucket
	globalRateLimit  time.Duration
	customRateLimits []*customRateLimit
}

// NewRatelimiter returns a new RateLimiter
func NewRatelimiter() *RateLimiter {

	return &RateLimiter{
		buckets:      make(map[string]*Bucket),
		bucketHashes: make(map[string]*Bucket),
		global:       new(int64),
		customRateLimits: []*customRateLimit{
			{
				suffix:   "//reactions//",
				requests: 1,
				reset:    200 * time.Millisecond,
			},
		},
	}
}

// GetBucket retrieves or creates a bucket
func (r *RateLimiter) GetBucket(key string) *Bucket {
	r.Lock()
	defer r.Unlock()

	if bucket, ok := r.buckets[key]; ok {
		return bucket
	}

	b := &Bucket{
		Remaining: 1,
		Key:       key,
		global:    r.global,
	}

	// Check if there is a custom ratelimit set for this bucket ID.
	for _, rl := range r.customRateLimits {
		if strings.HasSuffix(b.Key, rl.suffix) {
			b.customRateLimit = rl
			break
		}
	}

	r.buckets[key] = b
	return b
}

// RegisterBucketHash links a Discord rate limit bucket hash to the bucket for the given URL key.
// If the hash is already known, the URL entry in buckets is aliased to the existing shared bucket.
// Must be called after bucket.Release() so the bucket lock is no longer held.
func (r *RateLimiter) RegisterBucketHash(urlKey, hash string) {
	r.Lock()
	defer r.Unlock()

	if b, ok := r.bucketHashes[hash]; ok {
		// Hash already known: alias this URL to the existing shared bucket
		r.buckets[urlKey] = b
	} else {
		// New hash: register the current bucket as the canonical one for this hash
		if b, ok := r.buckets[urlKey]; ok {
			r.bucketHashes[hash] = b
		}
	}
}

// GetWaitTime returns the duration you should wait for a Bucket
func (r *RateLimiter) GetWaitTime(b *Bucket, minRemaining int) time.Duration {
	// If we ran out of calls and the reset time is still ahead of us
	// then we need to take it easy and relax a little
	if b.Remaining < minRemaining && b.reset.After(time.Now()) {
		return time.Until(b.reset)
	}

	// Check for global ratelimits
	sleepTo := time.Unix(0, atomic.LoadInt64(r.global))
	if time.Now().Before(sleepTo) {
		return time.Until(sleepTo)
	}

	return 0
}

// LockBucket Locks until a request can be made
func (r *RateLimiter) LockBucket(bucketID string) *Bucket {
	return r.LockBucketObject(r.GetBucket(bucketID))
}

// LockBucketObject Locks an already resolved bucket until a request can be made
func (r *RateLimiter) LockBucketObject(b *Bucket) *Bucket {
	b.Lock()

	if wait := r.GetWaitTime(b, 1); wait > 0 {
		time.Sleep(wait)
	}

	b.Remaining--
	return b
}

// Bucket represents a ratelimit bucket, each bucket gets ratelimited individually (-global ratelimits)
type Bucket struct {
	sync.Mutex
	Key       string
	Remaining int
	Limit     int
	reset     time.Time
	global    *int64

	lastReset        time.Time
	customRateLimit  *customRateLimit
	Userdata         interface{}
	discordBucketID  string // value of X-RateLimit-Bucket, set after first response

	// Scope is set on 429 responses: "user", "global", or "shared".
	Scope string
}

// Release unlocks the bucket and reads the headers to update the buckets ratelimit info
// and locks up the whole thing in case if there's a global ratelimit.
func (b *Bucket) Release(headers http.Header) error {
	defer b.Unlock()

	// Check if the bucket uses a custom ratelimiter
	if rl := b.customRateLimit; rl != nil {
		if time.Since(b.lastReset) >= rl.reset {
			b.Remaining = rl.requests - 1
			b.lastReset = time.Now()
		}
		if b.Remaining < 1 {
			b.reset = time.Now().Add(rl.reset)
		}
		return nil
	}

	if headers == nil {
		return nil
	}

	remaining := headers.Get("X-RateLimit-Remaining")
	reset := headers.Get("X-RateLimit-Reset")
	global := headers.Get("X-RateLimit-Global")
	resetAfter := headers.Get("X-RateLimit-Reset-After")

	// X-RateLimit-Bucket identifies the shared rate limit group for this route.
	if h := headers.Get("X-RateLimit-Bucket"); h != "" {
		b.discordBucketID = h
	}

	// X-RateLimit-Scope is returned on 429 responses: "user", "global", or "shared".
	if scope := headers.Get("X-RateLimit-Scope"); scope != "" {
		b.Scope = scope
	}

	// Update global and per bucket reset time if the proper headers are available.
	// Priority: X-RateLimit-Reset-After (most precise, sub-second) > Retry-After (integer seconds,
	// standard HTTP header on 429s) > X-RateLimit-Reset (epoch timestamp, requires clock skew correction).
	if resetAfter != "" {
		parsedAfter, err := strconv.ParseFloat(resetAfter, 64)
		if err != nil {
			return err
		}

		whole, frac := math.Modf(parsedAfter)
		resetAt := time.Now().Add(time.Duration(whole) * time.Second).Add(time.Duration(frac*1000) * time.Millisecond)

		// Lock either this single bucket or all buckets
		if global != "" {
			atomic.StoreInt64(b.global, resetAt.UnixNano())
		} else {
			b.reset = resetAt
		}
	} else if retryAfter := headers.Get("Retry-After"); retryAfter != "" {
		// Retry-After is an integer number of seconds, sent on 429 responses.
		parsedAfter, err := strconv.ParseInt(retryAfter, 10, 64)
		if err != nil {
			return err
		}
		resetAt := time.Now().Add(time.Duration(parsedAfter) * time.Second)
		if global != "" {
			atomic.StoreInt64(b.global, resetAt.UnixNano())
		} else {
			b.reset = resetAt
		}
	} else if reset != "" {
		// Calculate the reset time by using the date header returned from discord
		discordTime, err := http.ParseTime(headers.Get("Date"))
		if err != nil {
			return err
		}

		unix, err := strconv.ParseFloat(reset, 64)
		if err != nil {
			return err
		}

		// Calculate the time until reset and add it to the current local time
		// some extra time is added because without it i still encountered 429's.
		// The added amount is the lowest amount that gave no 429's
		// in 1k requests
		whole, frac := math.Modf(unix)
		delta := time.Unix(int64(whole), 0).Add(time.Duration(frac*1000)*time.Millisecond).Sub(discordTime) + time.Millisecond*250
		b.reset = time.Now().Add(delta)
	}

	// Update remaining if header is present
	if remaining != "" {
		parsedRemaining, err := strconv.ParseInt(remaining, 10, 32)
		if err != nil {
			return err
		}
		b.Remaining = int(parsedRemaining)
	}

	// Update limit if header is present
	if limit := headers.Get("X-RateLimit-Limit"); limit != "" {
		parsedLimit, err := strconv.ParseInt(limit, 10, 32)
		if err != nil {
			return err
		}
		b.Limit = int(parsedLimit)
	}

	return nil
}
