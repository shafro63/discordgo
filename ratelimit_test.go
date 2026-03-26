package discordgo

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// This test takes ~2 seconds to run
func TestRatelimitReset(t *testing.T) {
	rl := NewRatelimiter()

	sendReq := func(endpoint string) {
		bucket := rl.LockBucket(endpoint)
		bucket.Release(&RateLimitData{
			Remaining: 0,
			Limit:     5,
			ResetAt:   time.Now().Add(2 * time.Second),
		})
	}

	sent := time.Now()
	sendReq("/guilds/99/channels")
	sendReq("/guilds/55/channels")
	sendReq("/guilds/66/channels")

	sendReq("/guilds/99/channels")
	sendReq("/guilds/55/channels")
	sendReq("/guilds/66/channels")

	if time.Since(sent) >= time.Second && time.Since(sent) < time.Second*4 {
		t.Log("OK", time.Since(sent))
	} else {
		t.Error("Did not ratelimit correctly, got:", time.Since(sent))
	}
}

// This test takes ~1 second to run
func TestRatelimitGlobal(t *testing.T) {
	rl := NewRatelimiter()

	sendReq := func(endpoint string) {
		bucket := rl.LockBucket(endpoint)
		bucket.Release(&RateLimitData{
			Global:    true,
			Remaining: -1,
			ResetAt:   time.Now().Add(time.Second),
		})
	}

	sent := time.Now()
	sendReq("/guilds/99/channels")
	time.Sleep(100 * time.Millisecond)
	sendReq("/guilds/55/channels")

	if time.Since(sent) >= time.Second && time.Since(sent) < time.Second*2 {
		t.Log("OK", time.Since(sent))
	} else {
		t.Error("Did not ratelimit correctly, got:", time.Since(sent))
	}
}

// TestRatelimitScopeHeader verifies that Scope is stored on the Bucket.
func TestRatelimitScopeHeader(t *testing.T) {
	rl := NewRatelimiter()
	bucket := rl.LockBucket("/guilds/99/channels")

	bucket.Release(&RateLimitData{
		Scope:     "shared",
		Remaining: -1,
		ResetAt:   time.Now().Add(time.Second),
	})
	if bucket.Scope != "shared" {
		t.Errorf("expected Scope=shared, got %q", bucket.Scope)
	}
}

// TestRatelimitBucketHash verifies that two different URLs with different major
// parameters get independent buckets even when sharing the same Discord bucket hash.
func TestRatelimitBucketHash(t *testing.T) {
	rl := NewRatelimiter()
	hash := "abcd1234"

	sendReq := func(endpoint string) *Bucket {
		bucket := rl.LockBucket(endpoint)
		bucket.Release(&RateLimitData{
			Remaining:  4,
			Limit:      5,
			BucketHash: hash,
			ResetAt:    time.Now().Add(10 * time.Second),
		})
		rl.RegisterBucketHash(bucket.originalKey, bucket.discordBucketID)
		return bucket
	}

	b1 := sendReq("/channels/123/messages")
	b2 := sendReq("/channels/456/messages")

	// Different major params → independent buckets
	if b1 == b2 {
		t.Error("expected different Bucket pointers for different channel IDs")
	}
	// Same route + same major param → same bucket
	if b1 != rl.GetBucket("/channels/123/messages") {
		t.Error("expected same bucket for /channels/123/messages after hash registration")
	}
	if b1.Remaining != 4 {
		t.Errorf("expected b1.Remaining=4, got %d", b1.Remaining)
	}
	if b2.Remaining != 4 {
		t.Errorf("expected b2.Remaining=4, got %d", b2.Remaining)
	}
}

// TestRatelimitMajorParamIndependence verifies that exhausting the rate limit
// on one channel does not block a different channel on the same route.
func TestRatelimitMajorParamIndependence(t *testing.T) {
	rl := NewRatelimiter()

	b1 := rl.LockBucket("/channels/123/messages")
	b1.Release(&RateLimitData{Remaining: 0, ResetAt: time.Now().Add(5 * time.Second)})

	b2 := rl.GetBucket("/channels/456/messages")
	if wait := rl.GetWaitTime(b2, 1); wait > 0 {
		t.Errorf("channel 456 should not be rate limited, but got wait=%v", wait)
	}
}

// TestRatelimitGlobalTokenBucket verifies the proactive 50 req/s global limit.
func TestRatelimitGlobalTokenBucket(t *testing.T) {
	rl := NewRatelimiter()

	var warned int64
	rl.onGlobalRateLimit = func(bucketID string) {
		atomic.AddInt64(&warned, 1)
	}

	// Drain all 50 tokens — none should block.
	for i := 0; i < 50; i++ {
		rl.acquireGlobalToken("/channels/123/messages")
	}

	if atomic.LoadInt64(&warned) != 0 {
		t.Error("onGlobalRateLimit should not fire for the first 50 requests")
	}

	// 51st should block and trigger the warning callback.
	done := make(chan struct{})
	go func() {
		rl.acquireGlobalToken("/channels/123/messages")
		close(done)
	}()

	select {
	case <-done:
		// The refiller eventually unblocked it.
	case <-time.After(100 * time.Millisecond):
		// Expected: still blocked after 100ms.
	}

	if atomic.LoadInt64(&warned) != 1 {
		t.Errorf("expected onGlobalRateLimit to fire once, got %d", atomic.LoadInt64(&warned))
	}

	// Interaction endpoints are always exempt (never blocks).
	rl.acquireGlobalToken("/interactions/123/abc/callback")
}

// TestExtractMajorParam verifies major parameter extraction for all route types.
func TestExtractMajorParam(t *testing.T) {
	tests := []struct {
		input     string
		wantRoute string
		wantMajor string
	}{
		{"/channels/123/messages", "channels/{channel_id}/messages", "123"},
		{"/channels/999/messages/", "channels/{channel_id}/messages/", "999"},
		{"/guilds/456/members", "guilds/{guild_id}/members", "456"},
		{"/guilds/99/channels", "guilds/{guild_id}/channels", "99"},
		{"/webhooks/789/tokenABC/messages/@original", "webhooks/{webhook_id}/{webhook_token}/messages/@original", "789:tokenABC"},
		{"https://discord.com/api/v9/channels/123/messages", "channels/{channel_id}/messages", "123"},
		{"https://discord.com/api/v9/guilds/456/members/", "guilds/{guild_id}/members/", "456"},
		{"/users/@me", "users/@me", ""},
		{"/gateway/bot", "gateway/bot", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			gotRoute, gotMajor := extractMajorParam(tt.input)
			if gotRoute != tt.wantRoute {
				t.Errorf("route = %q, want %q", gotRoute, tt.wantRoute)
			}
			if gotMajor != tt.wantMajor {
				t.Errorf("major = %q, want %q", gotMajor, tt.wantMajor)
			}
		})
	}
}

// TestRatelimitHashMigration verifies that buckets migrate from synthetic keys
// to hash-based keys after RegisterBucketHash.
func TestRatelimitHashMigration(t *testing.T) {
	rl := NewRatelimiter()

	b1 := rl.LockBucket("/channels/123/messages")
	b1.Release(&RateLimitData{
		Remaining:  3,
		BucketHash: "hashXYZ",
		ResetAt:    time.Now().Add(5 * time.Second),
	})

	rl.RegisterBucketHash(b1.originalKey, b1.discordBucketID)

	b2 := rl.GetBucket("/channels/123/messages")
	if b1 != b2 {
		t.Error("expected same bucket pointer after hash migration")
	}
	if b2.Key != "hashXYZ:123" {
		t.Errorf("expected key 'hashXYZ:123', got %q", b2.Key)
	}
	if b2.Remaining != 3 {
		t.Errorf("expected Remaining=3, got %d", b2.Remaining)
	}
}

func BenchmarkRatelimitSingleEndpoint(b *testing.B) {
	rl := NewRatelimiter()
	for i := 0; i < b.N; i++ {
		sendBenchReq("/guilds/99/channels", rl)
	}
}

func BenchmarkRatelimitParallelMultiEndpoints(b *testing.B) {
	rl := NewRatelimiter()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sendBenchReq("/guilds/"+strconv.Itoa(i)+"/channels", rl)
			i++
		}
	})
}

func sendBenchReq(endpoint string, rl *RateLimiter) {
	bucket := rl.LockBucket(endpoint)
	bucket.Release(&RateLimitData{Remaining: 10, Limit: 10})
}
