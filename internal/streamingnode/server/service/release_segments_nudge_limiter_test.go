package service

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestNudgeLimiterAllowAndExpiry(t *testing.T) {
	limiter := &nudgeLimiter{interval: time.Minute}
	key := nudgeKey{collectionID: 10, vchannel: "vchannel"}
	now := time.Now()

	assert.True(t, limiter.allow(key, now))
	assert.False(t, limiter.allow(key, now.Add(time.Second)))
	assert.False(t, limiter.allow(key, now.Add(59*time.Second)))
	assert.True(t, limiter.allow(key, now.Add(time.Minute)))
}

// The limit is per key: one busy key must not suppress another key's nudge.
func TestNudgeLimiterKeysAreIndependent(t *testing.T) {
	limiter := &nudgeLimiter{interval: time.Minute}
	now := time.Now()

	keyA := nudgeKey{collectionID: 10, vchannel: "vchannel-a"}
	keyB := nudgeKey{collectionID: 10, vchannel: "vchannel-b"}
	keyC := nudgeKey{collectionID: 20, vchannel: "vchannel-a"}

	assert.True(t, limiter.allow(keyA, now))
	assert.True(t, limiter.allow(keyB, now))
	assert.True(t, limiter.allow(keyC, now))
	assert.False(t, limiter.allow(keyA, now))
	assert.False(t, limiter.allow(keyB, now))
	assert.False(t, limiter.allow(keyC, now))
}

// The map must stay bounded by recent activity, not grow with every channel
// that was ever released on this node.
func TestNudgeLimiterPrunesStaleEntries(t *testing.T) {
	limiter := &nudgeLimiter{interval: time.Minute}
	now := time.Now()
	for i := 0; i < 1000; i++ {
		limiter.allow(nudgeKey{collectionID: int64(i), vchannel: "vchannel"}, now)
	}
	assert.Len(t, limiter.lastNudge, 1000)

	// One call after the interval sweeps everything that can no longer suppress.
	limiter.allow(nudgeKey{collectionID: 9999, vchannel: "vchannel"}, now.Add(2*time.Minute))
	assert.Len(t, limiter.lastNudge, 1)
}

// A negative interval disables the limit entirely (test-only escape hatch).
func TestNudgeLimiterDisabled(t *testing.T) {
	limiter := &nudgeLimiter{interval: -1}
	key := nudgeKey{collectionID: 10, vchannel: "vchannel"}
	now := time.Now()
	for i := 0; i < 10; i++ {
		assert.True(t, limiter.allow(key, now))
	}
	assert.Empty(t, limiter.lastNudge)
}

// With no override the cadence derives from dataNode.flushRetryInterval, so an
// operator who slows the flush retry also slows the nudge.
func TestNudgeLimiterDefaultInterval(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.DataNodeCfg.FlushRetryInterval.Key, "1000")
	defer params.Reset(params.DataNodeCfg.FlushRetryInterval.Key)

	limiter := &nudgeLimiter{}
	assert.Equal(t, nudgeIntervalFlushRetryMultiplier*time.Second, limiter.currentInterval())
}

func TestNudgeLimiterConcurrentAllow(t *testing.T) {
	limiter := &nudgeLimiter{interval: time.Minute}
	key := nudgeKey{collectionID: 10, vchannel: "vchannel"}
	now := time.Now()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		granted int
	)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if limiter.allow(key, now) {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, 1, granted, "exactly one concurrent caller may append the nudge")
}
