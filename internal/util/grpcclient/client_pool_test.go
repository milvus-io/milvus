package grpcclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// newFakePool builds a pool of nil-conn wrappers (Close() is nil-safe), so the
// pool bookkeeping (round-robin / reset / close) can be exercised without real
// gRPC dialing.
func newFakePool(n int) []*clientConnWrapper[*mockClient] {
	pool := make([]*clientConnWrapper[*mockClient], n)
	for i := range pool {
		pool[i] = &clientConnWrapper[*mockClient]{client: &mockClient{}}
	}
	return pool
}

func TestClientBase_PoolRoundRobin(t *testing.T) {
	base := &ClientBase[*mockClient]{}
	pool := newFakePool(3)
	base.grpcClientPool = pool

	counts := map[*clientConnWrapper[*mockClient]]int{}
	for i := 0; i < 9; i++ {
		counts[base.pickLocked()]++
	}
	for i, w := range pool {
		assert.Equalf(t, 3, counts[w], "conn %d picked %d times, want even round-robin (3)", i, counts[w])
	}
}

func TestClientBase_PoolSizeOneIsSingleConn(t *testing.T) {
	base := &ClientBase[*mockClient]{}
	pool := newFakePool(1)
	base.grpcClientPool = pool
	for i := 0; i < 5; i++ {
		assert.Same(t, pool[0], base.pickLocked(), "size-1 pool must always return the one conn")
	}
}

func TestClientBase_CloseClearsWholePool(t *testing.T) {
	base := &ClientBase[*mockClient]{}
	base.grpcClientPool = newFakePool(4)
	assert.NoError(t, base.Close())
	assert.Nil(t, base.grpcClientPool, "Close must drop every pooled conn")
}

func TestClientBase_ResetClearsWholePool(t *testing.T) {
	base := &ClientBase[*mockClient]{}
	pool := newFakePool(4)
	base.grpcClientPool = pool

	base.resetConnection(pool[0], true) // forceReset bypasses minResetInterval throttle

	base.grpcClientMtx.RLock()
	got := base.grpcClientPool
	base.grpcClientMtx.RUnlock()
	assert.Nil(t, got, "resetting one conn must drop the entire old pool (whole-pool reset)")
}

func TestClientBase_ResetIgnoresStaleWrapper(t *testing.T) {
	base := &ClientBase[*mockClient]{}
	pool := newFakePool(3)
	base.grpcClientPool = pool

	// a wrapper that belongs to an already-replaced pool must not reset the live pool
	stale := &clientConnWrapper[*mockClient]{client: &mockClient{}}
	base.resetConnection(stale, true)

	base.grpcClientMtx.RLock()
	got := base.grpcClientPool
	base.grpcClientMtx.RUnlock()
	assert.Len(t, got, 3, "stale wrapper from an old pool must not reset the current pool")
}
