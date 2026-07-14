package grpcclient

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestClientBase_GetGrpcClient_FastPathReuse: when the pool is already at
// poolSize, GetGrpcClient takes the lock-light fast path — it round-robins an
// existing connection and never resolves the address or dials.
func TestClientBase_GetGrpcClient_FastPathReuse(t *testing.T) {
	base := &ClientBase[*mockClient]{poolSize: 2}
	base.grpcClientPool = newFakePool(2) // n == poolSize

	resolved := false
	base.getAddrFunc = func() (string, error) {
		resolved = true
		return "", nil
	}

	w, err := base.GetGrpcClient(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, w)
	assert.False(t, resolved, "a full pool must not resolve the address or dial")
}

// TestClientBase_GetGrpcClient_DialFailureReusesExisting: when the pool is below
// poolSize and a fresh dial fails, GetGrpcClient prefers reusing an already-open
// connection over failing the call outright.
func TestClientBase_GetGrpcClient_DialFailureReusesExisting(t *testing.T) {
	existing := &clientConnWrapper[*mockClient]{client: &mockClient{}}
	base := &ClientBase[*mockClient]{
		poolSize:    2,
		DialTimeout: time.Millisecond, // dial fails fast
	}
	base.grpcClientPool = []*clientConnWrapper[*mockClient]{existing}
	base.getAddrFunc = func() (string, error) { return "127.0.0.1:0", nil } // unconnectable

	w, err := base.GetGrpcClient(context.Background())
	assert.NoError(t, err, "a dial failure with a warm pool must reuse, not error")
	assert.Same(t, existing, w)
}

// TestClientBase_GetGrpcClient_GrowLockContendedReuses: when the pool is warm but
// another caller already holds the grow lock, GetGrpcClient reuses an existing
// connection instead of waiting on the in-flight dial (no thundering herd).
func TestClientBase_GetGrpcClient_GrowLockContendedReuses(t *testing.T) {
	existing := &clientConnWrapper[*mockClient]{client: &mockClient{}}
	base := &ClientBase[*mockClient]{poolSize: 2}
	base.grpcClientPool = []*clientConnWrapper[*mockClient]{existing}

	base.dialMtx.Lock() // simulate another caller currently growing the pool
	defer base.dialMtx.Unlock()

	resolved := false
	base.getAddrFunc = func() (string, error) {
		resolved = true
		return "", nil
	}

	w, err := base.GetGrpcClient(context.Background())
	assert.NoError(t, err)
	assert.Same(t, existing, w, "contended grow lock + warm pool must reuse")
	assert.False(t, resolved, "the reuse path must not resolve the address or dial")
}

// TestClientBase_GetGrpcClient_GrowsPoolOnSuccess: against a real local server,
// the pool grows one connection per call until it reaches poolSize, then stays
// capped — exercising the dial → append → addr.Store path and the re-check.
func TestClientBase_GetGrpcClient_GrowsPoolOnSuccess(t *testing.T) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	base := &ClientBase[*mockClient]{
		poolSize:         2,
		DialTimeout:      5 * time.Second,
		KeepAliveTime:    10 * time.Second,
		KeepAliveTimeout: 3 * time.Second,
	}
	base.getAddrFunc = func() (string, error) { return lis.Addr().String(), nil }
	base.newGrpcClient = func(cc *grpc.ClientConn) *mockClient { return &mockClient{} }
	defer func() { _ = base.Close() }()

	// call #1 dials connection #1 and records the address
	w, err := base.GetGrpcClient(context.Background())
	require.NoError(t, err)
	require.NotNil(t, w)
	assert.Len(t, base.grpcClientPool, 1, "first call appends connection #1")
	assert.Equal(t, lis.Addr().String(), base.addr.Load())

	// call #2 grows the pool to poolSize
	_, err = base.GetGrpcClient(context.Background())
	require.NoError(t, err)
	assert.Len(t, base.grpcClientPool, 2, "second call grows the pool to poolSize")

	// call #3 is the fast path: pool capped at poolSize, no further growth
	_, err = base.GetGrpcClient(context.Background())
	require.NoError(t, err)
	assert.Len(t, base.grpcClientPool, 2, "pool stays capped at poolSize")
}

func TestClientBase_GetGrpcClientAfterCloseReturnsClosing(t *testing.T) {
	base := &ClientBase[*mockClient]{
		poolSize: 2,
		grpcClientPool: []*clientConnWrapper[*mockClient]{
			{client: &mockClient{}},
		},
	}
	addrCalls := 0
	base.getAddrFunc = func() (string, error) {
		addrCalls++
		return "", assert.AnError
	}

	require.NoError(t, base.Close())
	w, err := base.GetGrpcClient(context.Background())

	assert.Nil(t, w)
	assert.ErrorIs(t, err, grpc.ErrClientConnClosing)
	assert.Zero(t, addrCalls, "a closed client must not resolve an address or redial")
}

func TestClientBase_CallAfterCloseFastFails(t *testing.T) {
	base := &ClientBase[*mockClient]{
		poolSize:    2,
		MaxAttempts: 3,
		grpcClientPool: []*clientConnWrapper[*mockClient]{
			{client: &mockClient{}},
		},
	}
	addrCalls := 0
	base.getAddrFunc = func() (string, error) {
		addrCalls++
		return "", assert.AnError
	}

	require.NoError(t, base.Close())
	callerCalls := 0
	_, err := base.Call(context.Background(), func(client *mockClient) (any, error) {
		callerCalls++
		return struct{}{}, nil
	})

	assert.ErrorIs(t, err, grpc.ErrClientConnClosing)
	assert.Zero(t, callerCalls, "the RPC callback must not run after Close")
	assert.Zero(t, addrCalls, "Call after Close must not attempt to reconnect")
}
