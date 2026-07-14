// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcclient

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// startLocalServer starts a real gRPC server on a random localhost port and
// returns a ClientBase wired to dial it, with getAddrFunc gated by the given
// channels: the grower signals `started` when it enters address resolution
// (already holding dialMtx) and then blocks until `proceed` is closed. This
// opens a deterministic window with a dial in flight.
func startGatedBase(t *testing.T, started, proceed chan struct{}) (*ClientBase[*mockClient], func()) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()

	base := &ClientBase[*mockClient]{
		poolSize:         2,
		DialTimeout:      5 * time.Second,
		KeepAliveTime:    10 * time.Second,
		KeepAliveTimeout: 3 * time.Second,
	}
	gateOnce := sync.Once{}
	base.getAddrFunc = func() (string, error) {
		gateOnce.Do(func() {
			close(started)
			<-proceed
		})
		return lis.Addr().String(), nil
	}
	base.newGrpcClient = func(cc *grpc.ClientConn) *mockClient { return &mockClient{} }
	return base, srv.Stop
}

// TestClientBase_CloseWaitsForInFlightDial: Close must be mutually exclusive
// with pool growth (lock order dialMtx → grpcClientMtx). A dial that is in
// flight when Close is called has to land in the pool first so Close tears it
// down with everything else; without that, the dial would append a live
// connection to an already-drained pool and leak it forever.
func TestClientBase_CloseWaitsForInFlightDial(t *testing.T) {
	started := make(chan struct{})
	proceed := make(chan struct{})
	base, stop := startGatedBase(t, started, proceed)
	defer stop()

	growErr := make(chan error, 1)
	go func() {
		_, err := base.GetGrpcClient(context.Background())
		growErr <- err
	}()
	<-started // grower holds dialMtx, dial in flight

	closeDone := make(chan struct{})
	go func() {
		_ = base.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		t.Fatal("Close must wait for the in-flight dial, not run concurrently with it")
	case <-time.After(100 * time.Millisecond):
	}

	close(proceed) // let the dial land
	require.NoError(t, <-growErr, "the gated grow must complete normally")
	select {
	case <-closeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not finish after the dial landed")
	}

	base.grpcClientMtx.RLock()
	defer base.grpcClientMtx.RUnlock()
	assert.Empty(t, base.grpcClientPool,
		"the dialed connection must be drained by Close, not appended after it")
}

func TestClientBase_GetGrpcClientCannotQueueBehindClose(t *testing.T) {
	started := make(chan struct{})
	proceed := make(chan struct{})
	close(proceed)
	base, stop := startGatedBase(t, started, proceed)
	defer stop()

	w, err := base.GetGrpcClient(context.Background())
	require.NoError(t, err)
	require.NotNil(t, w)

	// Keep Close blocked in wrapper.Close after it has detached the pool. While
	// Close still owns dialMtx, a new GetGrpcClient must observe the terminal
	// closed state instead of queueing behind Close and redialing afterwards.
	w.Pin()
	closeDone := make(chan struct{})
	go func() {
		_ = base.Close()
		close(closeDone)
	}()

	require.Eventually(t, func() bool {
		base.grpcClientMtx.RLock()
		defer base.grpcClientMtx.RUnlock()
		return len(base.grpcClientPool) == 0
	}, time.Second, time.Millisecond, "Close did not detach the pool")

	type getResult struct {
		wrapper *clientConnWrapper[*mockClient]
		err     error
	}
	getDone := make(chan getResult, 1)
	go func() {
		got, getErr := base.GetGrpcClient(context.Background())
		getDone <- getResult{wrapper: got, err: getErr}
	}()

	var result getResult
	returnedWhileClosing := false
	select {
	case result = <-getDone:
		returnedWhileClosing = true
	case <-time.After(time.Second):
	}

	w.Unpin()
	select {
	case <-closeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not finish after the pinned RPC was released")
	}

	if !returnedWhileClosing {
		// Drain and close any connection resurrected by the pre-fix behavior before
		// failing, so the regression test does not leak resources itself.
		result = <-getDone
		if result.wrapper != nil {
			_ = result.wrapper.Close()
		}
		t.Fatal("GetGrpcClient queued behind Close instead of observing the terminal closed state")
	}
	require.Nil(t, result.wrapper)
	require.ErrorIs(t, result.err, grpc.ErrClientConnClosing)
}

// TestClientBase_ResetWaitsForInFlightDial: same mutual exclusion for
// resetConnection. Without it, a force reset racing an in-flight dial clears
// the pool and address, and the dial then re-appends a connection to the
// pre-reset address and restores that stale address.
func TestClientBase_ResetWaitsForInFlightDial(t *testing.T) {
	started := make(chan struct{})
	proceed := make(chan struct{})
	base, stop := startGatedBase(t, started, proceed)
	defer stop()

	// Warm the pool with connection #1 so reset has a wrapper to invalidate.
	close(proceed) // first dial is not the one under test; let it through
	w1, err := base.GetGrpcClient(context.Background())
	require.NoError(t, err)
	require.Len(t, base.grpcClientPool, 1)

	// Re-arm the gate for the dial under test and force address re-resolution
	// so the grower blocks inside the gated getAddrFunc.
	prevAddr := base.addr.Load()
	started2 := make(chan struct{})
	proceed2 := make(chan struct{})
	gateOnce := sync.Once{}
	base.getAddrFunc = func() (string, error) {
		gateOnce.Do(func() {
			close(started2)
			<-proceed2
		})
		return prevAddr, nil
	}
	base.addr.Store("") // force the grower through getAddrFunc

	growDone := make(chan struct{})
	go func() {
		_, _ = base.GetGrpcClient(context.Background()) // grows pool to #2
		close(growDone)
	}()
	<-started2 // grower holds dialMtx

	resetDone := make(chan struct{})
	go func() {
		base.resetConnection(w1, true)
		close(resetDone)
	}()

	select {
	case <-resetDone:
		t.Fatal("resetConnection must wait for the in-flight dial, not run concurrently with it")
	case <-time.After(100 * time.Millisecond):
	}

	close(proceed2) // getAddrFunc returns the pre-reset address; dial #2 lands
	<-growDone
	select {
	case <-resetDone:
	case <-time.After(10 * time.Second):
		t.Fatal("resetConnection did not finish after the dial landed")
	}

	base.grpcClientMtx.RLock()
	defer base.grpcClientMtx.RUnlock()
	assert.Empty(t, base.grpcClientPool,
		"reset must tear down the freshly dialed connection with the pool, not leave it behind")
	assert.Empty(t, base.addr.Load(),
		"reset must leave the address cleared; the in-flight dial must not restore %q", prevAddr)
}

// TestClientBase_ConcurrentGrowResetClose hammers GetGrpcClient concurrently
// with force resets and a final Close under -race. Its real assertion is the
// lock order dialMtx → grpcClientMtx: an ordering violation shows up here as a
// deadlock (test timeout) or a race report.
func TestClientBase_ConcurrentGrowResetClose(t *testing.T) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	base := &ClientBase[*mockClient]{
		poolSize:         4,
		DialTimeout:      5 * time.Second,
		KeepAliveTime:    10 * time.Second,
		KeepAliveTimeout: 3 * time.Second,
	}
	base.getAddrFunc = func() (string, error) { return lis.Addr().String(), nil }
	base.newGrpcClient = func(cc *grpc.ClientConn) *mockClient { return &mockClient{} }

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 50; i++ {
				w, err := base.GetGrpcClient(context.Background())
				if err != nil {
					continue
				}
				if rng.Intn(10) == 0 {
					base.resetConnection(w, true)
				}
			}
		}(int64(g))
	}
	wg.Wait()
	assert.NoError(t, base.Close())

	base.grpcClientMtx.RLock()
	defer base.grpcClientMtx.RUnlock()
	assert.Empty(t, base.grpcClientPool, "Close must leave the pool drained")
}
