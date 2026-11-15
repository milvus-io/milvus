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
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type recoveringCoordinator struct {
	rootcoordpb.UnimplementedRootCoordServer
	serverID atomic.Int64
	state    atomic.Int32
}

func (s *recoveringCoordinator) GetComponentStates(context.Context, *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{Status: merr.Success(), State: &milvuspb.ComponentInfo{
		NodeID: s.serverID.Load(), StateCode: commonpb.StateCode(s.state.Load()),
	}}, nil
}

func newRecoveringCoordinator(t *testing.T) (*recoveringCoordinator, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	service := &recoveringCoordinator{}
	service.serverID.Store(96)
	service.state.Store(int32(commonpb.StateCode_Healthy))
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.ServerIDValidationUnaryServerInterceptor(service.serverID.Load)))
	rootcoordpb.RegisterRootCoordServer(server, service)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return service, listener.Addr().String()
}

func newRecoveringClient(t *testing.T, addr string, discover func() int64) *ClientBase[rootcoordpb.RootCoordClient] {
	t.Helper()
	base := &ClientBase[rootcoordpb.RootCoordClient]{
		DialTimeout: time.Second, MaxAttempts: 4, InitialBackoff: 2, MaxBackoff: 2,
		ClientMaxRecvSize: 1 << 20, ClientMaxSendSize: 1 << 20,
	}
	base.SetRole(typeutil.MixCoordRole)
	base.SetGetAddrFunc(func() (string, error) { base.SetNodeID(discover()); return addr, nil })
	base.SetNewGrpcClientFunc(func(conn *grpc.ClientConn) rootcoordpb.RootCoordClient { return rootcoordpb.NewRootCoordClient(conn) })
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	return base
}

func coordinatorHealth(ctx context.Context, base *ClientBase[rootcoordpb.RootCoordClient]) (any, error) {
	return base.Call(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

func TestCoordinatorRecoveryNewIdentity(t *testing.T) {
	service, addr := newRecoveringCoordinator(t)
	base := newRecoveringClient(t, addr, service.serverID.Load)
	_, err := coordinatorHealth(context.Background(), base)
	require.NoError(t, err)
	// The process at the same address now has a new identity; the existing
	// connection still injects 96. A two-second backoff exceeds this deadline.
	service.serverID.Store(99)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := coordinatorHealth(ctx, base)
	require.NoError(t, err, "a newly discovered identity must be tried without the old backoff")
	require.EqualValues(t, 99, result.(*milvuspb.ComponentStates).GetState().GetNodeID())
}

func TestCoordinatorRecoveryOldSession(t *testing.T) {
	service, addr := newRecoveringCoordinator(t)
	base := newRecoveringClient(t, addr, func() int64 { return 96 })
	base.InitialBackoff, base.MaxBackoff = 0.1, 0.1
	_, err := coordinatorHealth(context.Background(), base)
	require.NoError(t, err)
	service.serverID.Store(99)
	var attempts []time.Time
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = retry.WithMaxAttemptsContext(ctx, 3)
	_, err = base.Call(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		attempts = append(attempts, time.Now())
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
	require.True(t, IsServerIDMismatchErr(err))
	require.Len(t, attempts, 3)
	require.GreaterOrEqual(t, attempts[1].Sub(attempts[0]), 100*time.Millisecond, "old session must retain backoff")
	require.GreaterOrEqual(t, attempts[2].Sub(attempts[1]), 200*time.Millisecond, "repeated old session must not trigger immediate retry")
}

func TestCoordinatorRecoveryAttemptLimit(t *testing.T) {
	for _, attempts := range []uint{1, 2} {
		t.Run(strconv.FormatUint(uint64(attempts), 10), func(t *testing.T) {
			service, addr := newRecoveringCoordinator(t)
			var discoveries atomic.Int32
			base := newRecoveringClient(t, addr, func() int64 { return int64(discoveries.Add(1)) })
			service.serverID.Store(99) // Every discovered identity is stale, even when it changes.
			ctx := retry.WithMaxAttemptsContext(context.Background(), attempts)
			calls := 0
			_, err := base.Call(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
				calls++
				return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
			})
			require.True(t, IsServerIDMismatchErr(err))
			require.EqualValues(t, attempts, calls)
			require.EqualValues(t, attempts, discoveries.Load(), "no reconnect after final attempt")
		})
	}
}

func TestCoordinatorRecoveryReconnectFailure(t *testing.T) {
	service, addr := newRecoveringCoordinator(t)
	base := newRecoveringClient(t, addr, service.serverID.Load)
	base.InitialBackoff, base.MaxBackoff = 0.05, 0.05
	_, err := coordinatorHealth(context.Background(), base)
	require.NoError(t, err)
	service.serverID.Store(99)
	var discoveries []time.Time
	base.SetGetAddrFunc(func() (string, error) {
		discoveries = append(discoveries, time.Now())
		// Updating the mutable NodeID alone is not a successful reconnect.
		base.SetNodeID(99)
		return "", merr.ErrServiceUnavailable
	})
	ctx := retry.WithMaxAttemptsContext(context.Background(), 3)
	_, err = coordinatorHealth(ctx, base)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Len(t, discoveries, 3, "one eager probe plus fresh discovery on each remaining attempt")
	require.GreaterOrEqual(t, discoveries[1].Sub(discoveries[0]), 50*time.Millisecond)
	require.GreaterOrEqual(t, discoveries[2].Sub(discoveries[1]), 100*time.Millisecond)
}

func TestCoordinatorRecoveryConcurrentReset(t *testing.T) {
	service, addr := newRecoveringCoordinator(t)
	base := newRecoveringClient(t, addr, service.serverID.Load)
	old, err := base.GetGrpcClient(context.Background())
	require.NoError(t, err)
	service.serverID.Store(99)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	_, err = base.Call(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		calls++
		resp, rpcErr := client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
		if calls == 1 {
			require.True(t, IsServerIDMismatchErr(rpcErr))
			// Another caller has already installed the new connection before
			// this caller handles its old connection's mismatch.
			base.resetConnection(old, true)
			_, connectErr := base.GetGrpcClient(ctx)
			require.NoError(t, connectErr)
		}
		return resp, rpcErr
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestCoordinatorRecoveryCancellationAndClose(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "close"}[closeClient], func(t *testing.T) {
			service, addr := newRecoveringCoordinator(t)
			base := newRecoveringClient(t, addr, service.serverID.Load)
			_, err := coordinatorHealth(context.Background(), base)
			require.NoError(t, err)
			service.serverID.Store(99)
			started, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			base.SetGetAddrFunc(func() (string, error) {
				close(started)
				<-release
				base.SetNodeID(99)
				return addr, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := coordinatorHealth(ctx, base); done <- err }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("reconnect did not start before backoff")
			}
			if closeClient {
				require.NoError(t, base.Close())
			} else {
				cancel()
			}
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(time.Second):
				t.Fatal("cancellation/Close did not release reconnect waiter")
			}
		})
	}
}

// Adapt the public Call path to the health check interface used by StreamingNode.
type recoveryHealthClient struct {
	base   *ClientBase[rootcoordpb.RootCoordClient]
	calls  []time.Time
	onCall func(int)
}

func (c *recoveryHealthClient) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	c.calls = append(c.calls, time.Now())
	c.onCall(len(c.calls))
	result, err := coordinatorHealth(ctx, c.base)
	if err != nil {
		return nil, err
	}
	return result.(*milvuspb.ComponentStates), nil
}

func TestCoordinatorRecoveryNotHealthy(t *testing.T) {
	service, addr := newRecoveringCoordinator(t)
	base := newRecoveringClient(t, addr, service.serverID.Load)
	_, err := coordinatorHealth(context.Background(), base)
	require.NoError(t, err)
	service.serverID.Store(99)
	service.state.Store(int32(commonpb.StateCode_Initializing))
	health := &recoveryHealthClient{base: base, onCall: func(n int) {
		if n == 3 {
			service.state.Store(int32(commonpb.StateCode_Healthy))
		}
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, componentutil.WaitForComponentHealthy(ctx, health, "mixcoord", 3, 50*time.Millisecond))
	require.Len(t, health.calls, 3)
	require.GreaterOrEqual(t, health.calls[1].Sub(health.calls[0]), 50*time.Millisecond)
	require.GreaterOrEqual(t, health.calls[2].Sub(health.calls[1]), 100*time.Millisecond)
}

func TestCoordinatorRecoveryMultipleClients(t *testing.T) {
	service, addr := newRecoveringCoordinator(t)
	const clients = 32
	bases := make([]*ClientBase[rootcoordpb.RootCoordClient], clients)
	for i := range bases {
		bases[i] = newRecoveringClient(t, addr, service.serverID.Load)
		_, err := coordinatorHealth(context.Background(), bases[i])
		require.NoError(t, err)
	}
	service.serverID.Store(99)
	var attempts atomic.Int32
	var mu sync.Mutex
	var successes []time.Time
	start := make(chan struct{})
	results := make(chan error, clients)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, base := range bases {
		go func() {
			<-start
			_, err := base.Call(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
				attempts.Add(1)
				resp, err := client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
				if err == nil {
					mu.Lock()
					successes = append(successes, time.Now())
					mu.Unlock()
				}
				return resp, err
			})
			results <- err
		}()
	}
	started := time.Now()
	close(start)
	for range clients {
		require.NoError(t, <-results)
	}
	require.EqualValues(t, clients*2, attempts.Load(), "one mismatch plus one fresh RPC per client")
	require.Len(t, successes, clients)
	t.Logf("%d clients: %d attempts, first success %s, last success %s, completion spread %s", clients, attempts.Load(), successes[0].Sub(started), successes[len(successes)-1].Sub(started), successes[len(successes)-1].Sub(successes[0]))
}

func TestCoordinatorRecoveryDialFailure(t *testing.T) {
	service, addr := newRecoveringCoordinator(t)
	base := newRecoveringClient(t, addr, service.serverID.Load)
	_, err := coordinatorHealth(context.Background(), base)
	require.NoError(t, err)
	service.serverID.Store(99)
	// TCP accepts connections, but no gRPC transport becomes ready.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	base.DialTimeout = 20 * time.Millisecond
	base.InitialBackoff, base.MaxBackoff = 0.05, 0.05
	dials := 0
	base.SetGetAddrFunc(func() (string, error) {
		dials++
		base.SetNodeID(99)
		return listener.Addr().String(), nil
	})
	started := time.Now()
	ctx := retry.WithMaxAttemptsContext(context.Background(), 2)
	_, err = coordinatorHealth(ctx, base)
	require.ErrorIs(t, err, ErrConnect)
	require.Equal(t, 2, dials, "failed eager dial must not replace the last attempt after backoff")
	require.GreaterOrEqual(t, time.Since(started), 50*time.Millisecond)
}

func TestCoordinatorRecoveryNewIdentityNotReady(t *testing.T) {
	service, addr := newRecoveringCoordinator(t)
	base := newRecoveringClient(t, addr, service.serverID.Load)
	base.InitialBackoff, base.MaxBackoff = 0.1, 0.1
	_, err := coordinatorHealth(context.Background(), base)
	require.NoError(t, err)
	service.serverID.Store(99)
	var attempts []time.Time
	_, err = base.Call(context.Background(), func(client rootcoordpb.RootCoordClient) (any, error) {
		attempts = append(attempts, time.Now())
		resp, err := client.GetComponentStates(context.Background(), &milvuspb.GetComponentStatesRequest{})
		if err == nil && len(attempts) == 2 {
			resp.Status = merr.Status(merr.ErrServiceNotReady)
		}
		return resp, err
	})
	require.NoError(t, err)
	require.Len(t, attempts, 3)
	require.Less(t, attempts[1].Sub(attempts[0]), 100*time.Millisecond)
	require.GreaterOrEqual(t, attempts[2].Sub(attempts[1]), 100*time.Millisecond, "new identity does not imply service readiness")
}

func TestCoordinatorRecoveryDuringBackoff(t *testing.T) {
	for _, failure := range []string{"old session", "lookup failure", "dial failure"} {
		t.Run(failure, func(t *testing.T) {
			service, addr := newRecoveringCoordinator(t)
			base := newRecoveringClient(t, addr, service.serverID.Load)
			_, err := coordinatorHealth(context.Background(), base)
			require.NoError(t, err)
			service.serverID.Store(99)
			base.InitialBackoff, base.MaxBackoff = 0.5, 0.5
			base.DialTimeout = 20 * time.Millisecond

			// Accept TCP without serving gRPC to exercise a failed eager dial.
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			var recovered atomic.Bool
			var lookups []time.Time
			base.SetGetAddrFunc(func() (string, error) {
				lookups = append(lookups, time.Now())
				if recovered.Load() {
					base.SetNodeID(99)
					return addr, nil
				}
				switch failure {
				case "lookup failure":
					return "", merr.ErrServiceUnavailable
				case "dial failure":
					base.SetNodeID(99)
					return listener.Addr().String(), nil
				default:
					base.SetNodeID(96)
					return addr, nil
				}
			})

			// Recovery is driven by elapsed time after the failed RPC, not by
			// lookup count. Both HEAD's lazy reconnect and the optimized path
			// must succeed using the one remaining attempt after this backoff.
			published := make(chan time.Time, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx = retry.WithMaxAttemptsContext(ctx, 2)
			calls := 0
			started := time.Now()
			result, err := base.Call(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
				calls++
				resp, rpcErr := client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
				if calls == 1 {
					require.True(t, IsServerIDMismatchErr(rpcErr))
					timer := time.AfterFunc(150*time.Millisecond, func() {
						publication := time.Now()
						recovered.Store(true)
						published <- publication
					})
					t.Cleanup(func() { timer.Stop() })
				}
				return resp, rpcErr
			})
			require.NoError(t, err, "recovery during backoff must remain visible to the last attempt")
			require.Equal(t, 2, calls)
			require.EqualValues(t, 99, result.(*milvuspb.ComponentStates).GetState().GetNodeID())
			publication := <-published
			require.NotEmpty(t, lookups)
			require.True(t, lookups[len(lookups)-1].After(publication), "last attempt must discover after recovery")
			require.GreaterOrEqual(t, time.Since(started), 500*time.Millisecond)
			t.Logf("recovery at %s, successful RPC at %s, lookups=%d, RPCs=%d", publication.Sub(started), time.Since(started), len(lookups), calls)
		})
	}
}
