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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

func TestConnectionAttemptBudget(t *testing.T) {
	for _, attempts := range []uint{1, 3} {
		t.Run(strconv.Itoa(int(attempts)), func(t *testing.T) {
			calls := 0
			base := &ClientBase[*mockClient]{MaxAttempts: 10, isNode: true, role: "querynode-1906"}
			session := sessionutil.NewMockSession(t)
			// Even a still-registered worker must not keep a one-attempt read waiting.
			if attempts > 1 {
				session.EXPECT().GetSessions(mock.Anything, mock.Anything).Return(map[string]*sessionutil.Session{
					"querynode-1906": {SessionRaw: sessionutil.SessionRaw{ServerID: 1906}},
				}, 0, nil)
			}
			base.SetNodeID(1906)
			base.sess = session
			base.SetGetAddrFunc(func() (string, error) {
				calls++
				return "", wrapErrConnect("dead-worker", context.DeadlineExceeded)
			})
			ctx := retry.WithMaxAttemptsContext(context.Background(), attempts)
			_, err := base.Call(ctx, func(*mockClient) (any, error) {
				t.Fatal("RPC must not run after failed dial")
				return nil, nil
			})
			require.ErrorIs(t, err, ErrConnect)
			require.Equal(t, int(attempts), calls)
		})
	}
}

func TestReadFailFastOnConnectionError(t *testing.T) {
	base := &ClientBase[*mockClient]{MaxAttempts: 10, isNode: true, role: "querynode-1906"}
	base.sess = sessionutil.NewMockSession(t) // A failed dial must not block on etcd.
	dials := 0
	base.SetGetAddrFunc(func() (string, error) {
		dials++
		return "", wrapErrConnect("dead-worker", context.DeadlineExceeded)
	})
	ctx := WithFailFastOnConnectionError(retry.WithMaxAttemptsContext(context.Background(), 2))
	_, err := base.Call(ctx, func(*mockClient) (any, error) {
		t.Fatal("RPC must not run after a failed dial")
		return nil, nil
	})
	require.ErrorIs(t, err, ErrConnect)
	require.Equal(t, 1, dials)
}

func TestReadRetriesTransientRPCError(t *testing.T) {
	base := &ClientBase[*mockClient]{
		MaxAttempts: 10, InitialBackoff: 0.001, MaxBackoff: 0.001,
		minResetInterval: time.Hour, isNode: true,
	}
	base.lastReset.Store(time.Now()) // Keep the established connection for the retry.
	base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}
	calls := 0
	ctx := WithFailFastOnConnectionError(retry.WithMaxAttemptsContext(context.Background(), 2))
	_, err := base.Call(ctx, func(*mockClient) (any, error) {
		calls++
		if calls == 1 {
			return nil, status.Error(codes.Unavailable, "transient worker error")
		}
		return merr.Status(nil), nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestReadFailedReconnectReturnsAfterOneRPC(t *testing.T) {
	base := &ClientBase[*mockClient]{
		MaxAttempts: 10, InitialBackoff: 0.001, MaxBackoff: 0.001,
		isNode: true, role: "querynode-1906",
	}
	base.sess = sessionutil.NewMockSession(t)
	base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}
	dials := 0
	base.SetGetAddrFunc(func() (string, error) {
		dials++
		return "", wrapErrConnect("dead-worker", context.DeadlineExceeded)
	})
	calls := 0
	ctx := WithFailFastOnConnectionError(retry.WithMaxAttemptsContext(context.Background(), 2))
	_, err := base.Call(ctx, func(*mockClient) (any, error) {
		calls++
		return nil, status.Error(codes.Unavailable, "worker exited")
	})
	require.ErrorIs(t, err, ErrConnect)
	require.Equal(t, 1, calls)
	require.Equal(t, 1, dials)
}

func TestFastFailDoesNotReconnect(t *testing.T) {
	base := &ClientBase[*mockClient]{MaxAttempts: 3}
	base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}
	dials := 0
	base.SetGetAddrFunc(func() (string, error) {
		dials++
		return "", wrapErrConnect("dead-worker", context.DeadlineExceeded)
	})
	_, err := base.Call(context.Background(), func(*mockClient) (any, error) {
		return nil, grpc.ErrClientConnClosing
	})
	require.True(t, IsConnectionClosingErr(err))
	require.Nil(t, base.grpcClient)
	require.Zero(t, dials, "fast failure must return before reconnecting")
	_, err = base.Call(retry.WithMaxAttemptsContext(context.Background(), 1), func(*mockClient) (any, error) {
		t.Fatal("RPC must not execute without a connection")
		return nil, nil
	})
	require.ErrorIs(t, err, ErrConnect)
	require.Equal(t, 1, dials, "the next call owns reconnection")
}

func TestNodeIDMismatchDoesNotReconnect(t *testing.T) {
	base := &ClientBase[*mockClient]{MaxAttempts: 3, isNode: true}
	base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}
	dials := 0
	base.SetGetAddrFunc(func() (string, error) {
		dials++
		return "", wrapErrConnect("stale-worker", context.DeadlineExceeded)
	})
	mismatch := status.Error(codes.Unknown, merr.ErrNodeNotMatch.Error())
	_, err := base.Call(context.Background(), func(*mockClient) (any, error) { return nil, mismatch })
	require.ErrorIs(t, err, mismatch)
	require.Nil(t, base.grpcClient)
	require.Zero(t, dials)
}

func TestConnectionWaiterCancellationAndClose(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	base := &ClientBase[*mockClient]{}
	base.SetGetAddrFunc(func() (string, error) {
		close(started)
		<-release
		return "", wrapErrConnect("dead-worker", context.DeadlineExceeded)
	})
	done := make(chan error, 1)
	go func() { _, err := base.GetGrpcClient(context.Background()); done <- err }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := base.GetGrpcClient(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	// Close must not wait for the dial's state lock.
	require.NoError(t, base.Close())
	close(release)
	require.True(t, IsConnectionClosingErr(<-done))
	_, err = base.GetGrpcClient(context.Background())
	require.True(t, IsConnectionClosingErr(err))
}

func TestCloseDuringSuccessfulDial(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	go server.Serve(listener)
	defer server.Stop()
	started, release := make(chan struct{}), make(chan struct{})
	base := &ClientBase[*mockClient]{DialTimeout: time.Second}
	base.SetNewGrpcClientFunc(func(*grpc.ClientConn) *mockClient { return &mockClient{} })
	base.SetGetAddrFunc(func() (string, error) {
		close(started)
		<-release
		return listener.Addr().String(), nil
	})
	done := make(chan error, 1)
	go func() { _, err := base.GetGrpcClient(context.Background()); done <- err }()
	<-started
	require.NoError(t, base.Close())
	close(release)
	require.True(t, IsConnectionClosingErr(<-done))
	require.Nil(t, base.grpcClient)
}

func TestCanceledInitiatorDoesNotCancelSharedDial(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	started := make(chan struct{})
	base := &ClientBase[*mockClient]{DialTimeout: time.Second}
	defer base.Close()
	base.SetNewGrpcClientFunc(func(*grpc.ClientConn) *mockClient { return &mockClient{} })
	base.SetGetAddrFunc(func() (string, error) {
		close(started)
		return listener.Addr().String(), nil
	})

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()
	leaderDone := make(chan error, 1)
	go func() { _, err := base.GetGrpcClient(leaderCtx); leaderDone <- err }()
	<-started

	waiterCtx := &connectionWaitContext{Context: context.Background(), entered: make(chan struct{})}
	waiterDone := make(chan error, 1)
	go func() { _, err := base.GetGrpcClient(waiterCtx); waiterDone <- err }()
	<-waiterCtx.entered
	cancelLeader()
	select {
	case err := <-leaderDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("initiator did not return after cancellation")
	}

	server := grpc.NewServer()
	go server.Serve(listener)
	defer server.Stop()
	select {
	case err := <-waiterDone:
		require.NoError(t, err, "live waiter must survive initiator cancellation")
	case <-time.After(time.Second):
		t.Fatal("live waiter did not receive the shared connection")
	}
}

func TestCloseCancelsSharedDial(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	started := make(chan struct{})
	base := &ClientBase[*mockClient]{DialTimeout: 10 * time.Second}
	base.SetGetAddrFunc(func() (string, error) {
		close(started)
		return listener.Addr().String(), nil
	})
	done := make(chan error, 1)
	go func() { _, err := base.GetGrpcClient(context.Background()); done <- err }()
	<-started
	base.grpcClientMtx.RLock()
	pending := base.connecting
	base.grpcClientMtx.RUnlock()
	require.NotNil(t, pending)
	require.NoError(t, base.Close())
	select {
	case err := <-done:
		require.True(t, IsConnectionClosingErr(err))
	case <-time.After(time.Second):
		t.Fatal("Close did not release the caller")
	}
	select {
	case <-pending.done:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the shared dial")
	}
}

func TestOneAttemptDoesNotBackOffOnRetriableStatus(t *testing.T) {
	base := &ClientBase[*mockClient]{MaxAttempts: 1, InitialBackoff: 2, MaxBackoff: 2}
	base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := base.Call(ctx, func(*mockClient) (any, error) { return merr.Status(merr.ErrServiceUnavailable), nil })
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	case <-time.After(time.Second):
		t.Fatal("one-attempt call backed off before returning its status")
	}
}

// Notify only after the waiter has reached the cancellable shared-dial wait.
type connectionWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *connectionWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestCompletedConnectionAttemptDoesNotRedialAfterReset(t *testing.T) {
	wrapper := &clientConnWrapper[*mockClient]{client: &mockClient{}}
	pending := &connectionAttempt[*mockClient]{done: make(chan struct{}), wrapper: wrapper}
	base := &ClientBase[*mockClient]{connecting: pending}
	base.SetGetAddrFunc(func() (string, error) {
		t.Error("a waiter must not start another dial in the same attempt")
		return "", wrapErrConnect("unexpected-redial", context.DeadlineExceeded)
	})
	ctx := &connectionWaitContext{Context: context.Background(), entered: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		got, err := base.GetGrpcClient(ctx)
		assert.Same(t, wrapper, got)
		assert.NoError(t, err)
	}()
	<-ctx.entered
	// The dial succeeded, but another RPC reset the published client before
	// this waiter resumed. Return that dial's wrapper instead of reacquiring.
	close(pending.done)
	<-done
}

func TestClosedClientDoesNotReturnCompletedConnection(t *testing.T) {
	wrapper := &clientConnWrapper[*mockClient]{client: &mockClient{}}
	pending := &connectionAttempt[*mockClient]{done: make(chan struct{}), wrapper: wrapper}
	base := &ClientBase[*mockClient]{connecting: pending}
	ctx := &connectionWaitContext{Context: context.Background(), entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, err := base.GetGrpcClient(ctx); done <- err }()
	<-ctx.entered
	require.NoError(t, base.Close())
	close(pending.done)
	require.True(t, IsConnectionClosingErr(<-done))
}

func TestConcurrentConnectionFailureIsShared(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	calls := 0
	const waiters = 32
	base := &ClientBase[*mockClient]{}
	base.SetGetAddrFunc(func() (string, error) {
		calls++
		close(started)
		<-release
		return "", wrapErrConnect("dead-worker", context.DeadlineExceeded)
	})
	done := make(chan error, waiters+1)
	go func() { _, err := base.GetGrpcClient(context.Background()); done <- err }()
	<-started
	for range waiters {
		ctx := &connectionWaitContext{Context: context.Background(), entered: make(chan struct{})}
		go func() { _, err := base.GetGrpcClient(ctx); done <- err }()
		<-ctx.entered
	}
	close(release)
	for range waiters + 1 {
		require.ErrorIs(t, <-done, ErrConnect)
	}
	require.Equal(t, 1, calls)
}

func TestCloseUnblocksConnectionWaiter(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	base := &ClientBase[*mockClient]{}
	base.SetGetAddrFunc(func() (string, error) {
		close(started)
		<-release
		return "", wrapErrConnect("dead-worker", context.DeadlineExceeded)
	})
	leaderDone := make(chan error, 1)
	go func() { _, err := base.GetGrpcClient(context.Background()); leaderDone <- err }()
	<-started
	ctx := &connectionWaitContext{Context: context.Background(), entered: make(chan struct{})}
	waiterDone := make(chan error, 1)
	go func() { _, err := base.GetGrpcClient(ctx); waiterDone <- err }()
	<-ctx.entered
	require.NoError(t, base.Close())
	require.True(t, IsConnectionClosingErr(<-waiterDone))
	close(release)
	require.True(t, IsConnectionClosingErr(<-leaderDone))
}
