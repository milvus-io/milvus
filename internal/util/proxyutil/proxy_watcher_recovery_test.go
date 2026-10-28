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

package proxyutil

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Inject failures at the RPC boundary so clientv3's authentication, error
// conversion and watch teardown paths run without monkey-patching Err().
type leaderChangedEtcdServer struct {
	etcdserverpb.UnimplementedAuthServer
	etcdserverpb.UnimplementedKVServer
	etcdserverpb.UnimplementedWatchServer
	initialAuthDone atomic.Bool
	authFailures    atomic.Int32
	rangeCalls      atomic.Int32
	watchRequests   chan *etcdserverpb.WatchCreateRequest
}

func (s *leaderChangedEtcdServer) Authenticate(context.Context, *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
	if !s.initialAuthDone.Swap(true) {
		return &etcdserverpb.AuthenticateResponse{Token: "initial"}, nil
	}
	// Fail both the first watch's authentication and the token refresh
	// triggered by the first snapshot Get during recovery.
	if s.authFailures.Add(1) <= 2 {
		return nil, rpctypes.ErrGRPCLeaderChanged
	}
	return &etcdserverpb.AuthenticateResponse{Token: "recovered"}, nil
}

func (s *leaderChangedEtcdServer) Range(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if s.rangeCalls.Add(1) == 1 {
		// The unary interceptor refreshes the token, whose Authenticate RPC
		// fails with leader changed. Get must propagate that error to Milvus.
		return nil, rpctypes.ErrGRPCInvalidAuthToken
	}
	return &etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 20},
		Kvs:    []*mvccpb.KeyValue{{Key: []byte("proxy-200"), Value: []byte(`{"ServerID":200}`)}},
	}, nil
}

func (s *leaderChangedEtcdServer) Watch(stream etcdserverpb.Watch_WatchServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	s.watchRequests <- req.GetCreateRequest()
	if err := stream.Send(&etcdserverpb.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 20}, WatchId: 1, Created: true,
	}); err != nil {
		return err
	}
	if err := stream.Send(&etcdserverpb.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 22}, WatchId: 1,
		Events: []*mvccpb.Event{
			{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("proxy-201"), Value: []byte(`{"ServerID":201}`), ModRevision: 21}},
			{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte("proxy-200"), ModRevision: 22}, PrevKv: &mvccpb.KeyValue{Value: []byte(`{"ServerID":200}`)}},
		},
	}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestProxyWatcher_LeaderChangedRecovery(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { listener.Close() })
	server := grpc.NewServer()
	service := &leaderChangedEtcdServer{watchRequests: make(chan *etcdserverpb.WatchCreateRequest, 1)}
	etcdserverpb.RegisterAuthServer(server, service)
	etcdserverpb.RegisterKVServer(server, service)
	etcdserverpb.RegisterWatchServer(server, service)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://etcd-recovery"},
		Username:  "test", Password: "test", DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		})},
	})
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })

	// Observe the real client error before handing it to the watch loop. This
	// distinguishes a terminal authentication failure from transparent reconnect.
	failedWatch := cli.Watch(ctx, "proxy", clientv3.WithPrefix())
	var response clientv3.WatchResponse
	select {
	case response = <-failedWatch:
	case <-ctx.Done():
		t.Fatal("watch did not report the authentication failure")
	}
	require.ErrorIs(t, response.Err(), rpctypes.ErrLeaderChanged)
	require.True(t, response.Canceled)

	snapshots, added, deleted := make(chan int64, 1), make(chan int64, 1), make(chan int64, 1)
	p := NewProxyWatcher(cli, func(sessions []*sessionutil.Session) {
		if len(sessions) == 1 {
			snapshots <- sessions[0].ServerID
		}
	})
	p.AddSessionFunc(func(s *sessionutil.Session) { added <- s.ServerID })
	p.DelSessionFunc(func(s *sessionutil.Session) { deleted <- s.ServerID })
	t.Cleanup(func() {
		p.Stop()
		cancel()
	})
	events := make(chan clientv3.WatchResponse, 1)
	events <- response
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		p.startWatchEtcd(ctx, events)
	}()
	t.Cleanup(func() {
		cancel()
		<-watchDone
	})

	receive := func(ch <-chan int64) int64 {
		t.Helper()
		select {
		case id := <-ch:
			return id
		case <-ctx.Done():
			t.Fatal("watch recovery did not deliver the expected session")
			return 0
		}
	}
	require.Equal(t, int64(200), receive(snapshots))
	require.Equal(t, int64(201), receive(added))
	require.Equal(t, int64(200), receive(deleted))
	require.EqualValues(t, 2, service.rangeCalls.Load(), "snapshot Get must retry the leader change during token refresh")
	select {
	case req := <-service.watchRequests:
		require.EqualValues(t, 21, req.StartRevision, "resume after the refreshed snapshot")
		require.True(t, req.PrevKv)
	case <-ctx.Done():
		t.Fatal("watch was not re-established")
	}
}

type proxyRecoveryEtcdServer struct {
	etcdserverpb.UnimplementedKVServer
	etcdserverpb.UnimplementedWatchServer
	rangeCalls    atomic.Int32
	watchCalls    atomic.Int32
	rangeFn       func(context.Context, int32) (*etcdserverpb.RangeResponse, error)
	watchRequests chan *etcdserverpb.WatchCreateRequest
	watchCanceled chan int64
}

func (s *proxyRecoveryEtcdServer) Range(ctx context.Context, _ *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return s.rangeFn(ctx, s.rangeCalls.Add(1))
}

func (s *proxyRecoveryEtcdServer) Watch(stream etcdserverpb.Watch_WatchServer) error {
	var active int64
	defer func() {
		if active != 0 {
			s.watchCanceled <- active
		}
	}()
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if cancel := req.GetCancelRequest(); cancel != nil {
			s.watchCanceled <- cancel.WatchId
			active = 0
			if err := stream.Send(&etcdserverpb.WatchResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 22}, WatchId: cancel.WatchId, Canceled: true,
			}); err != nil {
				return err
			}
			continue
		}
		create := req.GetCreateRequest()
		if create == nil {
			continue
		}
		id := int64(s.watchCalls.Add(1))
		active = id
		s.watchRequests <- create
		if err := stream.Send(&etcdserverpb.WatchResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: 20}, WatchId: id, Created: true,
		}); err != nil {
			return err
		}
		response := &etcdserverpb.WatchResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: 22}, WatchId: id,
			Events: []*mvccpb.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{
				Key: []byte("proxy-201"), Value: []byte(`{"ServerID":201}`), ModRevision: 22,
			}}},
		}
		if id == 1 {
			active = 0
			response = &etcdserverpb.WatchResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 20}, WatchId: id, Canceled: true, CompactRevision: 10,
			}
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func newProxyRecoveryClient(t *testing.T, rangeFn func(context.Context, int32) (*etcdserverpb.RangeResponse, error)) (*clientv3.Client, *proxyRecoveryEtcdServer) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { listener.Close() })
	server := grpc.NewServer()
	service := &proxyRecoveryEtcdServer{
		rangeFn: rangeFn, watchRequests: make(chan *etcdserverpb.WatchCreateRequest, 4), watchCanceled: make(chan int64, 4),
	}
	etcdserverpb.RegisterKVServer(server, service)
	etcdserverpb.RegisterWatchServer(server, service)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://proxy-recovery"}, DialTimeout: time.Second, MaxUnaryRetries: 1,
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		})},
	})
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })
	return cli, service
}

func proxySnapshot() *etcdserverpb.RangeResponse {
	return &etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 20},
		Kvs:    []*mvccpb.KeyValue{{Key: []byte("proxy-200"), Value: []byte(`{"ServerID":200}`)}},
	}
}

func TestProxyWatcher_StopDuringRecovery(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name         string
		blocked      bool
		cancelCaller bool
	}{
		{name: "backoff"},
		{name: "inflight_request", blocked: true},
		{name: "caller_canceled", blocked: true, cancelCaller: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			started, canceled := make(chan struct{}, 1), make(chan struct{}, 1)
			cli, _ := newProxyRecoveryClient(t, func(ctx context.Context, call int32) (*etcdserverpb.RangeResponse, error) {
				if call == 1 {
					return proxySnapshot(), nil
				}
				select {
				case started <- struct{}{}:
				default:
				}
				if tc.blocked {
					<-ctx.Done()
					canceled <- struct{}{}
					return nil, ctx.Err()
				}
				return nil, rpctypes.ErrGRPCLeaderChanged
			})
			p := NewProxyWatcher(cli)
			t.Cleanup(p.Stop)
			require.NoError(t, p.WatchProxy(ctx))
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("recovery request did not start")
			}
			stopped := make(chan struct{})
			go func() {
				if tc.cancelCaller {
					cancel()
					p.wg.Wait()
				} else {
					p.Stop()
				}
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(time.Second):
				cancel()
				t.Fatal("Stop did not cancel recovery")
			}
			if !tc.cancelCaller {
				require.NoError(t, ctx.Err(), "Stop must not depend on caller cancellation")
				require.ErrorIs(t, p.WatchProxy(ctx), context.Canceled, "a stopped watcher cannot restart")
			}
			if tc.blocked {
				select {
				case <-canceled:
				case <-time.After(time.Second):
					t.Fatal("Stop did not cancel the in-flight RPC")
				}
			}
		})
	}
}

func TestProxyWatcher_StopDuringInitialList(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	started := make(chan struct{})
	cli, _ := newProxyRecoveryClient(t, func(ctx context.Context, _ int32) (*etcdserverpb.RangeResponse, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	p := NewProxyWatcher(cli)
	t.Cleanup(p.Stop)
	watchResult := make(chan error, 1)
	go func() { watchResult <- p.WatchProxy(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("initial List did not start")
	}
	stopped := make(chan struct{})
	go func() { p.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("Stop did not cancel the initial List")
	}
	require.ErrorIs(t, <-watchResult, context.Canceled)
	require.NoError(t, ctx.Err())
}

func TestProxyWatcher_SnapshotTimeoutAborts(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().EtcdCfg.RequestTimeout.Key
	old := paramtable.Get().EtcdCfg.RequestTimeout.GetValue()
	require.NoError(t, paramtable.Get().Save(key, "100"))
	t.Cleanup(func() { paramtable.Get().Save(key, old) })
	blockUntilTimeout := func(ctx context.Context, _ int32) (*etcdserverpb.RangeResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	t.Run("initial", func(t *testing.T) {
		cli, service := newProxyRecoveryClient(t, blockUntilTimeout)
		p := NewProxyWatcher(cli)
		t.Cleanup(p.Stop)
		require.ErrorIs(t, p.WatchProxy(context.Background()), context.DeadlineExceeded)
		require.EqualValues(t, 1, service.rangeCalls.Load(), "a snapshot timeout must not be retried")
	})
	t.Run("recovery", func(t *testing.T) {
		cli, service := newProxyRecoveryClient(t, blockUntilTimeout)
		p := NewProxyWatcher(cli)
		t.Cleanup(p.Stop)
		events := make(chan clientv3.WatchResponse, 1)
		events <- clientv3.WatchResponse{CompactRevision: 1}
		require.Panics(t, func() { p.startWatchEtcd(context.Background(), events) })
		require.EqualValues(t, 1, service.rangeCalls.Load(), "a snapshot timeout must not be retried")
	})
}

func TestProxyWatcher_RetryInitialSnapshot(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cli, service := newProxyRecoveryClient(t, func(ctx context.Context, call int32) (*etcdserverpb.RangeResponse, error) {
		if call == 1 {
			return nil, rpctypes.ErrGRPCLeaderChanged
		}
		return proxySnapshot(), nil
	})
	snapshots := make(chan int64, 2)
	added := make(chan int64, 1)
	p := NewProxyWatcher(cli, func(sessions []*sessionutil.Session) {
		if len(sessions) == 1 {
			snapshots <- sessions[0].ServerID
		}
	})
	p.AddSessionFunc(func(session *sessionutil.Session) { added <- session.ServerID })
	t.Cleanup(p.Stop)
	require.NoError(t, p.WatchProxy(ctx), "transient startup errors must not leave the component without a watcher")
	require.EqualValues(t, 200, <-snapshots)
	select {
	case id := <-added:
		require.EqualValues(t, 201, id)
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not deliver events after the initial snapshot recovered")
	}
	require.EqualValues(t, 3, service.rangeCalls.Load(), "two initial attempts, then one resnapshot after compaction")
	require.NoError(t, ctx.Err())
}

func TestProxyWatcher_RecoveryPermissionDenied(t *testing.T) {
	paramtable.Init()
	cli, service := newProxyRecoveryClient(t, func(context.Context, int32) (*etcdserverpb.RangeResponse, error) {
		return nil, rpctypes.ErrGRPCPermissionDenied
	})
	p := NewProxyWatcher(cli)
	t.Cleanup(p.Stop)
	require.ErrorIs(t, p.WatchProxy(context.Background()), rpctypes.ErrPermissionDenied)
	require.EqualValues(t, 1, service.rangeCalls.Load(), "initial permission denial must return without retry")

	events := make(chan clientv3.WatchResponse, 1)
	events <- clientv3.WatchResponse{CompactRevision: 1}
	require.Panics(t, func() { p.startWatchEtcd(context.Background(), events) })
	require.EqualValues(t, 2, service.rangeCalls.Load(), "permission denial must not be retried")
}

func TestProxyWatcher_CanceledClosedWatch(t *testing.T) {
	p := NewProxyWatcher(nil)
	t.Cleanup(p.Stop)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := make(chan clientv3.WatchResponse)
	close(events)
	for range 100 {
		require.NotPanics(t, func() { p.startWatchEtcd(ctx, events) })
	}
}
