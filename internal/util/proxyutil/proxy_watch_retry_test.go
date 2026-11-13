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
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestProxyWatchAuthenticateLeaderChanged(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	originalRoot := params.EtcdCfg.RootPath.GetValue()
	root := "54137-proxy-" + t.Name()
	require.NoError(t, params.Save(params.EtcdCfg.RootPath.Key, root))
	defer params.Save(params.EtcdCfg.RootPath.Key, originalRoot)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var failures atomic.Int32
	var injected atomic.Int32
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: params.EtcdCfg.Endpoints.GetAsStrings(),
		Username:  "watch-retry-test", Password: t.Name(),
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if method == "/etcdserverpb.Auth/Authenticate" && failures.Load() > 0 {
				failures.Add(-1)
				injected.Add(1)
				return rpctypes.ErrGRPCLeaderChanged
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		})},
	})
	require.NoError(t, err)
	defer cli.Close()
	defer cli.Delete(context.Background(), root, clientv3.WithPrefix())
	key := path.Join(params.EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot, typeutil.ProxyRole+"-1")
	_, err = cli.Put(ctx, key, `{"ServerID":1}`)
	require.NoError(t, err)
	var reconciles atomic.Int32
	p := NewProxyWatcher(cli, func(sessions []*sessionutil.Session) {
		reconciles.Add(1)
		require.Len(t, sessions, 1)
		require.Equal(t, int64(1), sessions[0].ServerID)
	})
	defer p.Stop()
	added := make(chan int64, 10)
	p.AddSessionFunc(func(s *sessionutil.Session) { added <- s.ServerID })
	failures.Store(3)
	// First-stream setup failures and later failures use the same background
	// recovery loop. WatchProxy returns after initializing the snapshot.
	require.NoError(t, p.WatchProxy(ctx))
	require.Eventually(t, func() bool {
		return injected.Load() == 3 && reconciles.Load() == 4
	}, 8*time.Second, 10*time.Millisecond)
	_, err = cli.Put(ctx, key, `{"ServerID":2}`)
	require.NoError(t, err)
	select {
	case <-ctx.Done():
		t.Fatal("proxy watch did not recover")
	case id := <-added:
		require.Equal(t, int64(2), id)
	}
}

func TestProxyWatchSetupTermination(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name   string
		closed bool
		cancel bool
		want   error
	}{
		{name: "closed", closed: true, want: merr.WrapErrServiceUnavailable("proxy watch channel closed unexpectedly")},
		{name: "canceled", cancel: true, want: context.Canceled},
		{name: "permanent error", want: rpctypes.ErrAuthFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cli, err := etcd.GetRemoteEtcdClient(paramtable.Get().EtcdCfg.Endpoints.GetAsStrings())
				require.NoError(t, err)
				defer cli.Close()
				p := NewProxyWatcher(cli)
				defer p.Stop()
				mockey.Mock((*ProxyWatcher).getSessionsOnEtcd).Return([]*sessionutil.Session{}, int64(42), nil).Build()
				mockey.Mock((*clientv3.WatchResponse).Err).Return(rpctypes.ErrAuthFailed).Build()
				mockey.Mock(mockey.GetMethod(cli.Watcher, "Watch")).To(func(_ context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
					require.Equal(t, int64(43), clientv3.OpGet(key, opts...).Rev())
					ch := make(chan clientv3.WatchResponse, 1)
					switch {
					case tc.closed:
						close(ch)
					case tc.cancel:
						cancel()
					default:
						ch <- clientv3.WatchResponse{}
					}
					return ch
				}).Build()
				err = p.startWatchEtcd(ctx, 42)
				if tc.want == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tc.want)
				}
			})
		})
	}
}

// Stop must interrupt every phase, including the blocking clientv3.Watch call.
func TestProxyWatchStop(t *testing.T) {
	paramtable.Init()
	for _, phase := range []string{"initial snapshot", "watch setup", "watch events", "retry wait", "recovery snapshot"} {
		t.Run(phase, func(t *testing.T) {
			mockey.PatchConvey(phase, t, func() {
				cli, err := etcd.GetRemoteEtcdClient(paramtable.Get().EtcdCfg.Endpoints.GetAsStrings())
				require.NoError(t, err)
				defer cli.Close()
				p := NewProxyWatcher(cli)
				defer p.Stop()
				entered := make(chan struct{})
				var gets atomic.Int32
				mockey.Mock((*ProxyWatcher).getSessionsOnEtcd).To(func(_ *ProxyWatcher, ctx context.Context) ([]*sessionutil.Session, int64, error) {
					call := gets.Add(1)
					if phase == "initial snapshot" || (phase == "recovery snapshot" && call == 2) {
						close(entered)
						<-ctx.Done()
						return nil, 0, ctx.Err()
					}
					return nil, 42, nil
				}).Build()
				mockey.Mock(mockey.GetMethod(cli.Watcher, "Watch")).To(func(ctx context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
					ch := make(chan clientv3.WatchResponse, 1)
					if phase != "recovery snapshot" {
						close(entered)
					}
					switch phase {
					case "watch setup":
						<-ctx.Done()
					case "retry wait", "recovery snapshot":
						ch <- clientv3.WatchResponse{CompactRevision: 1}
					}
					return ch
				}).Build()
				started := make(chan error, 1)
				go func() { started <- p.WatchProxy(context.Background()) }()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("watch did not enter " + phase)
				}
				if phase != "initial snapshot" {
					select {
					case err := <-started:
						require.NoError(t, err)
					case <-time.After(time.Second):
						t.Fatal("WatchProxy waited for stream creation")
					}
				}
				stopped := make(chan struct{})
				go func() { p.Stop(); close(stopped) }()
				select {
				case <-stopped:
				case <-time.After(time.Second):
					t.Fatal("Stop blocked in " + phase)
				}
				if phase == "initial snapshot" {
					require.ErrorIs(t, <-started, context.Canceled)
				}
				require.ErrorIs(t, p.WatchProxy(context.Background()), context.Canceled)
			})
		})
	}
}

func TestProxyWatchResumesSnapshot(t *testing.T) {
	paramtable.Init()
	mockey.PatchConvey("reconcile after watch and snapshot failures", t, func() {
		cli, err := etcd.GetRemoteEtcdClient(paramtable.Get().EtcdCfg.Endpoints.GetAsStrings())
		require.NoError(t, err)
		defer cli.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var mu sync.Mutex
		sessions := make(map[int64]bool)
		p := NewProxyWatcher(cli, func(snapshot []*sessionutil.Session) {
			mu.Lock()
			defer mu.Unlock()
			sessions = make(map[int64]bool)
			for _, s := range snapshot {
				sessions[s.ServerID] = true
			}
		})
		defer p.Stop()
		p.AddSessionFunc(func(s *sessionutil.Session) { mu.Lock(); defer mu.Unlock(); sessions[s.ServerID] = true })
		p.DelSessionFunc(func(s *sessionutil.Session) { mu.Lock(); defer mu.Unlock(); delete(sessions, s.ServerID) })
		var gets, watches atomic.Int32
		mockey.Mock((*ProxyWatcher).getSessionsOnEtcd).To(func(_ *ProxyWatcher, _ context.Context) ([]*sessionutil.Session, int64, error) {
			call := gets.Add(1)
			if call == 2 {
				return nil, 0, rpctypes.ErrNoLeader
			}
			return []*sessionutil.Session{{SessionRaw: sessionutil.SessionRaw{ServerID: int64(call)}}}, int64(call * 10), nil
		}).Build()
		var previous context.Context
		mockey.Mock(mockey.GetMethod(cli.Watcher, "Watch")).To(func(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
			call := watches.Add(1)
			ch := make(chan clientv3.WatchResponse, 2)
			if call == 1 {
				require.Equal(t, int64(11), clientv3.OpGet(key, opts...).Rev())
				previous = ctx
				ch <- clientv3.WatchResponse{CompactRevision: 1}
			} else {
				require.ErrorIs(t, previous.Err(), context.Canceled)
				require.Equal(t, int64(31), clientv3.OpGet(key, opts...).Rev())
				ch <- clientv3.WatchResponse{Events: []*clientv3.Event{
					{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Value: []byte(`{"ServerID":4}`)}},
					{Type: mvccpb.DELETE, PrevKv: &mvccpb.KeyValue{Value: []byte(`{"ServerID":3}`)}},
				}}
			}
			return ch
		}).Build()
		require.NoError(t, p.WatchProxy(ctx))
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(sessions) == 1 && sessions[4]
		}, 5*time.Second, 10*time.Millisecond)
		p.Stop()
		require.Equal(t, int32(3), gets.Load())
		require.Equal(t, int32(2), watches.Load())
	})
}

func TestProxyWatchInitialSnapshotTimeout(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	originalTimeout := params.EtcdCfg.RequestTimeout.GetValue()
	require.NoError(t, params.Save(params.EtcdCfg.RequestTimeout.Key, "100"))
	defer params.Save(params.EtcdCfg.RequestTimeout.Key, originalTimeout)
	mockey.PatchConvey("initial snapshot still has a request timeout", t, func() {
		p := NewProxyWatcher(nil)
		defer p.Stop()
		mockey.Mock((*ProxyWatcher).getSessionsOnEtcd).To(func(_ *ProxyWatcher, ctx context.Context) ([]*sessionutil.Session, int64, error) {
			<-ctx.Done()
			return nil, 0, ctx.Err()
		}).Build()
		// The caller stays alive after the shorter initialization timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.ErrorIs(t, p.WatchProxy(ctx), context.DeadlineExceeded)
		require.NoError(t, ctx.Err())
	})
}

func TestProxyWatchRecoverySnapshotTimeout(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	originalTimeout := params.EtcdCfg.RequestTimeout.GetValue()
	require.NoError(t, params.Save(params.EtcdCfg.RequestTimeout.Key, "100"))
	defer params.Save(params.EtcdCfg.RequestTimeout.Key, originalTimeout)
	for _, outcome := range []string{"recovered", "parent deadline", "permission denied"} {
		t.Run(outcome, func(t *testing.T) {
			mockey.PatchConvey(outcome, t, func() {
				cli, err := etcd.GetRemoteEtcdClient(params.EtcdCfg.Endpoints.GetAsStrings())
				require.NoError(t, err)
				defer cli.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				var reconciled, added int64
				p := NewProxyWatcher(cli, func(sessions []*sessionutil.Session) {
					require.Len(t, sessions, 1)
					reconciled = sessions[0].ServerID
				})
				defer p.Stop()
				p.AddSessionFunc(func(s *sessionutil.Session) {
					added = s.ServerID
					cancel()
				})
				mockey.Mock((*ProxyWatcher).getSessionsOnEtcd).To(func(_ *ProxyWatcher, getCtx context.Context) ([]*sessionutil.Session, int64, error) {
					if outcome == "parent deadline" {
						<-getCtx.Done()
						return nil, 0, getCtx.Err()
					}
					// Simulate etcd remaining unavailable beyond RequestTimeout.
					select {
					case <-getCtx.Done():
						return nil, 0, getCtx.Err()
					case <-time.After(300 * time.Millisecond):
					}
					if outcome == "permission denied" {
						return nil, 0, rpctypes.ErrPermissionDenied
					}
					return []*sessionutil.Session{{SessionRaw: sessionutil.SessionRaw{ServerID: 2}}}, 20, nil
				}).Build()
				watches := 0
				mockey.Mock(mockey.GetMethod(cli.Watcher, "Watch")).To(func(_ context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
					watches++
					ch := make(chan clientv3.WatchResponse, 1)
					if watches == 1 {
						ch <- clientv3.WatchResponse{CompactRevision: 1}
					} else {
						require.Equal(t, int64(21), clientv3.OpGet(key, opts...).Rev())
						ch <- clientv3.WatchResponse{Events: []*clientv3.Event{
							{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Value: []byte(`{"ServerID":3}`)}},
						}}
					}
					return ch
				}).Build()
				err = p.startWatchEtcd(ctx, 10)
				switch outcome {
				case "recovered":
					require.ErrorIs(t, err, context.Canceled)
					require.Equal(t, int64(2), reconciled)
					require.Equal(t, int64(3), added)
					require.Equal(t, 2, watches)
				case "parent deadline":
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
				case "permission denied":
					require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
					require.NoError(t, ctx.Err())
				}
			})
		})
	}
}
