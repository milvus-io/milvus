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

package sessionutil

import (
	"context"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSessionWatchRetry(t *testing.T) {
	paramtable.Init()
	prolonged := make([]error, 10)
	for i := range prolonged {
		prolonged[i] = rpctypes.ErrLeaderChanged
	}
	for _, tc := range []struct {
		name        string
		getErrors   []error
		watchErrors []error
		want        error
		getCalls    int
		watchCalls  int
		cancel      bool
	}{
		{name: "recover repeated reads", getErrors: []error{rpctypes.ErrNoLeader, merr.Wrap(rpctypes.ErrTimeout, "get")}, getCalls: 3, watchCalls: 1},
		{name: "recover repeated stream setup", watchErrors: []error{rpctypes.ErrLeaderChanged, rpctypes.ErrStopped}, getCalls: 3, watchCalls: 3},
		{name: "permanent read", getErrors: []error{rpctypes.ErrPermissionDenied}, want: rpctypes.ErrPermissionDenied, getCalls: 1},
		{name: "permanent stream setup", watchErrors: []error{rpctypes.ErrAuthFailed}, want: rpctypes.ErrAuthFailed, getCalls: 1, watchCalls: 1},
		{name: "read recovery beyond old retry limit", getErrors: prolonged, getCalls: 11, watchCalls: 1},
		{name: "stream recovery beyond old retry limit", watchErrors: prolonged, getCalls: 11, watchCalls: 11},
		{name: "cancel during read retry", getErrors: []error{rpctypes.ErrLeaderChanged}, want: context.Canceled, getCalls: 1, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				cli, err := etcd.GetRemoteEtcdClient(paramtable.Get().EtcdCfg.Endpoints.GetAsStrings())
				require.NoError(t, err)
				defer cli.Close()
				s := &Session{ctx: ctx, etcdCli: cli, metaRoot: "54137-unit"}
				w := &sessionWatcher{s: s, ctx: ctx, prefix: "node", eventCh: make(chan *SessionEvent)}
				gets, watches := 0, 0
				nextError := func(errs []error, call int) error {
					if call <= len(errs) {
						return errs[call-1]
					}
					return nil
				}
				mockey.Mock((*Session).GetSessions).To(func(_ *Session, _ context.Context, prefix string) (map[string]*Session, int64, error) {
					gets++
					require.Equal(t, "node", prefix)
					if tc.cancel {
						cancel()
					}
					return map[string]*Session{"node-1": {SessionRaw: SessionRaw{ServerID: 1}}}, 42, nextError(tc.getErrors, gets)
				}).Build()
				mockey.Mock((*clientv3.WatchResponse).Err).To(func(resp *clientv3.WatchResponse) error {
					if resp.Header.Revision == -1 {
						return rpctypes.ErrLeaderChanged
					}
					return nextError(tc.watchErrors, int(resp.Header.Revision))
				}).Build()
				mockey.Mock(mockey.GetMethod(cli.Watcher, "Watch")).To(func(_ context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
					watches++
					require.Equal(t, path.Join(s.metaRoot, DefaultServiceRoot, w.prefix), key)
					require.Equal(t, int64(42), clientv3.OpGet(key, opts...).Rev())
					ch := make(chan clientv3.WatchResponse, 1)
					ch <- clientv3.WatchResponse{Header: etcdserverpb.ResponseHeader{Revision: int64(watches)}}
					return ch
				}).Build()
				reconciles := 0
				w.rewatch = func(sessions map[string]*Session) error {
					reconciles++
					require.Equal(t, int64(1), sessions["node-1"].ServerID)
					return nil
				}
				consume := func() {
					response := clientv3.WatchResponse{Header: etcdserverpb.ResponseHeader{Revision: -1}}
					for {
						w.handleWatchResponse(response)
						if ctx.Err() != nil {
							return
						}
						select {
						case <-ctx.Done():
							t.Fatal("watch recovery timed out")
						case response = <-w.rch:
							if response.Err() == nil {
								return
							}
						}
					}
				}
				if tc.want != nil && !tc.cancel {
					require.PanicsWithError(t, tc.want.Error(), consume)
				} else {
					require.NotPanics(t, consume)
				}
				require.Equal(t, tc.getCalls, gets)
				require.Equal(t, tc.watchCalls, watches)
				require.Equal(t, watches, reconciles)
				if tc.want != nil {
					_, ok := <-w.eventCh
					require.False(t, ok)
				} else {
					require.NotNil(t, w.rch)
					select {
					case <-w.eventCh:
						t.Fatal("event channel closed on successful recovery")
					default:
					}
				}
			})
		})
	}
}

func TestSessionWatchRecoveryRevision(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, err := etcd.GetRemoteEtcdClient(paramtable.Get().EtcdCfg.Endpoints.GetAsStrings())
	require.NoError(t, err)
	defer cli.Close()
	root := "54137-revision-" + t.Name()
	key := path.Join(root, DefaultServiceRoot, "node-1")
	defer cli.Delete(context.Background(), root, clientv3.WithPrefix())
	_, err = cli.Put(ctx, key, `{"ServerID":1}`)
	require.NoError(t, err)
	w := &sessionWatcher{
		ctx: ctx,
		s:   &Session{ctx: ctx, etcdCli: cli, metaRoot: root}, prefix: "node",
		eventCh: make(chan *SessionEvent, 10), validate: func(*Session) bool { return true },
		rewatch: func(sessions map[string]*Session) error {
			require.Equal(t, int64(1), sessions["node-1"].ServerID)
			// A write between the snapshot and watch creation must be replayed.
			_, err := cli.Put(ctx, key, `{"ServerID":2}`)
			return err
		},
	}
	require.NoError(t, w.handleWatchErr(rpctypes.ErrLeaderChanged))
	for {
		select {
		case <-ctx.Done():
			t.Fatal("missed update between snapshot and new watch")
		case resp := <-w.rch:
			require.NoError(t, resp.Err())
			w.handleWatchResponse(resp)
			for len(w.eventCh) > 0 {
				if (<-w.eventCh).Session.ServerID == 2 {
					return
				}
			}
		}
	}
}

// Exercise the real clientv3 getToken -> Authenticate -> ContextError ->
// isHaltErr path. Only the Authenticate RPC result is injected; KV and Watch
// use the local etcd server. No server auth settings or leadership are changed.
func TestSessionWatchAuthenticateLeaderChanged(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var failures atomic.Int32
	var injected atomic.Int32
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: paramtable.Get().EtcdCfg.Endpoints.GetAsStrings(),
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
	root := "54137-authenticate-" + t.Name()
	defer cli.Delete(context.Background(), root, clientv3.WithPrefix())
	key := path.Join(root, DefaultServiceRoot, "node-1")
	_, err = cli.Put(ctx, key, `{"ServerID":1}`)
	require.NoError(t, err)
	s := &Session{ctx: ctx, etcdCli: cli, metaRoot: root}
	reconciles := 0
	w := &sessionWatcher{
		ctx: ctx,
		s:   s, prefix: "node", eventCh: make(chan *SessionEvent, 10), validate: func(*Session) bool { return true },
		rewatch: func(sessions map[string]*Session) error {
			reconciles++
			require.Equal(t, int64(1), sessions["node-1"].ServerID)
			return nil
		},
	}
	failures.Store(3)
	w.rch = cli.Watch(ctx, key)
	for reconciles < 3 {
		select {
		case <-ctx.Done():
			t.Fatal("watch did not surface injected Authenticate error")
		case resp := <-w.rch:
			require.ErrorIs(t, resp.Err(), rpctypes.ErrLeaderChanged)
			require.NotPanics(t, func() { w.handleWatchResponse(resp) })
		}
	}
	require.Equal(t, int32(3), injected.Load())
	require.Equal(t, 3, reconciles)
	_, err = cli.Put(ctx, key, `{"ServerID":2}`)
	require.NoError(t, err)
	for {
		select {
		case <-ctx.Done():
			t.Fatal("watch did not recover after Authenticate failures")
		case resp := <-w.rch:
			require.NoError(t, resp.Err())
			w.handleWatchResponse(resp)
			for len(w.eventCh) > 0 {
				if (<-w.eventCh).Session.ServerID == 2 {
					return
				}
			}
		}
	}
}
