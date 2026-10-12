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
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	reconciles := 0
	p := NewProxyWatcher(cli, func(sessions []*sessionutil.Session) {
		reconciles++
		require.Len(t, sessions, 1)
		require.Equal(t, int64(1), sessions[0].ServerID)
	})
	defer p.Stop()
	added := make(chan int64, 10)
	p.AddSessionFunc(func(s *sessionutil.Session) { added <- s.ServerID })
	failures.Store(3)
	// The failed initial stream and two failed reconstructions all traverse
	// the real client's Authenticate error conversion and watch teardown.
	require.NotPanics(t, func() { p.startWatchEtcd(ctx, cli.Watch(ctx, key)) })
	require.Equal(t, int32(3), injected.Load())
	require.Equal(t, 3, reconciles)
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
		stop   bool
		want   error
	}{
		{name: "closed", closed: true, want: merr.WrapErrServiceUnavailable("proxy watch closed before creation")},
		{name: "canceled", cancel: true, want: context.Canceled},
		{name: "stopped", stop: true},
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
					case tc.stop:
						p.closeCh.Close()
					default:
						ch <- clientv3.WatchResponse{}
					}
					return ch
				}).Build()
				err = p.WatchProxy(ctx)
				if tc.want == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tc.want)
				}
			})
		})
	}
}
