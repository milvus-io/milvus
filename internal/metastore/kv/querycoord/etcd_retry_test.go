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

package querycoord

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type resourceGroupTxnServer struct {
	etcdserverpb.UnimplementedKVServer
	txn func(context.Context, *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error)
}

func (s *resourceGroupTxnServer) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	return s.txn(ctx, req)
}

func newResourceGroupTxnClient(t *testing.T, service *resourceGroupTxnServer) *clientv3.Client {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { listener.Close() })
	server := grpc.NewServer()
	etcdserverpb.RegisterKVServer(server, service)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://resource-group-etcd"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		})},
	})
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })
	return cli
}

// Exercise the actual catalog and etcdKV layers: mocking SaveResourceGroup to
// return an etcd sentinel misses errors that get stringified below the catalog.
func TestCatalogSaveResourceGroupRetriesEtcdErrors(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name      string
		rpcErr    error
		cause     error
		retriable bool
	}{
		{"leader changed", rpctypes.ErrGRPCLeaderChanged, rpctypes.ErrLeaderChanged, true},
		{"server stopped", rpctypes.ErrGRPCStopped, rpctypes.ErrStopped, true},
		{"permission denied", rpctypes.ErrGRPCPermissionDenied, rpctypes.ErrPermissionDenied, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var rpcCalls atomic.Int32
			requests := make(chan *etcdserverpb.TxnRequest, 3)
			cli := newResourceGroupTxnClient(t, &resourceGroupTxnServer{
				txn: func(_ context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
					requests <- req
					if rpcCalls.Add(1) == 1 {
						return nil, tc.rpcErr
					}
					return &etcdserverpb.TxnResponse{
						Header: &etcdserverpb.ResponseHeader{Revision: 2}, Succeeded: true,
					}, nil
				},
			})
			catalog := NewCatalog(etcdkv.NewEtcdKV(cli, "test"))
			rg := &querypb.ResourceGroup{Name: "rg", Nodes: []int64{101}}
			var firstErr error
			var attempts int
			err := retry.Do(ctx, func() error {
				attempts++
				err := catalog.SaveResourceGroup(ctx, rg)
				if attempts == 1 {
					firstErr = err
				}
				return err
			}, retry.Attempts(3), retry.Sleep(time.Millisecond), retry.RetryErr(etcd.IsRetriableEtcdErr))
			require.ErrorIs(t, firstErr, tc.cause)
			var etcdErr rpctypes.EtcdError
			require.ErrorAs(t, firstErr, &etcdErr)
			require.Equal(t, merr.Code(merr.ErrIoFailed), merr.Code(firstErr))
			require.Equal(t, tc.retriable, etcd.IsRetriableEtcdErr(firstErr))
			if tc.retriable {
				require.NoError(t, err)
				require.Equal(t, 2, attempts)
				require.EqualValues(t, 2, rpcCalls.Load())
				first, second := <-requests, <-requests
				require.Equal(t, first, second, "retry must persist the same resource group")
			} else {
				require.ErrorIs(t, err, tc.cause)
				require.Equal(t, 1, attempts)
				require.EqualValues(t, 1, rpcCalls.Load())
			}
		})
	}
}

func TestCatalogSaveResourceGroupPreservesRequestDeadline(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli := newResourceGroupTxnClient(t, &resourceGroupTxnServer{
		txn: func(ctx context.Context, _ *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	catalog := NewCatalog(etcdkv.NewEtcdKV(cli, "test", etcdkv.WithRequestTimeout(100*time.Millisecond)))
	err := catalog.SaveResourceGroup(ctx, &querypb.ResourceGroup{Name: "rg"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, merr.Code(merr.ErrIoFailed), merr.Code(err))
	require.NoError(t, ctx.Err(), "only the individual etcd request expired")
	require.False(t, etcd.IsRetriableEtcdErr(err))
}
