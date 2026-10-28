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
	require.NotPanics(t, func() { p.startWatchEtcd(ctx, events) })

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
