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

package etcd

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
)

// authFailingServer accepts the client's initial authentication and then fails
// every later Authenticate with err, the way a member does while it restarts or
// the cluster elects a leader.
type authFailingServer struct {
	etcdserverpb.UnimplementedAuthServer
	etcdserverpb.UnimplementedWatchServer
	authCalls atomic.Int32
	err       error
}

func (s *authFailingServer) Authenticate(context.Context, *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
	if s.authCalls.Add(1) == 1 {
		return &etcdserverpb.AuthenticateResponse{Token: "initial"}, nil
	}
	return nil, s.err
}

// With auth enabled, clientv3 authenticates before opening a watch stream and
// cancels the watch when that fails with a server error, instead of
// reconnecting. Every transient server error must therefore be classified as
// retriable, or the watch owner treats an etcd restart as fatal.
func TestIsRetriableWatchErr_WatchCanceledByAuthFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"leader changed", rpctypes.ErrGRPCLeaderChanged},
		{"no leader", rpctypes.ErrGRPCNoLeader},
		{"server stopped", rpctypes.ErrGRPCStopped},
		{"timeout", rpctypes.ErrGRPCTimeout},
		{"timeout due to leader fail", rpctypes.ErrGRPCTimeoutDueToLeaderFail},
		{"timeout due to connection lost", rpctypes.ErrGRPCTimeoutDueToConnectionLost},
		{"not capable", rpctypes.ErrGRPCNotCapable},
		{"unhealthy", rpctypes.ErrGRPCUnhealthy},
		{"too many requests", rpctypes.ErrGRPCRequestTooManyRequests},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			listener := bufconn.Listen(1024 * 1024)
			server := grpc.NewServer()
			service := &authFailingServer{err: c.err}
			etcdserverpb.RegisterAuthServer(server, service)
			etcdserverpb.RegisterWatchServer(server, service)
			go server.Serve(listener)
			defer server.Stop()

			cli, err := clientv3.New(clientv3.Config{
				Endpoints: []string{"http://etcd-auth-failure"},
				Username:  "test", Password: "test", DialTimeout: time.Second,
				DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				})},
			})
			require.NoError(t, err)
			defer cli.Close()

			var resp clientv3.WatchResponse
			select {
			case resp = <-cli.Watch(ctx, "key"):
			case <-ctx.Done():
				t.Fatal("watch did not report the authentication failure")
			}
			require.True(t, resp.Canceled)
			require.Error(t, resp.Err())
			require.True(t, IsRetriableWatchErr(resp.Err()), "watch canceled with %v", resp.Err())
		})
	}
}
