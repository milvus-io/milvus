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

package grpcproxy

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const streamBufSize = 1024 * 1024

// streamAuthTestServer implements the MilvusService stream endpoint needed by
// the wiring test.
type streamAuthTestServer struct {
	milvuspb.UnimplementedMilvusServiceServer
}

func (s *streamAuthTestServer) DumpMessages(*milvuspb.DumpMessagesRequest, milvuspb.MilvusService_DumpMessagesServer) error {
	return nil
}

// TestExternalStreamInterceptor_Wiring verifies that the stream interceptor
// chain built by newStreamInterceptorOption is actually installed on the
// external server and enforces authentication at the gRPC boundary. A
// regression that drops this chain would silently reopen the stream auth
// bypass (#52387).
func TestExternalStreamInterceptor_Wiring(t *testing.T) {
	paramtable.Init()
	proxy.Params.Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer proxy.Params.Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)

	// A non-nil global meta cache satisfies the auth readiness gate; the
	// unauthenticated path below is rejected on the missing authorization
	// header, before any credential lookup.
	proxy.InitEmptyGlobalCache()
	opt := newStreamInterceptorOption()

	lis := bufconn.Listen(streamBufSize)
	srv := grpc.NewServer(opt)
	milvuspb.RegisterMilvusServiceServer(srv, &streamAuthTestServer{})
	t.Cleanup(srv.Stop)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	client := milvuspb.NewMilvusServiceClient(conn)

	t.Run("unauthenticated DumpMessages is rejected", func(t *testing.T) {
		stream, err := client.DumpMessages(context.Background(), &milvuspb.DumpMessagesRequest{Pchannel: "ch"})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.Error(t, err)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("authorization-disabled stream reaches handler", func(t *testing.T) {
		proxy.Params.Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "false")
		defer proxy.Params.Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)

		// With authorization disabled both the auth and RBAC interceptors
		// pass through, so the request must reach the handler. This guards
		// against a chain that is too strict or mis-ordered.
		stream, err := client.DumpMessages(context.Background(), &milvuspb.DumpMessagesRequest{Pchannel: "ch"})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.Equal(t, io.EOF, err)
	})
}
