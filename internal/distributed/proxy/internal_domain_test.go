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
	"fmt"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// setInternalDomainPorts configures both internal-domain ports for the test
// and restores the defaults afterwards.
func setInternalDomainPorts(t *testing.T, grpcPort, httpPort int) {
	t.Helper()
	paramtable.Init()
	cfg := &paramtable.Get().ProxyCfg
	paramtable.Get().Save(cfg.InternalDomainGrpcPort.Key, strconv.Itoa(grpcPort))
	paramtable.Get().Save(cfg.InternalDomainHTTPPort.Key, strconv.Itoa(httpPort))
	t.Cleanup(func() {
		paramtable.Get().Reset(cfg.InternalDomainGrpcPort.Key)
		paramtable.Get().Reset(cfg.InternalDomainHTTPPort.Key)
	})
}

func TestInternalDomainPortsAreClosedByDefault(t *testing.T) {
	paramtable.Init()
	grpcPort, httpPort := internalDomainListeners()
	assert.Zero(t, grpcPort)
	assert.Zero(t, httpPort)

	// With both ports closed, starting the servers opens nothing.
	s := &Server{ctx: context.Background()}
	require.NoError(t, s.startInternalDomainServers())
	assert.Nil(t, s.internalDomainGrpcServer)
	assert.Nil(t, s.internalDomainHTTPServer)
	s.stopInternalDomainServers()
}

func TestInternalDomainPortsFollowTheConfiguration(t *testing.T) {
	setInternalDomainPorts(t, 26330, 0)
	grpcPort, httpPort := internalDomainListeners()
	assert.Equal(t, 26330, grpcPort)
	assert.Zero(t, httpPort)
}

func TestInternalDomainGrpcInterceptorMarksTheContext(t *testing.T) {
	var sawMark bool
	_, err := internalDomainMarkInterceptor(context.Background(), nil, nil,
		func(ctx context.Context, _ any) (any, error) {
			sawMark = extension.FromInternalDomain(ctx)
			return nil, nil
		})
	assert.NoError(t, err)
	assert.True(t, sawMark, "the handler must see the internal-domain mark")
}

func TestInternalDomainStreamInterceptorMarksTheContext(t *testing.T) {
	var sawMark bool
	err := internalDomainMarkStreamInterceptor(nil, plainServerStream{ctx: context.Background()}, nil,
		func(_ any, ss grpc.ServerStream) error {
			sawMark = extension.FromInternalDomain(ss.Context())
			return nil
		})
	assert.NoError(t, err)
	assert.True(t, sawMark)
}

type plainServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s plainServerStream) Context() context.Context { return s.ctx }

func TestInternalDomainListenersServe(t *testing.T) {
	paramtable.Init()
	accesslog.InitAccessLogger(paramtable.Get())
	mockProxy := mocks.NewMockProxy(t)
	mockProxy.EXPECT().ListCredUsers(mock.Anything, mock.Anything).
		Return(&milvuspb.ListCredUsersResponse{Status: merr.Success()}, nil).Maybe()

	grpcPort, httpPort := freeTestPort(t), freeTestPort(t)
	setInternalDomainPorts(t, grpcPort, httpPort)
	s := &Server{ctx: context.Background(), proxy: mockProxy}
	require.NoError(t, s.startInternalDomainServers())
	t.Cleanup(s.stopInternalDomainServers)

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := milvuspb.NewMilvusServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.ListCredUsers(ctx, &milvuspb.ListCredUsersRequest{})
	require.NoError(t, err, "the internal gRPC listener must answer the full MilvusService")

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", httpPort))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "the internal REST listener must serve /metrics")
}

func freeTestPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

func TestInternalDomainStartFailureIsAMerrError(t *testing.T) {
	occupied, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupied.Close() })
	port := occupied.Addr().(*net.TCPAddr).Port

	t.Run("grpc", func(t *testing.T) {
		setInternalDomainPorts(t, port, 0)
		s := &Server{ctx: context.Background()}
		err := s.startInternalDomainServers()
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrServiceInternal,
			"a failed internal-domain listener must be classified, not raw")
	})
	t.Run("rest", func(t *testing.T) {
		setInternalDomainPorts(t, 0, port)
		s := &Server{ctx: context.Background()}
		err := s.startInternalDomainServers()
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})
}

func TestInternalDomainGrpcTLSFailureIsAMerrError(t *testing.T) {
	paramtable.Init()
	params := &paramtable.Get().ProxyGrpcServerCfg
	paramtable.Get().Save(params.TLSMode.Key, "1")
	paramtable.Get().Save(params.ServerPemPath.Key, "/nonexistent/server.pem")
	paramtable.Get().Save(params.ServerKeyPath.Key, "/nonexistent/server.key")
	t.Cleanup(func() {
		paramtable.Get().Reset(params.TLSMode.Key)
		paramtable.Get().Reset(params.ServerPemPath.Key)
		paramtable.Get().Reset(params.ServerKeyPath.Key)
	})
	s := &Server{ctx: context.Background()}
	err := s.startInternalDomainGrpc(freeTestPort(t))
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestInternalDomainRestServerCarriesTheHTTPTimeouts(t *testing.T) {
	paramtable.Init()
	accesslog.InitAccessLogger(paramtable.Get())
	mockProxy := mocks.NewMockProxy(t)
	s := &Server{ctx: context.Background(), proxy: mockProxy}
	require.NoError(t, s.startInternalDomainRest(freeTestPort(t)))
	t.Cleanup(func() { _ = s.internalDomainHTTPServer.Close() })
	assert.Equal(t, 5*time.Second, s.internalDomainHTTPServer.ReadHeaderTimeout,
		"the header-read timeout is what closes the Slowloris window")
	assert.Equal(t, 300*time.Second, s.internalDomainHTTPServer.IdleTimeout)
	assert.Zero(t, s.internalDomainHTTPServer.MaxHeaderBytes,
		"unset, so net/http applies its 1MiB default; the external listener's 16MiB "+
			"is an HTTP/2 shared-port concession that does not apply to this socket")
}
