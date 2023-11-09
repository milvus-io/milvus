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
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	os.Exit(m.Run())
}

type mockClient struct{}

func (c *mockClient) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{}, nil
}

func TestClientBase_SetRole(t *testing.T) {
	base := ClientBase[*mockClient]{}
	expect := "abc"
	base.SetRole("abc")
	assert.Equal(t, expect, base.GetRole())
}

func TestClientBase_GetRole(t *testing.T) {
	base := ClientBase[*mockClient]{}
	assert.Equal(t, "", base.GetRole())
}

func TestClientBase_connect(t *testing.T) {
	t.Run("failed to connect", func(t *testing.T) {
		base := ClientBase[*mockClient]{
			getAddrFunc: func() (string, error) {
				return "", nil
			},
			DialTimeout: time.Millisecond,
		}
		err := base.connect(context.Background())
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrConnect))
	})

	t.Run("failed to get addr", func(t *testing.T) {
		errMock := errors.New("mocked")
		base := ClientBase[*mockClient]{
			getAddrFunc: func() (string, error) {
				return "", errMock
			},
			DialTimeout: time.Millisecond,
		}
		err := base.connect(context.Background())
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errMock))
	})
}

func TestClientBase_NodeSessionNotExist(t *testing.T) {
	base := ClientBase[*mockClient]{
		maxCancelError: 10,
		MaxAttempts:    3,
	}
	base.SetGetAddrFunc(func() (string, error) {
		return "", errors.New("mocked address error")
	})
	base.role = typeutil.QueryNodeRole
	mockSession := sessionutil.NewMockSession(t)
	mockSession.EXPECT().GetSessions(mock.Anything).Return(nil, 0, nil)
	base.sess = mockSession
	base.grpcClientMtx.Lock()
	base.grpcClient = nil
	base.grpcClientMtx.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := base.Call(ctx, func(client *mockClient) (any, error) {
		return struct{}{}, nil
	})
	assert.True(t, errors.Is(err, merr.ErrNodeNotFound))

	// test querynode/datanode/indexnode/proxy already down, but new node start up with same ip and port
	base.grpcClientMtx.Lock()
	base.grpcClient = &mockClient{}
	base.grpcClientMtx.Unlock()
	_, err = base.Call(ctx, func(client *mockClient) (any, error) {
		return struct{}{}, status.Errorf(codes.Unknown, merr.ErrNodeNotMatch.Error())
	})
	assert.True(t, errors.Is(err, merr.ErrNodeNotFound))

	// test querynode/datanode/indexnode/proxy down, return unavailable error
	base.grpcClientMtx.Lock()
	base.grpcClient = &mockClient{}
	base.grpcClientMtx.Unlock()
	_, err = base.Call(ctx, func(client *mockClient) (any, error) {
		return struct{}{}, status.Errorf(codes.Unavailable, "fake error")
	})
	assert.True(t, errors.Is(err, merr.ErrNodeNotFound))
}

func TestClientBase_Call(t *testing.T) {
	testCall(t, false)
}

func TestClientBase_CompressCall(t *testing.T) {
	testCall(t, true)
}

func testCall(t *testing.T, compressed bool) {
	// mock client with nothing
	base := ClientBase[*mockClient]{
		maxCancelError: 10,
		MaxAttempts:    3,
	}
	base.CompressionEnabled = compressed
	initClient := func() {
		base.grpcClientMtx.Lock()
		base.grpcClient = &mockClient{}
		base.grpcClientMtx.Unlock()
	}
	base.MaxAttempts = 1
	base.SetGetAddrFunc(func() (string, error) {
		return "", errors.New("mocked address error")
	})

	t.Run("Call normal return", func(t *testing.T) {
		initClient()
		_, err := base.Call(context.Background(), func(client *mockClient) (any, error) {
			return struct{}{}, nil
		})
		assert.NoError(t, err)
	})

	t.Run("Call with canceled context", func(t *testing.T) {
		initClient()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := base.Call(ctx, func(client *mockClient) (any, error) {
			return struct{}{}, nil
		})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("Call canceled in caller func", func(t *testing.T) {
		initClient()
		ctx, cancel := context.WithCancel(context.Background())
		errMock := errors.New("mocked")
		_, err := base.Call(ctx, func(client *mockClient) (any, error) {
			cancel()
			return nil, errMock
		})

		assert.Error(t, err)
		assert.True(t, errors.Is(err, errMock))
		base.grpcClientMtx.RLock()
		// client shall not be reset
		assert.NotNil(t, base.grpcClient)
		base.grpcClientMtx.RUnlock()
	})

	t.Run("Call returns non-grpc error", func(t *testing.T) {
		initClient()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errMock := errors.New("mocked")
		_, err := base.Call(ctx, func(client *mockClient) (any, error) {
			return nil, errMock
		})

		assert.Error(t, err)
		assert.True(t, errors.Is(err, errMock))
		base.grpcClientMtx.RLock()
		// client shall not be reset
		assert.NotNil(t, base.grpcClient)
		base.grpcClientMtx.RUnlock()
	})

	t.Run("Call returns Unavailable grpc error", func(t *testing.T) {
		initClient()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errGrpc := status.Error(codes.Unavailable, "mocked")
		_, err := base.Call(ctx, func(client *mockClient) (any, error) {
			return nil, errGrpc
		})

		assert.Error(t, err)
		assert.True(t, errors.Is(err, errGrpc))
		base.grpcClientMtx.RLock()
		// client shall not be reset
		assert.Nil(t, base.grpcClient)
		base.grpcClientMtx.RUnlock()
	})

	t.Run("Call returns canceled grpc error within limit", func(t *testing.T) {
		initClient()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		defer func() {
			base.ctxCounter.Store(0)
		}()
		errGrpc := status.Error(codes.Canceled, "mocked")
		_, err := base.Call(ctx, func(client *mockClient) (any, error) {
			return nil, errGrpc
		})

		assert.Error(t, err)
		assert.True(t, errors.Is(err, errGrpc))
		base.grpcClientMtx.RLock()
		// client shall not be reset
		assert.NotNil(t, base.grpcClient)
		base.grpcClientMtx.RUnlock()
	})
	t.Run("Call returns canceled grpc error exceed limit", func(t *testing.T) {
		initClient()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		base.ctxCounter.Store(10)
		defer func() {
			base.ctxCounter.Store(0)
		}()
		errGrpc := status.Error(codes.Canceled, "mocked")
		_, err := base.Call(ctx, func(client *mockClient) (any, error) {
			return nil, errGrpc
		})

		assert.Error(t, err)
		assert.True(t, errors.Is(err, errGrpc))
		base.grpcClientMtx.RLock()
		// client shall not be reset
		assert.Nil(t, base.grpcClient)
		base.grpcClientMtx.RUnlock()
	})

	base.grpcClientMtx.Lock()
	base.grpcClient = nil
	base.grpcClientMtx.Unlock()
	base.SetGetAddrFunc(func() (string, error) { return "", nil })

	t.Run("Call with connect failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := base.Call(ctx, func(client *mockClient) (any, error) {
			return struct{}{}, nil
		})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrConnect))
	})
}

func TestClientBase_Recall(t *testing.T) {
	// mock client with nothing
	base := ClientBase[*mockClient]{}
	initClient := func() {
		base.grpcClientMtx.Lock()
		base.grpcClient = &mockClient{}
		base.grpcClientMtx.Unlock()
	}
	base.MaxAttempts = 1
	base.SetGetAddrFunc(func() (string, error) {
		return "", errors.New("mocked address error")
	})

	t.Run("Recall normal return", func(t *testing.T) {
		initClient()
		_, err := base.ReCall(context.Background(), func(client *mockClient) (any, error) {
			return struct{}{}, nil
		})
		assert.NoError(t, err)
	})

	t.Run("ReCall with canceled context", func(t *testing.T) {
		initClient()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := base.ReCall(ctx, func(client *mockClient) (any, error) {
			return struct{}{}, nil
		})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("ReCall canceled in caller func", func(t *testing.T) {
		initClient()
		ctx, cancel := context.WithCancel(context.Background())
		errMock := errors.New("mocked")
		_, err := base.ReCall(ctx, func(client *mockClient) (any, error) {
			cancel()
			return nil, errMock
		})

		assert.Error(t, err)
		assert.True(t, errors.Is(err, errMock))
		base.grpcClientMtx.RLock()
		// client shall not be reset
		assert.NotNil(t, base.grpcClient)
		base.grpcClientMtx.RUnlock()
	})

	base.grpcClientMtx.Lock()
	base.grpcClient = nil
	base.grpcClientMtx.Unlock()
	base.SetGetAddrFunc(func() (string, error) { return "", nil })

	t.Run("ReCall with connect failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := base.ReCall(ctx, func(client *mockClient) (any, error) {
			return struct{}{}, nil
		})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrConnect))
	})
}

func TestClientBase_CheckGrpcError(t *testing.T) {
	base := ClientBase[*mockClient]{}
	base.grpcClient = &mockClient{}
	base.MaxAttempts = 1

	ctx := context.Background()
	retry, reset, _ := base.checkGrpcErr(ctx, status.Errorf(codes.Canceled, "fake context canceled"))
	assert.True(t, retry)
	assert.True(t, reset)

	retry, reset, _ = base.checkGrpcErr(ctx, status.Errorf(codes.Unimplemented, "fake context canceled"))
	assert.False(t, retry)
	assert.False(t, reset)

	// test serverId mismatch
	retry, reset, _ = base.checkGrpcErr(ctx, status.Errorf(codes.Unknown, merr.ErrNodeNotMatch.Error()))
	assert.True(t, retry)
	assert.True(t, reset)

	// test cross cluster
	retry, reset, _ = base.checkGrpcErr(ctx, status.Errorf(codes.Unknown, merr.ErrServiceCrossClusterRouting.Error()))
	assert.True(t, retry)
	assert.True(t, reset)

	// test default
	retry, reset, _ = base.checkGrpcErr(ctx, status.Errorf(codes.Unknown, merr.ErrNodeNotFound.Error()))
	assert.True(t, retry)
	assert.True(t, reset)
}

type server struct {
	helloworld.UnimplementedGreeterServer
	reqCounter   uint
	SuccessCount uint
}

func (s *server) SayHello(ctx context.Context, in *helloworld.HelloRequest) (*helloworld.HelloReply, error) {
	log.Printf("Received: %s", in.Name)
	s.reqCounter++
	if s.reqCounter%s.SuccessCount == 0 {
		log.Printf("success %d", s.reqCounter)
		return &helloworld.HelloReply{Message: strings.ToUpper(in.Name)}, nil
	}
	return nil, status.Errorf(codes.Unavailable, "server: fail it")
}

func TestClientBase_RetryPolicy(t *testing.T) {
	// server
	lis, err := net.Listen("tcp", "localhost:")
	address := lis.Addr()
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}
	kasp := keepalive.ServerParameters{
		Time:    60 * time.Second,
		Timeout: 60 * time.Second,
	}

	maxAttempts := 1
	s := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
	)
	helloworld.RegisterGreeterServer(s, &server{SuccessCount: uint(maxAttempts)})
	reflection.Register(s)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	defer s.Stop()

	clientBase := ClientBase[rootcoordpb.RootCoordClient]{
		ClientMaxRecvSize:      1 * 1024 * 1024,
		ClientMaxSendSize:      1 * 1024 * 1024,
		DialTimeout:            60 * time.Second,
		KeepAliveTime:          60 * time.Second,
		KeepAliveTimeout:       60 * time.Second,
		RetryServiceNameConfig: "rootcoordpb.GetComponentStates",
		MaxAttempts:            maxAttempts,
		InitialBackoff:         10.0,
		MaxBackoff:             60.0,
	}
	clientBase.SetRole(typeutil.DataCoordRole)
	clientBase.SetGetAddrFunc(func() (string, error) {
		return address.String(), nil
	})
	clientBase.SetNewGrpcClientFunc(func(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
		return rootcoordpb.NewRootCoordClient(cc)
	})
	defer clientBase.Close()

	ctx := context.Background()
	randID := rand.Int63()
	res, err := clientBase.Call(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		return &milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				NodeID: randID,
			},
		}, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, res.(*milvuspb.ComponentStates).GetState().GetNodeID(), randID)
}

func TestClientBase_Compression(t *testing.T) {
	lis, err := net.Listen("tcp", "localhost:")
	address := lis.Addr()
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}
	kasp := keepalive.ServerParameters{
		Time:    60 * time.Second,
		Timeout: 60 * time.Second,
	}

	maxAttempts := 1
	s := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
	)
	helloworld.RegisterGreeterServer(s, &server{SuccessCount: uint(1)})
	reflection.Register(s)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	defer s.Stop()

	clientBase := ClientBase[rootcoordpb.RootCoordClient]{
		ClientMaxRecvSize:      1 * 1024 * 1024,
		ClientMaxSendSize:      1 * 1024 * 1024,
		DialTimeout:            60 * time.Second,
		KeepAliveTime:          60 * time.Second,
		KeepAliveTimeout:       60 * time.Second,
		RetryServiceNameConfig: "helloworld.Greeter",
		MaxAttempts:            maxAttempts,
		InitialBackoff:         10.0,
		MaxBackoff:             60.0,
		CompressionEnabled:     true,
	}
	clientBase.SetRole(typeutil.DataCoordRole)
	clientBase.SetGetAddrFunc(func() (string, error) {
		return address.String(), nil
	})
	clientBase.SetNewGrpcClientFunc(func(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
		return rootcoordpb.NewRootCoordClient(cc)
	})
	defer clientBase.Close()

	ctx := context.Background()
	randID := rand.Int63()
	res, err := clientBase.Call(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		return &milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				NodeID: randID,
			},
			Status: merr.Success(),
		}, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, res.(*milvuspb.ComponentStates).GetState().GetNodeID(), randID)
}
