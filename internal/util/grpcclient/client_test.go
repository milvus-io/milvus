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
	"bytes"
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
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
		isNode:         true,
	}
	base.SetGetAddrFunc(func() (string, error) {
		return "", errors.New("mocked address error")
	})
	base.role = typeutil.QueryNodeRole
	mockSession := sessionutil.NewMockSession(t)
	mockSession.EXPECT().GetSessions(mock.Anything, mock.Anything).Return(nil, 0, nil)
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
	base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}
	base.grpcClientMtx.Unlock()
	_, err = base.Call(ctx, func(client *mockClient) (any, error) {
		return struct{}{}, status.Error(codes.Unknown, merr.ErrNodeNotMatch.Error())
	})
	assert.True(t, IsServerIDMismatchErr(err))

	// test querynode/datanode/indexnode/proxy down, return unavailable error
	base.grpcClientMtx.Lock()
	base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}
	base.grpcClientMtx.Unlock()
	_, err = base.Call(ctx, func(client *mockClient) (any, error) {
		return struct{}{}, status.Error(codes.Unavailable, "fake error")
	})
	assert.True(t, errors.Is(err, merr.ErrNodeNotFound))
}

func TestClientBase_Call(t *testing.T) {
	testCall(t)
}

func testCall(t *testing.T) {
	// mock client with nothing
	base := ClientBase[*mockClient]{
		maxCancelError: 10,
		MaxAttempts:    3,
		isNode:         true,
	}
	initClient := func() {
		base.grpcClientMtx.Lock()
		base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}
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

	t.Run("Call with stream method", func(t *testing.T) {
		initClient()
		_, err := base.Call(context.Background(), func(client *mockClient) (any, error) {
			return streamrpc.NewMockClientStream(t), nil
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
		base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}
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
	base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}
	base.MaxAttempts = 1

	ctx := context.Background()
	retry, reset, forceReset, _ := base.checkGrpcErr(ctx, status.Error(codes.Canceled, "fake context canceled"))
	assert.True(t, retry)
	assert.True(t, reset)
	assert.False(t, forceReset)

	retry, reset, forceReset, _ = base.checkGrpcErr(ctx, status.Error(codes.Unimplemented, "fake context canceled"))
	assert.False(t, retry)
	assert.True(t, reset)
	assert.True(t, forceReset)

	retry, reset, forceReset, _ = base.checkGrpcErr(ctx, status.Error(codes.Unavailable, "fake context canceled"))
	assert.True(t, retry)
	assert.True(t, reset)
	assert.True(t, forceReset)

	// test serverId mismatch (coord connection, isNode=false: should retry)
	retry, reset, forceReset, _ = base.checkGrpcErr(ctx, status.Error(codes.Unknown, merr.ErrNodeNotMatch.Error()))
	assert.True(t, retry)
	assert.True(t, reset)
	assert.True(t, forceReset)

	// test cross cluster
	retry, reset, forceReset, _ = base.checkGrpcErr(ctx, status.Error(codes.Unknown, merr.ErrServiceCrossClusterRouting.Error()))
	assert.True(t, retry)
	assert.True(t, reset)
	assert.True(t, forceReset)

	// test default
	retry, reset, forceReset, _ = base.checkGrpcErr(ctx, status.Error(codes.Unknown, merr.ErrNodeNotFound.Error()))
	assert.True(t, retry)
	assert.True(t, reset)
	assert.False(t, forceReset)
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
	return nil, status.Error(codes.Unavailable, "server: fail it")
}

func TestClientBase_RetryPolicy(t *testing.T) {
	// server
	lis, err := net.Listen("tcp", "localhost:")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	address := lis.Addr()
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
		// s.Stop() causes Serve to return; ignore the error
		s.Serve(lis)
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

// mockCompressionServer echoes the request payload back, so a compressed RPC
// carries real bytes in both directions.
type mockCompressionServer struct {
	rootcoordpb.UnimplementedRootCoordServer
}

func (s *mockCompressionServer) CreateCollection(_ context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{Reason: string(in.GetSchema())}, nil
}

// inboundEncoding records the grpc-encoding the server saw on each request.
type inboundEncoding struct{ seen chan string }

func (o *inboundEncoding) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (o *inboundEncoding) HandleRPC(_ context.Context, event stats.RPCStats) {
	if h, ok := event.(*stats.InHeader); ok {
		select {
		case o.seen <- h.Compression:
		default:
		}
	}
}

func (o *inboundEncoding) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (o *inboundEncoding) HandleConn(context.Context, stats.ConnStats) {}

// Exercise a real compressed RPC end to end for every algorithm. Both
// directions carry a payload on purpose: grpc's compress() returns early on
// `in.Len() == 0`, so an RPC with empty request and response never reaches a
// codec at all and would pass with the whole compression path broken.
func TestClientBase_Compression(t *testing.T) {
	lis, err := net.Listen("tcp", "localhost:")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	address := lis.Addr()

	observed := &inboundEncoding{seen: make(chan string, 8)}
	s := grpc.NewServer(
		grpc.StatsHandler(observed),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    60 * time.Second,
			Timeout: 60 * time.Second,
		}),
	)
	rootcoordpb.RegisterRootCoordServer(s, &mockCompressionServer{})
	reflection.Register(s)
	go func() {
		// s.Stop() causes Serve to return; ignore the error
		s.Serve(lis)
	}()
	defer s.Stop()

	// Compressible, and well past the 128KB block size of every codec here, so
	// the payload spans several blocks rather than a single one.
	payload := bytes.Repeat([]byte("milvus grpc compression payload "), (512<<10)/32)

	for _, tc := range []struct {
		name      string
		enabled   bool
		algorithm string
		wire      string // grpc-encoding the server must see
	}{
		{name: "disabled", enabled: false, algorithm: Zstd, wire: ""},
		{name: Zstd, enabled: true, algorithm: Zstd, wire: Zstd},
		{name: Snappy, enabled: true, algorithm: Snappy, wire: Snappy},
		{name: S2, enabled: true, algorithm: S2, wire: S2},
		// an algorithm no build registers must degrade to zstd rather than
		// failing every RPC on the connection
		{name: "unknown falls back to zstd", enabled: true, algorithm: "brotli", wire: Zstd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientBase := ClientBase[rootcoordpb.RootCoordClient]{
				ClientMaxRecvSize:      8 * 1024 * 1024,
				ClientMaxSendSize:      8 * 1024 * 1024,
				DialTimeout:            60 * time.Second,
				KeepAliveTime:          60 * time.Second,
				KeepAliveTimeout:       60 * time.Second,
				RetryServiceNameConfig: "milvus.proto.rootcoord.RootCoord",
				MaxAttempts:            1,
				InitialBackoff:         10.0,
				MaxBackoff:             60.0,
				CompressionEnabled:     tc.enabled,
				CompressionAlgorithm:   tc.algorithm,
			}
			clientBase.SetRole(typeutil.DataCoordRole)
			clientBase.SetGetAddrFunc(func() (string, error) { return address.String(), nil })
			clientBase.SetNewGrpcClientFunc(func(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
				return rootcoordpb.NewRootCoordClient(cc)
			})
			defer clientBase.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			for len(observed.seen) > 0 { // drop anything an earlier subtest left
				<-observed.seen
			}

			res, err := clientBase.Call(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
				return client.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{Schema: payload})
			})
			assert.NoError(t, err)
			// The echo proves the request was decompressed by the server and
			// the response decompressed by the client, both intact.
			assert.Equal(t, string(payload), res.(*commonpb.Status).GetReason())

			select {
			case got := <-observed.seen:
				assert.Equal(t, tc.wire, got, "grpc-encoding on the wire")
			case <-ctx.Done():
				t.Fatal("server never reported a request header")
			}
		})
	}
}

func TestVerifySession(t *testing.T) {
	base := ClientBase[*mockClient]{}
	mockSession := sessionutil.NewMockSession(t)
	expectedErr := errors.New("mocked")
	mockSession.EXPECT().GetSessions(mock.Anything, mock.Anything).Return(nil, 0, expectedErr)
	base.sess = mockSession

	ctx := context.Background()
	err := base.verifySession(ctx)
	assert.ErrorIs(t, err, expectedErr)

	base.lastSessionCheck.Store(time.Unix(0, 0))
	base.NodeID = *atomic.NewInt64(1)
	base.role = typeutil.RootCoordRole
	mockSession2 := sessionutil.NewMockSession(t)
	mockSession2.EXPECT().GetSessions(mock.Anything, mock.Anything).Return(
		map[string]*sessionutil.Session{
			typeutil.RootCoordRole: {
				SessionRaw: sessionutil.SessionRaw{
					ServerID: 1,
				},
			},
		},
		0,
		nil,
	)
	base.sess = mockSession2
	err = base.verifySession(ctx)
	assert.NoError(t, err)

	base.lastSessionCheck.Store(time.Unix(0, 0))
	base.NodeID = *atomic.NewInt64(2)
	err = base.verifySession(ctx)
	assert.ErrorIs(t, err, merr.ErrNodeNotMatch)

	base.lastSessionCheck.Store(time.Unix(0, 0))
	base.NodeID = *atomic.NewInt64(1)
	base.role = typeutil.QueryNodeRole
	err = base.verifySession(ctx)
	assert.ErrorIs(t, err, merr.ErrNodeNotFound)
}

func TestClientBase_CheckGrpcError_ServerIDMismatch_Node(t *testing.T) {
	base := ClientBase[*mockClient]{
		isNode: true,
	}
	base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}

	ctx := context.Background()

	// Node connection: ServerIDMismatch should fast-fail (no retry) with reset+forceReset
	retry, reset, forceReset, err := base.checkGrpcErr(ctx, status.Error(codes.Unknown, merr.ErrNodeNotMatch.Error()))
	assert.False(t, retry)
	assert.True(t, reset)
	assert.True(t, forceReset)
	assert.True(t, IsServerIDMismatchErr(err))
}

func TestClientBase_ServerIDMismatch_NodeFastFail(t *testing.T) {
	// Test the full Call() path: ServerIDMismatch on a node connection
	// should fail immediately without retrying.
	callCount := 0
	base := ClientBase[*mockClient]{
		maxCancelError: 10,
		MaxAttempts:    3,
		isNode:         true,
	}
	base.SetGetAddrFunc(func() (string, error) {
		return "", errors.New("mocked address error")
	})
	base.role = typeutil.QueryNodeRole
	mockSession := sessionutil.NewMockSession(t)
	base.sess = mockSession
	base.grpcClientMtx.Lock()
	base.grpcClient = &clientConnWrapper[*mockClient]{client: &mockClient{}}
	base.grpcClientMtx.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := base.Call(ctx, func(client *mockClient) (any, error) {
		callCount++
		return struct{}{}, status.Error(codes.Unknown, merr.ErrNodeNotMatch.Error())
	})
	assert.True(t, IsServerIDMismatchErr(err))
	// The caller should be invoked exactly once (no retries)
	assert.Equal(t, 1, callCount)
}

func TestIsConnectionClosingErr(t *testing.T) {
	// Positive case — the exact exported sentinel
	assert.True(t, IsConnectionClosingErr(grpc.ErrClientConnClosing))

	// Positive case — wrapped sentinel still matches via errors.Is
	err := errors.Wrap(grpc.ErrClientConnClosing, "outer context")
	assert.True(t, IsConnectionClosingErr(err))

	// Positive case — status with same code and message (proto.Equal match)
	err = status.Error(codes.Canceled, "grpc: the client connection is closing")
	assert.True(t, IsConnectionClosingErr(err))

	// Negative — normal canceled
	err = status.Error(codes.Canceled, "context canceled")
	assert.False(t, IsConnectionClosingErr(err))

	// Negative — non-grpc error
	err = errors.New("random error")
	assert.False(t, IsConnectionClosingErr(err))
}
