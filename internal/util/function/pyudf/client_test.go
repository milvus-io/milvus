// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pyudf

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/proto/pyudfpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type clientTestServer struct {
	pyudfpb.UnimplementedPyUDFWorkerServer
	execute func(context.Context, *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error)
}

func (s *clientTestServer) Execute(ctx context.Context, req *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
	return s.execute(ctx, req)
}

func startClientTestServer(t *testing.T, execute func(context.Context, *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error)) (Config, *grpc.Server) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := defaultTestConfig()
	cfg.Address = listener.Addr().String()
	server := grpc.NewServer(grpc.MaxRecvMsgSize(cfg.MaxMessageBytes), grpc.MaxSendMsgSize(cfg.MaxMessageBytes))
	pyudfpb.RegisterPyUDFWorkerServer(server, &clientTestServer{execute: execute})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return cfg, server
}

func clientInput(t *testing.T, counts ...int) []*arrow.Chunked {
	t.Helper()
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { allocator.AssertSize(t, 0) })
	var chunks []arrow.Array
	for _, count := range counts {
		builder := array.NewFloat64Builder(allocator)
		for i := 0; i < count; i++ {
			builder.Append(float64(i))
		}
		chunks = append(chunks, builder.NewArray())
		builder.Release()
	}
	column := arrow.NewChunked(arrow.PrimitiveTypes.Float64, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}
	t.Cleanup(column.Release)
	return []*arrow.Chunked{column}
}

func clientRequest(t *testing.T) ExecuteRequest {
	return ExecuteRequest{ResourceName: "rank", UDFPath: "/tmp/rank.whl", Stage: " arbitrary stage ", Inputs: clientInput(t, 0, 2)}
}

func newTestClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	c, err := NewClient(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, CloseClients()) })
	return c
}

func echoExecute(ctx context.Context, req *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
	columns, err := decodeOutputs(ctx, req.Inputs)
	if err != nil {
		return nil, err
	}
	defer releaseColumns(columns)
	inputs := columns
	if len(req.InputColumnIndices) > 0 {
		inputs = make([]*arrow.Chunked, len(req.InputColumnIndices))
		for i, index := range req.InputColumnIndices {
			inputs[i] = columns[index]
		}
	}
	payload, err := encodeInputs(ctx, inputs)
	if err != nil {
		return nil, err
	}
	return &pyudfpb.ExecuteResponse{Result: &pyudfpb.ExecuteResponse_Outputs{Outputs: payload}}, nil
}

func TestClientConnectionsAndConcurrency(t *testing.T) {
	for _, count := range []int{1, 3} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			const calls = 12
			entered := make(chan string, calls)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			cfg, _ := startClientTestServer(t, func(ctx context.Context, req *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
				p, _ := peer.FromContext(ctx)
				entered <- p.Addr.String()
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return echoExecute(ctx, req)
			})
			cfg.ConnectionPoolSize = count
			c := newTestClient(t, cfg)
			other := newTestClient(t, cfg)
			require.Same(t, c, other)
			request := clientRequest(t)
			results := make(chan error, calls)
			for i := 0; i < calls; i++ {
				go func() { out, err := c.Execute(context.Background(), request); releaseColumns(out); results <- err }()
			}
			peers := make(map[string]int)
			for i := 0; i < calls; i++ {
				select {
				case addr := <-entered:
					peers[addr]++
				case <-time.After(5 * time.Second):
					t.Fatal("client queued concurrent calls")
				}
			}
			require.Len(t, peers, count)
			for _, n := range peers {
				assert.Equal(t, calls/count, n)
			}
			unblock()
			for i := 0; i < calls; i++ {
				require.NoError(t, <-results)
			}
			out, err := other.Execute(context.Background(), request)
			require.NoError(t, err)
			releaseColumns(out)
			require.NoError(t, CloseClients())
			for _, conn := range c.conns {
				assert.Equal(t, connectivity.Shutdown, conn.GetState())
			}
			_, err = c.Execute(context.Background(), request)
			assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
		})
	}
}

func TestClientConnectionRollback(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Address = "127.0.0.1:19998"
	var opened *grpc.ClientConn
	cause := errors.New("dial construction failed")
	_, err := newClient(cfg, func(target string, options ...grpc.DialOption) (*grpc.ClientConn, error) {
		if opened != nil {
			return nil, cause
		}
		var err error
		opened, err = grpc.NewClient(target, options...)
		return opened, err
	})
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, connectivity.Shutdown, opened.GetState())
	c := newTestClient(t, cfg)
	require.Len(t, c.conns, cfg.ConnectionPoolSize)
}

func TestClientFailureMapping(t *testing.T) {
	tests := []struct {
		code     pyudfpb.ErrorCode
		expected error
	}{
		{pyudfpb.ErrorCode_INVALID_ARGUMENT, merr.ErrParameterInvalid},
		{pyudfpb.ErrorCode_UDF_FAILED, merr.ErrFunctionFailed},
		{pyudfpb.ErrorCode_RESOURCE_NOT_FOUND, merr.ErrIoKeyNotFound},
		{pyudfpb.ErrorCode_RESOURCE_PERMISSION_DENIED, merr.ErrIoPermissionDenied},
		{pyudfpb.ErrorCode_RESOURCE_IO_FAILED, merr.ErrIoFailed},
		{pyudfpb.ErrorCode_OUT_OF_MEMORY, merr.ErrServiceMemoryLimitExceeded},
		{pyudfpb.ErrorCode_UNSUPPORTED, merr.ErrServiceUnimplemented},
		{pyudfpb.ErrorCode_INTERNAL, merr.ErrServiceInternal},
		{0, merr.ErrServiceInternal},
		{99, merr.ErrServiceInternal},
	}
	for _, test := range tests {
		t.Run(test.code.String(), func(t *testing.T) {
			cfg, _ := startClientTestServer(t, func(context.Context, *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
				return &pyudfpb.ExecuteResponse{Result: &pyudfpb.ExecuteResponse_Error{Error: &pyudfpb.ExecuteError{Code: test.code, Message: "failure 50%"}}}, nil
			})
			out, err := newTestClient(t, cfg).Execute(context.Background(), clientRequest(t))
			require.Nil(t, out)
			require.ErrorIs(t, err, test.expected)
			assert.Equal(t, merr.Code(test.expected), merr.Code(err))
			assert.False(t, merr.Status(err).Retriable)
			assert.Equal(t, test.code == pyudfpb.ErrorCode_INVALID_ARGUMENT, merr.Status(err).ExtraInfo["is_input_error"] == "true")
		})
	}
	for _, test := range []struct {
		code     codes.Code
		expected error
		retry    bool
	}{
		{codes.Unavailable, merr.ErrServiceUnavailable, true},
		{codes.ResourceExhausted, merr.ErrServiceResourceInsufficient, true},
		{codes.Unimplemented, merr.ErrServiceUnimplemented, false},
		{codes.Canceled, context.Canceled, false},
		{codes.DeadlineExceeded, context.DeadlineExceeded, false},
		{codes.Internal, merr.ErrServiceInternal, false},
		{codes.InvalidArgument, merr.ErrServiceInternal, false},
	} {
		t.Run(test.code.String(), func(t *testing.T) {
			cfg, _ := startClientTestServer(t, func(context.Context, *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
				return nil, status.Error(test.code, "failure")
			})
			out, err := newTestClient(t, cfg).Execute(context.Background(), clientRequest(t))
			require.Nil(t, out)
			require.ErrorIs(t, err, test.expected)
			assert.Equal(t, merr.Code(test.expected), merr.Code(err))
			assert.Equal(t, test.retry, merr.Status(err).Retriable)
			assert.Contains(t, err.Error(), test.code.String())
		})
	}
}

func TestClientDeadlineAndNoReplay(t *testing.T) {
	t.Run("timeout_keeps_connection", func(t *testing.T) {
		cfg, _ := startClientTestServer(t, func(ctx context.Context, req *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
			if req.Stage == "block" {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return echoExecute(ctx, req)
		})
		cfg.ConnectionPoolSize = 1
		cfg.RPCTimeout = 5 * time.Second
		c := newTestClient(t, cfg)
		req := clientRequest(t)
		req.Stage = "block"
		_, err := c.Execute(context.Background(), req)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = c.Execute(ctx, req)
		require.ErrorIs(t, err, context.Canceled)
		req.Stage = ""
		out, err := c.Execute(context.Background(), req)
		require.NoError(t, err)
		releaseColumns(out)
	})
	t.Run("disconnect_after_execution", func(t *testing.T) {
		var calls atomic.Int32
		var server *grpc.Server
		cfg, s := startClientTestServer(t, func(ctx context.Context, req *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
			calls.Add(1)
			_ = grpc.SendHeader(ctx, metadata.Pairs("executed", "true"))
			server.Stop()
			return echoExecute(ctx, req)
		})
		server = s
		out, err := newTestClient(t, cfg).Execute(context.Background(), clientRequest(t))
		require.Nil(t, out)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.EqualValues(t, 1, calls.Load())
	})
}

func TestClientReconnect(t *testing.T) {
	cfg, server := startClientTestServer(t, echoExecute)
	cfg.ConnectionPoolSize = 1
	c := newTestClient(t, cfg)
	req := clientRequest(t)
	out, err := c.Execute(context.Background(), req)
	require.NoError(t, err)
	releaseColumns(out)
	conn := c.conns[0]
	server.Stop()
	// A caller observes failure; the client never retries this invocation.
	_, err = c.Execute(context.Background(), req)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Eventually(t, func() bool { return conn.GetState() != connectivity.Ready }, time.Second, time.Millisecond)
	listener, err := net.Listen("tcp", cfg.Address)
	require.NoError(t, err)
	replacement := grpc.NewServer()
	pyudfpb.RegisterPyUDFWorkerServer(replacement, &clientTestServer{execute: echoExecute})
	go func() { _ = replacement.Serve(listener) }()
	t.Cleanup(replacement.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			break
		}
		conn.Connect()
		require.True(t, conn.WaitForStateChange(ctx, state), "connection did not recover")
	}
	out, err = c.Execute(ctx, req)
	require.NoError(t, err)
	releaseColumns(out)
	require.Same(t, conn, c.conns[0])
}

func TestClientSharedConfiguration(t *testing.T) {
	cfg, _ := startClientTestServer(t, echoExecute)
	t.Cleanup(func() { assert.NoError(t, CloseClients()) })
	clients := make(chan *Client, 8)
	failures := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { client, err := NewClient(cfg); clients <- client; failures <- err }()
	}
	c := <-clients
	require.NoError(t, <-failures)
	require.NotNil(t, c)
	for i := 1; i < 8; i++ {
		require.Same(t, c, <-clients)
		require.NoError(t, <-failures)
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.Address = "127.0.0.1:19997" },
		func(c *Config) { c.ConnectionPoolSize++ },
		func(c *Config) { c.MaxMessageBytes *= 2 },
		func(c *Config) { c.RPCTimeout += time.Second },
	} {
		changed := cfg
		change(&changed)
		_, err := NewClient(changed)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
	out, err := c.Execute(context.Background(), clientRequest(t))
	require.NoError(t, err)
	releaseColumns(out)
	require.NoError(t, CloseClients())
	require.NoError(t, CloseClients())
	replacement := newTestClient(t, cfg)
	require.NotSame(t, c, replacement)
	out, err = replacement.Execute(context.Background(), clientRequest(t))
	require.NoError(t, err)
	releaseColumns(out)
}

func TestClientLargeIPC(t *testing.T) {
	for _, kind := range []string{"large_message", "wide_columns", "many_queries"} {
		t.Run(kind, func(t *testing.T) {
			cfg, _ := startClientTestServer(t, echoExecute)
			c := newTestClient(t, cfg)
			req := clientRequest(t)
			switch kind {
			case "large_message":
				req.Inputs = clientInput(t, 0, 2, 700000)
			case "wide_columns":
				column := req.Inputs[0]
				req.Inputs = make([]*arrow.Chunked, 2048)
				for i := range req.Inputs {
					req.Inputs[i] = column
				}
			case "many_queries":
				counts := make([]int, 16385)
				for i := range counts {
					counts[i] = i % 2
				}
				req.Inputs = clientInput(t, counts...)
			}
			outputs, err := c.Execute(context.Background(), req)
			require.NoError(t, err)
			defer releaseColumns(outputs)
			require.Len(t, outputs, len(req.Inputs))
			for i, output := range outputs {
				require.Len(t, output.Chunks(), len(req.Inputs[i].Chunks()))
				for j, chunk := range output.Chunks() {
					require.True(t, array.Equal(chunk, req.Inputs[i].Chunk(j)))
				}
			}
		})
	}
}
