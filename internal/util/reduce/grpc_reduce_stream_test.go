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

package reduce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type shutdownClientStream struct {
	ctx        context.Context
	started    chan struct{}
	closeCalls atomic.Int32
	closeErr   error
}

type grpcTestClientStream[Request, Response any] struct {
	grpc.ClientStream
	ctx       context.Context
	request   Request
	response  Response
	recvErr   error
	recvCalls int
}

func (s *grpcTestClientStream[Request, Response]) Send(request Request) error {
	s.request = request
	return nil
}

func (s *grpcTestClientStream[Request, Response]) Recv() (Response, error) {
	s.recvCalls++
	return s.response, s.recvErr
}

func (*grpcTestClientStream[Request, Response]) CloseSend() error { return nil }

type grpcTestClient struct {
	viewpb.ViewQueryServiceClient
	query  grpcTestClientStream[*viewpb.QueryOnViewStreamRequest, *viewpb.QueryOnViewStreamResponse]
	search grpcTestClientStream[*viewpb.SearchOnViewStreamRequest, *viewpb.SearchOnViewStreamResponse]
}

func (c *grpcTestClient) QueryOnViewStream(ctx context.Context, _ ...grpc.CallOption) (viewpb.ViewQueryService_QueryOnViewStreamClient, error) {
	c.query.ctx = ctx
	return &c.query, nil
}

func (c *grpcTestClient) SearchOnViewStream(ctx context.Context, _ ...grpc.CallOption) (viewpb.ViewQueryService_SearchOnViewStreamClient, error) {
	c.search.ctx = ctx
	return &c.search, nil
}

func TestGRPCReduceStreamProtocol(t *testing.T) {
	queryChunk, searchChunk := &internalpb.RetrieveResults{}, &internalpb.SearchResults{}
	for _, query := range []bool{true, false} {
		for _, tc := range []struct {
			name   string
			query  *viewpb.QueryOnViewStreamResponse
			search *viewpb.SearchOnViewStreamResponse
			err    error
		}{
			{"chunk", &viewpb.QueryOnViewStreamResponse{Payload: &viewpb.QueryOnViewStreamResponse_Chunk{Chunk: queryChunk}}, &viewpb.SearchOnViewStreamResponse{Payload: &viewpb.SearchOnViewStreamResponse_Chunk{Chunk: searchChunk}}, nil},
			{"nil_response", nil, nil, nil},
			{"metadata", &viewpb.QueryOnViewStreamResponse{Payload: &viewpb.QueryOnViewStreamResponse_Metadata{Metadata: queryChunk}}, &viewpb.SearchOnViewStreamResponse{Payload: &viewpb.SearchOnViewStreamResponse_Metadata{Metadata: searchChunk}}, nil},
			{"empty_payload", &viewpb.QueryOnViewStreamResponse{}, &viewpb.SearchOnViewStreamResponse{}, nil},
			{"nil_chunk", &viewpb.QueryOnViewStreamResponse{Payload: &viewpb.QueryOnViewStreamResponse_Chunk{}}, &viewpb.SearchOnViewStreamResponse{Payload: &viewpb.SearchOnViewStreamResponse_Chunk{}}, nil},
			{"unavailable", nil, nil, status.Error(codes.Unavailable, "worker disconnected")},
			{"eof", nil, nil, fmt.Errorf("wrapped: %w", io.EOF)},
		} {
			t.Run(fmt.Sprintf("query_%t/%s", query, tc.name), func(t *testing.T) {
				client := &grpcTestClient{}
				client.query.response, client.query.recvErr = tc.query, tc.err
				client.search.response, client.search.recvErr = tc.search, tc.err
				var recv func() (any, error)
				var interrupt func() error
				var closeStream func() error
				var ctx context.Context
				var recvCalls *int
				var expected any
				if query {
					request := &viewpb.QueryOnViewRequest{}
					stream, err := NewQueryGRPCReduceStream(t.Context(), client, request)
					require.NoError(t, err)
					require.Same(t, request, client.query.request.GetRequest())
					recv = func() (any, error) { return stream.Recv() }
					interrupt = func() error { _, err := stream.Interrupt(); return err }
					closeStream, ctx, recvCalls, expected = stream.Close, client.query.ctx, &client.query.recvCalls, queryChunk
				} else {
					request := &viewpb.SearchOnViewRequest{}
					stream, err := NewSearchGRPCReduceStream(t.Context(), client, request)
					require.NoError(t, err)
					require.Same(t, request, client.search.request.GetRequest())
					recv = func() (any, error) { return stream.Recv() }
					interrupt = func() error { _, err := stream.Interrupt(); return err }
					closeStream, ctx, recvCalls, expected = stream.Close, client.search.ctx, &client.search.recvCalls, searchChunk
				}
				t.Cleanup(func() { require.NoError(t, closeStream()) })
				require.Zero(t, *recvCalls, "opening must not prefetch a Chunk")
				interruptErr := interrupt()
				if query {
					require.ErrorIs(t, interruptErr, merr.ErrServiceUnimplemented)
				} else {
					require.EqualError(t, interruptErr, "gRPC ReduceStream Interrupt is not implemented")
					require.Equal(t, merr.Code(errors.New("untyped")), merr.Code(interruptErr))
				}
				chunk, err := recv()
				switch tc.name {
				case "chunk":
					require.NoError(t, err)
					require.Same(t, expected, chunk)
				case "eof":
					require.Equal(t, io.EOF, err)
					_, err = recv()
					require.Equal(t, io.EOF, err)
					require.Equal(t, 1, *recvCalls)
				case "unavailable":
					require.Same(t, tc.err, err)
					require.Equal(t, codes.Unavailable, status.Code(err))
				default:
					require.Nil(t, chunk)
					require.Error(t, err)
					if query {
						require.ErrorIs(t, err, merr.ErrServiceInternal)
					} else {
						require.Equal(t, merr.Code(errors.New("untyped")), merr.Code(err))
					}
				}
				require.NoError(t, closeStream())
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				_, err = recv()
				if tc.name == "eof" {
					require.Equal(t, io.EOF, err)
				} else {
					require.ErrorIs(t, err, io.ErrClosedPipe)
				}
			})
		}
	}
}

func (s *shutdownClientStream) Recv() (int, error) {
	close(s.started)
	<-s.ctx.Done()
	return 0, s.ctx.Err()
}

func (s *shutdownClientStream) CloseSend() error {
	s.closeCalls.Add(1)
	return s.closeErr
}

func TestGRPCReduceStreamConcurrentClose(t *testing.T) {
	for _, mode := range []string{"blocked_recv", "starting_recv"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			closeErr := errors.New("close send failed")
			client := &shutdownClientStream{ctx: ctx, started: make(chan struct{}), closeErr: closeErr}
			stream := &grpcReduceStream[int]{recv: client.Recv, closeSend: client.CloseSend, cancel: cancel}
			start := make(chan struct{})
			received := make(chan error, 1)
			go func() {
				<-start
				_, err := stream.Recv()
				received <- err
			}()
			if mode == "blocked_recv" {
				close(start)
				select {
				case <-client.started:
				case <-time.After(5 * time.Second):
					t.Fatal("gRPC receive did not start")
				}
			}
			closed := make(chan error, 4)
			for range cap(closed) {
				go func() {
					<-start
					closed <- stream.Close()
				}()
			}
			if mode == "starting_recv" {
				close(start)
			}
			select {
			case err := <-received:
				require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, io.ErrClosedPipe), "%v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("gRPC receive was not canceled")
			}
			for range cap(closed) {
				select {
				case err := <-closed:
					require.ErrorIs(t, err, closeErr)
				case <-time.After(5 * time.Second):
					t.Fatal("concurrent gRPC Close did not finish")
				}
			}
			require.Equal(t, int32(1), client.closeCalls.Load())
			_, err := stream.Recv()
			require.ErrorIs(t, err, io.ErrClosedPipe)
		})
	}
}
