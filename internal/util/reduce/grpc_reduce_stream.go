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
	"io"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// grpcReduceStream shares the lifecycle of Query and Search RPCs; constructors
// retain their typed request/response handling and existing error contracts.
type grpcReduceStream[T any] struct {
	recv         func() (T, error)  // Receives and unwraps one operation-specific response.
	closeSend    func() error       // Closes the request-scoped RPC's send side.
	cancel       context.CancelFunc // Cancels the RPC and unblocks Recv.
	interruptErr func() error       // Preserves the operation's unimplemented Interrupt error.

	closeOnce sync.Once   // Ensures the RPC is closed once.
	closeErr  error       // Result of closing the client send side.
	closed    atomic.Bool // Records Close across concurrent Recv and Close calls.
	finished  bool        // Whether the server has returned EOF.
}

// NewQueryGRPCReduceStream opens QueryOnViewStream and sends its initial request.
func NewQueryGRPCReduceStream(ctx context.Context, client viewpb.ViewQueryServiceClient, request *viewpb.QueryOnViewRequest) (ReduceStream[*internalpb.RetrieveResults], error) {
	if client == nil {
		return nil, merr.WrapErrServiceInternalMsg("NewGRPCReduceStream requires a gRPC client")
	}
	if request == nil {
		return nil, merr.WrapErrServiceInternalMsg("NewGRPCReduceStream requires a QueryOnView request")
	}

	streamContext, cancel := context.WithCancel(ctx)
	clientStream, err := client.QueryOnViewStream(streamContext)
	if err != nil {
		cancel()
		return nil, err
	}
	// Send EOF can hide the server status, which must be read by Recv.
	if err := clientStream.Send(&viewpb.QueryOnViewStreamRequest{
		Payload: &viewpb.QueryOnViewStreamRequest_Request{Request: request},
	}); err != nil && !errors.Is(err, io.EOF) {
		cancel()
		_ = clientStream.CloseSend()
		return nil, err
	}

	return &grpcReduceStream[*internalpb.RetrieveResults]{
		recv: func() (*internalpb.RetrieveResults, error) {
			response, err := clientStream.Recv()
			if err != nil {
				return nil, err
			}
			if response == nil {
				return nil, merr.WrapErrServiceInternalMsg("QueryOnViewStream returned a nil response")
			}
			if chunk := response.GetChunk(); chunk != nil {
				return chunk, nil
			}
			if response.GetMetadata() != nil {
				return nil, merr.WrapErrServiceInternalMsg("QueryOnViewStream returned metadata during Recv")
			}
			return nil, merr.WrapErrServiceInternalMsg("QueryOnViewStream returned a response without a payload")
		},
		closeSend: clientStream.CloseSend,
		cancel:    cancel,
		interruptErr: func() error {
			return merr.WrapErrServiceUnimplemented(status.Error(codes.Unimplemented, "gRPC Query ReduceStream Interrupt is not implemented"))
		},
	}, nil
}

// NewSearchGRPCReduceStream opens SearchOnViewStream and sends its initial request.
func NewSearchGRPCReduceStream(ctx context.Context, client viewpb.ViewQueryServiceClient, request *viewpb.SearchOnViewRequest) (ReduceStream[*internalpb.SearchResults], error) {
	if client == nil {
		return nil, errors.New("NewGRPCReduceStream requires a gRPC client")
	}
	if request == nil {
		return nil, errors.New("NewGRPCReduceStream requires a SearchOnView request")
	}

	streamContext, cancel := context.WithCancel(ctx)
	clientStream, err := client.SearchOnViewStream(streamContext)
	if err != nil {
		cancel()
		return nil, err
	}
	// Send EOF can hide the server status, which must be read by Recv.
	if err := clientStream.Send(&viewpb.SearchOnViewStreamRequest{
		Payload: &viewpb.SearchOnViewStreamRequest_Request{Request: request},
	}); err != nil && !errors.Is(err, io.EOF) {
		cancel()
		_ = clientStream.CloseSend()
		return nil, err
	}

	return &grpcReduceStream[*internalpb.SearchResults]{
		recv: func() (*internalpb.SearchResults, error) {
			response, err := clientStream.Recv()
			if err != nil {
				return nil, err
			}
			if response == nil {
				return nil, errors.New("SearchOnViewStream returned a nil response")
			}
			if chunk := response.GetChunk(); chunk != nil {
				return chunk, nil
			}
			if response.GetMetadata() != nil {
				return nil, errors.New("SearchOnViewStream returned metadata during Recv")
			}
			return nil, errors.New("SearchOnViewStream returned response without a payload")
		},
		closeSend: clientStream.CloseSend,
		cancel:    cancel,
		interruptErr: func() error {
			return errors.New("gRPC ReduceStream Interrupt is not implemented")
		},
	}, nil
}

func (s *grpcReduceStream[T]) Recv() (T, error) {
	var zero T
	if s.finished {
		return zero, io.EOF
	}
	if s.closed.Load() {
		return zero, io.ErrClosedPipe
	}

	chunk, err := s.recv()
	if errors.Is(err, io.EOF) {
		s.finished = true
		return zero, io.EOF
	}
	return chunk, err
}

func (s *grpcReduceStream[T]) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.closeErr = s.closeSend()
		s.cancel()
	})
	return s.closeErr
}

func (s *grpcReduceStream[T]) Interrupt() (T, error) {
	var zero T
	return zero, s.interruptErr()
}
