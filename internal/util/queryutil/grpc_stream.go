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

package queryutil

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type grpcReduceStream struct {
	client viewpb.ViewQueryService_QueryOnViewStreamClient
	cancel context.CancelFunc

	closeOnce sync.Once
	closeErr  error
	closed    bool
	finished  bool
}

// NewGRPCReduceStream opens QueryOnViewStream and sends its initial request.
func NewGRPCReduceStream(ctx context.Context, client viewpb.ViewQueryServiceClient, request *viewpb.QueryOnViewRequest) (ReduceStream, error) {
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
	if err := clientStream.Send(&viewpb.QueryOnViewStreamRequest{
		Payload: &viewpb.QueryOnViewStreamRequest_Request{Request: request},
	}); err != nil {
		cancel()
		_ = clientStream.CloseSend()
		return nil, err
	}

	return &grpcReduceStream{client: clientStream, cancel: cancel}, nil
}

func (s *grpcReduceStream) Recv() (*internalpb.RetrieveResults, error) {
	if s.finished {
		return nil, io.EOF
	}
	if s.closed {
		return nil, io.ErrClosedPipe
	}

	response, err := s.client.Recv()
	if errors.Is(err, io.EOF) {
		s.finished = true
		return nil, io.EOF
	}
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
}

func (s *grpcReduceStream) Close() error {
	s.closeOnce.Do(func() {
		s.closed = true
		s.closeErr = s.client.CloseSend()
		s.cancel()
	})
	return s.closeErr
}

func (s *grpcReduceStream) Interrupt() (*internalpb.RetrieveResults, error) {
	return nil, merr.WrapErrServiceUnimplemented(status.Error(codes.Unimplemented, "gRPC Query ReduceStream Interrupt is not implemented"))
}
