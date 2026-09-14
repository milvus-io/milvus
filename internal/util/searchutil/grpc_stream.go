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

package searchutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type grpcReduceStream struct {
	client viewpb.ViewQueryService_SearchOnViewStreamClient
	cancel context.CancelFunc

	closeOnce sync.Once
	closeErr  error
	closed    bool
	finished  bool
}

// NewGRPCReduceStream opens SearchOnViewStream and sends its initial request.
func NewGRPCReduceStream(ctx context.Context, client viewpb.ViewQueryServiceClient, request *viewpb.SearchOnViewRequest) (ReduceStream, error) {
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
	if err := clientStream.Send(&viewpb.SearchOnViewStreamRequest{
		Payload: &viewpb.SearchOnViewStreamRequest_Request{Request: request},
	}); err != nil {
		cancel()
		_ = clientStream.CloseSend()
		return nil, err
	}

	return &grpcReduceStream{
		client: clientStream,
		cancel: cancel,
	}, nil
}

func (s *grpcReduceStream) Recv() (*internalpb.SearchResults, error) {
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
		return nil, errors.New("SearchOnViewStream returned a nil response")
	}
	if chunk := response.GetChunk(); chunk != nil {
		return chunk, nil
	}
	if response.GetMetadata() != nil {
		return nil, errors.New("SearchOnViewStream returned metadata during Recv")
	}
	return nil, fmt.Errorf("SearchOnViewStream returned response without a payload")
}

func (s *grpcReduceStream) Close() error {
	s.closeOnce.Do(func() {
		s.closed = true
		s.closeErr = s.client.CloseSend()
		s.cancel()
	})
	return s.closeErr
}

// Interrupt is reserved for the bidirectional stream lifecycle implementation.
func (s *grpcReduceStream) Interrupt() (*internalpb.SearchResults, error) {
	return nil, errors.New("gRPC ReduceStream Interrupt is not implemented")
}
