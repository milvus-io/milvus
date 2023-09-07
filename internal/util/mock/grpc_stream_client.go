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

package mock

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var _ grpc.ClientStream = &MockClientStream{}

type MockClientStream struct{}

func (s *MockClientStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *MockClientStream) Trailer() metadata.MD {
	return nil
}

func (s *MockClientStream) CloseSend() error {
	return nil
}

func (s *MockClientStream) Context() context.Context {
	return nil
}

func (s *MockClientStream) SendMsg(m interface{}) error {
	return nil
}

func (s *MockClientStream) RecvMsg(m interface{}) error {
	return nil
}

var _ querypb.QueryNode_QueryStreamClient = &GrpcQueryStreamClient{}

type GrpcQueryStreamClient struct {
	MockClientStream
}

func (c *GrpcQueryStreamClient) Recv() (*internalpb.RetrieveResults, error) {
	return &internalpb.RetrieveResults{}, nil
}

var _ querypb.QueryNode_QueryStreamSegmentsClient = &GrpcQueryStreamSegmentsClient{}

type GrpcQueryStreamSegmentsClient struct {
	MockClientStream
}

func (c *GrpcQueryStreamSegmentsClient) Recv() (*internalpb.RetrieveResults, error) {
	return &internalpb.RetrieveResults{}, nil
}
