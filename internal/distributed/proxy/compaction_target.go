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

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

func (s *Server) CreateCompactionTarget(ctx context.Context, req *milvuspb.CreateCompactionTargetRequest) (*milvuspb.CreateCompactionTargetResponse, error) {
	return s.proxy.CreateCompactionTarget(ctx, req)
}

func (s *Server) DescribeCompactionTarget(ctx context.Context, req *milvuspb.DescribeCompactionTargetRequest) (*milvuspb.DescribeCompactionTargetResponse, error) {
	return s.proxy.DescribeCompactionTarget(ctx, req)
}

func (s *Server) ListCompactionTargets(ctx context.Context, req *milvuspb.ListCompactionTargetsRequest) (*milvuspb.ListCompactionTargetsResponse, error) {
	return s.proxy.ListCompactionTargets(ctx, req)
}

func (s *Server) DropCompactionTarget(ctx context.Context, req *milvuspb.DropCompactionTargetRequest) (*milvuspb.DropCompactionTargetResponse, error) {
	return s.proxy.DropCompactionTarget(ctx, req)
}
