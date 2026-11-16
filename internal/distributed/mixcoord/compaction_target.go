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

package grpcmixcoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func (s *Server) CreateCompactionTarget(ctx context.Context, req *datapb.CreateCompactionTargetRequest) (*datapb.CreateCompactionTargetResponse, error) {
	return s.mixCoord.CreateCompactionTarget(ctx, req)
}

func (s *Server) GetCompactionTargetBinding(ctx context.Context, req *datapb.GetCompactionTargetBindingRequest) (*datapb.GetCompactionTargetBindingResponse, error) {
	return s.mixCoord.GetCompactionTargetBinding(ctx, req)
}

func (s *Server) DescribeCompactionTarget(ctx context.Context, req *datapb.DescribeCompactionTargetRequest) (*datapb.DescribeCompactionTargetResponse, error) {
	return s.mixCoord.DescribeCompactionTarget(ctx, req)
}

func (s *Server) ListCompactionTargets(ctx context.Context, req *datapb.ListCompactionTargetsRequest) (*datapb.ListCompactionTargetsResponse, error) {
	return s.mixCoord.ListCompactionTargets(ctx, req)
}

func (s *Server) DropCompactionTarget(ctx context.Context, req *datapb.DropCompactionTargetRequest) (*datapb.DropCompactionTargetResponse, error) {
	return s.mixCoord.DropCompactionTarget(ctx, req)
}
