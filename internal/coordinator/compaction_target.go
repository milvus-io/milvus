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

package coordinator

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func (s *mixCoordImpl) CreateCompactionTarget(ctx context.Context, req *datapb.CreateCompactionTargetRequest) (*datapb.CreateCompactionTargetResponse, error) {
	return s.datacoordServer.CreateCompactionTarget(ctx, req)
}

func (s *mixCoordImpl) GetCompactionTargetBinding(ctx context.Context, req *datapb.GetCompactionTargetBindingRequest) (*datapb.GetCompactionTargetBindingResponse, error) {
	return s.datacoordServer.GetCompactionTargetBinding(ctx, req)
}

func (s *mixCoordImpl) DescribeCompactionTarget(ctx context.Context, req *datapb.DescribeCompactionTargetRequest) (*datapb.DescribeCompactionTargetResponse, error) {
	return s.datacoordServer.DescribeCompactionTarget(ctx, req)
}

func (s *mixCoordImpl) ListCompactionTargets(ctx context.Context, req *datapb.ListCompactionTargetsRequest) (*datapb.ListCompactionTargetsResponse, error) {
	return s.datacoordServer.ListCompactionTargets(ctx, req)
}

func (s *mixCoordImpl) DropCompactionTarget(ctx context.Context, req *datapb.DropCompactionTargetRequest) (*datapb.DropCompactionTargetResponse, error) {
	return s.datacoordServer.DropCompactionTarget(ctx, req)
}
