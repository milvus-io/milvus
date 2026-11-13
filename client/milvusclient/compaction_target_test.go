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

package milvusclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

type CompactionTargetSuite struct {
	MockSuiteBase
}

func TestCompactionTarget(t *testing.T) {
	suite.Run(t, new(CompactionTargetSuite))
}

func (s *CompactionTargetSuite) TestCreateReturnsDurableHandle() {
	ctx := context.Background()
	s.mock.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *milvuspb.CreateCompactionTargetRequest) (*milvuspb.CreateCompactionTargetResponse, error) {
			s.Equal("products", req.GetCollectionName())
			s.Equal("inventory", req.GetDbName())
			s.Equal([]int64{10, 20}, req.GetRewrite().GetSegmentIds())
			return &milvuspb.CreateCompactionTargetResponse{Status: &commonpb.Status{}, TargetId: 42, TotalRows: 200}, nil
		}).Once()

	got, err := s.client.CreateCompactionTarget(ctx, NewRewriteCompactionTargetOption("products", 10, 20).WithDbName("inventory"))
	s.Require().NoError(err)
	s.Equal(int64(42), got.GetTargetId())
	s.Equal(int64(200), got.GetTotalRows())
}

func (s *CompactionTargetSuite) TestCreateDoesNotRetryAfterUnavailable() {
	s.mock.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "lost reply")).Once()

	result, err := s.client.CreateCompactionTarget(context.Background(), NewRewriteCompactionTargetOption("products"))
	s.ErrorIs(err, ErrCompactionTargetCreateOutcomeUnknown)
	s.Nil(result)
}

func (s *CompactionTargetSuite) TestCreateRejectsMalformedSuccess() {
	for _, response := range []*milvuspb.CreateCompactionTargetResponse{
		{TargetId: 42},
		{Status: &commonpb.Status{}},
		{Status: &commonpb.Status{}, TargetId: 42, TotalRows: -1},
		{Status: &commonpb.Status{}, TargetId: 42, Optimize: &milvuspb.OptimizeTargetCreateResult{InputTargetSizeBytes: 20, ActualTargetSizeBytes: 20}},
	} {
		s.mock.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything).Return(response, nil).Once()
		result, err := s.client.CreateCompactionTarget(context.Background(), NewRewriteCompactionTargetOption("products"))
		s.ErrorIs(err, ErrCompactionTargetCreateOutcomeUnknown)
		s.Nil(result)
	}
}

func (s *CompactionTargetSuite) TestCreatePreservesUnimplemented() {
	s.mock.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unimplemented, "old server")).Once()
	_, err := s.client.CreateCompactionTarget(context.Background(), NewRewriteCompactionTargetOption("products"))
	s.Equal(codes.Unimplemented, status.Code(err))
}

func (s *CompactionTargetSuite) TestWaitUsesTerminalState() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := &milvuspb.DescribeCompactionTargetRequest{TargetId: 52}
	s.mock.EXPECT().DescribeCompactionTarget(mock.Anything, req).
		Return(&milvuspb.DescribeCompactionTargetResponse{
			Status: &commonpb.Status{},
			Target: &milvuspb.CompactionTargetInfo{
				TargetId: 52, State: milvuspb.CompactionTargetState_COMPACTION_TARGET_STATE_ACTIVE, RemainingRows: proto.Int64(0),
			},
		}, nil).Once()
	s.mock.EXPECT().DescribeCompactionTarget(mock.Anything, req).
		Return(&milvuspb.DescribeCompactionTargetResponse{
			Status: &commonpb.Status{},
			Target: &milvuspb.CompactionTargetInfo{
				TargetId: 52, State: milvuspb.CompactionTargetState_COMPACTION_TARGET_STATE_INACTIVE, StopReason: "operator_dropped",
			},
		}, nil).Once()

	target, err := s.client.WaitForCompactionTarget(ctx, 52, time.Millisecond)
	s.ErrorContains(err, "operator_dropped")
	s.Equal("operator_dropped", target.GetStopReason())
}
