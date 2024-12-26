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
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

type ResourceGroupSuite struct {
	MockSuiteBase
}

func (s *ResourceGroupSuite) TestListResourceGroups() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().ListResourceGroups(mock.Anything, mock.Anything).Return(&milvuspb.ListResourceGroupsResponse{
			ResourceGroups: []string{"rg1", "rg2"},
		}, nil).Once()
		rgs, err := s.client.ListResourceGroups(ctx, NewListResourceGroupsOption())
		s.NoError(err)
		s.Equal([]string{"rg1", "rg2"}, rgs)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().ListResourceGroups(mock.Anything, mock.Anything).Return(nil, errors.New("mocked")).Once()
		_, err := s.client.ListResourceGroups(ctx, NewListResourceGroupsOption())
		s.Error(err)
	})
}

func (s *ResourceGroupSuite) TestCreateResourceGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		rgName := fmt.Sprintf("rg_%s", s.randString(6))
		s.mock.EXPECT().CreateResourceGroup(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, crgr *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
			s.Equal(rgName, crgr.GetResourceGroup())
			s.Equal(int32(5), crgr.GetConfig().GetRequests().GetNodeNum())
			s.Equal(int32(10), crgr.GetConfig().GetLimits().GetNodeNum())
			return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
		}).Once()
		opt := NewCreateResourceGroupOption(rgName).WithNodeLimit(10).WithNodeRequest(5)
		err := s.client.CreateResourceGroup(ctx, opt)
		s.NoError(err)
	})

	s.Run("failure", func() {
		rgName := fmt.Sprintf("rg_%s", s.randString(6))
		s.mock.EXPECT().CreateResourceGroup(mock.Anything, mock.Anything).Return(nil, errors.New("mocked")).Once()
		opt := NewCreateResourceGroupOption(rgName).WithNodeLimit(10).WithNodeRequest(5)
		err := s.client.CreateResourceGroup(ctx, opt)
		s.Error(err)
	})
}

func (s *ResourceGroupSuite) TestDropResourceGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		rgName := fmt.Sprintf("rg_%s", s.randString(6))
		s.mock.EXPECT().DropResourceGroup(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, drgr *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
			s.Equal(rgName, drgr.GetResourceGroup())
			return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
		}).Once()
		opt := NewDropResourceGroupOption(rgName)
		err := s.client.DropResourceGroup(ctx, opt)
		s.NoError(err)
	})

	s.Run("failure", func() {
		rgName := fmt.Sprintf("rg_%s", s.randString(6))
		s.mock.EXPECT().DropResourceGroup(mock.Anything, mock.Anything).Return(nil, errors.New("mocked")).Once()
		opt := NewDropResourceGroupOption(rgName)
		err := s.client.DropResourceGroup(ctx, opt)
		s.Error(err)
	})
}

func TestResourceGroup(t *testing.T) {
	suite.Run(t, new(ResourceGroupSuite))
}
