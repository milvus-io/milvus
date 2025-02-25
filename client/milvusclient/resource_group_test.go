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
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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

func (s *ResourceGroupSuite) TestDescribeResourceGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		limit := rand.Int31n(10) + 1
		request := rand.Int31n(10) + 1
		rgName := fmt.Sprintf("rg_%s", s.randString(6))
		transferFroms := []string{s.randString(6), s.randString(6)}
		transferTos := []string{s.randString(6), s.randString(6)}
		labels := map[string]string{
			"label1": s.randString(10),
		}
		node := entity.NodeInfo{
			NodeID:   rand.Int63(),
			Address:  s.randString(6),
			HostName: s.randString(10),
		}
		s.mock.EXPECT().DescribeResourceGroup(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, drgr *milvuspb.DescribeResourceGroupRequest) (*milvuspb.DescribeResourceGroupResponse, error) {
			s.Equal(rgName, drgr.GetResourceGroup())
			return &milvuspb.DescribeResourceGroupResponse{
				ResourceGroup: &milvuspb.ResourceGroup{
					Name: rgName,
					Config: &rgpb.ResourceGroupConfig{
						Requests: &rgpb.ResourceGroupLimit{
							NodeNum: request,
						},
						Limits: &rgpb.ResourceGroupLimit{
							NodeNum: limit,
						},
						TransferFrom: lo.Map(transferFroms, func(transfer string, i int) *rgpb.ResourceGroupTransfer {
							return &rgpb.ResourceGroupTransfer{
								ResourceGroup: transfer,
							}
						}),
						TransferTo: lo.Map(transferTos, func(transfer string, i int) *rgpb.ResourceGroupTransfer {
							return &rgpb.ResourceGroupTransfer{
								ResourceGroup: transfer,
							}
						}),
						NodeFilter: &rgpb.ResourceGroupNodeFilter{
							NodeLabels: entity.MapKvPairs(labels),
						},
					},
					Nodes: []*commonpb.NodeInfo{
						{NodeId: node.NodeID, Address: node.Address, Hostname: node.HostName},
					},
				},
			}, nil
		}).Once()
		opt := NewDescribeResourceGroupOption(rgName)
		rg, err := s.client.DescribeResourceGroup(ctx, opt)
		s.NoError(err)
		s.Equal(rgName, rg.Name)
		s.Equal(limit, rg.Config.Limits.NodeNum)
		s.Equal(request, rg.Config.Requests.NodeNum)
		s.ElementsMatch(lo.Map(transferFroms, func(transferFrom string, _ int) *entity.ResourceGroupTransfer {
			return &entity.ResourceGroupTransfer{ResourceGroup: transferFrom}
		}), rg.Config.TransferFrom)
		s.ElementsMatch(lo.Map(transferTos, func(transferTo string, _ int) *entity.ResourceGroupTransfer {
			return &entity.ResourceGroupTransfer{ResourceGroup: transferTo}
		}), rg.Config.TransferTo)
		s.Equal(labels, rg.Config.NodeFilter.NodeLabels)
		s.ElementsMatch([]entity.NodeInfo{node}, rg.Nodes)
	})

	s.Run("failure", func() {
		rgName := fmt.Sprintf("rg_%s", s.randString(6))
		s.mock.EXPECT().DescribeResourceGroup(mock.Anything, mock.Anything).Return(nil, errors.New("mocked")).Once()
		opt := NewDescribeResourceGroupOption(rgName)
		_, err := s.client.DescribeResourceGroup(ctx, opt)
		s.Error(err)
	})
}

func (s *ResourceGroupSuite) TestUpdateResourceGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		limit := rand.Int31n(10) + 1
		request := rand.Int31n(10) + 1
		rgName := fmt.Sprintf("rg_%s", s.randString(6))
		transferFroms := []string{s.randString(6), s.randString(6)}
		transferTos := []string{s.randString(6), s.randString(6)}
		labels := map[string]string{
			"label1": s.randString(10),
		}
		s.mock.EXPECT().UpdateResourceGroups(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, urgr *milvuspb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
			config, ok := urgr.GetResourceGroups()[rgName]
			s.Require().True(ok)
			s.Equal(request, config.GetRequests().GetNodeNum())
			s.Equal(limit, config.GetLimits().GetNodeNum())
			s.ElementsMatch(transferFroms, lo.Map(config.GetTransferFrom(), func(transfer *rgpb.ResourceGroupTransfer, i int) string {
				return transfer.GetResourceGroup()
			}))
			s.ElementsMatch(transferTos, lo.Map(config.GetTransferTo(), func(transfer *rgpb.ResourceGroupTransfer, i int) string {
				return transfer.GetResourceGroup()
			}))
			s.Equal(labels, entity.KvPairsMap(config.GetNodeFilter().GetNodeLabels()))
			return merr.Success(), nil
		}).Once()
		opt := NewUpdateResourceGroupOption(rgName, &entity.ResourceGroupConfig{
			Requests: entity.ResourceGroupLimit{NodeNum: request},
			Limits:   entity.ResourceGroupLimit{NodeNum: limit},
			TransferFrom: []*entity.ResourceGroupTransfer{
				{ResourceGroup: transferFroms[0]},
				{ResourceGroup: transferFroms[1]},
			},
			TransferTo: []*entity.ResourceGroupTransfer{
				{ResourceGroup: transferTos[0]},
				{ResourceGroup: transferTos[1]},
			},
			NodeFilter: entity.ResourceGroupNodeFilter{
				NodeLabels: labels,
			},
		})
		err := s.client.UpdateResourceGroup(ctx, opt)
		s.NoError(err)
	})

	s.Run("failure", func() {
		rgName := fmt.Sprintf("rg_%s", s.randString(6))
		s.mock.EXPECT().UpdateResourceGroups(mock.Anything, mock.Anything).Return(nil, errors.New("mocked")).Once()
		opt := NewUpdateResourceGroupOption(rgName, &entity.ResourceGroupConfig{})
		err := s.client.UpdateResourceGroup(ctx, opt)
		s.Error(err)
	})
}

func (s *ResourceGroupSuite) TestTransferReplica() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		collName := fmt.Sprintf("rg_%s", s.randString(6))
		dbName := fmt.Sprintf("db_%s", s.randString(6))
		from := fmt.Sprintf("rg_%s", s.randString(6))
		to := fmt.Sprintf("rg_%s", s.randString(6))
		replicaNum := rand.Int63n(10) + 1
		s.mock.EXPECT().TransferReplica(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, tr *milvuspb.TransferReplicaRequest) (*commonpb.Status, error) {
			s.Equal(collName, tr.GetCollectionName())
			s.Equal(dbName, tr.GetDbName())
			s.Equal(from, tr.GetSourceResourceGroup())
			s.Equal(to, tr.GetTargetResourceGroup())
			return merr.Success(), nil
		}).Once()
		opt := NewTransferReplicaOption(collName, from, to, replicaNum).WithDBName(dbName)
		err := s.client.TransferReplica(ctx, opt)
		s.NoError(err)
	})

	s.Run("failure", func() {
		rgName := fmt.Sprintf("rg_%s", s.randString(6))
		from := fmt.Sprintf("rg_%s", s.randString(6))
		to := fmt.Sprintf("rg_%s", s.randString(6))
		s.mock.EXPECT().TransferReplica(mock.Anything, mock.Anything).Return(nil, errors.New("mocked")).Once()
		opt := NewTransferReplicaOption(rgName, from, to, 1)
		err := s.client.TransferReplica(ctx, opt)
		s.Error(err)
	})
}

func TestResourceGroup(t *testing.T) {
	suite.Run(t, new(ResourceGroupSuite))
}
