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

	"github.com/samber/lo"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func (c *Client) ListResourceGroups(ctx context.Context, opt ListResourceGroupsOption, callOptions ...grpc.CallOption) ([]string, error) {
	req := opt.Request()

	var rgs []string
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.ListResourceGroups(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		rgs = resp.GetResourceGroups()
		return nil
	})

	return rgs, err
}

func (c *Client) CreateResourceGroup(ctx context.Context, opt CreateResourceGroupOption, callOptions ...grpc.CallOption) error {
	req := opt.Request()

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.CreateResourceGroup(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})

	return err
}

func (c *Client) DropResourceGroup(ctx context.Context, opt DropResourceGroupOption, callOptions ...grpc.CallOption) error {
	req := opt.Request()

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DropResourceGroup(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})

	return err
}

func (c *Client) DescribeResourceGroup(ctx context.Context, opt DescribeResourceGroupOption, callOptions ...grpc.CallOption) (*entity.ResourceGroup, error) {
	req := opt.Request()

	var rg *entity.ResourceGroup
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DescribeResourceGroup(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		resultRg := resp.GetResourceGroup()
		rg = &entity.ResourceGroup{
			Name:             resultRg.GetName(),
			Capacity:         resultRg.GetCapacity(),
			NumAvailableNode: resultRg.GetNumAvailableNode(),
			NumLoadedReplica: resultRg.GetNumLoadedReplica(),
			NumOutgoingNode:  resultRg.GetNumOutgoingNode(),
			NumIncomingNode:  resultRg.GetNumIncomingNode(),
			Config: &entity.ResourceGroupConfig{
				Requests: entity.ResourceGroupLimit{
					NodeNum: resultRg.GetConfig().GetRequests().GetNodeNum(),
				},
				Limits: entity.ResourceGroupLimit{
					NodeNum: resultRg.GetConfig().GetLimits().GetNodeNum(),
				},
				TransferFrom: lo.Map(resultRg.GetConfig().GetTransferFrom(), func(transfer *rgpb.ResourceGroupTransfer, i int) *entity.ResourceGroupTransfer {
					return &entity.ResourceGroupTransfer{
						ResourceGroup: transfer.GetResourceGroup(),
					}
				}),
				TransferTo: lo.Map(resultRg.GetConfig().GetTransferTo(), func(transfer *rgpb.ResourceGroupTransfer, i int) *entity.ResourceGroupTransfer {
					return &entity.ResourceGroupTransfer{
						ResourceGroup: transfer.GetResourceGroup(),
					}
				}),
				NodeFilter: entity.ResourceGroupNodeFilter{
					NodeLabels: entity.KvPairsMap(resultRg.GetConfig().GetNodeFilter().GetNodeLabels()),
				},
			},
			Nodes: lo.Map(resultRg.GetNodes(), func(node *commonpb.NodeInfo, i int) entity.NodeInfo {
				return entity.NodeInfo{
					NodeID:   node.GetNodeId(),
					Address:  node.GetAddress(),
					HostName: node.GetHostname(),
				}
			}),
		}

		return nil
	})
	return rg, err
}

func (c *Client) UpdateResourceGroup(ctx context.Context, opt UpdateResourceGroupOption, callOptions ...grpc.CallOption) error {
	req := opt.Request()

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.UpdateResourceGroups(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})

	return err
}

func (c *Client) TransferReplica(ctx context.Context, opt TransferReplicaOption, callOptions ...grpc.CallOption) error {
	req := opt.Request()

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.TransferReplica(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})

	return err
}

func (c *Client) DescribeReplica(ctx context.Context, opt DescribeReplicaOption, callOptions ...grpc.CallOption) ([]*entity.ReplicaInfo, error) {
	req := opt.Request()

	var result []*entity.ReplicaInfo

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.GetReplicas(ctx, req, callOptions...)

		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		result = lo.Map(resp.GetReplicas(), func(replica *milvuspb.ReplicaInfo, _ int) *entity.ReplicaInfo {
			return &entity.ReplicaInfo{
				ReplicaID: replica.GetReplicaID(),
				Shards: lo.Map(replica.GetShardReplicas(), func(shardReplica *milvuspb.ShardReplica, _ int) *entity.Shard {
					return &entity.Shard{
						ChannelName: shardReplica.GetDmChannelName(),
						ShardNodes:  shardReplica.GetNodeIds(),
						ShardLeader: shardReplica.GetLeaderID(),
					}
				}),
				Nodes:             replica.GetNodeIds(),
				ResourceGroupName: replica.GetResourceGroupName(),
				NumOutboundNode:   replica.GetNumOutboundNode(),
			}
		})

		return nil
	})
	return result, err
}
