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
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type ListResourceGroupsOption interface {
	Request() *milvuspb.ListResourceGroupsRequest
}

type listResourceGroupsOption struct{}

func (opt *listResourceGroupsOption) Request() *milvuspb.ListResourceGroupsRequest {
	return &milvuspb.ListResourceGroupsRequest{}
}

func NewListResourceGroupsOption() *listResourceGroupsOption {
	return &listResourceGroupsOption{}
}

type CreateResourceGroupOption interface {
	Request() *milvuspb.CreateResourceGroupRequest
}

type createResourceGroupOption struct {
	name        string
	nodeRequest int
	nodeLimit   int
}

func (opt *createResourceGroupOption) WithNodeRequest(nodeRequest int) *createResourceGroupOption {
	opt.nodeRequest = nodeRequest
	return opt
}

func (opt *createResourceGroupOption) WithNodeLimit(nodeLimit int) *createResourceGroupOption {
	opt.nodeLimit = nodeLimit
	return opt
}

func (opt *createResourceGroupOption) Request() *milvuspb.CreateResourceGroupRequest {
	return &milvuspb.CreateResourceGroupRequest{
		ResourceGroup: opt.name,
		Config: &rgpb.ResourceGroupConfig{
			Requests: &rgpb.ResourceGroupLimit{
				NodeNum: int32(opt.nodeRequest),
			},
			Limits: &rgpb.ResourceGroupLimit{
				NodeNum: int32(opt.nodeLimit),
			},
		},
	}
}

func NewCreateResourceGroupOption(name string) *createResourceGroupOption {
	return &createResourceGroupOption{name: name}
}

type DropResourceGroupOption interface {
	Request() *milvuspb.DropResourceGroupRequest
}

type dropResourceGroupOption struct {
	name string
}

func (opt *dropResourceGroupOption) Request() *milvuspb.DropResourceGroupRequest {
	return &milvuspb.DropResourceGroupRequest{
		ResourceGroup: opt.name,
	}
}

func NewDropResourceGroupOption(name string) *dropResourceGroupOption {
	return &dropResourceGroupOption{name: name}
}

type DescribeResourceGroupOption interface {
	Request() *milvuspb.DescribeResourceGroupRequest
}

type describeResourceGroupOption struct {
	name string
}

func (opt *describeResourceGroupOption) Request() *milvuspb.DescribeResourceGroupRequest {
	return &milvuspb.DescribeResourceGroupRequest{
		ResourceGroup: opt.name,
	}
}

func NewDescribeResourceGroupOption(name string) *describeResourceGroupOption {
	return &describeResourceGroupOption{name: name}
}

type UpdateResourceGroupOption interface {
	Request() *milvuspb.UpdateResourceGroupsRequest
}

type updateResourceGroupOption struct {
	name     string
	rgConfig *entity.ResourceGroupConfig
}

func (opt *updateResourceGroupOption) Request() *milvuspb.UpdateResourceGroupsRequest {
	return &milvuspb.UpdateResourceGroupsRequest{
		ResourceGroups: map[string]*rgpb.ResourceGroupConfig{
			opt.name: {
				Requests: &rgpb.ResourceGroupLimit{
					NodeNum: opt.rgConfig.Requests.NodeNum,
				},
				Limits: &rgpb.ResourceGroupLimit{
					NodeNum: opt.rgConfig.Limits.NodeNum,
				},
				TransferFrom: lo.Map(opt.rgConfig.TransferFrom, func(transfer *entity.ResourceGroupTransfer, i int) *rgpb.ResourceGroupTransfer {
					return &rgpb.ResourceGroupTransfer{
						ResourceGroup: transfer.ResourceGroup,
					}
				}),
				TransferTo: lo.Map(opt.rgConfig.TransferTo, func(transfer *entity.ResourceGroupTransfer, i int) *rgpb.ResourceGroupTransfer {
					return &rgpb.ResourceGroupTransfer{
						ResourceGroup: transfer.ResourceGroup,
					}
				}),
				NodeFilter: &rgpb.ResourceGroupNodeFilter{
					NodeLabels: entity.MapKvPairs(opt.rgConfig.NodeFilter.NodeLabels),
				},
			},
		},
	}
}

func NewUpdateResourceGroupOption(name string, resourceGroupConfig *entity.ResourceGroupConfig) *updateResourceGroupOption {
	return &updateResourceGroupOption{
		name:     name,
		rgConfig: resourceGroupConfig,
	}
}

type TransferReplicaOption interface {
	Request() *milvuspb.TransferReplicaRequest
}

type transferReplicaOption struct {
	collectionName string
	sourceRG       string
	targetRG       string
	replicaNum     int64
	dbName         string
}

func (opt *transferReplicaOption) WithDBName(dbName string) *transferReplicaOption {
	opt.dbName = dbName
	return opt
}

func (opt *transferReplicaOption) Request() *milvuspb.TransferReplicaRequest {
	return &milvuspb.TransferReplicaRequest{
		CollectionName:      opt.collectionName,
		SourceResourceGroup: opt.sourceRG,
		TargetResourceGroup: opt.targetRG,
		NumReplica:          opt.replicaNum,
		DbName:              opt.dbName,
	}
}

func NewTransferReplicaOption(collectionName, sourceGroup, targetGroup string, replicaNum int64) *transferReplicaOption {
	return &transferReplicaOption{
		collectionName: collectionName,
		sourceRG:       sourceGroup,
		targetRG:       targetGroup,
		replicaNum:     replicaNum,
	}
}

type DescribeReplicaOption interface {
	Request() *milvuspb.GetReplicasRequest
}

type describeReplicaOption struct {
	collectionName string
}

func (opt *describeReplicaOption) Request() *milvuspb.GetReplicasRequest {
	return &milvuspb.GetReplicasRequest{
		CollectionName: opt.collectionName,
	}
}

func NewDescribeReplicaOption(collectionName string) *describeReplicaOption {
	return &describeReplicaOption{collectionName: collectionName}
}
