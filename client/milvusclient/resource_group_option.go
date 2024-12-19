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
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
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
