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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func (c *Client) CreatePrivilegeGroup(ctx context.Context, option CreatePrivilegeGroupOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.CreatePrivilegeGroup(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) DropPrivilegeGroup(ctx context.Context, option DropPrivilegeGroupOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DropPrivilegeGroup(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) ListPrivilegeGroups(ctx context.Context, option ListPrivilegeGroupsOption, callOptions ...grpc.CallOption) ([]*entity.PrivilegeGroup, error) {
	req := option.Request()

	var privilegeGroups []*entity.PrivilegeGroup
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		r, err := milvusService.ListPrivilegeGroups(ctx, req, callOptions...)
		if err != nil {
			return err
		}
		for _, pg := range r.PrivilegeGroups {
			privileges := lo.Map(pg.Privileges, func(p *milvuspb.PrivilegeEntity, _ int) string {
				return p.Name
			})
			privilegeGroups = append(privilegeGroups, &entity.PrivilegeGroup{
				GroupName:  pg.GroupName,
				Privileges: privileges,
			})
		}
		return nil
	})
	return privilegeGroups, err
}

func (c *Client) AddPrivilegesToGroup(ctx context.Context, option AddPrivilegeToGroupOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperatePrivilegeGroup(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) RemovePrivilegesFromGroup(ctx context.Context, option RemovePrivilegeFromGroupOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperatePrivilegeGroup(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) GrantPrivilegeV2(ctx context.Context, option GrantPrivilegeV2Option, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperatePrivilegeV2(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) RevokePrivilegeV2(ctx context.Context, option RevokePrivilegeV2Option, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperatePrivilegeV2(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

// Deprecated, use `AddPrivilegesToGroup` or `RemovePrivilegesFromGroup` instead
func (c *Client) OperatePrivilegeGroup(ctx context.Context, option OperatePrivilegeGroupOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperatePrivilegeGroup(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

// Deprecated, use `GrantPrivilegeV2` instead
func (c *Client) GrantV2(ctx context.Context, option GrantV2Option, callOptions ...grpc.CallOption) error {
	return c.GrantPrivilegeV2(ctx, option, callOptions...)
}

// Deprecated, use `RevokePrivilegeV2` instead
func (c *Client) RevokeV2(ctx context.Context, option RevokeV2Option, callOptions ...grpc.CallOption) error {
	return c.RevokePrivilegeV2(ctx, option, callOptions...)
}
