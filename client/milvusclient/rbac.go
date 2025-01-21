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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func (c *Client) ListUsers(ctx context.Context, opt ListUserOption, callOpts ...grpc.CallOption) ([]string, error) {
	var users []string
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.ListCredUsers(ctx, opt.Request(), callOpts...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		users = resp.GetUsernames()
		return nil
	})
	return users, err
}

func (c *Client) DescribeUser(ctx context.Context, opt DescribeUserOption, callOpts ...grpc.CallOption) (*entity.User, error) {
	var user *entity.User
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.SelectUser(ctx, opt.Request(), callOpts...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		if len(resp.GetResults()) == 0 {
			return errors.New("not user found")
		}
		result := resp.GetResults()[0]
		user = &entity.User{
			UserName: result.GetUser().GetName(),
			Roles:    lo.Map(result.GetRoles(), func(r *milvuspb.RoleEntity, _ int) string { return r.GetName() }),
		}

		return nil
	})

	return user, err
}

func (c *Client) CreateUser(ctx context.Context, opt CreateUserOption, callOpts ...grpc.CallOption) error {
	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.CreateCredential(ctx, opt.Request(), callOpts...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) UpdatePassword(ctx context.Context, opt UpdatePasswordOption, callOpts ...grpc.CallOption) error {
	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.UpdateCredential(ctx, opt.Request(), callOpts...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) DropUser(ctx context.Context, opt DropUserOption, callOpts ...grpc.CallOption) error {
	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DeleteCredential(ctx, opt.Request(), callOpts...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) ListRoles(ctx context.Context, opt ListRoleOption, callOpts ...grpc.CallOption) ([]string, error) {
	var roles []string
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.SelectRole(ctx, opt.Request(), callOpts...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		roles = lo.Map(resp.GetResults(), func(r *milvuspb.RoleResult, _ int) string {
			return r.GetRole().GetName()
		})
		return nil
	})
	return roles, err
}

func (c *Client) CreateRole(ctx context.Context, opt CreateRoleOption, callOpts ...grpc.CallOption) error {
	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.CreateRole(ctx, opt.Request(), callOpts...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) GrantRole(ctx context.Context, opt GrantRoleOption, callOpts ...grpc.CallOption) error {
	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperateUserRole(ctx, opt.Request(), callOpts...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) RevokeRole(ctx context.Context, opt RevokeRoleOption, callOpts ...grpc.CallOption) error {
	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperateUserRole(ctx, opt.Request(), callOpts...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) DropRole(ctx context.Context, opt DropRoleOption, callOpts ...grpc.CallOption) error {
	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DropRole(ctx, opt.Request(), callOpts...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) DescribeRole(ctx context.Context, option DescribeRoleOption, callOptions ...grpc.CallOption) (*entity.Role, error) {
	req := option.Request()

	var role *entity.Role
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.SelectGrant(ctx, req, callOptions...)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		if len(resp.GetEntities()) == 0 {
			return errors.New("role not found")
		}

		role = &entity.Role{
			RoleName: req.GetEntity().GetRole().GetName(),
			Privileges: lo.Map(resp.GetEntities(), func(g *milvuspb.GrantEntity, _ int) entity.GrantItem {
				return entity.GrantItem{
					Object:     g.Object.GetName(),
					ObjectName: g.GetObjectName(),
					RoleName:   g.GetRole().GetName(),
					Grantor:    g.GetGrantor().GetUser().GetName(),
					Privilege:  g.GetGrantor().GetPrivilege().GetName(),
				}
			}),
		}
		return nil
	})
	return role, err
}

func (c *Client) GrantPrivilege(ctx context.Context, option GrantPrivilegeOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperatePrivilege(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) RevokePrivilege(ctx context.Context, option RevokePrivilegeOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperatePrivilege(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) GrantV2(ctx context.Context, option GrantV2Option, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperatePrivilegeV2(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

func (c *Client) RevokeV2(ctx context.Context, option RevokeV2Option, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperatePrivilegeV2(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

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

func (c *Client) OperatePrivilegeGroup(ctx context.Context, option OperatePrivilegeGroupOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.OperatePrivilegeGroup(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}
