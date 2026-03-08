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

// GetServerVersionOption is the interface for GetServerVersion request.
type GetServerVersionOption interface {
	Request() *milvuspb.GetVersionRequest
}

type getServerVersionOption struct{}

func (opt *getServerVersionOption) Request() *milvuspb.GetVersionRequest {
	return &milvuspb.GetVersionRequest{}
}

func NewGetServerVersionOption() *getServerVersionOption {
	return &getServerVersionOption{}
}

// GetServerVersion returns connect Milvus instance version.
func (c *Client) GetServerVersion(ctx context.Context, option GetServerVersionOption, callOptions ...grpc.CallOption) (string, error) {
	req := option.Request()

	var version string

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.GetVersion(ctx, req, callOptions...)
		version = resp.GetVersion()
		return merr.CheckRPCCall(resp, err)
	})
	return version, err
}

type GetPersistentSegmentInfoOption interface {
	Request() *milvuspb.GetPersistentSegmentInfoRequest
}

type getPersistentSegmentInfoOption struct {
	collectionName string
}

func (opt *getPersistentSegmentInfoOption) Request() *milvuspb.GetPersistentSegmentInfoRequest {
	return &milvuspb.GetPersistentSegmentInfoRequest{
		CollectionName: opt.collectionName,
	}
}

func NewGetPersistentSegmentInfoOption(collectionName string) GetPersistentSegmentInfoOption {
	return &getPersistentSegmentInfoOption{
		collectionName: collectionName,
	}
}

func (c *Client) GetPersistentSegmentInfo(ctx context.Context, option GetPersistentSegmentInfoOption) ([]*entity.Segment, error) {
	req := option.Request()

	var segments []*entity.Segment

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.GetPersistentSegmentInfo(ctx, req)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		segments = lo.Map(resp.GetInfos(), func(info *milvuspb.PersistentSegmentInfo, _ int) *entity.Segment {
			return &entity.Segment{
				ID:           info.GetSegmentID(),
				CollectionID: info.GetCollectionID(),
				ParititionID: info.GetPartitionID(),
				NumRows:      info.GetNumRows(),
				State:        info.GetState(),
			}
		})
		return nil
	})

	return segments, err
}

type BackupRBACOption interface {
	Request() *milvuspb.BackupRBACMetaRequest
}

type backupRBACOption struct{}

func (opt *backupRBACOption) Request() *milvuspb.BackupRBACMetaRequest {
	return &milvuspb.BackupRBACMetaRequest{}
}

func NewBackupRBACOption() BackupRBACOption {
	return &backupRBACOption{}
}

func (c *Client) BackupRBAC(ctx context.Context, option BackupRBACOption, callOptions ...grpc.CallOption) (*entity.RBACMeta, error) {
	req := option.Request()

	var meta *entity.RBACMeta

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.BackupRBAC(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		rbacMeta := resp.GetRBACMeta()

		meta = &entity.RBACMeta{
			Users: lo.Map(rbacMeta.GetUsers(), func(user *milvuspb.UserInfo, _ int) *entity.UserInfo {
				return &entity.UserInfo{
					UserDescription: entity.UserDescription{
						Name:  user.GetUser(),
						Roles: lo.Map(user.GetRoles(), func(role *milvuspb.RoleEntity, _ int) string { return role.GetName() }),
					},
					Password: user.GetPassword(),
				}
			}),
			Roles: lo.Map(rbacMeta.GetRoles(), func(role *milvuspb.RoleEntity, _ int) *entity.Role {
				return &entity.Role{
					RoleName: role.GetName(),
				}
			}),
			RoleGrants: lo.Map(rbacMeta.GetGrants(), func(grant *milvuspb.GrantEntity, _ int) *entity.RoleGrants {
				return &entity.RoleGrants{
					Object:        grant.GetObject().GetName(),
					ObjectName:    grant.GetObjectName(),
					RoleName:      grant.GetRole().GetName(),
					GrantorName:   grant.GetGrantor().GetUser().GetName(),
					PrivilegeName: grant.GetGrantor().GetPrivilege().GetName(),
					DbName:        grant.GetDbName(),
				}
			}),
			PrivilegeGroups: lo.Map(rbacMeta.GetPrivilegeGroups(), func(group *milvuspb.PrivilegeGroupInfo, _ int) *entity.PrivilegeGroup {
				return &entity.PrivilegeGroup{
					GroupName:  group.GetGroupName(),
					Privileges: lo.Map(group.GetPrivileges(), func(privilege *milvuspb.PrivilegeEntity, _ int) string { return privilege.GetName() }),
				}
			}),
		}

		return nil
	})
	return meta, err
}

type RestoreRBACOption interface {
	Request() *milvuspb.RestoreRBACMetaRequest
}

type restoreRBACOption struct {
	meta *entity.RBACMeta
}

func (opt *restoreRBACOption) Request() *milvuspb.RestoreRBACMetaRequest {
	return &milvuspb.RestoreRBACMetaRequest{
		RBACMeta: &milvuspb.RBACMeta{
			Users: lo.Map(opt.meta.Users, func(user *entity.UserInfo, _ int) *milvuspb.UserInfo {
				return &milvuspb.UserInfo{
					User:     user.Name,
					Roles:    lo.Map(user.Roles, func(role string, _ int) *milvuspb.RoleEntity { return &milvuspb.RoleEntity{Name: role} }),
					Password: user.Password,
				}
			}),
			Roles: lo.Map(opt.meta.Roles, func(role *entity.Role, _ int) *milvuspb.RoleEntity {
				return &milvuspb.RoleEntity{Name: role.RoleName}
			}),
			Grants: lo.Map(opt.meta.RoleGrants, func(grant *entity.RoleGrants, _ int) *milvuspb.GrantEntity {
				return &milvuspb.GrantEntity{
					Object:     &milvuspb.ObjectEntity{Name: grant.Object},
					ObjectName: grant.ObjectName,
					Role:       &milvuspb.RoleEntity{Name: grant.RoleName},
					Grantor: &milvuspb.GrantorEntity{
						User: &milvuspb.UserEntity{
							Name: grant.GrantorName,
						},
						Privilege: &milvuspb.PrivilegeEntity{
							Name: grant.PrivilegeName,
						},
					},
					DbName: grant.DbName,
				}
			}),
			PrivilegeGroups: lo.Map(opt.meta.PrivilegeGroups, func(group *entity.PrivilegeGroup, _ int) *milvuspb.PrivilegeGroupInfo {
				return &milvuspb.PrivilegeGroupInfo{
					GroupName: group.GroupName,
					Privileges: lo.Map(group.Privileges, func(privilege string, _ int) *milvuspb.PrivilegeEntity {
						return &milvuspb.PrivilegeEntity{Name: privilege}
					}),
				}
			}),
		},
	}
}

func NewRestoreRBACOption(meta *entity.RBACMeta) RestoreRBACOption {
	return &restoreRBACOption{meta: meta}
}

func (c *Client) RestoreRBAC(ctx context.Context, option RestoreRBACOption, callOptions ...grpc.CallOption) error {
	req := option.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.RestoreRBAC(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}
