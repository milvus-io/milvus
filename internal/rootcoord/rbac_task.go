/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func executeDeleteCredentialTaskSteps(ctx context.Context, core *Core, username string) error {
	redoTask := newBaseRedoTask(core.stepExecutor)
	redoTask.AddSyncStep(NewSimpleStep("delete credential meta data", func(ctx context.Context) ([]nestedStep, error) {
		err := core.meta.DeleteCredential(ctx, username)
		if err != nil {
			log.Ctx(ctx).Warn("delete credential meta data failed", zap.String("username", username), zap.Error(err))
		}
		return nil, err
	}))
	redoTask.AddAsyncStep(NewSimpleStep("delete credential cache", func(ctx context.Context) ([]nestedStep, error) {
		err := core.ExpireCredCache(ctx, username)
		if err != nil {
			log.Ctx(ctx).Warn("delete credential cache failed", zap.String("username", username), zap.Error(err))
		}
		return nil, err
	}))
	redoTask.AddAsyncStep(NewSimpleStep("delete user role cache for the user", func(ctx context.Context) ([]nestedStep, error) {
		err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: int32(typeutil.CacheDeleteUser),
			OpKey:  username,
		})
		if err != nil {
			log.Ctx(ctx).Warn("delete user role cache failed for the user", zap.String("username", username), zap.Error(err))
		}
		return nil, err
	}))

	return redoTask.Execute(ctx)
}

func executeDropRoleTaskSteps(ctx context.Context, core *Core, roleName string, foreDrop bool) error {
	redoTask := newBaseRedoTask(core.stepExecutor)
	redoTask.AddSyncStep(NewSimpleStep("drop role meta data", func(ctx context.Context) ([]nestedStep, error) {
		err := core.meta.DropRole(ctx, util.DefaultTenant, roleName)
		if err != nil {
			log.Ctx(ctx).Warn("drop role mata data failed", zap.String("role_name", roleName), zap.Error(err))
		}
		return nil, err
	}))
	redoTask.AddAsyncStep(NewSimpleStep("drop the privilege list of this role", func(ctx context.Context) ([]nestedStep, error) {
		if !foreDrop {
			return nil, nil
		}
		err := core.meta.DropGrant(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: roleName})
		if err != nil {
			log.Ctx(ctx).Warn("drop the privilege list failed for the role", zap.String("role_name", roleName), zap.Error(err))
		}
		return nil, err
	}))
	redoTask.AddAsyncStep(NewSimpleStep("drop role cache", func(ctx context.Context) ([]nestedStep, error) {
		err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: int32(typeutil.CacheDropRole),
			OpKey:  roleName,
		})
		if err != nil {
			log.Ctx(ctx).Warn("delete user role cache failed for the role", zap.String("role_name", roleName), zap.Error(err))
		}
		return nil, err
	}))
	return redoTask.Execute(ctx)
}

func executeOperateUserRoleTaskSteps(ctx context.Context, core *Core, in *milvuspb.OperateUserRoleRequest) error {
	username := in.Username
	roleName := in.RoleName
	operateType := in.Type
	redoTask := newBaseRedoTask(core.stepExecutor)
	redoTask.AddSyncStep(NewSimpleStep("operate user role meta data", func(ctx context.Context) ([]nestedStep, error) {
		err := core.meta.OperateUserRole(ctx, util.DefaultTenant, &milvuspb.UserEntity{Name: username}, &milvuspb.RoleEntity{Name: roleName}, operateType)
		if err != nil && !common.IsIgnorableError(err) {
			log.Ctx(ctx).Warn("operate user role mata data failed",
				zap.String("username", username), zap.String("role_name", roleName),
				zap.Any("operate_type", operateType),
				zap.Error(err))
			return nil, err
		}
		return nil, nil
	}))
	redoTask.AddAsyncStep(NewSimpleStep("operate user role cache", func(ctx context.Context) ([]nestedStep, error) {
		var opType int32
		switch operateType {
		case milvuspb.OperateUserRoleType_AddUserToRole:
			opType = int32(typeutil.CacheAddUserToRole)
		case milvuspb.OperateUserRoleType_RemoveUserFromRole:
			opType = int32(typeutil.CacheRemoveUserFromRole)
		default:
			errMsg := "invalid operate type for the OperateUserRole api"
			log.Ctx(ctx).Warn(errMsg,
				zap.String("username", username), zap.String("role_name", roleName),
				zap.Any("operate_type", operateType),
			)
			return nil, nil
		}
		if err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: opType,
			OpKey:  funcutil.EncodeUserRoleCache(username, roleName),
		}); err != nil {
			log.Ctx(ctx).Warn("fail to refresh policy info cache",
				zap.String("username", username), zap.String("role_name", roleName),
				zap.Any("operate_type", operateType),
				zap.Error(err),
			)
			return nil, err
		}
		return nil, nil
	}))
	return redoTask.Execute(ctx)
}

func executeOperatePrivilegeTaskSteps(ctx context.Context, core *Core, in *milvuspb.OperatePrivilegeRequest) error {
	privName := in.Entity.Grantor.Privilege.Name
	redoTask := newBaseRedoTask(core.stepExecutor)
	redoTask.AddSyncStep(NewSimpleStep("operate privilege meta data", func(ctx context.Context) ([]nestedStep, error) {
		if !util.IsAnyWord(privName) {
			// set up privilege name for metastore
			dbPrivName, err := core.getMetastorePrivilegeName(ctx, privName)
			if err != nil {
				return nil, err
			}
			in.Entity.Grantor.Privilege.Name = dbPrivName
		}

		err := core.meta.OperatePrivilege(ctx, util.DefaultTenant, in.Entity, in.Type)
		if err != nil && !common.IsIgnorableError(err) {
			log.Ctx(ctx).Warn("fail to operate the privilege", zap.Any("in", in), zap.Error(err))
			return nil, err
		}
		return nil, nil
	}))
	redoTask.AddAsyncStep(NewSimpleStep("operate privilege cache", func(ctx context.Context) ([]nestedStep, error) {
		// set back to expand privilege group
		in.Entity.Grantor.Privilege.Name = privName
		var opType int32
		switch in.Type {
		case milvuspb.OperatePrivilegeType_Grant:
			opType = int32(typeutil.CacheGrantPrivilege)
		case milvuspb.OperatePrivilegeType_Revoke:
			opType = int32(typeutil.CacheRevokePrivilege)
		default:
			log.Ctx(ctx).Warn("invalid operate type for the OperatePrivilege api", zap.Any("in", in))
			return nil, nil
		}
		grants := []*milvuspb.GrantEntity{in.Entity}

		allGroups, err := core.getPrivilegeGroups(ctx)
		if err != nil {
			return nil, err
		}
		groups := lo.SliceToMap(allGroups, func(group *milvuspb.PrivilegeGroupInfo) (string, []*milvuspb.PrivilegeEntity) {
			return group.GroupName, group.Privileges
		})
		expandGrants, err := core.expandPrivilegeGroups(ctx, grants, groups)
		if err != nil {
			return nil, err
		}
		// if there is same grant in the other privilege groups, the grant should not be removed from the cache
		if in.Type == milvuspb.OperatePrivilegeType_Revoke {
			metaGrants, err := core.meta.SelectGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{
				Role:   in.Entity.Role,
				DbName: in.Entity.DbName,
			})
			if err != nil {
				return nil, err
			}
			metaExpandGrants, err := core.expandPrivilegeGroups(ctx, metaGrants, groups)
			if err != nil {
				return nil, err
			}
			expandGrants = lo.Filter(expandGrants, func(g1 *milvuspb.GrantEntity, _ int) bool {
				return !lo.ContainsBy(metaExpandGrants, func(g2 *milvuspb.GrantEntity) bool {
					return proto.Equal(g1, g2)
				})
			})
		}
		if err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: opType,
			OpKey:  funcutil.PolicyForPrivileges(expandGrants),
		}); err != nil {
			log.Ctx(ctx).Warn("fail to refresh policy info cache", zap.Any("in", in), zap.Error(err))
			return nil, err
		}
		return nil, nil
	}))

	return redoTask.Execute(ctx)
}

func executeRestoreRBACTaskSteps(ctx context.Context, core *Core, in *milvuspb.RestoreRBACMetaRequest) error {
	redoTask := newBaseRedoTask(core.stepExecutor)
	redoTask.AddSyncStep(NewSimpleStep("restore rbac meta data", func(ctx context.Context) ([]nestedStep, error) {
		if err := core.meta.RestoreRBAC(ctx, util.DefaultTenant, in.RBACMeta); err != nil {
			log.Ctx(ctx).Warn("fail to restore rbac meta data", zap.Any("in", in), zap.Error(err))
			return nil, err
		}
		return nil, nil
	}))
	redoTask.AddAsyncStep(NewSimpleStep("operate privilege cache", func(ctx context.Context) ([]nestedStep, error) {
		if err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: int32(typeutil.CacheRefresh),
		}); err != nil {
			log.Ctx(ctx).Warn("fail to refresh policy info cache", zap.Any("in", in), zap.Error(err))
			return nil, err
		}
		return nil, nil
	}))

	return redoTask.Execute(ctx)
}

func executeOperatePrivilegeGroupTaskSteps(ctx context.Context, core *Core, in *milvuspb.OperatePrivilegeGroupRequest) error {
	redoTask := newBaseRedoTask(core.stepExecutor)
	redoTask.AddSyncStep(NewSimpleStep("operate privilege group", func(ctx context.Context) ([]nestedStep, error) {
		groups, err := core.meta.ListPrivilegeGroups(ctx)
		if err != nil && !common.IsIgnorableError(err) {
			log.Ctx(ctx).Warn("fail to list privilege groups", zap.Error(err))
			return nil, err
		}
		currGroups := lo.SliceToMap(groups, func(group *milvuspb.PrivilegeGroupInfo) (string, []*milvuspb.PrivilegeEntity) {
			return group.GroupName, group.Privileges
		})

		// get roles granted to the group
		roles, err := core.meta.GetPrivilegeGroupRoles(ctx, in.GroupName)
		if err != nil {
			return nil, err
		}

		newGroups := make(map[string][]*milvuspb.PrivilegeEntity)
		for k, v := range currGroups {
			if k != in.GroupName {
				newGroups[k] = v
				continue
			}
			switch in.Type {
			case milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup:
				newPrivs := lo.Union(v, in.Privileges)
				newGroups[k] = lo.UniqBy(newPrivs, func(p *milvuspb.PrivilegeEntity) string {
					return p.Name
				})

				// check if privileges are the same object type
				objectTypes := lo.SliceToMap(newPrivs, func(p *milvuspb.PrivilegeEntity) (string, struct{}) {
					return util.GetObjectType(p.Name), struct{}{}
				})
				if len(objectTypes) > 1 {
					return nil, errors.New("privileges are not the same object type")
				}
			case milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup:
				newPrivs, _ := lo.Difference(v, in.Privileges)
				newGroups[k] = newPrivs
			default:
				return nil, errors.New("invalid operate type")
			}
		}

		var rolesToRevoke []*milvuspb.GrantEntity
		var rolesToGrant []*milvuspb.GrantEntity
		compareGrants := func(a, b *milvuspb.GrantEntity) bool {
			return a.Role.Name == b.Role.Name &&
				a.Object.Name == b.Object.Name &&
				a.ObjectName == b.ObjectName &&
				a.Grantor.User.Name == b.Grantor.User.Name &&
				a.Grantor.Privilege.Name == b.Grantor.Privilege.Name &&
				a.DbName == b.DbName
		}
		for _, role := range roles {
			grants, err := core.meta.SelectGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{
				Role:   role,
				DbName: util.AnyWord,
			})
			if err != nil {
				return nil, err
			}
			currGrants, err := core.expandPrivilegeGroups(ctx, grants, currGroups)
			if err != nil {
				return nil, err
			}
			newGrants, err := core.expandPrivilegeGroups(ctx, grants, newGroups)
			if err != nil {
				return nil, err
			}

			toRevoke := lo.Filter(currGrants, func(item *milvuspb.GrantEntity, _ int) bool {
				return !lo.ContainsBy(newGrants, func(newItem *milvuspb.GrantEntity) bool {
					return compareGrants(item, newItem)
				})
			})

			toGrant := lo.Filter(newGrants, func(item *milvuspb.GrantEntity, _ int) bool {
				return !lo.ContainsBy(currGrants, func(currItem *milvuspb.GrantEntity) bool {
					return compareGrants(item, currItem)
				})
			})

			rolesToRevoke = append(rolesToRevoke, toRevoke...)
			rolesToGrant = append(rolesToGrant, toGrant...)
		}

		if len(rolesToRevoke) > 0 {
			opType := int32(typeutil.CacheRevokePrivilege)
			if err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
				OpType: opType,
				OpKey:  funcutil.PolicyForPrivileges(rolesToRevoke),
			}); err != nil {
				log.Ctx(ctx).Warn("fail to refresh policy info cache for revoke privileges in operate privilege group", zap.Any("in", in), zap.Error(err))
				return nil, err
			}
		}

		if len(rolesToGrant) > 0 {
			opType := int32(typeutil.CacheGrantPrivilege)
			if err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
				OpType: opType,
				OpKey:  funcutil.PolicyForPrivileges(rolesToGrant),
			}); err != nil {
				log.Ctx(ctx).Warn("fail to refresh policy info cache for grants privilege in operate privilege group", zap.Any("in", in), zap.Error(err))
				return nil, err
			}
		}
		return nil, nil
	}))

	redoTask.AddSyncStep(NewSimpleStep("operate privilege group meta data", func(ctx context.Context) ([]nestedStep, error) {
		err := core.meta.OperatePrivilegeGroup(ctx, in.GroupName, in.Privileges, in.Type)
		if err != nil && !common.IsIgnorableError(err) {
			log.Ctx(ctx).Warn("fail to operate privilege group", zap.Error(err))
		}
		return nil, err
	}))

	return redoTask.Execute(ctx)
}
