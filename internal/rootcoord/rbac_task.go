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
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func executeOperatePrivilegeTaskSteps(ctx context.Context, core *Core, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	privName := entity.Grantor.Privilege.Name
	if err := func() error {
		// set up privilege name for metastore
		dbPrivName, err := core.getMetastorePrivilegeName(ctx, privName)
		if err != nil {
			return err
		}
		entity.Grantor.Privilege.Name = dbPrivName

		err = core.meta.OperatePrivilege(ctx, util.DefaultTenant, entity, operateType)
		if err != nil && !common.IsIgnorableError(err) {
			log.Ctx(ctx).Warn("fail to operate the privilege", zap.Any("in", entity), zap.Error(err))
			return err
		}
		return nil
	}(); err != nil {
		return errors.Wrap(err, "failed to operate the privilege")
	}

	if err := func() error {
		// set back to expand privilege group
		entity.Grantor.Privilege.Name = privName
		var opType int32
		switch operateType {
		case milvuspb.OperatePrivilegeType_Grant:
			opType = int32(typeutil.CacheGrantPrivilege)
		case milvuspb.OperatePrivilegeType_Revoke:
			opType = int32(typeutil.CacheRevokePrivilege)
		default:
			log.Ctx(ctx).Warn("invalid operate type for the OperatePrivilege api", zap.Any("operate_type", operateType))
			return errors.New("invalid operate type for the OperatePrivilege api")
		}
		grants := []*milvuspb.GrantEntity{entity}

		allGroups, err := core.getDefaultAndCustomPrivilegeGroups(ctx)
		if err != nil {
			return err
		}
		groups := lo.SliceToMap(allGroups, func(group *milvuspb.PrivilegeGroupInfo) (string, []*milvuspb.PrivilegeEntity) {
			return group.GroupName, group.Privileges
		})
		expandGrants, err := core.expandPrivilegeGroups(ctx, grants, groups)
		if err != nil {
			return err
		}
		// if there is same grant in the other privilege groups, the grant should not be removed from the cache
		if operateType == milvuspb.OperatePrivilegeType_Revoke {
			metaGrants, err := core.meta.SelectGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{
				Role:   entity.Role,
				DbName: entity.DbName,
			})
			if err != nil {
				return err
			}
			metaExpandGrants, err := core.expandPrivilegeGroups(ctx, metaGrants, groups)
			if err != nil {
				return err
			}
			expandGrants = lo.Filter(expandGrants, func(g1 *milvuspb.GrantEntity, _ int) bool {
				return !lo.ContainsBy(metaExpandGrants, func(g2 *milvuspb.GrantEntity) bool {
					return proto.Equal(g1, g2)
				})
			})
		}
		if len(expandGrants) > 0 {
			if err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
				OpType: opType,
				OpKey:  funcutil.PolicyForPrivileges(expandGrants),
			}); err != nil {
				log.Ctx(ctx).Warn("fail to refresh policy info cache", zap.Any("in", entity), zap.Error(err))
				return err
			}
		}
		return nil
	}(); err != nil {
		return errors.Wrap(err, "failed to refresh policy info cache")
	}
	return nil
}

func executeOperatePrivilegeGroupTaskSteps(ctx context.Context, core *Core, in *milvuspb.PrivilegeGroupInfo, operateType milvuspb.OperatePrivilegeGroupType) error {
	if err := func() error {
		groups, err := core.meta.ListPrivilegeGroups(ctx)
		if err != nil && !common.IsIgnorableError(err) {
			log.Ctx(ctx).Warn("fail to list privilege groups", zap.Error(err))
			return err
		}
		currGroups := lo.SliceToMap(groups, func(group *milvuspb.PrivilegeGroupInfo) (string, []*milvuspb.PrivilegeEntity) {
			return group.GroupName, group.Privileges
		})

		// get roles granted to the group
		roles, err := core.meta.GetPrivilegeGroupRoles(ctx, in.GroupName)
		if err != nil {
			return err
		}
		newGroups := make(map[string][]*milvuspb.PrivilegeEntity)
		for k, v := range currGroups {
			if k != in.GroupName {
				newGroups[k] = v
				continue
			}
			switch operateType {
			case milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup:
				newPrivs := lo.Union(v, in.Privileges)
				newGroups[k] = lo.UniqBy(newPrivs, func(p *milvuspb.PrivilegeEntity) string {
					return p.Name
				})
			case milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup:
				newPrivs, _ := lo.Difference(v, in.Privileges)
				newGroups[k] = newPrivs
			default:
				return errors.New("invalid operate type")
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
				return err
			}
			currGrants, err := core.expandPrivilegeGroups(ctx, grants, currGroups)
			if err != nil {
				return err
			}
			newGrants, err := core.expandPrivilegeGroups(ctx, grants, newGroups)
			if err != nil {
				return err
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
				return err
			}
		}

		if len(rolesToGrant) > 0 {
			opType := int32(typeutil.CacheGrantPrivilege)
			if err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
				OpType: opType,
				OpKey:  funcutil.PolicyForPrivileges(rolesToGrant),
			}); err != nil {
				log.Ctx(ctx).Warn("fail to refresh policy info cache for grants privilege in operate privilege group", zap.Any("in", in), zap.Error(err))
				return err
			}
		}
		return nil
	}(); err != nil {
		return errors.Wrap(err, "failed to refresh policy info cache")
	}
	if err := core.meta.OperatePrivilegeGroup(ctx, in.GroupName, in.Privileges, operateType); err != nil && !common.IsIgnorableError(err) {
		log.Ctx(ctx).Warn("fail to operate privilege group", zap.Error(err))
		return errors.Wrap(err, "failed to operate privilege group")
	}
	return nil
}
