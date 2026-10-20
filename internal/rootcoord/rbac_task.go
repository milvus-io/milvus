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
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// policyKeysOf returns the de-duplicated proxy policy cache keys of the
// expanded grants, in first-seen order. A key is role + resource + privilege
// and does not carry the grantor: grants that differ only in who granted them
// are the same key, which is the unit the proxy cache stores and revokes.
func policyKeysOf(grants []*milvuspb.GrantEntity) []string {
	return lo.Uniq(lo.Map(grants, func(g *milvuspb.GrantEntity, _ int) string {
		return funcutil.PolicyForPrivilege(g.Role.Name, g.Object.Name, g.ObjectName, g.Grantor.Privilege.Name, g.DbName)
	}))
}

// policyKeysWithout returns the keys that are not in exclude, keeping their order.
func policyKeysWithout(keys []string, exclude []string) []string {
	excluded := typeutil.NewSet(exclude...)
	return lo.Filter(keys, func(key string, _ int) bool {
		return !excluded.Contain(key)
	})
}

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
			mlog.Warn(ctx, "fail to operate the privilege", mlog.Any("in", entity), mlog.Err(err))
			return err
		}
		return nil
	}(); err != nil {
		return merr.Wrap(err, "failed to operate the privilege")
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
			mlog.Warn(ctx, "invalid operate type for the OperatePrivilege api", mlog.Any("operate_type", operateType))
			return merr.WrapErrParameterInvalidMsg("invalid operate type for the OperatePrivilege api")
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
		keys := policyKeysOf(expandGrants)
		// a policy key still produced by the remaining grants of the role (another
		// privilege group, or the same privilege granted by another user) must
		// stay in the cache
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
			keys = policyKeysWithout(keys, policyKeysOf(metaExpandGrants))
		}
		if len(keys) > 0 {
			if err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
				OpType: opType,
				OpKey:  strings.Join(keys, "|"),
			}); err != nil {
				mlog.Warn(ctx, "fail to refresh policy info cache", mlog.Any("in", entity), mlog.Err(err))
				return err
			}
		}
		return nil
	}(); err != nil {
		return merr.Wrap(err, "failed to refresh policy info cache")
	}
	return nil
}

func executeOperatePrivilegeGroupTaskSteps(ctx context.Context, core *Core, in *milvuspb.PrivilegeGroupInfo, operateType milvuspb.OperatePrivilegeGroupType) error {
	if err := func() error {
		// Expand with the built-in groups as well, the same set the grant/revoke
		// path and ListPolicy use to build the proxy policy cache. A privilege that
		// a role still holds through a built-in group (e.g. Query via
		// CollectionReadOnly) must survive its removal from a custom group;
		// expanding only the custom groups made it look revoked.
		groups, err := core.getDefaultAndCustomPrivilegeGroups(ctx)
		if err != nil && !common.IsIgnorableError(err) {
			mlog.Warn(ctx, "fail to list privilege groups", mlog.Err(err))
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
				newPrivs := append(append([]*milvuspb.PrivilegeEntity{}, v...), in.Privileges...)
				newGroups[k] = lo.UniqBy(newPrivs, func(p *milvuspb.PrivilegeEntity) string {
					return p.Name
				})
			case milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup:
				removedPrivs := lo.SliceToMap(in.Privileges, func(p *milvuspb.PrivilegeEntity) (string, struct{}) {
					return p.Name, struct{}{}
				})
				newGroups[k] = lo.Filter(v, func(p *milvuspb.PrivilegeEntity, _ int) bool {
					_, ok := removedPrivs[p.Name]
					return !ok
				})
			default:
				return merr.WrapErrParameterInvalidMsg("invalid operate type")
			}
		}

		// Diff the policy keys, not the grants: the cache holds keys, a key has no
		// grantor, and a set lookup keeps the diff linear in the number of
		// expanded grants (a role holding a built-in group on many collections
		// expands into thousands of them).
		var keysToRevoke []string
		var keysToGrant []string
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

			currKeys := policyKeysOf(currGrants)
			newKeys := policyKeysOf(newGrants)
			keysToRevoke = append(keysToRevoke, policyKeysWithout(currKeys, newKeys)...)
			keysToGrant = append(keysToGrant, policyKeysWithout(newKeys, currKeys)...)
		}

		if len(keysToRevoke) > 0 {
			opType := int32(typeutil.CacheRevokePrivilege)
			if err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
				OpType: opType,
				OpKey:  strings.Join(keysToRevoke, "|"),
			}); err != nil {
				mlog.Warn(ctx, "fail to refresh policy info cache for revoke privileges in operate privilege group", mlog.Any("in", in), mlog.Err(err))
				return err
			}
		}

		if len(keysToGrant) > 0 {
			opType := int32(typeutil.CacheGrantPrivilege)
			if err := core.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
				OpType: opType,
				OpKey:  strings.Join(keysToGrant, "|"),
			}); err != nil {
				mlog.Warn(ctx, "fail to refresh policy info cache for grants privilege in operate privilege group", mlog.Any("in", in), mlog.Err(err))
				return err
			}
		}
		return nil
	}(); err != nil {
		return merr.Wrap(err, "failed to refresh policy info cache")
	}
	if err := core.meta.OperatePrivilegeGroup(ctx, in.GroupName, in.Privileges, operateType); err != nil && !common.IsIgnorableError(err) {
		mlog.Warn(ctx, "fail to operate privilege group", mlog.Err(err))
		return merr.Wrap(err, "failed to operate privilege group")
	}
	return nil
}
