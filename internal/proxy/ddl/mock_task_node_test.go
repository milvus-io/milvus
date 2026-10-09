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

package ddl

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// mockTaskNode is a taskmodel.TaskNode stub for white-box tests whose methods
// are not invoked on the exercised paths. The privilege checks mirror the
// root-owned enforcement so authorization-gated tests behave as in production.
type mockTaskNode struct {
	metaCache metacache.Cache
	chMgr     channelmgr.ChannelsMgr
	mixCoord  types.MixCoordClient
	lbPolicy  shardclient.LBPolicy
}

func (n *mockTaskNode) GetMetaCache() metacache.Cache        { return n.metaCache }
func (n *mockTaskNode) MixCoord() types.MixCoordClient       { return n.mixCoord }
func (n *mockTaskNode) LBPolicy() shardclient.LBPolicy       { return n.lbPolicy }
func (n *mockTaskNode) ShardMgr() shardclient.ShardClientMgr { return nil }
func (n *mockTaskNode) ChMgr() channelmgr.ChannelsMgr        { return n.chMgr }
func (n *mockTaskNode) TsoAllocator() taskmodel.TsoAllocator { return nil }
func (n *mockTaskNode) ResolveRLSEnforcement(ctx context.Context, _ metacache.Cache, rlsEnabled, rlsForce, skipRLS bool, _, _, _ string) (bool, error) {
	if !rlsEnabled || !skipRLS {
		return rlsEnabled, nil
	}
	if rlsForce {
		return false, merr.WrapErrPrivilegeNotPermitted(
			"%s operation denied by RLS: skip_rls is not allowed when rls.force is enabled on collection %s",
			"import", "")
	}
	return false, nil
}

func (n *mockTaskNode) CheckManageRLSPrivilege(ctx context.Context, _ metacache.Cache, _ *milvuspb.AlterCollectionRequest, dbName, collectionName string) error {
	if !paramtable.Get().CommonCfg.AuthorizationEnabled.GetAsBool() {
		return nil
	}
	privilegeName := commonpb.ObjectPrivilege_PrivilegeManageRLS.String()
	username, _, err := contextutil.GetAuthInfoFromContext(ctx)
	if err != nil {
		return merr.WrapErrPrivilegeNotAuthenticated("fail to get authentication info: %v", err)
	}
	if !paramtable.Get().CommonCfg.RootShouldBindRole.GetAsBool() && username == util.UserRoot {
		return nil
	}
	roleNames := privilege.GetPrivilegeCache().GetUserRole(username)
	roleNames = append(roleNames, util.RolePublic)
	object := funcutil.PolicyForResource(dbName, commonpb.ObjectType_Collection.String(), collectionName)
	enforcer := privilege.GetEnforcer()
	for _, roleName := range roleNames {
		isPermit, cached, version := privilege.GetResultCache(roleName, object, privilegeName)
		if !cached {
			isPermit, err = enforcer.Enforce(roleName, object, privilegeName)
			if err != nil {
				return err
			}
			privilege.SetResultCache(roleName, object, privilegeName, isPermit, version)
		}
		if isPermit {
			return nil
		}
	}
	return merr.WrapErrPrivilegeNotPermitted("%s is required", privilegeName)
}

func (n *mockTaskNode) CheckClusterPrivilege(ctx context.Context, _ interface{}, _ string, objectPrivilege string) error {
	if !paramtable.Get().CommonCfg.AuthorizationEnabled.GetAsBool() {
		return nil
	}
	username, _, err := contextutil.GetAuthInfoFromContext(ctx)
	if err != nil {
		return merr.WrapErrPrivilegeNotAuthenticated("fail to get authentication info: %v", err)
	}
	if !paramtable.Get().CommonCfg.RootShouldBindRole.GetAsBool() && username == util.UserRoot {
		return nil
	}
	roleNames := privilege.GetPrivilegeCache().GetUserRole(username)
	roleNames = append(roleNames, util.RolePublic)
	object := funcutil.PolicyForResource(util.AnyWord, commonpb.ObjectType_Global.String(), util.AnyWord)
	enforcer := privilege.GetEnforcer()
	for _, roleName := range roleNames {
		isPermit, cached, version := privilege.GetResultCache(roleName, object, objectPrivilege)
		if !cached {
			isPermit, err = enforcer.Enforce(roleName, object, objectPrivilege)
			if err != nil {
				return err
			}
			privilege.SetResultCache(roleName, object, objectPrivilege, isPermit, version)
		}
		if isPermit {
			return nil
		}
	}
	return merr.WrapErrPrivilegeNotPermitted("%s is required", objectPrivilege)
}
