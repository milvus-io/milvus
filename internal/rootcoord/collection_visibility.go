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

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/rbacutil"
)

func collectionVisibilityGroups(groups []*milvuspb.PrivilegeGroupInfo) []string {
	var names []string
	for _, group := range groups {
		name := group.GetGroupName()
		if !util.IsBuiltinPrivilegeGroup(name) || Params.RbacConfig.IsCollectionPrivilegeGroup(name) {
			names = append(names, name)
		}
	}
	return names
}

// A complete role replacement preserves visibility supported by another grant,
// and an explicit empty role removes the last visible resource after revoke.
func (c *Core) collectionVisibilityForRole(ctx context.Context, role *milvuspb.RoleEntity, groups []*milvuspb.PrivilegeGroupInfo) (*internalpb.CollectionVisibilitySnapshot, error) {
	grants, err := c.meta.SelectGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{Role: role, DbName: util.AnyWord})
	if err != nil {
		return nil, err
	}
	result := rbacutil.CompileCollectionVisibility(grants, collectionVisibilityGroups(groups))
	if len(result.Roles) == 0 {
		result.Roles = []*internalpb.RoleCollectionVisibility{{Role: role.GetName()}}
	}
	return result, nil
}
