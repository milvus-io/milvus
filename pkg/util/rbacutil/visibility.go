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

package rbacutil

import (
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
)

const CollectionVisibilityFormatVersion = 1

// CollectionVisibilityName preserves RootCoord's raw-grant visibility contract.
// In particular, an empty custom group can grant visibility without granting an
// action. Expanded Casbin policies cannot reconstruct that distinction.
func CollectionVisibilityName(grant *milvuspb.GrantEntity, isVisibilityGroup func(string) bool) (string, bool) {
	object := grant.GetObject().GetName()
	privilege := grant.GetGrantor().GetPrivilege().GetName()
	if object == commonpb.ObjectType_Global.String() &&
		privilege == util.PrivilegeNameForAPI(commonpb.ObjectPrivilege_PrivilegeAll.String()) {
		return util.AnyWord, true
	}
	if object == commonpb.ObjectType_Collection.String() || isVisibilityGroup(privilege) {
		return grant.GetObjectName(), true
	}
	return "", false
}

// CompileCollectionVisibility compiles raw grants, before action/group expansion.
// groupNames contains custom groups and collection-level built-in groups only.
func CompileCollectionVisibility(grants []*milvuspb.GrantEntity, groupNames []string) *internalpb.CollectionVisibilitySnapshot {
	groups := make(map[string]bool, len(groupNames))
	for _, name := range groupNames {
		groups[name] = true
	}
	roles := make(map[string]map[[2]string]struct{})
	for _, grant := range grants {
		role := grant.GetRole().GetName()
		if role == "" || role == util.RolePublic {
			continue
		}
		name, visible := CollectionVisibilityName(grant, func(name string) bool { return groups[name] })
		if !visible {
			continue
		}
		if roles[role] == nil {
			roles[role] = make(map[[2]string]struct{})
		}
		roles[role][[2]string{grant.GetDbName(), name}] = struct{}{}
	}
	result := &internalpb.CollectionVisibilitySnapshot{FormatVersion: CollectionVisibilityFormatVersion}
	for role, rules := range roles {
		entry := &internalpb.RoleCollectionVisibility{Role: role}
		for rule := range rules {
			entry.Rules = append(entry.Rules, &internalpb.CollectionVisibilityRule{DbName: rule[0], CollectionName: rule[1]})
		}
		sort.Slice(entry.Rules, func(i, j int) bool {
			a, b := entry.Rules[i], entry.Rules[j]
			if a.DbName != b.DbName {
				return a.DbName < b.DbName
			}
			return a.CollectionName < b.CollectionName
		})
		result.Roles = append(result.Roles, entry)
	}
	sort.Slice(result.Roles, func(i, j int) bool { return result.Roles[i].Role < result.Roles[j].Role })
	return result
}

// CollectionVisibilityIndex is owned by the privilege cache's mutex. Matching is
// exact or the literal '*', just like RootCoord; it is deliberately not globMatch.
type CollectionVisibilityIndex map[string]map[string]map[string]struct{}

func (index CollectionVisibilityIndex) Replace(roles []*internalpb.RoleCollectionVisibility) {
	for _, role := range roles {
		dbs := make(map[string]map[string]struct{})
		for _, rule := range role.GetRules() {
			if dbs[rule.GetDbName()] == nil {
				dbs[rule.GetDbName()] = make(map[string]struct{})
			}
			dbs[rule.GetDbName()][rule.GetCollectionName()] = struct{}{}
		}
		index[role.GetRole()] = dbs
	}
}

func (index CollectionVisibilityIndex) CanSee(roles []string, database, collection string) bool {
	for _, role := range roles {
		if role == util.RoleAdmin {
			return true
		}
		if role == util.RolePublic {
			continue
		}
		dbs := index[role]
		contains := func(collections map[string]struct{}) bool {
			if _, ok := collections[collection]; ok {
				return true
			}
			_, ok := collections[util.AnyWord]
			return ok
		}
		if database == util.AnyWord {
			// RC's wildcard request DB selects grants from every database.
			for _, collections := range dbs {
				if contains(collections) {
					return true
				}
			}
		} else if contains(dbs[database]) || contains(dbs[util.AnyWord]) {
			return true
		}
	}
	return false
}
