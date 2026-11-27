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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestCollectionVisibilityProjection(t *testing.T) {
	grant := func(role, db, collection, object, privilege string) *milvuspb.GrantEntity {
		return &milvuspb.GrantEntity{
			Role: &milvuspb.RoleEntity{Name: role}, DbName: db, ObjectName: collection,
			Object:  &milvuspb.ObjectEntity{Name: object},
			Grantor: &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: privilege}},
		}
	}
	grants := []*milvuspb.GrantEntity{
		grant("reader", "tenant_a", "orders", "Collection", "Search"),
		grant("reader", "tenant_a", "orders", "Collection", "Query"),
		grant("public", "*", "*", "Global", "All"),
		grant("global", "tenant_b", "ignored", "Global", "All"),
		grant("empty_group", "tenant_b", "orders", "Global", "EmptyGroup"),
		grant("builtin", "tenant_b", "*", "Global", "CollectionReadOnly"),
		grant("database_only", "tenant_b", "*", "Global", "DatabaseReadOnly"),
		grant("cross_db", "*", "orders", "Collection", "Query"),
		grant("literal", "tenant_b", "ord*", "Collection", "Query"),
	}
	snapshot := CompileCollectionVisibility(grants, []string{"EmptyGroup", "CollectionReadOnly"})
	require.EqualValues(t, CollectionVisibilityFormatVersion, snapshot.GetFormatVersion())
	index := make(CollectionVisibilityIndex)
	index.Replace(snapshot.GetRoles())
	require.Len(t, index["reader"]["tenant_a"], 1, "overlapping actions contribute one visible resource")
	for _, tc := range []struct {
		name, db, collection string
		roles                []string
		want                 bool
	}{
		{"own database", "tenant_a", "orders", []string{"reader"}, true},
		{"same name other database", "tenant_b", "orders", []string{"reader"}, false},
		{"other resource", "tenant_a", "secret", []string{"reader"}, false},
		{"public method permission is insufficient", "tenant_b", "orders", []string{"public"}, false},
		{"admin without grants", "tenant_b", "secret", []string{"admin"}, true},
		{"empty custom group", "tenant_b", "orders", []string{"empty_group"}, true},
		{"collection builtin", "tenant_b", "secret", []string{"builtin"}, true},
		{"database builtin is not collection visibility", "tenant_b", "orders", []string{"database_only"}, false},
		{"global all within grant DB", "tenant_b", "secret", []string{"global"}, true},
		{"global all outside grant DB", "tenant_a", "secret", []string{"global"}, false},
		{"wildcard database", "tenant_c", "orders", []string{"cross_db"}, true},
		{"wildcard database keeps collection scope", "tenant_c", "secret", []string{"cross_db"}, false},
		{"role union", "tenant_b", "orders", []string{"reader", "empty_group"}, true},
		{"no roles", "tenant_b", "orders", nil, false},
		{"literal star is not glob", "tenant_b", "orders", []string{"literal"}, false},
		{"literal name", "tenant_b", "ord*", []string{"literal"}, true},
		{"RC wildcard database selection", "*", "orders", []string{"reader"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, index.CanSee(tc.roles, tc.db, tc.collection))
		})
	}

	// A role replacement removes only that role, not grants held through another
	// role. An explicit empty replacement revokes the final visible resource.
	index.Replace([]*internalpb.RoleCollectionVisibility{{Role: "reader"}})
	require.False(t, index.CanSee([]string{"reader"}, "tenant_a", "orders"))
	require.True(t, index.CanSee([]string{"cross_db"}, "tenant_a", "orders"))
}
