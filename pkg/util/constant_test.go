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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

func TestGetReplicateConfigurationPrivilege(t *testing.T) {
	// Verify privilege is in ClusterReadOnlyPrivileges
	// The privileges are stored with the "Privilege" prefix stripped via MetaStore2API
	privilegeName := MetaStore2API(commonpb.ObjectPrivilege_PrivilegeGetReplicateConfiguration.String())
	found := false
	for _, p := range ClusterReadOnlyPrivileges {
		if p == privilegeName {
			found = true
			break
		}
	}
	assert.True(t, found, "PrivilegeGetReplicateConfiguration should be in ClusterReadOnlyPrivileges")
}

func TestExprPrivilegeDefinition(t *testing.T) {
	assert.Equal(t, "Expr", MetaStore2API(PrivilegeExpr))
	assert.Equal(t, "Expr", PrivilegeNameForAPI(PrivilegeExpr))
	assert.Equal(t, PrivilegeExpr, PrivilegeNameForMetastore("Expr"))
	assert.True(t, IsPrivilegeNameDefined("Expr"))
	assert.Equal(t, milvuspb.PrivilegeLevel_Cluster.String(), GetPrivilegeLevel("Expr"))
}

// TestPrivilegeImportBinlogRegistration pins the three registrations that make
// PrivilegeImportBinlog usable. Missing any one of them fails differently and
// none of them fails loudly at runtime:
//   - ObjectPrivileges[Global]: the privilege cannot be granted at all
//   - AdminPrivilegeGroup:      the builtin admin role silently lacks it
//   - ClusterAdminPrivileges:   GetPrivilegeLevel misclassifies it, so the
//     proxy interceptor authorizes it against the connection's database
//     instead of cluster-wide
func TestPrivilegeImportBinlogRegistration(t *testing.T) {
	name := commonpb.ObjectPrivilege_PrivilegeImportBinlog.String()

	assert.Contains(t, ObjectPrivileges[commonpb.ObjectType_Global.String()],
		MetaStore2API(name), "must be grantable on the Global object")

	assert.Contains(t, AdminPrivilegeGroup, name,
		"builtin admin role must carry it")

	assert.Contains(t, ClusterAdminPrivileges, MetaStore2API(name),
		"must be classified as a cluster-level privilege")

	assert.Equal(t, milvuspb.PrivilegeLevel_Cluster.String(),
		GetPrivilegeLevel(MetaStore2API(name)),
		"cluster level is what makes the interceptor authorize it with dbName=AnyWord")

	assert.NotContains(t, CollectionReadWritePrivileges, MetaStore2API(name),
		"must not ride along with collection-level roles")
}
