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

func TestSkipRLSPrivilegeDefinition(t *testing.T) {
	privilegeName := MetaStore2API(commonpb.ObjectPrivilege_PrivilegeSkipRLS.String())
	assert.Equal(t, "SkipRLS", privilegeName)
	assert.Equal(t, commonpb.ObjectType_Collection.String(), GetObjectType(privilegeName))
	assert.Equal(t, commonpb.ObjectPrivilege_PrivilegeSkipRLS.String(), PrivilegeNameForMetastore(privilegeName))
	assert.True(t, IsPrivilegeNameDefined(privilegeName))
	assert.Equal(t, milvuspb.PrivilegeLevel_Collection.String(), GetPrivilegeLevel(privilegeName))
	assert.Contains(t, CollectionAdminPrivileges, privilegeName)
	assert.Contains(t, AdminPrivilegeGroup, commonpb.ObjectPrivilege_PrivilegeSkipRLS.String())
	assert.NotContains(t, ReadWritePrivilegeGroup, commonpb.ObjectPrivilege_PrivilegeSkipRLS.String())
}

func TestRLSManagementPrivilegeDefinitions(t *testing.T) {
	for _, privilege := range []commonpb.ObjectPrivilege{
		commonpb.ObjectPrivilege_PrivilegeViewRLS,
		commonpb.ObjectPrivilege_PrivilegeManageRLS,
	} {
		privilegeName := MetaStore2API(privilege.String())
		assert.Equal(t, commonpb.ObjectType_Collection.String(), GetObjectType(privilegeName))
		assert.Equal(t, privilege.String(), PrivilegeNameForMetastore(privilegeName))
		assert.True(t, IsPrivilegeNameDefined(privilegeName))
		assert.Equal(t, milvuspb.PrivilegeLevel_Collection.String(), GetPrivilegeLevel(privilegeName))
		assert.NotContains(t, CollectionReadOnlyPrivileges, privilegeName)
		assert.NotContains(t, CollectionReadWritePrivileges, privilegeName)
		assert.Contains(t, CollectionAdminPrivileges, privilegeName)
		assert.Contains(t, AdminPrivilegeGroup, privilege.String())
		assert.NotContains(t, ReadWritePrivilegeGroup, privilege.String())
	}
}
