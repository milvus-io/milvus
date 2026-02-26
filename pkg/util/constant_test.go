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

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
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

func TestSnapshotPrivilegesInDatabaseGroups(t *testing.T) {
	describeSnapshot := MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDescribeSnapshot.String())
	listSnapshots := MetaStore2API(commonpb.ObjectPrivilege_PrivilegeListSnapshots.String())
	createSnapshot := MetaStore2API(commonpb.ObjectPrivilege_PrivilegeCreateSnapshot.String())
	dropSnapshot := MetaStore2API(commonpb.ObjectPrivilege_PrivilegeDropSnapshot.String())
	restoreSnapshot := MetaStore2API(commonpb.ObjectPrivilege_PrivilegeRestoreSnapshot.String())

	tests := []struct {
		name      string
		privilege string
		inGroups  map[string][]string
		notIn     map[string][]string
	}{
		{
			name:      "DescribeSnapshot in DatabaseReadOnly/ReadWrite/Admin",
			privilege: describeSnapshot,
			inGroups: map[string][]string{
				"DatabaseReadOnly":  DatabaseReadOnlyPrivileges,
				"DatabaseReadWrite": DatabaseReadWritePrivileges,
				"DatabaseAdmin":     DatabaseAdminPrivileges,
			},
			notIn: map[string][]string{
				"ClusterReadOnly": ClusterReadOnlyPrivileges,
				"ClusterAdmin":    ClusterAdminPrivileges,
			},
		},
		{
			name:      "ListSnapshots in DatabaseReadOnly/ReadWrite/Admin",
			privilege: listSnapshots,
			inGroups: map[string][]string{
				"DatabaseReadOnly":  DatabaseReadOnlyPrivileges,
				"DatabaseReadWrite": DatabaseReadWritePrivileges,
				"DatabaseAdmin":     DatabaseAdminPrivileges,
			},
			notIn: map[string][]string{
				"ClusterReadOnly": ClusterReadOnlyPrivileges,
				"ClusterAdmin":    ClusterAdminPrivileges,
			},
		},
		{
			name:      "CreateSnapshot in DatabaseReadWrite/Admin only",
			privilege: createSnapshot,
			inGroups: map[string][]string{
				"DatabaseReadWrite": DatabaseReadWritePrivileges,
				"DatabaseAdmin":     DatabaseAdminPrivileges,
			},
			notIn: map[string][]string{
				"DatabaseReadOnly": DatabaseReadOnlyPrivileges,
				"ClusterReadWrite": ClusterReadWritePrivileges,
				"ClusterAdmin":     ClusterAdminPrivileges,
			},
		},
		{
			name:      "DropSnapshot in DatabaseReadWrite/Admin only",
			privilege: dropSnapshot,
			inGroups: map[string][]string{
				"DatabaseReadWrite": DatabaseReadWritePrivileges,
				"DatabaseAdmin":     DatabaseAdminPrivileges,
			},
			notIn: map[string][]string{
				"DatabaseReadOnly": DatabaseReadOnlyPrivileges,
				"ClusterReadWrite": ClusterReadWritePrivileges,
				"ClusterAdmin":     ClusterAdminPrivileges,
			},
		},
		{
			name:      "RestoreSnapshot in DatabaseAdmin only",
			privilege: restoreSnapshot,
			inGroups: map[string][]string{
				"DatabaseAdmin": DatabaseAdminPrivileges,
			},
			notIn: map[string][]string{
				"DatabaseReadOnly":  DatabaseReadOnlyPrivileges,
				"DatabaseReadWrite": DatabaseReadWritePrivileges,
				"ClusterAdmin":      ClusterAdminPrivileges,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for groupName, group := range tc.inGroups {
				assert.True(t, lo.Contains(group, tc.privilege),
					"%s should be in %s", tc.privilege, groupName)
			}
			for groupName, group := range tc.notIn {
				assert.False(t, lo.Contains(group, tc.privilege),
					"%s should NOT be in %s", tc.privilege, groupName)
			}
		})
	}
}
