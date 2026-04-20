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

package paramtable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util"
)

// TestRbacConfig_BuiltinPrivilegesMatchGoConstants guards against drift between
// configs/milvus.yaml and the authoritative Go constants in pkg/util/constant.go.
// When a new privilege is added to util.XxxPrivileges, the matching yaml list
// must be updated in the same PR or this test fails.
//
// Comparing GetAsStrings() against util.XxxPrivileges (not against another
// ParamItem-derived value) is what makes the drift detectable: the assertion
// walks the full resolution chain (etcd override -> yaml file -> DefaultValue)
// and compares the runtime value with the compile-time truth.
func TestRbacConfig_BuiltinPrivilegesMatchGoConstants(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	cfg := &params.RbacConfig

	cases := []struct {
		name     string
		got      []string
		expected []string
	}{
		{"ClusterReadOnly", cfg.ClusterReadOnlyPrivileges.GetAsStrings(), util.ClusterReadOnlyPrivileges},
		{"ClusterReadWrite", cfg.ClusterReadWritePrivileges.GetAsStrings(), util.ClusterReadWritePrivileges},
		{"ClusterAdmin", cfg.ClusterAdminPrivileges.GetAsStrings(), util.ClusterAdminPrivileges},
		{"DatabaseReadOnly", cfg.DBReadOnlyPrivileges.GetAsStrings(), util.DatabaseReadOnlyPrivileges},
		{"DatabaseReadWrite", cfg.DBReadWritePrivileges.GetAsStrings(), util.DatabaseReadWritePrivileges},
		{"DatabaseAdmin", cfg.DBAdminPrivileges.GetAsStrings(), util.DatabaseAdminPrivileges},
		{"CollectionReadOnly", cfg.CollectionReadOnlyPrivileges.GetAsStrings(), util.CollectionReadOnlyPrivileges},
		{"CollectionReadWrite", cfg.CollectionReadWritePrivileges.GetAsStrings(), util.CollectionReadWritePrivileges},
		{"CollectionAdmin", cfg.CollectionAdminPrivileges.GetAsStrings(), util.CollectionAdminPrivileges},
	}
	for _, c := range cases {
		assert.ElementsMatchf(t, c.expected, c.got,
			"privilege group %q out of sync: configs/milvus.yaml `common.security.rbac.*.privileges` "+
				"drifted from util.%sPrivileges in pkg/util/constant.go. "+
				"Update the yaml list to match the Go constant.", c.name, c.name)
	}
}

func TestRbacConfig_OverridePrivileges(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))

	clusterReadWrite := `SelectOwnership,SelectUser,DescribeResourceGroup`

	params.Save(params.RbacConfig.Enabled.Key, "true")
	params.Save(params.RbacConfig.ClusterReadWritePrivileges.Key, clusterReadWrite)

	defer func() {
		params.Reset(params.RbacConfig.Enabled.Key)
		params.Reset(params.RbacConfig.ClusterReadWritePrivileges.Key)
	}()

	group := params.RbacConfig.GetDefaultPrivilegeGroup("ClusterReadWrite")
	assert.Equal(t, len(group.Privileges), 3)
}
