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
)

func TestRbacConfig_DefaultPrivileges(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	cfg := &params.RbacConfig
	assert.Equal(t, len(cfg.GetDefaultPrivilegeGroupNames()), 9)
	assert.Equal(t, cfg.Enabled.GetAsBool(), false)
	assert.Equal(t, cfg.ClusterReadOnlyPrivileges.GetAsStrings(), cfg.GetDefaultPrivilegeGroupPrivileges("ClusterReadOnly"))
	assert.Equal(t, cfg.ClusterReadWritePrivileges.GetAsStrings(), cfg.GetDefaultPrivilegeGroupPrivileges("ClusterReadWrite"))
	assert.Equal(t, cfg.ClusterAdminPrivileges.GetAsStrings(), cfg.GetDefaultPrivilegeGroupPrivileges("ClusterAdmin"))
	assert.Equal(t, cfg.DBReadOnlyPrivileges.GetAsStrings(), cfg.GetDefaultPrivilegeGroupPrivileges("DatabaseReadOnly"))
	assert.Equal(t, cfg.DBReadWritePrivileges.GetAsStrings(), cfg.GetDefaultPrivilegeGroupPrivileges("DatabaseReadWrite"))
	assert.Equal(t, cfg.DBAdminPrivileges.GetAsStrings(), cfg.GetDefaultPrivilegeGroupPrivileges("DatabaseAdmin"))
	assert.Equal(t, cfg.CollectionReadOnlyPrivileges.GetAsStrings(), cfg.GetDefaultPrivilegeGroupPrivileges("CollectionReadOnly"))
	assert.Equal(t, cfg.CollectionReadWritePrivileges.GetAsStrings(), cfg.GetDefaultPrivilegeGroupPrivileges("CollectionReadWrite"))
	assert.Equal(t, cfg.CollectionAdminPrivileges.GetAsStrings(), cfg.GetDefaultPrivilegeGroupPrivileges("CollectionAdmin"))
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
