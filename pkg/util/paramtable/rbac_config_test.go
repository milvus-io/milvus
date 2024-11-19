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

	"github.com/milvus-io/milvus/pkg/util"
)

func TestRbacConfig_Init(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	cfg := &params.RbacConfig
	assert.Equal(t, cfg.Enabled.GetAsBool(), false)
	assert.Equal(t, cfg.ClusterReadOnlyPrivileges.GetAsStrings(), util.BuiltinPrivilegeGroups["ClusterReadOnly"])
	assert.Equal(t, cfg.ClusterReadWritePrivileges.GetAsStrings(), util.BuiltinPrivilegeGroups["ClusterReadWrite"])
	assert.Equal(t, cfg.ClusterAdminPrivileges.GetAsStrings(), util.BuiltinPrivilegeGroups["ClusterAdmin"])
	assert.Equal(t, cfg.DBReadOnlyPrivileges.GetAsStrings(), util.BuiltinPrivilegeGroups["DatabaseReadOnly"])
	assert.Equal(t, cfg.DBReadWritePrivileges.GetAsStrings(), util.BuiltinPrivilegeGroups["DatabaseReadWrite"])
	assert.Equal(t, cfg.DBAdminPrivileges.GetAsStrings(), util.BuiltinPrivilegeGroups["DatabaseAdmin"])
	assert.Equal(t, cfg.CollectionReadOnlyPrivileges.GetAsStrings(), util.BuiltinPrivilegeGroups["CollectionReadOnly"])
	assert.Equal(t, cfg.CollectionReadWritePrivileges.GetAsStrings(), util.BuiltinPrivilegeGroups["CollectionReadWrite"])
	assert.Equal(t, cfg.CollectionAdminPrivileges.GetAsStrings(), util.BuiltinPrivilegeGroups["CollectionAdmin"])
}
