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
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"

	"github.com/milvus-io/milvus/pkg/v3/util"
)

func TestRbacConfig_DefaultPrivileges(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	cfg := &params.RbacConfig
	assert.Equal(t, len(cfg.GetDefaultPrivilegeGroupNames()), 9)
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

	params.Save(params.RbacConfig.ClusterReadWritePrivileges.Key, clusterReadWrite)

	defer func() {
		params.Reset(params.RbacConfig.ClusterReadWritePrivileges.Key)
	}()

	group := params.RbacConfig.GetDefaultPrivilegeGroup("ClusterReadWrite")
	assert.Equal(t, len(group.Privileges), 3)
}

// TestRbacConfig_NoYamlKey_FallsBackToDefault locks the invariant that, when the
// 9 built-in privilege group yaml keys are absent (which is the post-#49167 shipping
// state), ParamItem.GetAsStrings() returns the current Go constants from util.*Privileges.
// This is what makes the fix structurally correct: adding a new privilege to a
// constant array in pkg/util/constant.go automatically flows through at runtime
// without needing to touch configs/milvus.yaml.
func TestRbacConfig_NoYamlKey_FallsBackToDefault(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	cfg := &params.RbacConfig

	assert.Equal(t, cfg.ClusterReadOnlyPrivileges.GetAsStrings(), util.ClusterReadOnlyPrivileges)
	assert.Equal(t, cfg.ClusterReadWritePrivileges.GetAsStrings(), util.ClusterReadWritePrivileges)
	assert.Equal(t, cfg.ClusterAdminPrivileges.GetAsStrings(), util.ClusterAdminPrivileges)
	assert.Equal(t, cfg.DBReadOnlyPrivileges.GetAsStrings(), util.DatabaseReadOnlyPrivileges)
	assert.Equal(t, cfg.DBReadWritePrivileges.GetAsStrings(), util.DatabaseReadWritePrivileges)
	assert.Equal(t, cfg.DBAdminPrivileges.GetAsStrings(), util.DatabaseAdminPrivileges)
	assert.Equal(t, cfg.CollectionReadOnlyPrivileges.GetAsStrings(), util.CollectionReadOnlyPrivileges)
	assert.Equal(t, cfg.CollectionReadWritePrivileges.GetAsStrings(), util.CollectionReadWritePrivileges)
	assert.Equal(t, cfg.CollectionAdminPrivileges.GetAsStrings(), util.CollectionAdminPrivileges)
}

// TestRbacConfig_EmptyStringMeansEmptyGroup locks the semantic that an empty
// yaml value (e.g. `privileges:` with no value) means "this group has no
// privileges" — the user's explicit intent. This must NOT silently fall back
// to the Go default. A deployment (e.g. managed cloud) may legitimately want
// to zero out a built-in group; preserving that capability matters.
func TestRbacConfig_EmptyStringMeansEmptyGroup(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))

	params.Save(params.RbacConfig.ClusterAdminPrivileges.Key, "")
	defer params.Reset(params.RbacConfig.ClusterAdminPrivileges.Key)

	got := params.RbacConfig.ClusterAdminPrivileges.GetAsStrings()
	assert.NotEqual(t, util.ClusterAdminPrivileges, got,
		"empty yaml value must not silently fall back to Go defaults")
	// Accept either [] or [""], both are unambiguous "no privileges".
	if len(got) == 1 {
		assert.Equal(t, "", got[0])
	} else {
		assert.Empty(t, got)
	}
}

// TestMilvusYamlHasNoRbacBuiltinPrivilegeKeys guards against accidentally
// re-adding RBAC privilege group keys to configs/milvus.yaml (via Export=true
// flip, hand edit, or regeneration with the wrong flags). The shipping yaml
// must not contain these keys; defaults must come from Go source.
func TestMilvusYamlHasNoRbacBuiltinPrivilegeKeys(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	assert.True(t, ok, "runtime.Caller failed")
	// filepath.Dir(thisFile) is pkg/util/paramtable — 3 levels up is repo root
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
	yamlPath := filepath.Join(repoRoot, "configs", "milvus.yaml")

	raw, err := os.ReadFile(yamlPath)
	assert.NoError(t, err, "reading %s", yamlPath)

	var tree map[string]interface{}
	err = yaml.Unmarshal(raw, &tree)
	assert.NoError(t, err)

	// Drill into common.security.rbac
	common, _ := tree["common"].(map[interface{}]interface{})
	security, _ := common["security"].(map[interface{}]interface{})
	rbac, present := security["rbac"]
	assert.False(t, present,
		"configs/milvus.yaml must not contain common.security.rbac.* keys; "+
			"built-in privilege group defaults live in pkg/util/constant.go, not yaml. "+
			"found: %v", rbac)
}
