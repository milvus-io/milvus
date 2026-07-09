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

package roles

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRoles(t *testing.T) {
	ss := strings.SplitN("abcdef", "=", 2)
	assert.Equal(t, len(ss), 1)
	ss = strings.SplitN("adb=def", "=", 2)
	assert.Equal(t, len(ss), 2)

	paramtable.Init()
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	localPath := filepath.Join(rootPath, "test-dir")

	err := os.RemoveAll(localPath)
	assert.NoError(t, err)

	err = os.MkdirAll(localPath, os.ModeDir)
	assert.NoError(t, err)
	_, err = os.Create(filepath.Join(localPath, "child"))
	assert.NoError(t, err)

	err = os.RemoveAll(localPath)
	assert.NoError(t, err)
	_, err = os.Stat(localPath)
	assert.Error(t, err)
	assert.Equal(t, true, os.IsNotExist(err))
}

func TestMilvusRolesResolveLocalFileResourceMode(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	oldRole := paramtable.GetRole()
	defer paramtable.SetRole(oldRole)
	defer fileresource.ResetLocalModeForTest()
	paramtable.SetRole(typeutil.MixtureRole)

	tests := []struct {
		name     string
		roles    *MilvusRoles
		qn       string
		dn       string
		pn       string
		expected fileresource.Mode
	}{
		{name: "query node sync overrides data node ref", roles: &MilvusRoles{EnableQueryNode: true, EnableDataNode: true}, qn: "sync", dn: "ref", pn: "close", expected: fileresource.SyncMode},
		{name: "data node ref overrides query node close", roles: &MilvusRoles{EnableQueryNode: true, EnableDataNode: true}, qn: "close", dn: "ref", pn: "close", expected: fileresource.RefMode},
		{name: "proxy sync overrides data node ref", roles: &MilvusRoles{EnableProxy: true, EnableDataNode: true}, qn: "close", dn: "ref", pn: "sync", expected: fileresource.SyncMode},
		{name: "streaming node follows query node", roles: &MilvusRoles{EnableStreamingNode: true}, qn: "sync", dn: "close", pn: "close", expected: fileresource.SyncMode},
		{name: "disabled roles are ignored", roles: &MilvusRoles{EnableProxy: true}, qn: "sync", dn: "ref", pn: "close", expected: fileresource.CloseMode},
		{name: "all enabled roles close", roles: &MilvusRoles{EnableQueryNode: true, EnableDataNode: true, EnableProxy: true}, qn: "close", dn: "close", pn: "close", expected: fileresource.CloseMode},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldQN := params.CommonCfg.QNFileResourceMode.SwapTempValue(test.qn)
			oldDN := params.CommonCfg.DNFileResourceMode.SwapTempValue(test.dn)
			oldPN := params.CommonCfg.PNFileResourceMode.SwapTempValue(test.pn)
			defer params.CommonCfg.QNFileResourceMode.SwapTempValue(oldQN)
			defer params.CommonCfg.DNFileResourceMode.SwapTempValue(oldDN)
			defer params.CommonCfg.PNFileResourceMode.SwapTempValue(oldPN)
			fileresource.ResetLocalModeForTest()

			mode := test.roles.resolveLocalFileResourceMode()
			require.Equal(t, test.expected, mode)
			require.Equal(t, test.expected, fileresource.GetLocalMode(fileresource.CloseMode))
		})
	}
}

func TestCleanLocalDir(t *testing.T) {
	paramtable.Init()
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	localPath := filepath.Join(rootPath, "test-dir")

	// clean data
	assert.NotPanics(t, func() {
		cleanLocalDir(localPath)
	})

	// create dir and file
	err := os.MkdirAll(localPath, os.ModeDir)
	assert.NoError(t, err)
	_, err = os.Create(filepath.Join(localPath, "child"))
	assert.NoError(t, err)

	// clean with path exist
	assert.NotPanics(t, func() {
		cleanLocalDir(localPath)
	})

	_, err = os.Stat(localPath)
	assert.Error(t, err)
	assert.Equal(t, true, os.IsNotExist(err))
	// clean with path not exist
	assert.NotPanics(t, func() {
		cleanLocalDir(localPath)
	})
}
