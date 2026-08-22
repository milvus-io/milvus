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

	"github.com/milvus-io/milvus/internal/storagev2"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func TestFilesystemMetricsRegisteredWithRolesRegistry(t *testing.T) {
	dir := t.TempDir()
	_, err := storagev2.GetFilesystemMetricsWithConfig(&indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    dir,
	})
	require.NoError(t, err)

	gathered, err := Registry.GoRegistry.Gather()
	require.NoError(t, err)

	missingFamilies := map[string]struct{}{
		"milvus_storage_filesystem_read_count":                 {},
		"milvus_storage_filesystem_write_count":                {},
		"milvus_storage_filesystem_read_bytes":                 {},
		"milvus_storage_filesystem_write_bytes":                {},
		"milvus_storage_filesystem_get_file_info_count":        {},
		"milvus_storage_filesystem_failed_count":               {},
		"milvus_storage_filesystem_multi_part_upload_created":  {},
		"milvus_storage_filesystem_multi_part_upload_finished": {},
	}
	expectedDisplayKeyPrefix := "file://" + dir + "#fs:"
	for _, family := range gathered {
		if _, ok := missingFamilies[family.GetName()]; !ok {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "fs" && strings.HasPrefix(label.GetValue(), expectedDisplayKeyPrefix) {
					delete(missingFamilies, family.GetName())
					break
				}
			}
		}
	}
	require.Empty(t, missingFamilies)
}

func TestResolveFileResourceMode(t *testing.T) {
	params := paramtable.Get()
	setMode := func(item *paramtable.ParamItem, value string) {
		t.Helper()
		assert.NoError(t, params.Save(item.Key, value))
		t.Cleanup(func() { params.Reset(item.Key) })
	}
	setMode(&params.CommonCfg.QNFileResourceMode, "sync")
	setMode(&params.CommonCfg.DNFileResourceMode, "ref")
	params.Reset(params.CommonCfg.ProxyFileResourceMode.Key)
	assert.Equal(t, "close", params.CommonCfg.ProxyFileResourceMode.GetValue())

	tests := []struct {
		name          string
		queryNode     bool
		dataNode      bool
		proxy         bool
		streamingNode bool
		mixCoord      bool
		expected      fileresource.Mode
	}{
		{name: "query node wins", queryNode: true, dataNode: true, expected: fileresource.SyncMode},
		{name: "data node ref", dataNode: true, proxy: true, expected: fileresource.RefMode},
		{name: "proxy close", proxy: true, expected: fileresource.CloseMode},
		{name: "streaming follows query node", streamingNode: true, expected: fileresource.SyncMode},
		{name: "no file resource role", mixCoord: true, expected: fileresource.CloseMode},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			roles := MilvusRoles{
				EnableQueryNode:     test.queryNode,
				EnableDataNode:      test.dataNode,
				EnableProxy:         test.proxy,
				EnableStreamingNode: test.streamingNode,
				EnableMixCoord:      test.mixCoord,
			}
			assert.Equal(t, test.expected, roles.resolveFileResourceMode())
		})
	}

	t.Run("proxy sync opt-in", func(t *testing.T) {
		setMode(&params.CommonCfg.ProxyFileResourceMode, "sync")
		roles := MilvusRoles{EnableProxy: true}
		assert.Equal(t, fileresource.SyncMode, roles.resolveFileResourceMode())
	})
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
