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

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

func TestComponentNum(t *testing.T) {
	tests := []struct {
		name  string
		roles MilvusRoles
		count int
	}{
		{name: "empty"},
		{name: "proxy", roles: MilvusRoles{EnableProxy: true}, count: 1},
		{name: "querynode", roles: MilvusRoles{EnableQueryNode: true}, count: 1},
		{name: "datanode", roles: MilvusRoles{EnableDataNode: true}, count: 1},
		{name: "streamingnode with embedded querynode", roles: MilvusRoles{EnableStreamingNode: true, EnableQueryNode: true}, count: 2},
		{name: "mixcoord", roles: MilvusRoles{EnableMixCoord: true}, count: 1},
		{name: "cdc", roles: MilvusRoles{EnableCDC: true}, count: 1},
		{name: "standalone", roles: MilvusRoles{EnableMixCoord: true, EnableProxy: true, EnableQueryNode: true, EnableDataNode: true, EnableStreamingNode: true}, count: 5},
		{name: "legacy mixture coordinators", roles: MilvusRoles{EnableRootCoord: true, EnableQueryCoord: true, EnableDataCoord: true}, count: 1},
		{name: "overlapping coordinator flags", roles: MilvusRoles{EnableMixCoord: true, EnableRootCoord: true, EnableQueryCoord: true, EnableDataCoord: true}, count: 1},
		{name: "partial legacy coordinator flags do not start a component", roles: MilvusRoles{EnableRootCoord: true, EnableQueryCoord: true, EnableProxy: true}, count: 1},
	}
	for i := range tests {
		test := &tests[i]
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.count, test.roles.componentNum())
		})
	}
}
