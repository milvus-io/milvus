// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexInfo(t *testing.T) {
	indexInfo := newIndexInfo()

	buildID := UniqueID(0)
	indexID := UniqueID(0)
	indexPaths := []string{"test-index-paths"}
	indexName := "test-index-name"
	indexParams := make(map[string]string)

	indexInfo.setBuildID(buildID)
	indexInfo.setIndexID(indexID)
	indexInfo.setReadyLoad(true)
	indexInfo.setIndexName(indexName)
	indexInfo.setIndexPaths(indexPaths)
	indexInfo.setIndexParams(indexParams)

	resBuildID := indexInfo.getBuildID()
	assert.Equal(t, buildID, resBuildID)
	resIndexID := indexInfo.getIndexID()
	assert.Equal(t, indexID, resIndexID)
	resLoad := indexInfo.getReadyLoad()
	assert.True(t, resLoad)
	resName := indexInfo.getIndexName()
	assert.Equal(t, indexName, resName)
	resPaths := indexInfo.getIndexPaths()
	assert.Equal(t, len(indexPaths), len(resPaths))
	assert.Len(t, resPaths, 1)
	assert.Equal(t, indexPaths[0], resPaths[0])
	resParams := indexInfo.getIndexParams()
	assert.Equal(t, len(indexParams), len(resParams))
}
