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

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

func TestLoadIndexInfo(t *testing.T) {
	indexParams := make([]*commonpb.KeyValuePair, 0)
	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   "index_type",
		Value: "IVF_PQ",
	})
	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   "index_mode",
		Value: "cpu",
	})

	indexBytes, err := genIndexBinarySet()
	assert.NoError(t, err)
	indexPaths := make([]string, 0)
	indexPaths = append(indexPaths, "IVF")

	loadIndexInfo, err := newLoadIndexInfo()
	assert.Nil(t, err)
	for _, indexParam := range indexParams {
		err = loadIndexInfo.appendIndexParam(indexParam.Key, indexParam.Value)
		assert.NoError(t, err)
	}
	err = loadIndexInfo.appendFieldInfo(0)
	assert.NoError(t, err)
	err = loadIndexInfo.appendIndex(indexBytes, indexPaths)
	assert.NoError(t, err)

	deleteLoadIndexInfo(loadIndexInfo)
}
