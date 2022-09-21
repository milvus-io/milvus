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

package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
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
	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   "metric_type",
		Value: "L2",
	})

	indexBytes, err := genIndexBinarySet()
	assert.NoError(t, err)
	indexPaths := make([]string, 0)
	indexPaths = append(indexPaths, "IVF")

	loadIndexInfo, err := newLoadIndexInfo()
	assert.Nil(t, err)

	indexInfo := &querypb.FieldIndexInfo{
		FieldID:        UniqueID(0),
		IndexParams:    indexParams,
		IndexFilePaths: indexPaths,
	}

	fieldType := schemapb.DataType_FloatVector
	err = loadIndexInfo.appendLoadIndexInfo(indexBytes, indexInfo, 0, 0, 0, fieldType)
	assert.NoError(t, err)

	deleteLoadIndexInfo(loadIndexInfo)
}
