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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/indexnode"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

func genIndexBinarySet() ([][]byte, error) {
	const (
		msgLength = 1000
		DIM       = 16
	)

	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_PQ"
	indexParams["index_mode"] = "cpu"
	indexParams["dim"] = "16"
	indexParams["k"] = "10"
	indexParams["nlist"] = "100"
	indexParams["nprobe"] = "10"
	indexParams["m"] = "4"
	indexParams["nbits"] = "8"
	indexParams["metric_type"] = "L2"
	indexParams["SLICE_SIZE"] = "4"

	typeParams := make(map[string]string)
	typeParams["dim"] = strconv.Itoa(DIM)
	var indexRowData []float32
	for n := 0; n < msgLength; n++ {
		for i := 0; i < DIM; i++ {
			indexRowData = append(indexRowData, float32(n*i))
		}
	}

	index, err := indexnode.NewCIndex(typeParams, indexParams)
	if err != nil {
		return nil, err
	}

	err = index.BuildFloatVecIndexWithoutIds(indexRowData)
	if err != nil {
		return nil, err
	}

	// save index to minio
	binarySet, err := index.Serialize()
	if err != nil {
		return nil, err
	}

	bytesSet := make([][]byte, 0)
	for i := range binarySet {
		bytesSet = append(bytesSet, binarySet[i].Value)
	}
	return bytesSet, nil
}

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
