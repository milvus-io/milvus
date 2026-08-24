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

//go:build cgo

package expr

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/fileresource"
)

type xgboostRealParityExpected struct {
	Features [][]float32 `json:"features"`
	Raw      []float32   `json:"raw"`
	Default  []float32   `json:"default"`
}

func TestXGBoostRealModelParityLocal(t *testing.T) {
	modelPath := "testdata/xgboost/binary_logistic_2f.ubj"
	expectedPath := "testdata/xgboost/binary_logistic_2f_expected.json"

	data, err := os.ReadFile(expectedPath)
	require.NoError(t, err)
	var expected xgboostRealParityExpected
	require.NoError(t, json.Unmarshal(data, &expected))
	require.NotEmpty(t, expected.Features)
	require.Len(t, expected.Raw, len(expected.Features[0]))
	require.Len(t, expected.Default, len(expected.Features[0]))

	model, err := loadXGBoostModel(&fileresource.ResolvedFileResource{
		Name:      "real_xgboost_model",
		Path:      modelPath,
		LocalPath: modelPath,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, closeXGBoostModel(model)) }()
	require.Equal(t, len(expected.Features), model.numFeatures)

	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	inputs := make([]*arrow.Chunked, 0, len(expected.Features))
	for _, values := range expected.Features {
		builder := array.NewFloat32Builder(pool)
		builder.AppendValues(values, nil)
		arr := builder.NewArray()
		builder.Release()
		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{arr})
		arr.Release()
		inputs = append(inputs, chunked)
	}
	defer func() {
		for _, input := range inputs {
			input.Release()
		}
	}()

	raw, err := predictXGBoostArrowChunks(model, inputs, false, pool)
	require.NoError(t, err)
	defer raw.Release()
	assertChunkCloseTo(t, raw, expected.Raw)

	def, err := predictXGBoostArrowChunks(model, inputs, true, pool)
	require.NoError(t, err)
	defer def.Release()
	assertChunkCloseTo(t, def, expected.Default)
}

func assertChunkCloseTo(t *testing.T, chunked *arrow.Chunked, expected []float32) {
	t.Helper()
	require.Equal(t, 1, len(chunked.Chunks()))
	arr := chunked.Chunk(0).(*array.Float32)
	require.Equal(t, len(expected), arr.Len())
	for i, value := range expected {
		require.InDelta(t, value, arr.Value(i), 1e-5, "row %d", i)
	}
}
