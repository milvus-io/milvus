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

package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCalculateIndexTaskSlot(t *testing.T) {
	pt := paramtable.Get()
	heavyKey := pt.DataCoordCfg.IndexTaskSlotUsage.Key
	scalarKey := pt.DataCoordCfg.ScalarIndexTaskSlotUsage.Key
	workerSlotKey := pt.DataNodeCfg.WorkerSlotUnit.Key
	buildParallelKey := pt.DataNodeCfg.BuildParallel.Key
	require.NoError(t, pt.Save(heavyKey, "64"))
	require.NoError(t, pt.Save(scalarKey, "16"))
	require.NoError(t, pt.Save(workerSlotKey, "16"))
	require.NoError(t, pt.Save(buildParallelKey, "1"))
	defer pt.Reset(heavyKey)
	defer pt.Reset(scalarKey)
	defer pt.Reset(workerSlotKey)
	defer pt.Reset(buildParallelKey)

	const mib = int64(1024 * 1024)
	fmIndexParams := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexFMINDEX}}
	invertedParams := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexINVERTED}}
	testCases := []struct {
		name         string
		fieldSize    int64
		wantFMIndex  int64
		wantInverted int64
	}{
		{name: "small", fieldSize: 5 * mib, wantFMIndex: 1, wantInverted: 1},
		{name: "medium", fieldSize: 50 * mib, wantFMIndex: 1, wantInverted: 1},
		{name: "large_below_512mb", fieldSize: 200 * mib, wantFMIndex: 4, wantInverted: 4},
		{name: "exactly_512mb", fieldSize: 512 * mib, wantFMIndex: 10, wantInverted: 4},
		{name: "above_512mb", fieldSize: 512*mib + 1, wantFMIndex: 10, wantInverted: 16},
		{name: "one_gib", fieldSize: 1024 * mib, wantFMIndex: 20, wantInverted: 32},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantFMIndex, calculateIndexTaskSlot(tc.fieldSize, 1, fmIndexParams))
			assert.Equal(t, tc.wantInverted, calculateIndexTaskSlot(tc.fieldSize, 1, invertedParams))
		})
	}

	// Vector slots cover retained input, build workspace and output memory.
	hnswParams := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}}
	assert.Equal(t, int64(2), calculateIndexTaskSlot(200*mib, 1, hnswParams))
}

func TestEstimateFMIndexBuildPeakBytes(t *testing.T) {
	const mib = int64(1024 * 1024)
	defaultParams := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexFMINDEX}}

	// For a single long row the compact-SA peak is ~9.66x payload: source data,
	// int32 text + SA, sampled bitmap/rank directory, and 1/8 sampled SA values.
	peak := estimateFMIndexBuildPeakBytes(100*mib, 1, defaultParams)
	assert.Greater(t, peak, int64(float64(100*mib)*9.65))
	assert.Less(t, peak, int64(float64(100*mib)*9.68))

	// More rows add separators and the actual std::string/string_view/boundary
	// allocations even when payload bytes are identical.
	manyRowsPeak := estimateFMIndexBuildPeakBytes(100*mib, 1_000_000, defaultParams)
	assert.Greater(t, manyRowsPeak, peak)

	// The sampled-SA rate is a real build-memory knob and must affect admission.
	rate4 := append(defaultParams, &commonpb.KeyValuePair{Key: indexparamcheck.FmSaSampleRateKey, Value: "4"})
	rate64 := append(defaultParams, &commonpb.KeyValuePair{Key: indexparamcheck.FmSaSampleRateKey, Value: "64"})
	assert.Greater(t,
		estimateFMIndexBuildPeakBytes(100*mib, 1, rate4),
		estimateFMIndexBuildPeakBytes(100*mib, 1, rate64),
	)

	// Crossing INT32_MAX symbols selects the int64 text + SA path and creates a
	// visible discontinuity that the estimator must preserve.
	compactPeak := estimateFMIndexBuildPeakBytes(int64(^uint32(0)>>1)-1, 0, defaultParams)
	widePeak := estimateFMIndexBuildPeakBytes(int64(^uint32(0)>>1), 0, defaultParams)
	assert.Greater(t, widePeak, compactPeak)
}

func TestFMIndexBuildTaskSlotsStandaloneRatio(t *testing.T) {
	pt := paramtable.Get()
	workerSlotKey := pt.DataNodeCfg.WorkerSlotUnit.Key
	buildParallelKey := pt.DataNodeCfg.BuildParallel.Key
	standaloneRatioKey := pt.DataNodeCfg.StandaloneSlotRatio.Key
	require.NoError(t, pt.Save(workerSlotKey, "16"))
	require.NoError(t, pt.Save(buildParallelKey, "1"))
	require.NoError(t, pt.Save(standaloneRatioKey, "0.25"))
	defer pt.Reset(workerSlotKey)
	defer pt.Reset(buildParallelKey)
	defer pt.Reset(standaloneRatioKey)

	oldRole := paramtable.GetRole()
	paramtable.SetRole(typeutil.StandaloneRole)
	defer paramtable.SetRole(oldRole)

	params := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexFMINDEX}}
	// A ~9.66 GiB peak consumes five standalone slots when the 0.25 factor
	// exposes four slots per 8 GiB memory unit.
	assert.Equal(t, int64(5), fmIndexBuildTaskSlots(1024*1024*1024, 1, params))
}
