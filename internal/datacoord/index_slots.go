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

package datacoord

import (
	"math"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func getFMIndexBuildParam(indexParams []*commonpb.KeyValuePair, key string, defaultValue int64) int64 {
	for _, param := range indexParams {
		if param.GetKey() != key {
			continue
		}
		value, err := strconv.ParseInt(param.GetValue(), 10, 64)
		if err == nil && value > 0 {
			return value
		}
		break
	}
	return defaultValue
}

// estimateFMIndexBuildPeakBytes mirrors the allocations in
// fmindex::FMIndex::Build. fieldSize is the uncompressed VARCHAR payload size;
// numRows determines the per-row object/view/boundary overhead and separator
// count. The larger of suffix-array construction and wavelet construction is
// returned because their scratch buffers do not all overlap.
func estimateFMIndexBuildPeakBytes(fieldSize, numRows int64, indexParams []*commonpb.KeyValuePair) int64 {
	fieldSize = max(fieldSize, 0)
	numRows = max(numRows, 0)
	saSampleRate := getFMIndexBuildParam(indexParams, indexparamcheck.FmSaSampleRateKey, fmIndexDefaultSASampleRate)
	blockBytes := getFMIndexBuildParam(indexParams, indexparamcheck.FmBlockBytesKey, fmIndexDefaultBlockBytes)

	// One separator per row and one trailing sentinel are appended to the text.
	textSymbols := saturatingAdd(fieldSize, numRows, 1)
	rowBitmaps := saturatingMul(ceilDiv(numRows, 8), 2) // valid + null bitmap
	inputAndRows := saturatingAdd(
		fieldSize,
		saturatingMul(numRows, 32),                  // FieldData std::string objects (libstdc++)
		saturatingMul(numRows, 16),                  // build-time std::string_view array
		saturatingMul(saturatingAdd(numRows, 1), 8), // document boundaries
		rowBitmaps,
	)

	// The sampled-SA bitmap and its rank9 directory coexist with the suffix
	// array and remain live through wavelet construction.
	sampleWords := ceilDiv(textSymbols, 64)
	sampleBitmap := saturatingMul(sampleWords, 8)
	sampleDirectory := saturatingMul(saturatingAdd(ceilDiv(sampleWords, 8), 1), 16)
	sampleCount := saturatingAdd((textSymbols-1)/saSampleRate, 1)
	sampleWidth := int64(4)
	if textSymbols >= 1<<32 {
		sampleWidth = 8
	}
	samples := saturatingMul(sampleCount, sampleWidth)
	shared := saturatingAdd(inputAndRows, sampleBitmap, sampleDirectory, samples)

	// Compact builds hold int32 text + int32 SA (including libsais scratch).
	// The C++ decision uses the pre-sentinel length, so textSymbols itself can
	// equal INT32_MAX and still use the compact path.
	var suffixArrayScratch int64
	if textSymbols <= math.MaxInt32 {
		suffixArrayScratch = saturatingAdd(
			saturatingMul(textSymbols, 4),
			saturatingMul(saturatingAdd(textSymbols, fmIndexSuffixArrayExtra), 4),
		)
	} else {
		suffixArrayScratch = saturatingMul(textSymbols, 16)
	}
	suffixArrayPeak := saturatingAdd(shared, suffixArrayScratch)

	// Wavelet construction holds two uint16 ping-pong buffers plus one packed
	// 2-bit vector and rank directory per quad level. Five levels is the maximum
	// for the byte alphabet plus separator/sentinel and is conservative.
	wordsPerBlock := max(blockBytes/8, 1)
	waveletWords := ceilDiv(textSymbols, 32)
	waveletBlocks := ceilDiv(waveletWords, wordsPerBlock)
	waveletSuperBlocks := ceilDiv(waveletBlocks, 64)
	waveletDirectoryPerLevel := saturatingAdd(
		saturatingMul(saturatingAdd(waveletSuperBlocks, 1), 32),
		saturatingMul(saturatingAdd(waveletBlocks, 1), 8),
	)
	waveletLevelBytes := saturatingAdd(saturatingMul(waveletWords, 8), waveletDirectoryPerLevel)
	waveletPeak := saturatingAdd(
		shared,
		saturatingMul(textSymbols, 4),
		saturatingMul(waveletLevelBytes, 5),
	)

	return max(suffixArrayPeak, waveletPeak)
}

func fmIndexBuildTaskSlots(fieldSize, numRows int64, indexParams []*commonpb.KeyValuePair) int64 {
	// CalculateNodeSlots exposes WorkerSlotUnit * BuildParallel slots per 8 GiB
	// memory unit. Use the inverse conversion so the coordinator and worker
	// account for the same amount of memory per slot.
	workerSlotsPerMemoryUnit := saturatingMul(
		max(Params.DataNodeCfg.WorkerSlotUnit.GetAsInt64(), 1),
		max(Params.DataNodeCfg.BuildParallel.GetAsInt64(), 1),
	)
	if paramtable.GetRole() == typeutil.StandaloneRole {
		workerSlotsPerMemoryUnit = max(
			int64(float64(workerSlotsPerMemoryUnit)*Params.DataNodeCfg.StandaloneSlotRatio.GetAsFloat()),
			1,
		)
	}
	bytesPerSlot := max(bytesPerWorkerMemoryUnit/workerSlotsPerMemoryUnit, 1)
	return max(ceilDiv(estimateFMIndexBuildPeakBytes(fieldSize, numRows, indexParams), bytesPerSlot), 1)
}

func calculateIndexTaskSlot(fieldSize, numRows int64, indexParams []*commonpb.KeyValuePair) int64 {
	indexType := GetIndexType(indexParams)
	if indexType == indexparamcheck.IndexFMINDEX {
		return fmIndexBuildTaskSlots(fieldSize, numRows, indexParams)
	}
	defaultSlots := Params.DataCoordCfg.IndexTaskSlotUsage.GetAsInt64()
	isHeavyIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)
	if !isHeavyIndex {
		defaultSlots = Params.DataCoordCfg.ScalarIndexTaskSlotUsage.GetAsInt64()
	}
	if fieldSize > 512*1024*1024 {
		taskSlot := max(fieldSize/512/1024/1024, 1) * defaultSlots
		return max(taskSlot, 1)
	} else if fieldSize > 100*1024*1024 {
		return max(defaultSlots/4, 1)
	} else if fieldSize > 10*1024*1024 {
		return max(defaultSlots/16, 1)
	}
	return max(defaultSlots/64, 1)
}

const (
	fmIndexDefaultSASampleRate = int64(8)
	fmIndexDefaultBlockBytes   = int64(64)
	fmIndexSuffixArrayExtra    = int64(6144)
	bytesPerWorkerMemoryUnit   = int64(8 * 1024 * 1024 * 1024)
)
