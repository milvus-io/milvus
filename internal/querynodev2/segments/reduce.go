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

package segments

/*
#cgo pkg-config: milvus_segcore

#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"
*/
import "C"

import (
	"context"
	"fmt"
)

type SliceInfo struct {
	SliceNQs   []int64
	SliceTopKs []int64
}

// SearchResult contains a pointer to the search result in C++ memory
type SearchResult struct {
	cSearchResult C.CSearchResult
}

// SearchResultDataBlobs is the CSearchResultsDataBlobs in C++
type (
	SearchResultDataBlobs = C.CSearchResultDataBlobs
	StreamSearchReducer   = C.CSearchStreamReducer
)

// RetrieveResult contains a pointer to the retrieve result in C++ memory
type RetrieveResult struct {
	cRetrieveResult C.CRetrieveResult
}

func ParseSliceInfo(originNQs []int64, originTopKs []int64, nqPerSlice int64) *SliceInfo {
	sInfo := &SliceInfo{
		SliceNQs:   make([]int64, 0),
		SliceTopKs: make([]int64, 0),
	}

	if nqPerSlice == 0 {
		return sInfo
	}

	for i := 0; i < len(originNQs); i++ {
		for j := 0; j < int(originNQs[i]/nqPerSlice); j++ {
			sInfo.SliceNQs = append(sInfo.SliceNQs, nqPerSlice)
			sInfo.SliceTopKs = append(sInfo.SliceTopKs, originTopKs[i])
		}
		if tailSliceSize := originNQs[i] % nqPerSlice; tailSliceSize > 0 {
			sInfo.SliceNQs = append(sInfo.SliceNQs, tailSliceSize)
			sInfo.SliceTopKs = append(sInfo.SliceTopKs, originTopKs[i])
		}
	}

	return sInfo
}

func NewStreamReducer(ctx context.Context,
	plan *SearchPlan,
	sliceNQs []int64,
	sliceTopKs []int64,
) (StreamSearchReducer, error) {
	if plan.cSearchPlan == nil {
		return nil, fmt.Errorf("nil search plan")
	}
	if len(sliceNQs) == 0 {
		return nil, fmt.Errorf("empty slice nqs is not allowed")
	}
	if len(sliceNQs) != len(sliceTopKs) {
		return nil, fmt.Errorf("unaligned sliceNQs(len=%d) and sliceTopKs(len=%d)", len(sliceNQs), len(sliceTopKs))
	}
	cSliceNQSPtr := (*C.int64_t)(&sliceNQs[0])
	cSliceTopKSPtr := (*C.int64_t)(&sliceTopKs[0])
	cNumSlices := C.int64_t(len(sliceNQs))

	var streamReducer StreamSearchReducer
	status := C.NewStreamReducer(plan.cSearchPlan, cSliceNQSPtr, cSliceTopKSPtr, cNumSlices, &streamReducer)
	if err := HandleCStatus(ctx, &status, "MergeSearchResultsWithOutputFields failed"); err != nil {
		return nil, err
	}
	return streamReducer, nil
}

func StreamReduceSearchResult(ctx context.Context,
	newResult *SearchResult, streamReducer StreamSearchReducer,
) error {
	cSearchResults := make([]C.CSearchResult, 0)
	cSearchResults = append(cSearchResults, newResult.cSearchResult)
	cSearchResultPtr := &cSearchResults[0]

	status := C.StreamReduce(streamReducer, cSearchResultPtr, 1)
	if err := HandleCStatus(ctx, &status, "StreamReduceSearchResult failed"); err != nil {
		return err
	}
	return nil
}

func GetStreamReduceResult(ctx context.Context, streamReducer StreamSearchReducer) (SearchResultDataBlobs, error) {
	var cSearchResultDataBlobs SearchResultDataBlobs
	status := C.GetStreamReduceResult(streamReducer, &cSearchResultDataBlobs)
	if err := HandleCStatus(ctx, &status, "ReduceSearchResultsAndFillData failed"); err != nil {
		return nil, err
	}
	return cSearchResultDataBlobs, nil
}

func ReduceSearchResultsAndFillData(ctx context.Context, plan *SearchPlan, searchResults []*SearchResult,
	numSegments int64, sliceNQs []int64, sliceTopKs []int64,
) (SearchResultDataBlobs, error) {
	if plan.cSearchPlan == nil {
		return nil, fmt.Errorf("nil search plan")
	}

	if len(sliceNQs) == 0 {
		return nil, fmt.Errorf("empty slice nqs is not allowed")
	}

	if len(sliceNQs) != len(sliceTopKs) {
		return nil, fmt.Errorf("unaligned sliceNQs(len=%d) and sliceTopKs(len=%d)", len(sliceNQs), len(sliceTopKs))
	}

	cSearchResults := make([]C.CSearchResult, 0)
	for _, res := range searchResults {
		if res == nil {
			return nil, fmt.Errorf("nil searchResult detected when reduceSearchResultsAndFillData")
		}
		cSearchResults = append(cSearchResults, res.cSearchResult)
	}
	cSearchResultPtr := &cSearchResults[0]
	cNumSegments := C.int64_t(numSegments)
	cSliceNQSPtr := (*C.int64_t)(&sliceNQs[0])
	cSliceTopKSPtr := (*C.int64_t)(&sliceTopKs[0])
	cNumSlices := C.int64_t(len(sliceNQs))
	var cSearchResultDataBlobs SearchResultDataBlobs
	traceCtx := ParseCTraceContext(ctx)
	status := C.ReduceSearchResultsAndFillData(traceCtx.ctx, &cSearchResultDataBlobs, plan.cSearchPlan, cSearchResultPtr,
		cNumSegments, cSliceNQSPtr, cSliceTopKSPtr, cNumSlices)
	if err := HandleCStatus(ctx, &status, "ReduceSearchResultsAndFillData failed"); err != nil {
		return nil, err
	}
	return cSearchResultDataBlobs, nil
}

func GetSearchResultDataBlob(ctx context.Context, cSearchResultDataBlobs SearchResultDataBlobs, blobIndex int) ([]byte, error) {
	var blob C.CProto
	status := C.GetSearchResultDataBlob(&blob, cSearchResultDataBlobs, C.int32_t(blobIndex))
	if err := HandleCStatus(ctx, &status, "marshal failed"); err != nil {
		return nil, err
	}
	return GetCProtoBlob(&blob), nil
}

func DeleteSearchResultDataBlobs(cSearchResultDataBlobs SearchResultDataBlobs) {
	C.DeleteSearchResultDataBlobs(cSearchResultDataBlobs)
}

func DeleteStreamReduceHelper(cStreamReduceHelper StreamSearchReducer) {
	C.DeleteStreamSearchReducer(cStreamReduceHelper)
}

func DeleteSearchResults(results []*SearchResult) {
	if len(results) == 0 {
		return
	}
	for _, result := range results {
		if result != nil {
			C.DeleteSearchResult(result.cSearchResult)
		}
	}
}
