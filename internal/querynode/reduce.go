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

/*
#cgo CFLAGS: -I${SRCDIR}/../core/output/include
#cgo darwin LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath,"${SRCDIR}/../core/output/lib"
#cgo linux LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"

*/
import "C"
import (
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	memutil "github.com/milvus-io/milvus/internal/util/memutil"
	metricsinfo "github.com/milvus-io/milvus/internal/util/metricsinfo"
)

type sliceInfo struct {
	sliceNQs   []int32
	sliceTopKs []int32
}

// SearchResult contains a pointer to the search result in C++ memory
type SearchResult struct {
	cSearchResult C.CSearchResult
}

// searchResultDataBlobs is the CSearchResultsDataBlobs in C++
type searchResultDataBlobs = C.CSearchResultDataBlobs

// RetrieveResult contains a pointer to the retrieve result in C++ memory
type RetrieveResult struct {
	cRetrieveResult C.CRetrieveResult
}

func parseSliceInfo(originNQs []int64, originTopKs []int64, nqPerSlice int64) *sliceInfo {
	sInfo := &sliceInfo{
		sliceNQs:   make([]int32, 0),
		sliceTopKs: make([]int32, 0),
	}

	if nqPerSlice == 0 {
		return sInfo
	}

	for i := 0; i < len(originNQs); i++ {
		for j := 0; j < int(originNQs[i]/nqPerSlice); j++ {
			sInfo.sliceNQs = append(sInfo.sliceNQs, int32(nqPerSlice))
			sInfo.sliceTopKs = append(sInfo.sliceTopKs, int32(originTopKs[i]))
		}
		if tailSliceSize := originNQs[i] % nqPerSlice; tailSliceSize > 0 {
			sInfo.sliceNQs = append(sInfo.sliceNQs, int32(tailSliceSize))
			sInfo.sliceTopKs = append(sInfo.sliceTopKs, int32(originTopKs[i]))
		}
	}

	return sInfo
}

func reduceSearchResultsAndFillData(plan *SearchPlan, searchResults []*SearchResult,
	numSegments int64, sliceNQs []int32, sliceTopKs []int32) (searchResultDataBlobs, error) {
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
		cSearchResults = append(cSearchResults, res.cSearchResult)
	}
	cSearchResultPtr := (*C.CSearchResult)(&cSearchResults[0])
	cNumSegments := C.int64_t(numSegments)
	var cSliceNQSPtr = (*C.int32_t)(&sliceNQs[0])
	var cSliceTopKSPtr = (*C.int32_t)(&sliceTopKs[0])
	var cNumSlices = C.int32_t(len(sliceNQs))
	var cSearchResultDataBlobs searchResultDataBlobs
	status := C.ReduceSearchResultsAndFillData(&cSearchResultDataBlobs, plan.cSearchPlan, cSearchResultPtr,
		cNumSegments, cSliceNQSPtr, cSliceTopKSPtr, cNumSlices)
	if err := HandleCStatus(&status, "ReduceSearchResultsAndFillData failed"); err != nil {
		return nil, err
	}
	return cSearchResultDataBlobs, nil
}

func getReqSlices(nqOfReqs []int64, nqPerSlice int64) ([]int32, error) {
	if nqPerSlice == 0 {
		return nil, fmt.Errorf("zero nqPerSlice is not allowed")
	}

	slices := make([]int32, 0)
	for i := 0; i < len(nqOfReqs); i++ {
		for j := 0; j < int(nqOfReqs[i]/nqPerSlice); j++ {
			slices = append(slices, int32(nqPerSlice))
		}
		if tailSliceSize := nqOfReqs[i] % nqPerSlice; tailSliceSize > 0 {
			slices = append(slices, int32(tailSliceSize))
		}
	}
	return slices, nil
}

func getSearchResultDataBlob(cSearchResultDataBlobs searchResultDataBlobs, blobIndex int) ([]byte, error) {
	var blob C.CProto
	status := C.GetSearchResultDataBlob(&blob, cSearchResultDataBlobs, C.int32_t(blobIndex))
	if err := HandleCStatus(&status, "marshal failed"); err != nil {
		return nil, err
	}
	return GetCProtoBlob(&blob), nil
}

func deleteSearchResultDataBlobs(cSearchResultDataBlobs searchResultDataBlobs) {
	C.DeleteSearchResultDataBlobs(cSearchResultDataBlobs)
	// try to do a purgeMemory operation after DeleteSearchResultDataBlobs
	usedMem := metricsinfo.GetUsedMemoryCount()
	if usedMem == 0 {
		log.Error("Get 0 uesdMemory when deleteSearchResultDataBlobs, which is unexpected")
		return
	}
	maxBinsSize := uint64(float64(usedMem) * Params.CommonCfg.MemPurgeRatio)
	if err := memutil.PurgeMemory(maxBinsSize); err != nil {
		log.Error(err.Error())
	}
}

func deleteSearchResults(results []*SearchResult) {
	if len(results) == 0 {
		return
	}
	for _, result := range results {
		if result != nil {
			C.DeleteSearchResult(result.cSearchResult)
		}
	}
}
