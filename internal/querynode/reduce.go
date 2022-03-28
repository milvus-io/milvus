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
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

type sliceInfo struct {
	slices    []int32
	reqIDs    []UniqueID
	sourceIDs []UniqueID
	reqNum    map[UniqueID]int64
	reqCount  map[UniqueID]int64
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

func parseSliceInfo(originNQs []int64, nq int64, originReqIDs []UniqueID, sourceIDs []UniqueID) *sliceInfo {
	sInfo := &sliceInfo{
		slices:    make([]int32, 0),
		reqIDs:    make([]UniqueID, 0),
		sourceIDs: make([]UniqueID, 0),
		reqNum:    make(map[UniqueID]int64),
		reqCount:  make(map[UniqueID]int64),
	}

	if nq == 0 {
		return sInfo
	}

	if len(originNQs) != len(originReqIDs) {
		return sInfo
	}
	if len(originReqIDs) != len(sourceIDs) {
		return sInfo
	}

	for i := 0; i < len(originNQs); i++ {
		for j := 0; j < int(originNQs[i]/nq); j++ {
			sInfo.slices = append(sInfo.slices, int32(nq))
			sInfo.reqIDs = append(sInfo.reqIDs, originReqIDs[i])
			sInfo.sourceIDs = append(sInfo.sourceIDs, sourceIDs[i])
			sInfo.reqNum[originReqIDs[i]]++
		}
		if tailSliceSize := originNQs[i] % nq; tailSliceSize > 0 {
			sInfo.slices = append(sInfo.slices, int32(tailSliceSize))
			sInfo.reqIDs = append(sInfo.reqIDs, originReqIDs[i])
			sInfo.sourceIDs = append(sInfo.sourceIDs, sourceIDs[i])
			sInfo.reqNum[originReqIDs[i]]++
		}
	}

	return sInfo
}

// TODO: rename by SearchResult.SliceOffset
func (s *sliceInfo) getSliceOffset(i int) int64 {
	reqID := s.reqIDs[i]
	s.reqCount[reqID]++
	return s.reqCount[reqID]
}

// TODO: rename by SearchResult.SliceNum
func (s *sliceInfo) getSliceNum(i int) int64 {
	reqID := s.reqIDs[i]
	return s.reqNum[reqID]
}

func reduceSearchResultsAndFillData(plan *SearchPlan, searchResults []*SearchResult, numSegments int64) error {
	if plan.cSearchPlan == nil {
		return errors.New("nil search plan")
	}

	cSearchResults := make([]C.CSearchResult, 0)
	for _, res := range searchResults {
		cSearchResults = append(cSearchResults, res.cSearchResult)
	}
	cSearchResultPtr := (*C.CSearchResult)(&cSearchResults[0])
	cNumSegments := C.int64_t(numSegments)

	status := C.ReduceSearchResultsAndFillData(plan.cSearchPlan, cSearchResultPtr, cNumSegments)
	if err := HandleCStatus(&status, "ReduceSearchResultsAndFillData failed"); err != nil {
		return err
	}
	return nil
}

func marshal(collectionID UniqueID, msgID UniqueID, searchResults []*SearchResult, numSegments int, sliceNQs []int32, sliceTopKs []int32) (searchResultDataBlobs, error) {
	log.Debug("start marshal...",
		zap.Int64("collectionID", collectionID),
		zap.Int64("msgID", msgID),
		zap.Int32s("sliceNQs", sliceNQs),
		zap.Int32s("sliceTopKs", sliceTopKs))

	if len(sliceNQs) != len(sliceTopKs) {
		return nil, fmt.Errorf("unaligned sliceNQs(len=%d) and sliceTopKs(len=%d)", len(sliceNQs), len(sliceTopKs))
	}

	cSearchResults := make([]C.CSearchResult, 0)
	for _, res := range searchResults {
		cSearchResults = append(cSearchResults, res.cSearchResult)
	}
	cSearchResultPtr := (*C.CSearchResult)(&cSearchResults[0])

	var cNumSegments = C.int32_t(numSegments)
	var cSliceNQSPtr = (*C.int32_t)(&sliceNQs[0])
	var cSliceTopKSPtr = (*C.int32_t)(&sliceTopKs[0])
	var cNumSlices = C.int32_t(len(sliceNQs))

	var cSearchResultDataBlobs searchResultDataBlobs

	status := C.Marshal(&cSearchResultDataBlobs, cSearchResultPtr, cNumSegments, cSliceNQSPtr, cSliceTopKSPtr, cNumSlices)
	if err := HandleCStatus(&status, "ReorganizeSearchResults failed"); err != nil {
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
}

func deleteSearchResults(results []*SearchResult) {
	for _, result := range results {
		C.DeleteSearchResult(result.cSearchResult)
	}
}
