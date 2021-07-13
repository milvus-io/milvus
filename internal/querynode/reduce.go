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

/*
#cgo CFLAGS: -I${SRCDIR}/../core/output/include
#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"

*/
import "C"
import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"unsafe"

	"github.com/milvus-io/milvus/internal/log"
)

type SearchResult struct {
	cSearchResult C.CSearchResult
}

type MarshaledHits struct {
	cMarshaledHits C.CMarshaledHits
}

func reduceSearchResults(searchResults []*SearchResult, numSegments int64, inReduced []bool) error {
	cSearchResults := make([]C.CSearchResult, 0)
	for _, res := range searchResults {
		cSearchResults = append(cSearchResults, res.cSearchResult)
	}
	cSearchResultPtr := (*C.CSearchResult)(&cSearchResults[0])
	cNumSegments := C.long(numSegments)
	cInReduced := (*C.bool)(&inReduced[0])

	status := C.ReduceSearchResults(cSearchResultPtr, cNumSegments, cInReduced)

	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("reduceSearchResults failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return nil
}

func fillTargetEntry(plan *SearchPlan, searchResults []*SearchResult, matchedSegments []*Segment, inReduced []bool) error {
	wg := &sync.WaitGroup{}
	fmt.Println(inReduced)
	for i := range inReduced {
		if inReduced[i] {
			wg.Add(1)
			go func(i int) {
				err := matchedSegments[i].fillTargetEntry(plan, searchResults[i])
				if err != nil {
					log.Error(err.Error())
				}
				wg.Done()
			}(i)
		}
	}
	wg.Wait()
	return nil
}

func reorganizeSearchResults(plan *SearchPlan, searchRequests []*searchRequest, searchResults []*SearchResult, numSegments int64, inReduced []bool) (*MarshaledHits, error) {
	cPlaceholderGroups := make([]C.CPlaceholderGroup, 0)
	for _, pg := range searchRequests {
		cPlaceholderGroups = append(cPlaceholderGroups, (*pg).cPlaceholderGroup)
	}
	var cPlaceHolderGroupPtr = (*C.CPlaceholderGroup)(&cPlaceholderGroups[0])
	var cNumGroup = (C.long)(len(searchRequests))

	cSearchResults := make([]C.CSearchResult, 0)
	for _, res := range searchResults {
		cSearchResults = append(cSearchResults, res.cSearchResult)
	}
	cSearchResultPtr := (*C.CSearchResult)(&cSearchResults[0])

	var cNumSegments = C.long(numSegments)
	var cInReduced = (*C.bool)(&inReduced[0])
	var cMarshaledHits C.CMarshaledHits

	status := C.ReorganizeSearchResults(&cMarshaledHits, cPlaceHolderGroupPtr, cNumGroup, cSearchResultPtr, cInReduced, cNumSegments, plan.cSearchPlan)
	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return nil, errors.New("reorganizeSearchResults failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return &MarshaledHits{cMarshaledHits: cMarshaledHits}, nil
}

func reorganizeSingleSearchResult(plan *SearchPlan, placeholderGroups []*searchRequest, searchResult *SearchResult) (*MarshaledHits, error) {
	cPlaceholderGroups := make([]C.CPlaceholderGroup, 0)
	for _, pg := range placeholderGroups {
		cPlaceholderGroups = append(cPlaceholderGroups, (*pg).cPlaceholderGroup)
	}
	var cPlaceHolderGroupPtr = (*C.CPlaceholderGroup)(&cPlaceholderGroups[0])
	var cNumGroup = (C.long)(len(placeholderGroups))

	cSearchResult := searchResult.cSearchResult
	var cMarshaledHits C.CMarshaledHits

	status := C.ReorganizeSingleSearchResult(&cMarshaledHits, cPlaceHolderGroupPtr, cNumGroup, cSearchResult, plan.cSearchPlan)
	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return nil, errors.New("reorganizeSearchResults failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return &MarshaledHits{cMarshaledHits: cMarshaledHits}, nil
}

func (mh *MarshaledHits) getHitsBlobSize() int64 {
	res := C.GetHitsBlobSize(mh.cMarshaledHits)
	return int64(res)
}

func (mh *MarshaledHits) getHitsBlob() ([]byte, error) {
	byteSize := mh.getHitsBlobSize()
	result := make([]byte, byteSize)
	cResultPtr := unsafe.Pointer(&result[0])
	C.GetHitsBlob(mh.cMarshaledHits, cResultPtr)
	return result, nil
}

func (mh *MarshaledHits) hitBlobSizeInGroup(groupOffset int64) ([]int64, error) {
	cGroupOffset := (C.long)(groupOffset)
	numQueries := C.GetNumQueriesPeerGroup(mh.cMarshaledHits, cGroupOffset)
	result := make([]int64, int64(numQueries))
	cResult := (*C.long)(&result[0])
	C.GetHitSizePeerQueries(mh.cMarshaledHits, cGroupOffset, cResult)
	return result, nil
}

func deleteMarshaledHits(hits *MarshaledHits) {
	C.DeleteMarshaledHits(hits.cMarshaledHits)
}

func deleteSearchResults(results []*SearchResult) {
	for _, result := range results {
		C.DeleteSearchResult(result.cSearchResult)
	}
}
