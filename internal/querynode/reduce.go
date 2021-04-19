package querynodeimp

/*
#cgo CFLAGS: -I${SRCDIR}/../core/output/include
#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"

*/
import "C"
import (
	"errors"
	"strconv"
	"unsafe"
)

type SearchResult struct {
	cQueryResult C.CQueryResult
}

type MarshaledHits struct {
	cMarshaledHits C.CMarshaledHits
}

func reduceSearchResults(searchResults []*SearchResult, numSegments int64, inReduced []bool) error {
	cSearchResults := make([]C.CQueryResult, 0)
	for _, res := range searchResults {
		cSearchResults = append(cSearchResults, res.cQueryResult)
	}
	cSearchResultPtr := (*C.CQueryResult)(&cSearchResults[0])
	cNumSegments := C.long(numSegments)
	cInReduced := (*C.bool)(&inReduced[0])

	status := C.ReduceQueryResults(cSearchResultPtr, cNumSegments, cInReduced)

	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("reduceSearchResults failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return nil
}

func fillTargetEntry(plan *Plan, searchResults []*SearchResult, matchedSegments []*Segment, inReduced []bool) error {
	for i, value := range inReduced {
		if value {
			err := matchedSegments[i].fillTargetEntry(plan, searchResults[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func reorganizeQueryResults(plan *Plan, placeholderGroups []*PlaceholderGroup, searchResults []*SearchResult, numSegments int64, inReduced []bool) (*MarshaledHits, error) {
	cPlaceholderGroups := make([]C.CPlaceholderGroup, 0)
	for _, pg := range placeholderGroups {
		cPlaceholderGroups = append(cPlaceholderGroups, (*pg).cPlaceholderGroup)
	}
	var cPlaceHolderGroupPtr = (*C.CPlaceholderGroup)(&cPlaceholderGroups[0])
	var cNumGroup = (C.long)(len(placeholderGroups))

	cSearchResults := make([]C.CQueryResult, 0)
	for _, res := range searchResults {
		cSearchResults = append(cSearchResults, res.cQueryResult)
	}
	cSearchResultPtr := (*C.CQueryResult)(&cSearchResults[0])

	var cNumSegments = C.long(numSegments)
	var cInReduced = (*C.bool)(&inReduced[0])
	var cMarshaledHits C.CMarshaledHits

	status := C.ReorganizeQueryResults(&cMarshaledHits, cPlaceHolderGroupPtr, cNumGroup, cSearchResultPtr, cInReduced, cNumSegments, plan.cPlan)
	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return nil, errors.New("reorganizeQueryResults failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
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
		C.DeleteQueryResult(result.cQueryResult)
	}
}
