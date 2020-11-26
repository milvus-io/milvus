package querynode

/*
#cgo CFLAGS: -I${SRCDIR}/../core/output/include
#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"

*/
import "C"
import (
	"unsafe"
)

type SearchResult struct {
	cQueryResult C.CQueryResult
}

type MarshaledHits struct {
	cMarshaledHits C.CMarshaledHits
}

func reduceSearchResults(searchResults []*SearchResult, numSegments int64) *SearchResult {
	cSearchResults := make([]C.CQueryResult, 0)
	for _, res := range searchResults {
		cSearchResults = append(cSearchResults, res.cQueryResult)
	}
	cSearchResultPtr := (*C.CQueryResult)(&cSearchResults[0])
	cNumSegments := C.long(numSegments)
	res := C.ReduceQueryResults(cSearchResultPtr, cNumSegments)
	return &SearchResult{cQueryResult: res}
}

func (sr *SearchResult) reorganizeQueryResults(plan *Plan, placeholderGroups []*PlaceholderGroup) *MarshaledHits {
	cPlaceholderGroups := make([]C.CPlaceholderGroup, 0)
	for _, pg := range placeholderGroups {
		cPlaceholderGroups = append(cPlaceholderGroups, (*pg).cPlaceholderGroup)
	}
	cNumGroup := (C.long)(len(placeholderGroups))
	var cPlaceHolder = (*C.CPlaceholderGroup)(&cPlaceholderGroups[0])
	res := C.ReorganizeQueryResults(sr.cQueryResult, plan.cPlan, cPlaceHolder, cNumGroup)
	return &MarshaledHits{cMarshaledHits: res}
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
