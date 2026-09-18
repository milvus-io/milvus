package segcore

/*
#cgo pkg-config: milvus_core

#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"
*/
import "C"

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
)

type SearchResult struct {
	cSearchResult C.CSearchResult
}

func (r *SearchResult) Release() {
	C.DeleteSearchResult(r.cSearchResult)
	r.cSearchResult = nil
}

// SharedFilterBitsetResult owns one segment's shared filter bitset, produced
// by ComputeFilterBitset and reused by every branch of a shared-filter hybrid
// search on that segment. It is read-only once returned, so the concurrent
// branch searches may all hold it.
//
// The owner must call Release exactly once; a defer at the point of creation
// is the intended usage, and is what keeps this a local resource rather than a
// cache.
type SharedFilterBitsetResult struct {
	cSharedFilterBitsetResult C.CSharedFilterBitsetResult
}

func (r *SharedFilterBitsetResult) Release() {
	if r == nil || r.cSharedFilterBitsetResult == nil {
		return
	}
	C.DeleteSharedFilterBitsetResult(r.cSharedFilterBitsetResult)
	r.cSharedFilterBitsetResult = nil
}

type RetrieveResult struct {
	cRetrieveResult *C.CRetrieveResult
}

func (r *RetrieveResult) GetResult() (*segcorepb.RetrieveResults, error) {
	retrieveResult := new(segcorepb.RetrieveResults)
	if err := unmarshalCProto(r.cRetrieveResult, retrieveResult); err != nil {
		return nil, err
	}
	return retrieveResult, nil
}

func (r *RetrieveResult) Release() {
	C.DeleteRetrieveResult(r.cRetrieveResult)
	r.cRetrieveResult = nil
}

type InsertResult struct {
	InsertedRows int64
}

type DeleteResult struct{}

type LoadFieldDataResult struct{}
