package segcore

/*
#cgo pkg-config: milvus_core

#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"
*/
import "C"

import (
	"github.com/milvus-io/milvus/pkg/proto/segcorepb"
)

type SearchResult struct {
	cSearchResult C.CSearchResult
}

func (r *SearchResult) Release() {
	C.DeleteSearchResult(r.cSearchResult)
	r.cSearchResult = nil
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
