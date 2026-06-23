package segcore

/*
#cgo pkg-config: milvus_core

#include "segcore/plan_c.h"
#include "segcore/segment_c.h"
#include "segcore/search_result_export_c.h"
*/
import "C"

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

type SearchResult struct {
	cSearchResult C.CSearchResult
}

func (r *SearchResult) Release() {
	C.DeleteSearchResult(r.cSearchResult)
	r.cSearchResult = nil
}

// SearchResultMetadata is the post-search information attached to a
// SearchResult: group-by configuration plus accumulated storage cost.
// All fields are populated by a single CGO call (GetMetadata).
type SearchResultMetadata struct {
	HasGroupBy bool
	// GroupSize is meaningful only when HasGroupBy is true; 0 otherwise.
	GroupSize   int64
	StorageCost StorageCost
}

// GetMetadata returns the post-search metadata for this SearchResult. Storage
// cost is incremented by the segment search itself, by Arrow stream export
// when reading extra fields, and by FillOutputFieldsOrdered during late
// materialization — call this after all those phases complete.
func (r *SearchResult) GetMetadata() SearchResultMetadata {
	var has C.bool
	var gs, remote, total C.int64_t
	C.GetSearchResultMetadata(r.cSearchResult, &has, &gs, &remote, &total)
	return SearchResultMetadata{
		HasGroupBy: bool(has),
		GroupSize:  int64(gs),
		StorageCost: StorageCost{
			ScannedRemoteBytes: int64(remote),
			ScannedTotalBytes:  int64(total),
		},
	}
}

// ValidCount returns the count of rows that pass the filter (for two-stage search).
// Returns -1 if not set (normal search mode).
func (r *SearchResult) ValidCount() int64 {
	return int64(C.GetSearchResultValidCount(r.cSearchResult))
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
