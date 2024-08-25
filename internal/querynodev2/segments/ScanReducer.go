package segments

import (
	"context"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type ScanReducer struct {
}

func (r *ScanReducer) Reduce(ctx context.Context, results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	if len(results) != 1 {
		//hc---refine error handling here
		return nil, merr.ErrInvalidSearchResult
	}
	return results[0], nil
}

type ScanReducerSegCore struct {
}

func (r *ScanReducerSegCore) Reduce(ctx context.Context, results []*segcorepb.RetrieveResults, segments []Segment, plan *RetrievePlan) (*segcorepb.RetrieveResults, error) {
	if len(results) != 1 {
		//hc---refine error handling here
		return nil, merr.ErrInvalidSearchResult
	}
	return results[0], nil
}
