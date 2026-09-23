package segments

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// SearchSelectedSegments runs on handles already pinned by a query-view lease.
func SearchSelectedSegments(ctx context.Context, req *SearchRequest, selected []Segment) ([]*SearchResult, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	kind := SegmentTypeGrowing
	if len(selected) > 0 {
		kind = selected[0].Type()
	}
	return searchSegments(ctx, nil, selected, kind, req)
}

func RetrieveSelectedSegments(ctx context.Context, plan *RetrievePlan, req *querypb.QueryRequest, selected []Segment) ([]RetrieveSegmentResult, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	kind := SegmentTypeGrowing
	if len(selected) > 0 {
		kind = selected[0].Type()
	}
	return retrieveOnSegments(ctx, nil, selected, kind, plan, req)
}
