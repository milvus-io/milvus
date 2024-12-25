package segments

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
)

type InternalAggReducer struct {
	groupAggReducer *agg.GroupAggReducer
}

func NewInternalAggReducer(groupByFieldIds []int64, aggregates []*planpb.Aggregate, groupLimit int64) *InternalAggReducer {
	return &InternalAggReducer{
		agg.NewGroupAggReducer(groupByFieldIds, aggregates, groupLimit),
	}
}

func (reducer *InternalAggReducer) Reduce(ctx context.Context, results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	reducedAggRes, err := reducer.groupAggReducer.Reduce(ctx, agg.InternalResult2AggResult(results))
	return agg.AggResult2internalResult(reducedAggRes), err
}

type SegcoreAggReducer struct {
	groupAggReducer *agg.GroupAggReducer
}

func NewSegcoreAggReducer(groupByFieldIds []int64, aggregates []*planpb.Aggregate, groupLimit int64) *SegcoreAggReducer {
	return &SegcoreAggReducer{
		agg.NewGroupAggReducer(groupByFieldIds, aggregates, groupLimit),
	}
}

func (reducer *SegcoreAggReducer) Reduce(ctx context.Context, results []*segcorepb.RetrieveResults, segments []Segment, plan *RetrievePlan) (*segcorepb.RetrieveResults, error) {
	aggRes, err := agg.SegcoreResults2AggResult(results)
	if err != nil {
		return nil, err
	}
	reducedAggRes, err := reducer.groupAggReducer.Reduce(ctx, aggRes)
	if err != nil {
		return nil, err
	}
	if reducedAggRes == nil {
		return nil, fmt.Errorf("reduced Segcore Agg Result cannot be nil")
	}
	return agg.AggResult2segcoreResult(reducedAggRes), err
}
