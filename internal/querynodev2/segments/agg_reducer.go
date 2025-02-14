package segments

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/proto/segcorepb"
)

type InternalAggReducer struct {
	groupAggReducer *agg.GroupAggReducer
}

func NewInternalAggReducer(groupByFieldIds []int64, aggregates []*planpb.Aggregate, groupLimit int64, schema *schemapb.CollectionSchema) *InternalAggReducer {
	return &InternalAggReducer{
		agg.NewGroupAggReducer(groupByFieldIds, aggregates, groupLimit, schema),
	}
}

func (reducer *InternalAggReducer) Reduce(ctx context.Context, results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	reducedAggRes, err := reducer.groupAggReducer.Reduce(ctx, agg.InternalResult2AggResult(results))
	if err != nil {
		return nil, err
	}
	return agg.AggResult2internalResult(reducedAggRes), err
}

type SegcoreAggReducer struct {
	groupAggReducer *agg.GroupAggReducer
}

func NewSegcoreAggReducer(groupByFieldIds []int64, aggregates []*planpb.Aggregate, groupLimit int64, schema *schemapb.CollectionSchema) *SegcoreAggReducer {
	return &SegcoreAggReducer{
		agg.NewGroupAggReducer(groupByFieldIds, aggregates, groupLimit, schema),
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
