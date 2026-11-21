package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type MilvusAggReducer struct {
	groupAggReducer *agg.GroupAggReducer
	outputMap       *agg.AggregationFieldMap
}

func NewMilvusAggReducer(groupByFieldIds []int64, aggregates []*planpb.Aggregate,
	outputMap *agg.AggregationFieldMap, groupLimit int64, schema *schemapb.CollectionSchema,
) *MilvusAggReducer {
	// must ensure outputMap is not nil outside
	// Default groupLimit to -1 (no limit) if groupLimit <= 0
	if groupLimit <= 0 {
		groupLimit = -1
	}
	return &MilvusAggReducer{
		agg.NewGroupAggReducer(groupByFieldIds, aggregates, groupLimit, schema),
		outputMap,
	}
}

func (reducer *MilvusAggReducer) Reduce(results []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
	partials, err := reducer.groupAggReducer.DecodeInternalResults(results)
	if err != nil {
		return nil, err
	}
	merged, err := reducer.groupAggReducer.Merge(context.Background(), partials)
	if err != nil {
		return nil, err
	}
	reducedAggRes, err := reducer.groupAggReducer.Finalize(merged)
	if err != nil {
		return nil, err
	}
	reOrganizedFieldDatas, err := reducer.outputMap.Finalize(reducedAggRes.GetFieldDatas())
	if err != nil {
		return nil, err
	}
	return &milvuspb.QueryResults{FieldsData: reOrganizedFieldDatas, Status: merr.Success()}, nil
}
