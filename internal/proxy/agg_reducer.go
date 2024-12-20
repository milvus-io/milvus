package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
)

type MilvusAggReducer struct {
	groupAggReducer *agg.GroupAggReducer
	outputMap       *agg.AggregationFieldMap
}

func NewMilvusAggReducer(groupByFieldIds []int64, aggregates []*planpb.Aggregate,
	outputMap *agg.AggregationFieldMap,
) *MilvusAggReducer {
	// must ensure outputMap is not nil outside
	return &MilvusAggReducer{
		agg.NewGroupAggReducer(groupByFieldIds, aggregates),
		outputMap,
	}
}

func (reducer *MilvusAggReducer) Reduce(results []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
	reducedAggRes, err := reducer.groupAggReducer.Reduce(context.Background(), agg.InternalResult2AggResult(results))
	fieldCount := reducer.outputMap.Count()
	reOrganizedFieldDatas := make([]*schemapb.FieldData, fieldCount)
	for i := 0; i < fieldCount; i++ {
		reducedIdx := reducer.outputMap.IndexAt(i)
		reOrganizedFieldDatas[i] = reducedAggRes.GetFieldDatas()[reducedIdx]
		reOrganizedFieldDatas[i].FieldName = reducer.outputMap.NameAt(i)
	}
	return &milvuspb.QueryResults{FieldsData: reOrganizedFieldDatas}, err
}
