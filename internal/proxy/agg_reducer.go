package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type MilvusAggReducer struct {
	groupAggReducer *agg.GroupAggReducer
	outputMap       *agg.AggregationFieldMap
}

func NewMilvusAggReducer(groupByFieldIds []int64, aggregates []*planpb.Aggregate,
	outputMap *agg.AggregationFieldMap, groupLimit int64, schema *schemapb.CollectionSchema,
) *MilvusAggReducer {
	// must ensure outputMap is not nil outside
	return &MilvusAggReducer{
		agg.NewGroupAggReducer(groupByFieldIds, aggregates, groupLimit, schema),
		outputMap,
	}
}

func (reducer *MilvusAggReducer) Reduce(results []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
	reducedAggRes, err := reducer.groupAggReducer.Reduce(context.Background(), agg.InternalResult2AggResult(results))
	if err != nil {
		return nil, err
	}
	fieldCount := reducer.outputMap.Count()
	reOrganizedFieldDatas := make([]*schemapb.FieldData, fieldCount)
	for i := 0; i < fieldCount; i++ {
		reducedIdx := reducer.outputMap.IndexAt(i)
		reOrganizedFieldDatas[i] = reducedAggRes.GetFieldDatas()[reducedIdx]
		reOrganizedFieldDatas[i].FieldName = reducer.outputMap.NameAt(i)
	}
	return &milvuspb.QueryResults{FieldsData: reOrganizedFieldDatas, Status: merr.Status(err)}, nil
}
