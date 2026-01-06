package proxy

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
	reducedAggRes, err := reducer.groupAggReducer.Reduce(context.Background(), agg.InternalResult2AggResult(results))
	if err != nil {
		return nil, err
	}
	fieldCount := reducer.outputMap.Count()
	reOrganizedFieldDatas := make([]*schemapb.FieldData, fieldCount)
	reducedFieldDatas := reducedAggRes.GetFieldDatas()
	for i := 0; i < fieldCount; i++ {
		indices := reducer.outputMap.IndexesAt(i)
		if len(indices) == 0 {
			return nil, fmt.Errorf("no indices found for output field at index %d", i)
		} else if len(indices) == 1 {
			// Single index: direct copy (non-avg aggregation or group-by field)
			reOrganizedFieldDatas[i] = reducedFieldDatas[indices[0]]
			reOrganizedFieldDatas[i].FieldName = reducer.outputMap.NameAt(i)
		} else if len(indices) == 2 {
			// Two indices: avg aggregation (sum and count)
			sumFieldData := reducedFieldDatas[indices[0]]
			countFieldData := reducedFieldDatas[indices[1]]
			avgFieldData, err := agg.ComputeAvgFromSumAndCount(sumFieldData, countFieldData)
			if err != nil {
				return nil, fmt.Errorf("failed to compute avg for field %s: %w", reducer.outputMap.NameAt(i), err)
			}
			avgFieldData.FieldName = reducer.outputMap.NameAt(i)
			reOrganizedFieldDatas[i] = avgFieldData
		} else {
			return nil, fmt.Errorf("unexpected number of indices (%d) for output field at index %d, expected 1 or 2", len(indices), i)
		}
	}
	return &milvuspb.QueryResults{FieldsData: reOrganizedFieldDatas, Status: merr.Success()}, nil
}
