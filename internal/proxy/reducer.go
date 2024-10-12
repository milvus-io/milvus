package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
)

type milvusReducer interface {
	Reduce([]*internalpb.RetrieveResults) (*milvuspb.QueryResults, error)
}

func createMilvusReducer(ctx context.Context,
	params *queryParams,
	req *internalpb.RetrieveRequest,
	schema *schemapb.CollectionSchema,
	plan *planpb.PlanNode,
	collectionName string,
	outputMap *agg.AggregationFieldMap,
) milvusReducer {
	if len(req.GetAggregates()) > 0 || len(req.GetGroupByFieldIds()) > 0 {
		return NewMilvusAggReducer(req.GetGroupByFieldIds(), req.GetAggregates(), outputMap, plan.GetQuery().GetLimit(), schema)
	}
	return newDefaultLimitReducer(ctx, params, req, schema, collectionName)
}
