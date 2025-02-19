package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

type milvusReducer interface {
	Reduce([]*internalpb.RetrieveResults) (*milvuspb.QueryResults, error)
}

func createMilvusReducer(ctx context.Context, params *queryParams, req *internalpb.RetrieveRequest, schema *schemapb.CollectionSchema, plan *planpb.PlanNode, collectionName string) milvusReducer {
	if plan.GetQuery().GetIsCount() {
		return &cntReducer{
			collectionName: collectionName,
		}
	}
	return newDefaultLimitReducer(ctx, params, req, schema, collectionName)
}
