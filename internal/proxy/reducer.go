package proxy

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"

	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
)

type milvusReducer interface {
	Reduce([]*internalpb.RetrieveResults) (*milvuspb.QueryResults, error)
}

func createMilvusReducer(ctx context.Context, params *queryParams, req *internalpb.RetrieveRequest, schema *schemapb.CollectionSchema, plan *planpb.PlanNode, collectionName string) milvusReducer {
	if plan.GetQuery().GetIsCount() {
		return &cntReducer{}
	} else if req.GetIterationExtensionReduce() {
		params.limit = typeutil.Unlimited
	}
	return newDefaultLimitReducer(ctx, params, req, schema, collectionName)
}
