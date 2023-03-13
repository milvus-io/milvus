package querynode

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
)

type defaultLimitReducer struct {
	ctx    context.Context
	req    *querypb.QueryRequest
	schema *schemapb.CollectionSchema
}

func (r *defaultLimitReducer) Reduce(results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	return mergeInternalRetrieveResultsAndFillIfEmpty(r.ctx, results, r.req.GetReq().GetLimit(), r.req.GetReq().GetOutputFieldsId(), r.schema)
}

func newDefaultLimitReducer(ctx context.Context, req *querypb.QueryRequest, schema *schemapb.CollectionSchema) *defaultLimitReducer {
	return &defaultLimitReducer{
		ctx:    ctx,
		req:    req,
		schema: schema,
	}
}

type defaultLimitReducerSegcore struct {
	ctx    context.Context
	req    *querypb.QueryRequest
	schema *schemapb.CollectionSchema
}

func (r *defaultLimitReducerSegcore) Reduce(results []*segcorepb.RetrieveResults) (*segcorepb.RetrieveResults, error) {
	return mergeSegcoreRetrieveResultsAndFillIfEmpty(r.ctx, results, r.req.GetReq().GetLimit(), r.req.GetReq().GetOutputFieldsId(), r.schema)
}

func newDefaultLimitReducerSegcore(ctx context.Context, req *querypb.QueryRequest, schema *schemapb.CollectionSchema) *defaultLimitReducerSegcore {
	return &defaultLimitReducerSegcore{
		ctx:    ctx,
		req:    req,
		schema: schema,
	}
}
