package segments

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
)

type defaultLimitReducer struct {
	req    *querypb.QueryRequest
	schema *schemapb.CollectionSchema
}

func (r *defaultLimitReducer) Reduce(ctx context.Context, results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	return mergeInternalRetrieveResultsAndFillIfEmpty(ctx, results, r.req.GetReq().GetLimit(), r.req.GetReq().GetOutputFieldsId(), r.schema)
}

func newDefaultLimitReducer(req *querypb.QueryRequest, schema *schemapb.CollectionSchema) *defaultLimitReducer {
	return &defaultLimitReducer{
		req:    req,
		schema: schema,
	}
}

type extensionLimitReducer struct {
	req           *querypb.QueryRequest
	schema        *schemapb.CollectionSchema
	extendedLimit int64
}

func (r *extensionLimitReducer) Reduce(ctx context.Context, results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	return mergeInternalRetrieveResultsAndFillIfEmpty(ctx, results, r.extendedLimit, r.req.GetReq().GetOutputFieldsId(), r.schema)
}

func newExtensionLimitReducer(req *querypb.QueryRequest, schema *schemapb.CollectionSchema, extLimit int64) *extensionLimitReducer {
	return &extensionLimitReducer{
		req:           req,
		schema:        schema,
		extendedLimit: extLimit,
	}
}

type defaultLimitReducerSegcore struct {
	req    *querypb.QueryRequest
	schema *schemapb.CollectionSchema
}

func (r *defaultLimitReducerSegcore) Reduce(ctx context.Context, results []*segcorepb.RetrieveResults) (*segcorepb.RetrieveResults, error) {
	return mergeSegcoreRetrieveResultsAndFillIfEmpty(ctx, results, r.req.GetReq().GetLimit(), r.req.GetReq().GetOutputFieldsId(), r.schema)
}

func newDefaultLimitReducerSegcore(req *querypb.QueryRequest, schema *schemapb.CollectionSchema) *defaultLimitReducerSegcore {
	return &defaultLimitReducerSegcore{
		req:    req,
		schema: schema,
	}
}

type extensionLimitSegcoreReducer struct {
	req           *querypb.QueryRequest
	schema        *schemapb.CollectionSchema
	extendedLimit int64
}

func (r *extensionLimitSegcoreReducer) Reduce(ctx context.Context, results []*segcorepb.RetrieveResults) (*segcorepb.RetrieveResults, error) {
	return mergeSegcoreRetrieveResultsAndFillIfEmpty(ctx, results, r.extendedLimit, r.req.GetReq().GetOutputFieldsId(), r.schema)
}

func newExtensionLimitSegcoreReducer(req *querypb.QueryRequest, schema *schemapb.CollectionSchema, extLimit int64) *extensionLimitSegcoreReducer {
	return &extensionLimitSegcoreReducer{
		req:           req,
		schema:        schema,
		extendedLimit: extLimit,
	}
}
