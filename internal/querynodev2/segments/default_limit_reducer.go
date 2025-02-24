package segments

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
)

type defaultLimitReducer struct {
	req    *querypb.QueryRequest
	schema *schemapb.CollectionSchema
}

type mergeParam struct {
	limit          int64
	outputFieldsId []int64
	schema         *schemapb.CollectionSchema
	reduceType     reduce.IReduceType
}

func NewMergeParam(limit int64, outputFieldsId []int64, schema *schemapb.CollectionSchema, reduceType reduce.IReduceType) *mergeParam {
	return &mergeParam{
		limit:          limit,
		outputFieldsId: outputFieldsId,
		schema:         schema,
		reduceType:     reduceType,
	}
}

func (r *defaultLimitReducer) Reduce(ctx context.Context, results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	reduceParam := NewMergeParam(r.req.GetReq().GetLimit(), r.req.GetReq().GetOutputFieldsId(),
		r.schema, reduce.ToReduceType(r.req.GetReq().GetReduceType()))
	return mergeInternalRetrieveResultsAndFillIfEmpty(ctx, results, reduceParam)
}

func newDefaultLimitReducer(req *querypb.QueryRequest, schema *schemapb.CollectionSchema) *defaultLimitReducer {
	return &defaultLimitReducer{
		req:    req,
		schema: schema,
	}
}

type defaultLimitReducerSegcore struct {
	req     *querypb.QueryRequest
	schema  *schemapb.CollectionSchema
	manager *Manager
}

func (r *defaultLimitReducerSegcore) Reduce(ctx context.Context, results []*segcorepb.RetrieveResults, segments []Segment, plan *RetrievePlan) (*segcorepb.RetrieveResults, error) {
	mergeParam := NewMergeParam(r.req.GetReq().GetLimit(), r.req.GetReq().GetOutputFieldsId(), r.schema, reduce.ToReduceType(r.req.GetReq().GetReduceType()))
	return mergeSegcoreRetrieveResultsAndFillIfEmpty(ctx, results, mergeParam, segments, plan, r.manager)
}

func newDefaultLimitReducerSegcore(req *querypb.QueryRequest, schema *schemapb.CollectionSchema, manager *Manager) *defaultLimitReducerSegcore {
	return &defaultLimitReducerSegcore{
		req:     req,
		schema:  schema,
		manager: manager,
	}
}
