package segments

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
)

type internalReducer interface {
	Reduce(context.Context, []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error)
}

func CreateInternalReducer(req *querypb.QueryRequest, schema *schemapb.CollectionSchema) internalReducer {
	if req.GetReq().GetIsCount() {
		return &cntReducer{}
	}
	return newDefaultLimitReducer(req, schema)
}

type segCoreReducer interface {
	Reduce(context.Context, []*segcorepb.RetrieveResults, []Segment, *RetrievePlan) (*segcorepb.RetrieveResults, error)
}

func CreateSegCoreReducer(req *querypb.QueryRequest, schema *schemapb.CollectionSchema, manager *Manager) segCoreReducer {
	if req.GetReq().GetIsCount() {
		return &cntReducerSegCore{}
	}
	return newDefaultLimitReducerSegcore(req, schema, manager)
}
