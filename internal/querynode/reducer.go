package querynode

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/segcorepb"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type internalReducer interface {
	Reduce([]*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error)
}

func createInternalReducer(ctx context.Context, req *querypb.QueryRequest, schema *schemapb.CollectionSchema) internalReducer {
	if req.GetReq().GetIsCount() {
		return &cntReducer{}
	}
	return newDefaultLimitReducer(ctx, req, schema)
}

type segCoreReducer interface {
	Reduce([]*segcorepb.RetrieveResults) (*segcorepb.RetrieveResults, error)
}

func createSegCoreReducer(ctx context.Context, req *querypb.QueryRequest, schema *schemapb.CollectionSchema) segCoreReducer {
	if req.GetReq().GetIsCount() {
		return &cntReducerSegCore{}
	}
	return newDefaultLimitReducerSegcore(ctx, req, schema)
}
