package segments

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/segcorepb"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type internalReducer interface {
	Reduce(context.Context, []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error)
}

func CreateInternalReducer(req *querypb.QueryRequest, schema *schemapb.CollectionSchema) internalReducer {
	if req.GetReq().GetIsCount() {
		return &cntReducer{}
	} else if req.GetReq().GetIterationExtensionReduceRate() > 0 {
		extendedLimit := req.GetReq().GetIterationExtensionReduceRate() * req.GetReq().Limit
		return newExtensionLimitReducer(req, schema, extendedLimit)
	}
	return newDefaultLimitReducer(req, schema)
}

type segCoreReducer interface {
	Reduce(context.Context, []*segcorepb.RetrieveResults) (*segcorepb.RetrieveResults, error)
}

func CreateSegCoreReducer(req *querypb.QueryRequest, schema *schemapb.CollectionSchema) segCoreReducer {
	if req.GetReq().GetIsCount() {
		return &cntReducerSegCore{}
	} else if req.GetReq().GetIterationExtensionReduceRate() > 0 {
		extendedLimit := req.GetReq().GetIterationExtensionReduceRate() * req.GetReq().Limit
		return newExtensionLimitSegcoreReducer(req, schema, extendedLimit)
	}
	return newDefaultLimitReducerSegcore(req, schema)
}
