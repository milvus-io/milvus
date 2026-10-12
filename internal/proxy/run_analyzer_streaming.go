package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (node *Proxy) buildFieldAnalyzerRequest(ctx context.Context, req *milvuspb.RunAnalyzerRequest) (*streamingpb.StreamingFieldAnalyzer, error) {
	cache := node.GetMetaCache()
	id, err := cache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return nil, err
	}
	// Resolve field and channels by the original collection ID so a concurrent
	// drop/recreate cannot redirect this request to the new collection.
	info, err := cache.GetCollectionInfo(ctx, req.GetDbName(), "", id)
	if err != nil {
		return nil, err
	}
	fieldID, exists := info.Schema.MapFieldID(req.GetFieldName())
	if !exists {
		return nil, merr.WrapErrAsInputError(merr.WrapErrFieldNotFound(req.GetFieldName()))
	}
	if len(info.VChannels) == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("collection has no analyzer channel")
	}
	return &streamingpb.StreamingFieldAnalyzer{
		CollectionId: id, Vchannel: info.VChannels[0], FieldId: fieldID, AnalyzerNames: req.GetAnalyzerNames(),
	}, nil
}
