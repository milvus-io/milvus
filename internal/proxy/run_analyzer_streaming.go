package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/analyzerservice"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (t *RunAnalyzerTask) runFieldAnalyzer(ctx context.Context) (*milvuspb.RunAnalyzerResponse, error) {
	cache := t.GetMetaCache()
	req := t.RunAnalyzerRequest
	id, err := cache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return nil, err
	}
	if id != t.collectionID {
		return nil, merr.WrapErrCollectionNotFound(t.collectionID)
	}
	// Resolve field and channels from the original collection's metadata.
	// A name rebound by drop/recreate must not supply a different collection.
	info, err := cache.GetCollectionInfo(ctx, req.GetDbName(), "", t.collectionID)
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
	response, err := streaming.WAL().AnalyzerClient().RunAnalyzer(ctx, &streamingpb.StreamingNodeRunAnalyzerRequest{
		Placeholder: req.GetPlaceholder(), WithDetail: req.GetWithDetail(), WithHash: req.GetWithHash(),
		Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{
			CollectionId: id, Vchannel: info.VChannels[0], FieldId: fieldID, AnalyzerNames: req.GetAnalyzerNames(),
		}},
	})
	if err != nil {
		return nil, analyzerservice.PublicError(err)
	}
	return &milvuspb.RunAnalyzerResponse{Status: response.GetStatus(), Results: response.GetResults()}, nil
}
