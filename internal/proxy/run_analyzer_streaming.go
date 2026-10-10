package proxy

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/analyzerservice"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func (t *RunAnalyzerTask) runFieldAnalyzer(ctx context.Context) (*milvuspb.RunAnalyzerResponse, error) {
	cache := t.GetMetaCache()
	req := t.RunAnalyzerRequest
	var result *milvuspb.RunAnalyzerResponse
	originalID := t.collectionID
	err := retry.Handle(ctx, func() (bool, error) {
		result = nil
		id, err := cache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
		if err != nil {
			return false, err
		}
		if id != originalID {
			return false, merr.WrapErrCollectionNotFound(originalID)
		}
		// Read schema and channels from one ID-bound metadata snapshot. A name
		// rebound by drop/recreate must never supply the schema for the old ID.
		info, err := cache.GetCollectionInfo(ctx, req.GetDbName(), "", originalID)
		if err != nil {
			return false, err
		}
		schema := info.Schema
		fieldID, exists := schema.MapFieldID(req.GetFieldName())
		if !exists {
			return false, merr.WrapErrAsInputError(merr.WrapErrFieldNotFound(req.GetFieldName()))
		}
		if len(info.VChannels) == 0 {
			return true, merr.WrapErrServiceNotReadyMsg("collection has no analyzer channel")
		}
		version := schema.GetVersion()
		response, err := streaming.WAL().AnalyzerClient().RunAnalyzer(ctx, &streamingpb.StreamingNodeRunAnalyzerRequest{
			Placeholder: req.GetPlaceholder(), WithDetail: req.GetWithDetail(), WithHash: req.GetWithHash(),
			Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{
				CollectionId: id, Vchannel: info.VChannels[0], FieldId: fieldID, SchemaVersion: &version, AnalyzerNames: req.GetAnalyzerNames(),
			}},
		})
		if err != nil {
			return false, analyzerservice.PublicError(err)
		}
		result = &milvuspb.RunAnalyzerResponse{Status: response.GetStatus(), Results: response.GetResults()}
		if analyzerErr := merr.Error(response.GetStatus()); errors.Is(analyzerErr, merr.ErrCollectionSchemaVersionNotReady) {
			cache.RemoveCollection(ctx, req.GetDbName(), req.GetCollectionName())
			return true, analyzerErr
		}
		return false, nil
	}, retry.Attempts(3))
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// Preserve the analyzer Status even when schema-refresh retries are exhausted.
	if result != nil {
		return result, nil
	}
	return nil, err
}
