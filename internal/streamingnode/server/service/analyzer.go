package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/analyzerservice"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type analyzerProvider interface {
	RunAnalyzer(context.Context, *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error)
}

func (hs *handlerServiceImpl) RunAnalyzer(ctx context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch source := req.GetSource().(type) {
	case *streamingpb.StreamingNodeRunAnalyzerRequest_InlineAnalyzer:
		results, err := analyzer.Run(ctx, source.InlineAnalyzer.GetAnalyzerParams(), req.GetPlaceholder(), req.GetWithDetail(), req.GetWithHash())
		if err != nil {
			return nil, analyzerservice.StreamingError(err)
		}
		return &streamingpb.StreamingNodeRunAnalyzerResponse{Results: results}, nil
	case *streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer:
		field := source.FieldAnalyzer
		if field.GetCollectionId() == 0 || field.GetVchannel() == "" || field.SchemaVersion == nil || field.GetPchannel() == nil || field.GetPchannel().GetName() != funcutil.ToPhysicalChannel(field.GetVchannel()) {
			return nil, status.NewInvalidArgument("field analyzer requires collection, vchannel, schema version and matching pchannel")
		}
		raw, err := hs.walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(field.GetPchannel()))
		if err != nil {
			return nil, err
		}
		provider, ok := wal.Unwrap(raw).(analyzerProvider)
		if !ok {
			return nil, status.NewUnrecoverableError("WAL does not support analyzer execution")
		}
		resp, err := provider.RunAnalyzer(ctx, req)
		return resp, analyzerservice.StreamingError(err)
	default:
		return nil, status.NewInvalidArgument("analyzer source is required")
	}
}
