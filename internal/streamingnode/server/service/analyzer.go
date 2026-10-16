package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
		if err := fileresource.Prepare(ctx); err != nil {
			return &streamingpb.StreamingNodeRunAnalyzerResponse{
				Status: merr.Status(merr.Wrap(err, "prepare analyzer file resources")),
			}, nil
		}
		results, err := analyzer.Run(ctx, source.InlineAnalyzer.GetAnalyzerParams(), req.GetPlaceholder(), req.GetWithDetail(), req.GetWithHash())
		return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Status(err), Results: results}, nil
	case *streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer:
		field := source.FieldAnalyzer
		if field.GetCollectionId() == 0 || field.GetVchannel() == "" || field.GetPchannel() == nil || field.GetPchannel().GetName() != funcutil.ToPhysicalChannel(field.GetVchannel()) {
			return nil, status.NewInvalidArgument("field analyzer requires collection, vchannel and matching pchannel")
		}
		raw, err := hs.walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(field.GetPchannel()))
		if err != nil {
			return nil, err
		}
		provider, ok := wal.Unwrap(raw).(analyzerProvider)
		if !ok {
			return nil, status.NewUnrecoverableError("WAL does not support analyzer execution")
		}
		return provider.RunAnalyzer(ctx, req)
	default:
		return nil, status.NewInvalidArgument("analyzer source is required")
	}
}

// ValidateAnalyzer prepares the local inventory before native config validation.
// No collection, WAL ownership, QueryView or loaded segments are required.
func (hs *handlerServiceImpl) ValidateAnalyzer(ctx context.Context, req *streamingpb.StreamingNodeValidateAnalyzerRequest) (*streamingpb.StreamingNodeValidateAnalyzerResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := fileresource.Prepare(ctx); err != nil {
		return nil, analyzerPreparationError(err)
	}
	resourceIDs := typeutil.NewSet[int64]()
	for _, info := range req.GetAnalyzerInfos() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if info.GetParams() != "" {
			var params map[string]json.RawMessage
			if err := json.Unmarshal([]byte(info.GetParams()), &params); err != nil || params == nil {
				return &streamingpb.StreamingNodeValidateAnalyzerResponse{Status: merr.Status(merr.WrapErrParameterInvalidMsg("analyzer parameters must be a JSON object for field: %s, name: %s", info.GetField(), info.GetName()))}, nil
			}
		}
		ids, err := fileresource.ValidateAnalyzer(info.GetParams())
		if err != nil {
			err = merr.Wrapf(err, "validate analyzer failed for field: %s, name: %s", info.GetField(), info.GetName())
			// Validation results must not enter the readiness retry loop. In
			// particular, a valid reference can still encounter local I/O failures
			// after metadata validation; preserve their original classification.
			return &streamingpb.StreamingNodeValidateAnalyzerResponse{Status: merr.Status(err)}, nil
		}
		resourceIDs.Insert(ids...)
	}
	return &streamingpb.StreamingNodeValidateAnalyzerResponse{Status: merr.Success(), ResourceIds: resourceIDs.Collect()}, nil
}

// Preparation can return gRPC, context and network errors without merr metadata.
// Only known permanent storage failures should bypass the readiness retry loop.
func analyzerPreparationError(err error) error {
	err = merr.Wrap(err, "prepare analyzer file resources")
	if merr.IsNonRetryableErr(err) {
		return status.NewUnrecoverableError(err.Error())
	}
	return err
}
