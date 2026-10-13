package handler

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (hc *handlerClientImpl) RunAnalyzer(ctx context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
	if !hc.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("handler client is closing")
	}
	defer hc.lifetime.Done()
	field := req.GetFieldAnalyzer()
	if field == nil && req.GetInlineAnalyzer() == nil {
		return nil, status.NewInvalidArgument("analyzer source is required")
	}
	var pchannel string
	if field != nil {
		if field.GetVchannel() == "" {
			return nil, status.NewInvalidArgument("field analyzer vchannel is required")
		}
		pchannel = funcutil.ToPhysicalChannel(field.GetVchannel())
	}
	logger := mlog.With(mlog.FieldPChannel(pchannel), mlog.String("handler", "analyzer"))
	response, err := hc.createHandlerAfterStreamingNodeReady(ctx, logger, pchannel, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		client, err := hc.service.GetService(ctx)
		if err != nil {
			return nil, err
		}
		request := req
		if assign != nil {
			request = proto.Clone(req).(*streamingpb.StreamingNodeRunAnalyzerRequest)
			request.GetFieldAnalyzer().Pchannel = types.NewProtoFromPChannelInfo(assign.Channel)
		}
		return client.RunAnalyzer(ctx, request)
	})
	if err != nil {
		return nil, err
	}
	return response.(*streamingpb.StreamingNodeRunAnalyzerResponse), nil
}
