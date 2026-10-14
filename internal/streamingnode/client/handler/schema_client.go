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

func (hc *handlerClientImpl) GetCollectionSchema(ctx context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
	if !hc.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("handler client is closing")
	}
	defer hc.lifetime.Done()
	if req.GetCollectionId() == 0 || req.GetVchannel() == "" {
		return nil, status.NewInvalidArgument("schema lookup requires collection and vchannel")
	}
	pchannel := funcutil.ToPhysicalChannel(req.GetVchannel())
	logger := mlog.With(mlog.FieldPChannel(pchannel), mlog.String("handler", "schema"))
	response, err := hc.createHandlerAfterStreamingNodeReady(ctx, logger, pchannel, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		client, err := hc.service.GetService(ctx)
		if err != nil {
			return nil, err
		}
		request := proto.Clone(req).(*streamingpb.StreamingNodeGetCollectionSchemaRequest)
		request.Pchannel = types.NewProtoFromPChannelInfo(assign.Channel)
		return client.GetCollectionSchema(ctx, request)
	})
	if err != nil {
		return nil, err
	}
	return response.(*streamingpb.StreamingNodeGetCollectionSchemaResponse), nil
}
