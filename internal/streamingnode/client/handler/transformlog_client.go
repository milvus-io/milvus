package handler

import (
	"context"

	transformlogclient "github.com/milvus-io/milvus/internal/streamingnode/client/handler/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// AcquireTransformLogStream opens one assignment-aware remote stream.
func AcquireTransformLogStream(ctx context.Context, client HandlerClient, pchannel string) (wal.TransformLogStream, error) {
	provider, ok := client.(interface {
		AcquireTransformLogStream(context.Context, string) (wal.TransformLogStream, error)
	})
	if !ok {
		return nil, status.NewUnrecoverableError("handler client does not support transform subscriptions")
	}
	return provider.AcquireTransformLogStream(ctx, pchannel)
}

func (hc *handlerClientImpl) AcquireTransformLogStream(ctx context.Context, pchannel string) (wal.TransformLogStream, error) {
	if !hc.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrClientClosed
	}
	defer hc.lifetime.Done()
	if pchannel == "" {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	logger := mlog.With(mlog.FieldPChannel(pchannel), mlog.String("handler", "transformlog"))
	stream, err := hc.createHandlerAfterStreamingNodeReady(ctx, logger, pchannel, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		service, err := hc.service.GetService(ctx)
		if err != nil {
			return nil, err
		}
		return transformlogclient.CreateEventStream(ctx, &transformlogclient.EventStreamOptions{Assignment: assign}, service)
	})
	if err != nil {
		return nil, err
	}
	return stream.(wal.TransformLogStream), nil
}
