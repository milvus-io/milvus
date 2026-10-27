package handler

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	remote "github.com/milvus-io/milvus/internal/streamingnode/client/handler/replicadiscovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// AcquireReplicaWatchStream creates one owner-bound stream. Recovery belongs to
// the streaming client's resuming layer, as for AcquireTransformLogStream.
func (hc *handlerClientImpl) AcquireReplicaWatchStream(ctx context.Context, pchannel string) (wal.ReplicaWatchStream, error) {
	if !hc.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrClientClosed
	}
	defer hc.lifetime.Done()
	if pchannel == "" {
		return nil, status.NewInvalidArgument("pchannel is required")
	}
	logger := mlog.With(mlog.FieldPChannel(pchannel), mlog.String("handler", "replica-watch"))
	result, err := hc.createHandlerAfterStreamingNodeReady(ctx, logger, pchannel, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		local, err := registry.GetLocalAvailableWAL(assign.Channel)
		if err == nil {
			provider, ok := wal.Unwrap(local).(wal.ReplicaWatchProvider)
			if !ok {
				return nil, status.NewOnShutdownError("local replica directory unavailable")
			}
			return provider.AcquireReplicaWatchStream(ctx)
		}
		if !shouldUseRemoteWAL(err) {
			return nil, err
		}
		client, err := hc.service.GetService(ctx)
		if err != nil {
			return nil, err
		}
		stream, err := remote.CreateEventStream(ctx, &remote.EventStreamOptions{Assignment: assign}, client)
		if err != nil {
			return nil, status.ConvertStreamingError("StreamingNodeHandlerService.SubscribeReplicas", err)
		}
		return stream, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(wal.ReplicaWatchStream), nil
}
