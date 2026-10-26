package handler

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (qvc *queryViewClient) SubscribeReplicas(vchannel string, handler replicadiscovery.Handler) (*replicadiscovery.Subscription, error) {
	if !qvc.owner.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrClientClosed
	}
	defer qvc.owner.lifetime.Done()
	return qvc.replicas.Subscribe(vchannel, handler)
}

func (qvc *queryViewClient) createReplicaWatchStream(ctx context.Context, pchannel string) (replicadiscovery.Stream, error) {
	if !qvc.owner.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrClientClosed
	}
	defer qvc.owner.lifetime.Done()
	logger := mlog.With(mlog.FieldPChannel(pchannel), mlog.String("handler", "ViewQueryService.WatchVChannelReplicas"))
	result, err := qvc.owner.createHandlerAfterStreamingNodeReady(ctx, logger, pchannel, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		local, err := registry.GetLocalAvailableWAL(assign.Channel)
		if err == nil {
			provider, ok := wal.Unwrap(local).(replicadiscovery.Provider)
			if !ok {
				return nil, status.NewOnShutdownError("local replica directory is unavailable")
			}
			watch, err := provider.WatchReplicas()
			if err != nil {
				return nil, err
			}
			return replicadiscovery.NewLocalStream(ctx, watch, local.Unavailable()), nil
		}
		if !shouldUseRemoteWAL(err) {
			return nil, err
		}
		client, err := qvc.viewQueryService.GetService(ctx)
		if err != nil {
			return nil, err
		}
		attemptCtx, cancel := context.WithCancel(ctx)
		stream, err := client.WatchVChannelReplicas(worknodehandler.EncodeQueryViewPChannelToOutgoingContext(attemptCtx, assign.Channel))
		if err == nil {
			var response *viewpb.WatchVChannelReplicasResponse
			response, err = stream.Recv()
			if err == nil && !response.GetReady() {
				err = status.NewInvalidRequestSeq("first replica watch response must acknowledge stream creation")
			}
		}
		if err != nil {
			cancel()
			return nil, status.ConvertStreamingError("ViewQueryService.WatchVChannelReplicas", err)
		}
		// Release the attempt child on RPC completion as well as logical cancellation.
		context.AfterFunc(stream.Context(), cancel)
		return replicadiscovery.NewRemoteStream(stream, cancel), nil
	})
	if err != nil {
		return nil, err
	}
	return result.(replicadiscovery.Stream), nil
}
