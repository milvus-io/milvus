package service

import (
	"context"

	"github.com/cockroachdb/errors"

	replicahandler "github.com/milvus-io/milvus/internal/streamingnode/server/service/handler/replicadiscovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func serveReplicaWatch(manager walmanager.Manager, rpc streamingpb.StreamingNodeHandlerService_SubscribeReplicasServer) error {
	err := acquireAndServeReplicaWatch(manager, rpc)
	if err == nil || errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return err
	}
	return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
}

func acquireAndServeReplicaWatch(manager walmanager.Manager, rpc streamingpb.StreamingNodeHandlerService_SubscribeReplicasServer) error {
	req, err := contextutil.GetCreateReplicaWatchStream(rpc.Context())
	if err != nil {
		return status.NewInvalidArgument("create replica watch stream request is required")
	}
	raw, err := manager.GetAvailableWAL(types.NewPChannelInfoFromProto(req.GetPchannel()))
	if err != nil {
		return err
	}
	provider, ok := wal.Unwrap(raw).(replicadiscovery.Provider)
	if !ok {
		return status.NewOnShutdownError("replica directory unavailable")
	}
	stream, err := provider.AcquireReplicaWatchStream(rpc.Context())
	if err != nil {
		return err
	}
	defer stream.Close()
	return replicahandler.ServeStream(rpc, req.GetPchannel().GetName(), stream)
}
