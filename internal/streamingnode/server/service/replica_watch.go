package service

import (
	"context"

	"github.com/cockroachdb/errors"

	replicahandler "github.com/milvus-io/milvus/internal/streamingnode/server/service/handler/replicadiscovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
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
	channel := req.GetPchannel()
	if channel.GetName() == "" || channel.GetTerm() <= 0 || channel.GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE || req.WalReplicaId < 0 {
		return status.NewInvalidArgument("replica watch requires a PChannel and writer term")
	}
	route := types.PChannelInfo{Name: channel.Name, Term: channel.Term, WALReplicaID: req.WalReplicaId, AccessMode: types.AccessModeRW}
	raw, err := manager.GetAvailableWAL(route)
	if err != nil {
		return err
	}
	provider, ok := wal.Unwrap(raw).(wal.ReplicaWatchProvider)
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
