package snview

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (s *PChannelViewQueryServer) WatchVChannelReplicas(stream viewpb.ViewQueryService_WatchVChannelReplicasServer) error {
	err := s.watchVChannelReplicas(stream)
	if err == nil || errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return err
	}
	return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
}

func (s *PChannelViewQueryServer) watchVChannelReplicas(stream viewpb.ViewQueryService_WatchVChannelReplicasServer) error {
	pc, err := worknodehandler.DecodeQueryViewPChannelFromIncomingContext(stream.Context())
	if err != nil {
		return err
	}
	raw, err := s.walManager.GetAvailableWAL(pc)
	if err != nil {
		return err
	}
	provider, ok := wal.Unwrap(raw).(replicadiscovery.Provider)
	if !ok {
		return status.NewOnShutdownError("replica directory is unavailable")
	}
	watch, err := provider.WatchReplicas()
	if err != nil {
		return err
	}
	defer watch.Close()
	return replicadiscovery.Serve(stream, pc.Name, watch, raw.Unavailable())
}
