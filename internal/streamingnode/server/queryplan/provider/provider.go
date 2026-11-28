package provider

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type QueryPlanProvider interface {
	GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)
	GetMVCCTimestamp(ctx context.Context, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error)
}

type WALManager interface {
	GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error)
	GetAvailableRawWALByPChannel(string) (wal.WAL, error)
}

// QueryWriter resolves both planning and execution against the advertised writer.
// TODO: Enable RO queries once QueryView discovery can select a physical replica.
func QueryWriter(ctx context.Context, manager WALManager, vchannel string, requireTerm bool) (wal.WAL, error) {
	if vchannel == "" {
		return nil, viewerror.NewUnknownError("empty vchannel")
	}
	route, err := worknodehandler.DecodeQueryViewPChannelFromIncomingContext(ctx)
	if err != nil {
		return nil, viewerror.NewUnknownError("%s", err.Error())
	}
	if expected := funcutil.ToPhysicalChannel(vchannel); route.Name != expected {
		return nil, viewerror.NewUnknownError("query view pchannel metadata mismatch, expected %s, got %s", expected, route.Name)
	}
	if route.AccessMode != types.AccessModeRW || (requireTerm && route.Term <= 0) {
		return nil, viewerror.NewNotPrimaryError("query requires the serving writer for %s", route.Name)
	}
	var writer wal.WAL
	if route.Term != 0 {
		writer, err = manager.GetAvailableWAL(route)
	} else {
		writer, err = manager.GetAvailableRawWALByPChannel(route.Name)
		if err == nil {
			owner := writer.Channel()
			if owner.AccessMode != types.AccessModeRW || owner.WALReplicaID != route.WALReplicaID {
				return nil, viewerror.NewNotPrimaryError("writer instance changed for %s", route.Name)
			}
		}
	}
	if err != nil {
		return nil, viewerror.NewOnShutdownError("local WAL for vchannel %s is unavailable: %s", vchannel, err.Error())
	}
	return wal.Unwrap(writer), nil
}
