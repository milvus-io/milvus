package queryplan

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/queryplan/provider"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type walManager interface {
	GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error)
}

type Server struct {
	viewpb.UnimplementedQueryPlanServiceServer
	walManager walManager
}

func NewServer(walManager walManager) *Server {
	return &Server{walManager: walManager}
}

func (s *Server) GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	p, err := s.providerForVChannel(ctx, req.GetShardId().GetVchannel())
	if err != nil {
		return nil, toRPCError(err)
	}
	plan, err := p.GetQueryPlan(ctx, req)
	if err != nil {
		return nil, toRPCError(err)
	}
	return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
}

func (s *Server) GetMVCCTimestamp(ctx context.Context, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	p, err := s.providerForVChannel(ctx, req.GetVchannel())
	if err != nil {
		return nil, toRPCError(err)
	}
	resp, err := p.GetMVCCTimestamp(ctx, req)
	if err != nil {
		return nil, toRPCError(err)
	}
	return resp, nil
}

func (s *Server) providerForVChannel(ctx context.Context, vchannel string) (provider.QueryPlanProvider, error) {
	if s == nil || s.walManager == nil {
		return nil, viewerror.NewOnShutdownError("query plan service is unavailable")
	}
	if vchannel == "" {
		return nil, viewerror.NewUnknownError("empty vchannel")
	}
	pchannel, err := worknodehandler.DecodeQueryViewPChannelFromIncomingContext(ctx)
	if err != nil {
		return nil, viewerror.NewUnknownError("%s", err.Error())
	}
	expectedPChannel := funcutil.ToPhysicalChannel(vchannel)
	if pchannel.Name != expectedPChannel {
		return nil, viewerror.NewUnknownError("query view pchannel metadata mismatch, expected %s, got %s", expectedPChannel, pchannel.Name)
	}
	rawWAL, err := s.walManager.GetAvailableWAL(pchannel)
	if err != nil {
		return nil, viewerror.NewOnShutdownError("local WAL for vchannel %s is unavailable: %s", vchannel, err.Error())
	}
	p, ok := wal.Unwrap(rawWAL).(provider.QueryPlanProvider)
	if !ok {
		return nil, viewerror.NewUnknownError("local WAL for vchannel %s does not implement query plan provider", vchannel)
	}
	return p, nil
}

func toRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return err
	}
	var viewErr *viewerror.ViewError
	if errors.As(err, &viewErr) {
		return viewerror.NewGRPCStatusFromViewError(viewErr).Err()
	}
	return viewerror.NewGRPCStatusFromViewError(viewerror.AsViewError(err)).Err()
}
