package snview

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

var _ viewpb.ViewQueryServiceServer = (*PChannelViewQueryServer)(nil)

type PChannelViewQueryServer struct {
	viewpb.UnimplementedViewQueryServiceServer

	walManager pchannelWALProvider
	scheduler  viewquery.Scheduler
}

func NewPChannelViewQueryServer(walManager pchannelWALProvider, scheduler viewquery.Scheduler) *PChannelViewQueryServer {
	return &PChannelViewQueryServer{
		walManager: walManager,
		scheduler:  scheduler,
	}
}

func (s *PChannelViewQueryServer) SearchOnView(ctx context.Context, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	if req == nil || req.GetShardId() == nil {
		return viewquery.NewServer(nil, s.scheduler).SearchOnView(ctx, req)
	}
	provider, err := s.taskProviderForVChannel(ctx, req.GetShardId().GetVchannel())
	if err != nil {
		return nil, asViewQueryGRPCError(err)
	}
	return viewquery.NewServer(provider, s.scheduler).SearchOnView(ctx, req)
}

func (s *PChannelViewQueryServer) QueryOnView(ctx context.Context, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	if req == nil || req.GetShardId() == nil {
		return viewquery.NewServer(nil, s.scheduler).QueryOnView(ctx, req)
	}
	provider, err := s.taskProviderForVChannel(ctx, req.GetShardId().GetVchannel())
	if err != nil {
		return nil, asViewQueryGRPCError(err)
	}
	return viewquery.NewServer(provider, s.scheduler).QueryOnView(ctx, req)
}

func (s *PChannelViewQueryServer) RequeryOnView(ctx context.Context, req *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error) {
	return viewquery.NewServer(nil, s.scheduler).RequeryOnView(ctx, req)
}

func (s *PChannelViewQueryServer) taskProviderForVChannel(ctx context.Context, vchannel string) (viewquery.TaskProvider, error) {
	if s == nil || s.walManager == nil {
		return nil, viewerror.NewOnShutdownError("view query service is unavailable")
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
	provider, ok := wal.Unwrap(rawWAL).(viewquery.TaskProvider)
	if !ok {
		return nil, viewerror.NewUnknownError("local WAL for vchannel %s does not implement view query task provider", vchannel)
	}
	return provider, nil
}

func asViewQueryGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return err
	}
	return viewerror.NewGRPCStatusFromViewError(viewerror.AsViewError(err)).Err()
}

type pchannelWALProvider interface {
	GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error)
}
