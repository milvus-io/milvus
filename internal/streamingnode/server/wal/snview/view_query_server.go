package snview

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/queryplan/provider"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

var _ viewpb.ViewQueryServiceServer = (*PChannelViewQueryServer)(nil)

type PChannelViewQueryServer struct {
	viewpb.UnimplementedViewQueryServiceServer

	walManager provider.WALManager
	scheduler  viewquery.Scheduler
}

func NewPChannelViewQueryServer(walManager provider.WALManager, scheduler viewquery.Scheduler) *PChannelViewQueryServer {
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
		return nil, err
	}
	return viewquery.NewServer(provider, s.scheduler).SearchOnView(ctx, req)
}

func (s *PChannelViewQueryServer) SearchOnViewStream(stream viewpb.ViewQueryService_SearchOnViewStreamServer) error {
	initial, err := stream.Recv()
	if err != nil {
		return err
	}
	prefetched := &prefetchedSearchOnViewStream{
		ViewQueryService_SearchOnViewStreamServer: stream,
		initial: initial,
	}
	request := initial.GetRequest()
	if request == nil || request.GetShardId() == nil {
		return viewquery.NewServer(nil, s.scheduler).SearchOnViewStream(prefetched)
	}
	provider, err := s.taskProviderForVChannel(stream.Context(), request.GetShardId().GetVchannel())
	if err != nil {
		return err
	}
	return viewquery.NewServer(provider, s.scheduler).SearchOnViewStream(prefetched)
}

func (s *PChannelViewQueryServer) QueryOnView(ctx context.Context, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	if req == nil || req.GetShardId() == nil {
		return viewquery.NewServer(nil, s.scheduler).QueryOnView(ctx, req)
	}
	provider, err := s.taskProviderForVChannel(ctx, req.GetShardId().GetVchannel())
	if err != nil {
		return nil, err
	}
	return viewquery.NewServer(provider, s.scheduler).QueryOnView(ctx, req)
}

func (s *PChannelViewQueryServer) QueryOnViewStream(stream viewpb.ViewQueryService_QueryOnViewStreamServer) error {
	initial, err := stream.Recv()
	if err != nil {
		return err
	}
	prefetched := &prefetchedQueryOnViewStream{
		ViewQueryService_QueryOnViewStreamServer: stream,
		initial:                                  initial,
	}
	request := initial.GetRequest()
	if request == nil || request.GetShardId() == nil {
		return viewquery.NewServer(nil, s.scheduler).QueryOnViewStream(prefetched)
	}
	provider, err := s.taskProviderForVChannel(stream.Context(), request.GetShardId().GetVchannel())
	if err != nil {
		return err
	}
	return viewquery.NewServer(provider, s.scheduler).QueryOnViewStream(prefetched)
}

func (s *PChannelViewQueryServer) taskProviderForVChannel(ctx context.Context, vchannel string) (viewquery.TaskProvider, error) {
	if s == nil || s.walManager == nil {
		return nil, viewerror.NewOnShutdownError("view query service is unavailable")
	}
	writer, err := provider.QueryWriter(ctx, s.walManager, vchannel, false)
	if err != nil {
		return nil, err
	}
	provider, ok := writer.(viewquery.TaskProvider)
	if !ok {
		return nil, viewerror.NewUnknownError("local WAL for vchannel %s does not implement view query task provider", vchannel)
	}
	return provider, nil
}

type pchannelWALProvider interface {
	GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error)
}

type prefetchedSearchOnViewStream struct {
	viewpb.ViewQueryService_SearchOnViewStreamServer
	initial *viewpb.SearchOnViewStreamRequest
}

func (s *prefetchedSearchOnViewStream) Recv() (*viewpb.SearchOnViewStreamRequest, error) {
	if s.initial != nil {
		initial := s.initial
		s.initial = nil
		return initial, nil
	}
	return s.ViewQueryService_SearchOnViewStreamServer.Recv()
}

type prefetchedQueryOnViewStream struct {
	viewpb.ViewQueryService_QueryOnViewStreamServer
	initial *viewpb.QueryOnViewStreamRequest
}

func (s *prefetchedQueryOnViewStream) Recv() (*viewpb.QueryOnViewStreamRequest, error) {
	if s.initial != nil {
		initial := s.initial
		s.initial = nil
		return initial, nil
	}
	return s.ViewQueryService_QueryOnViewStreamServer.Recv()
}
