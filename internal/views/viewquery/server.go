package viewquery

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Server implements ViewQueryService as a thin provider+scheduler adapter.
type Server struct {
	viewpb.UnimplementedViewQueryServiceServer
	provider  TaskProvider
	scheduler Scheduler
}

func NewServer(provider TaskProvider, scheduler Scheduler) *Server {
	return &Server{
		provider:  provider,
		scheduler: scheduler,
	}
}

func (s *Server) SearchOnView(ctx context.Context, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	if err := validateSearchRequest(req); err != nil {
		return nil, err
	}
	tasks, err := s.provider.AcquireSearchSegmentTasks(
		ctx,
		qviews.FromProtoShardID(req.GetShardId()),
		qviews.FromProtoQueryViewVersion(req.GetVersion()),
		req.GetMvcc(),
		req.GetLegacyReq(),
	)
	if err != nil {
		return nil, toRPCError(err)
	}
	defer tasks.Release()
	if len(tasks.Tasks()) == 0 {
		return &viewpb.SearchOnViewResponse{LegacyResults: emptySearchResults(req.GetLegacyReq())}, nil
	}

	result, err := s.scheduler.Search(ctx, tasks)
	if err != nil {
		return nil, toRPCError(err)
	}
	return &viewpb.SearchOnViewResponse{LegacyResults: result}, nil
}

func (s *Server) QueryOnView(ctx context.Context, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	if err := validateQueryRequest(req); err != nil {
		return nil, err
	}
	tasks, err := s.provider.AcquireQuerySegmentTasks(
		ctx,
		qviews.FromProtoShardID(req.GetShardId()),
		qviews.FromProtoQueryViewVersion(req.GetVersion()),
		req.GetMvcc(),
		req.GetLegacyReq(),
	)
	if err != nil {
		return nil, toRPCError(err)
	}
	defer tasks.Release()
	if len(tasks.Tasks()) == 0 {
		return &viewpb.QueryOnViewResponse{LegacyResults: emptyQueryResults()}, nil
	}

	result, err := s.scheduler.Query(ctx, tasks)
	if err != nil {
		return nil, toRPCError(err)
	}
	return &viewpb.QueryOnViewResponse{LegacyResults: result}, nil
}

func (s *Server) RequeryOnView(context.Context, *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error) {
	return nil, status.Error(codes.Unimplemented, "RequeryOnView is not implemented")
}
