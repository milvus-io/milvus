package viewquery

import (
	"context"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	var (
		result *internalpb.SearchResults
		err    error
	)
	if req.GetLegacyReq().GetIsAdvanced() {
		result, err = s.executeAdvancedSearch(ctx, req)
	} else {
		result, err = s.executeSearch(ctx, req, req.GetLegacyReq())
	}
	if err != nil {
		return nil, toRPCError(err)
	}
	return &viewpb.SearchOnViewResponse{LegacyResults: result}, nil
}

func (s *Server) executeAdvancedSearch(ctx context.Context, req *viewpb.SearchOnViewRequest) (*internalpb.SearchResults, error) {
	legacyReq := req.GetLegacyReq()
	if len(legacyReq.GetSubReqs()) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("advanced search request has no sub-requests")
	}

	subRequests := make([]*internalpb.SearchRequest, len(legacyReq.GetSubReqs()))
	parent := proto.Clone(legacyReq).(*internalpb.SearchRequest)
	parent.SubReqs = nil
	for index, subReq := range legacyReq.GetSubReqs() {
		searchReq, err := BuildSubSearchRequest(parent, subReq)
		if err != nil {
			return nil, err
		}
		subRequests[index] = searchReq
	}

	results := make([]*internalpb.SearchResults, len(subRequests))
	group, groupCtx := errgroup.WithContext(ctx)
	for index := range subRequests {
		index := index
		group.Go(func() error {
			if legacyReq.GetSubReqs()[index].GetSkip() {
				results[index] = emptySearchResults(subRequests[index])
				return nil
			}
			result, err := s.executeSearch(groupCtx, req, subRequests[index])
			if err != nil {
				return err
			}
			if result == nil {
				return merr.WrapErrServiceInternalMsg("hybrid sub-search %d returned a nil result", index)
			}
			if !merr.Ok(result.GetStatus()) {
				return merr.Error(result.GetStatus())
			}
			results[index] = result
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return assembleAdvancedSearchResults(results), nil
}

func (s *Server) executeSearch(ctx context.Context, req *viewpb.SearchOnViewRequest, searchReq *internalpb.SearchRequest) (*internalpb.SearchResults, error) {
	tasks, err := s.provider.AcquireSearchSegmentTasks(
		ctx,
		qviews.FromProtoShardID(req.GetShardId()),
		qviews.FromProtoQueryViewVersion(req.GetVersion()),
		req.GetMvcc(),
		searchReq,
	)
	if err != nil {
		return nil, err
	}
	defer tasks.Release()
	if len(tasks.Tasks()) == 0 {
		return emptySearchResults(searchReq), nil
	}

	result, err := s.scheduler.Search(ctx, tasks)
	if err != nil {
		return nil, err
	}
	return result, nil
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
