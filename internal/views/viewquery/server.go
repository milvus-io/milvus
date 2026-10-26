package viewquery

import (
	"context"

	"golang.org/x/sync/errgroup"

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

func (s *Server) StatisticsOnView(ctx context.Context, req *viewpb.StatisticsOnViewRequest) (*viewpb.StatisticsOnViewResponse, error) {
	if err := validateCommonRequestShape(req.GetShardId(), req.GetVersion(), req.GetMvcc()); err != nil {
		return nil, err
	}
	rows, err := s.provider.Statistics(ctx, req)
	if err != nil {
		return nil, toRPCError(err)
	}
	return &viewpb.StatisticsOnViewResponse{RowCount: rows}, nil
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
		result, err = s.executeSearch(ctx, req, req.GetLegacyReq(), req.GetSegmentSelection())
	}
	if err != nil {
		return nil, toRPCError(err)
	}
	return &viewpb.SearchOnViewResponse{LegacyResults: result}, nil
}

func (s *Server) executeAdvancedSearch(ctx context.Context, req *viewpb.SearchOnViewRequest) (*internalpb.SearchResults, error) {
	legacyReq := req.GetLegacyReq()
	if len(req.GetSubSelections()) != 0 && len(req.GetSubSelections()) != len(legacyReq.GetSubReqs()) {
		return nil, merr.WrapErrServiceInternalMsg("hybrid segment selections do not match subrequests")
	}
	subRequests, err := BuildSubSearchRequests(legacyReq)
	if err != nil {
		return nil, err
	}

	results := make([]*internalpb.SearchResults, len(subRequests))
	group, groupCtx := errgroup.WithContext(ctx)
	for index := range subRequests {
		group.Go(func() error {
			if legacyReq.GetSubReqs()[index].GetSkip() {
				results[index] = emptySearchResults(subRequests[index])
				return nil
			}
			result, err := s.executeSearch(groupCtx, req, subRequests[index], subSelection(req, index))
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

func (s *Server) executeSearch(ctx context.Context, req *viewpb.SearchOnViewRequest, searchReq *internalpb.SearchRequest, selection *viewpb.SegmentSelection) (*internalpb.SearchResults, error) {
	tasks, err := s.provider.AcquireSearchSegmentTasks(
		ctx,
		qviews.FromProtoShardID(req.GetShardId()),
		qviews.FromProtoQueryViewVersion(req.GetVersion()),
		req.GetMvcc(),
		searchReq,
		selection,
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
		req.GetSegmentSelection(),
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

func subSelection(req *viewpb.SearchOnViewRequest, index int) *viewpb.SegmentSelection {
	if len(req.GetSubSelections()) != 0 {
		return req.GetSubSelections()[index]
	}
	return req.GetSegmentSelection()
}
