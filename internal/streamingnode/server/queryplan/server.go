package queryplan

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/queryplan/provider"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type Server struct {
	viewpb.UnimplementedQueryPlanServiceServer
	walManager provider.WALManager
}

func NewServer(walManager provider.WALManager) *Server {
	return &Server{walManager: walManager}
}

func (s *Server) GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	level, selectsMVCC := req.GetMvcc().(*viewpb.GetQueryPlanRequest_ConsistencyLevel)
	strong := selectsMVCC && level.ConsistencyLevel == commonpb.ConsistencyLevel_Strong
	p, err := s.providerForVChannel(ctx, req.GetShardId().GetVchannel(), strong)
	if err != nil {
		return nil, normalizeQueryPlanError(err)
	}
	resp, err := p.GetQueryPlan(ctx, req)
	if err != nil {
		return nil, normalizeQueryPlanError(err)
	}
	return resp, nil
}

func (s *Server) GetMVCCTimestamp(ctx context.Context, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	p, err := s.providerForVChannel(ctx, req.GetVchannel(), true)
	if err != nil {
		return nil, normalizeQueryPlanError(err)
	}
	resp, err := p.GetMVCCTimestamp(ctx, req)
	if err != nil {
		return nil, normalizeQueryPlanError(err)
	}
	return resp, nil
}

func (s *Server) providerForVChannel(ctx context.Context, vchannel string, strong bool) (provider.QueryPlanProvider, error) {
	if s == nil || s.walManager == nil {
		return nil, viewerror.NewOnShutdownError("query plan service is unavailable")
	}
	writer, err := provider.QueryWriter(ctx, s.walManager, vchannel, strong)
	if err != nil {
		return nil, err
	}
	p, ok := writer.(provider.QueryPlanProvider)
	if !ok {
		return nil, viewerror.NewUnknownError("local WAL for vchannel %s does not implement query plan provider", vchannel)
	}
	return p, nil
}

// A released load config can outlive a serving View. Replan against a fresh View;
// the shared server interceptor performs wire encoding after this domain decision.
func normalizeQueryPlanError(err error) error {
	if errors.Is(err, merr.ErrCollectionNotLoaded) {
		return viewerror.NewViewInvalidated("%s", err)
	}
	return err
}
