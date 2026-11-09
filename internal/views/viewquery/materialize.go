package viewquery

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"

	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (s *Server) MaterializeOnView(ctx context.Context, req *viewpb.MaterializeOnViewRequest) (*internalpb.MaterializeResults, error) {
	byPK, ok := req.GetRequest().(*viewpb.MaterializeOnViewRequest_QueryByPk)
	if !ok {
		return nil, merr.Wrap(merr.ErrServiceUnimplemented, "materialize: unsupported request implementation")
	}
	query := byPK.QueryByPk
	if err := validateQueryRequest(query); err != nil {
		return nil, err
	}
	r := query.GetLegacyReq()
	if req.GetIpcOptions().GetFormatVersion() != materialize.FormatVersion || r.GetLimit() != typeutil.Unlimited ||
		r.GetIsIterator() || r.GetIsCount() || len(r.GetAggregates()) != 0 ||
		len(r.GetGroupByFieldIds()) != 0 || len(r.GetOrderByFields()) != 0 {
		return nil, merr.Wrap(merr.ErrServiceUnimplemented, "materialize: unsupported format or retrieve shape")
	}
	// Requests can use the configured compressor. Replies carry their own IPC
	// compression and must not be compressed again by gRPC. Local calls have no
	// server transport stream, so SetSendCompressor is only needed for RPCs.
	if grpc.ServerTransportStreamFromContext(ctx) != nil {
		if err := grpc.SetSendCompressor(ctx, encoding.Identity); err != nil {
			return nil, err
		}
	}
	tasks, err := s.provider.AcquireQuerySegmentTasks(ctx,
		qviews.FromProtoShardID(query.GetShardId()), qviews.FromProtoQueryViewVersion(query.GetVersion()),
		query.GetMvcc(), r, query.GetSegmentSelection())
	if err != nil {
		return nil, err
	}
	defer tasks.Release()
	if len(tasks.Tasks()) == 0 {
		return &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{
			Status: merr.Success(), FormatVersion: materialize.FormatVersion,
		}}, nil
	}
	tasks = &MaterializeTasksWithOptions{QuerySegmentTasks: tasks, Options: req.GetIpcOptions()}
	result, err := s.scheduler.Materialize(ctx, tasks)
	if err != nil {
		return nil, err
	}
	result.Header.FormatVersion = materialize.FormatVersion
	return result, nil
}
