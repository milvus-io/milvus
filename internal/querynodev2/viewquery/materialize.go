package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	sharedviewquery "github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (s *Scheduler) Materialize(ctx context.Context, owner sharedviewquery.QuerySegmentTasks) (*internalpb.MaterializeResults, error) {
	selected, err := unwrapQNQuerySegmentTasks(owner)
	if err != nil {
		return nil, err
	}
	return s.executor.Materialize(ctx, selected, sharedviewquery.MaterializeOptions(owner))
}

func (e *DirectSegmentTaskExecutor) Materialize(ctx context.Context, selected []qnview.QNQuerySegmentTask, options *viewpb.MaterializeIPCOptions) (*internalpb.MaterializeResults, error) {
	collection, segments, handles, err := queryExecutionScope(selected)
	if err != nil {
		return nil, err
	}
	req := buildQNQueryRequest(selected[0].Request, selected[0].MVCC, handles)
	req.MaterializeFormatVersion = options.GetFormatVersion()
	req.MaterializeSchemaFingerprint = options.GetSchemaFingerprint()

	return tasks.ExecuteMaterializeOnSegments(ctx, collection, segments, req)
}
