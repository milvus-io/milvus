package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	sharedviewquery "github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (s *Scheduler) Materialize(ctx context.Context, owner sharedviewquery.QuerySegmentTasks) (*internalpb.MaterializeResults, error) {
	selected, err := unwrapSNQuerySegmentTasks(owner)
	if err != nil {
		return nil, err
	}
	return s.executor.Materialize(ctx, selected, sharedviewquery.MaterializeOptions(owner))
}

func (e *DirectSegmentTaskExecutor) Materialize(ctx context.Context, selected []snview.SNQuerySegmentTask, options *viewpb.MaterializeIPCOptions) (*internalpb.MaterializeResults, error) {
	nativeCollection, nativeSegments, handles := queryExecutionScope(selected)
	req := buildSNQueryRequest(selected[0].Request, selected[0].MVCC, selected[0].VChannel, handles)
	req.MaterializeFormatVersion = options.GetFormatVersion()
	req.MaterializeSchemaFingerprint = options.GetSchemaFingerprint()
	collection, segments, err := buildGrowingQueryTaskInputs(nativeCollection, nativeSegments, req)
	if err != nil {
		return nil, err
	}
	return tasks.ExecuteMaterializeOnSegments(ctx, collection, segments, req)
}
