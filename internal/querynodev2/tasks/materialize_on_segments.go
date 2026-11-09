package tasks

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// ExecuteMaterializeOnSegments borrows the caller's exact read schema and pinned
// segments. ExecuteOnSegments joins native work and encodes IPC before returning;
// neither cancellation nor completion releases the caller's resource handles.
func ExecuteMaterializeOnSegments(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.QueryRequest) (*internalpb.MaterializeResults, error) {
	task := NewMaterializeTask(ctx, collection, nil, req)
	if err := task.PreExecute(); err != nil {
		// Execute normally ends the scheduling span; rejected plans never run.
		task.scheduleSpan.End()
		return nil, err
	}
	if err := task.ExecuteOnSegments(selected); err != nil {
		return nil, err
	}
	return task.MaterializeResult(), nil
}
