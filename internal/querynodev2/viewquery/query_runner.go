package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type QueryTaskRunner interface {
	Query(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.QueryRequest, serverID int64) (*internalpb.RetrieveResults, error)
}

type queryTaskRunner struct{}

func (r queryTaskRunner) Query(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.QueryRequest, serverID int64) (*internalpb.RetrieveResults, error) {
	task := tasks.NewQueryTask(ctx, collection, nil, req)
	if err := task.PreExecute(); err != nil {
		return nil, err
	}
	if err := task.ExecuteOnSegments(selected); err != nil {
		return nil, err
	}
	return task.Result(), nil
}
