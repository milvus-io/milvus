package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type SearchTaskRunner interface {
	Search(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.SearchRequest, serverID int64) (*internalpb.SearchResults, error)
}

type searchTaskRunner struct{}

func (r searchTaskRunner) Search(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.SearchRequest, serverID int64) (*internalpb.SearchResults, error) {
	task := tasks.NewSearchTask(ctx, collection, nil, req, serverID)
	if err := task.ExecuteOnSegments(selected); err != nil {
		return nil, err
	}
	return task.SearchResult(), nil
}
