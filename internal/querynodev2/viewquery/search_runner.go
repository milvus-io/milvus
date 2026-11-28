package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type SearchTaskRunner interface {
	Search(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.SearchRequest, serverID int64) (*internalpb.SearchResults, error)
}

type searchTaskRunner struct{ schedule ScheduleRead }

func (r searchTaskRunner) Search(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.SearchRequest, serverID int64) (*internalpb.SearchResults, error) {
	task := tasks.NewSearchTaskOnSegments(ctx, collection, selected, req, serverID)
	var err error
	if paramtable.Get().QueryNodeCfg.MaxQueueWaitRatio.GetAsFloat() > 0 {
		err = r.schedule(task)
	} else {
		err = task.Execute()
	}
	if err != nil {
		return nil, err
	}
	return task.SearchResult(), nil
}
