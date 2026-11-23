package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type QueryTaskRunner interface {
	Query(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.QueryRequest, serverID int64) (*internalpb.RetrieveResults, error)
}

type queryTaskRunner struct{ schedule ScheduleRead }

func (r queryTaskRunner) Query(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.QueryRequest, serverID int64) (*internalpb.RetrieveResults, error) {
	task := tasks.NewQueryTaskOnSegments(ctx, collection, selected, req)
	var err error
	if paramtable.Get().QueryNodeCfg.MaxQueueWaitRatio.GetAsFloat() > 0 {
		err = r.schedule(task)
	} else if err = task.PreExecute(); err == nil {
		err = task.Execute()
	}
	if err != nil {
		return nil, err
	}
	return task.Result(), nil
}
