package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

var _ SegmentTaskExecutor = (*DirectSegmentTaskExecutor)(nil)

type DirectSegmentTaskExecutor struct {
	serverID     int64
	searchRunner SearchTaskRunner
	queryRunner  QueryTaskRunner
}

func NewDirectSegmentTaskExecutor(serverID int64) *DirectSegmentTaskExecutor {
	return &DirectSegmentTaskExecutor{
		serverID:     serverID,
		searchRunner: searchTaskRunner{},
		queryRunner:  queryTaskRunner{},
	}
}

func NewDirectSegmentTaskExecutorForTest(serverID int64, searchRunner SearchTaskRunner, queryRunner QueryTaskRunner) *DirectSegmentTaskExecutor {
	return &DirectSegmentTaskExecutor{
		serverID:     serverID,
		searchRunner: searchRunner,
		queryRunner:  queryRunner,
	}
}

func (e *DirectSegmentTaskExecutor) Search(ctx context.Context, tasks []qnview.QNSearchSegmentTask) (*internalpb.SearchResults, error) {
	collection, selected, handles, err := searchExecutionScope(tasks)
	if err != nil {
		return nil, err
	}
	req := buildQNSearchRequest(tasks[0].Request, tasks[0].MVCC, handles)
	return e.searchRunner.Search(ctx, collection, selected, req, e.serverID)
}

func (e *DirectSegmentTaskExecutor) Query(ctx context.Context, tasks []qnview.QNQuerySegmentTask) (*internalpb.RetrieveResults, error) {
	collection, selected, handles, err := queryExecutionScope(tasks)
	if err != nil {
		return nil, err
	}
	req := buildQNQueryRequest(tasks[0].Request, tasks[0].MVCC, handles)
	return e.queryRunner.Query(ctx, collection, selected, req, e.serverID)
}
