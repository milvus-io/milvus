package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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

func (e *DirectSegmentTaskExecutor) Search(ctx context.Context, tasks []snview.SNSearchSegmentTask) (*internalpb.SearchResults, error) {
	collection, selected, handles := searchExecutionScope(tasks)
	req := buildSNSearchRequest(tasks[0].Request, tasks[0].MVCC, tasks[0].VChannel, handles)
	mlog.Debug(ctx, "execute streamingnode search segment tasks",
		mlog.FieldCollectionID(req.GetReq().GetCollectionID()),
		mlog.FieldVChannel(tasks[0].VChannel),
		mlog.Uint64("mvccTimestamp", req.GetReq().GetMvccTimestamp()),
		mlog.Int("segmentCount", len(selected)),
	)
	return e.searchRunner.Search(ctx, collection, selected, req, e.serverID)
}

func (e *DirectSegmentTaskExecutor) Query(ctx context.Context, tasks []snview.SNQuerySegmentTask) (*internalpb.RetrieveResults, error) {
	collection, selected, handles := queryExecutionScope(tasks)
	req := buildSNQueryRequest(tasks[0].Request, tasks[0].MVCC, tasks[0].VChannel, handles)
	mlog.Debug(ctx, "execute streamingnode query segment tasks",
		mlog.FieldCollectionID(req.GetReq().GetCollectionID()),
		mlog.FieldVChannel(tasks[0].VChannel),
		mlog.Uint64("mvccTimestamp", req.GetReq().GetMvccTimestamp()),
		mlog.Int("segmentCount", len(selected)),
	)
	return e.queryRunner.Query(ctx, collection, selected, req, e.serverID)
}
