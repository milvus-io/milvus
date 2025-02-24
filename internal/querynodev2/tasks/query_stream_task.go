package tasks

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

var _ scheduler.Task = &QueryStreamTask{}

func NewQueryStreamTask(ctx context.Context,
	collection *segments.Collection,
	manager *segments.Manager,
	req *querypb.QueryRequest,
	srv streamrpc.QueryStreamServer,
	minMsgSize int,
	maxMsgSize int,
) *QueryStreamTask {
	return &QueryStreamTask{
		ctx:            ctx,
		collection:     collection,
		segmentManager: manager,
		req:            req,
		srv:            srv,
		minMsgSize:     minMsgSize,
		maxMsgSize:     maxMsgSize,
		notifier:       make(chan error, 1),
	}
}

type QueryStreamTask struct {
	ctx            context.Context
	collection     *segments.Collection
	segmentManager *segments.Manager
	req            *querypb.QueryRequest
	srv            streamrpc.QueryStreamServer
	minMsgSize     int
	maxMsgSize     int
	notifier       chan error
}

// Return the username which task is belong to.
// Return "" if the task do not contain any user info.
func (t *QueryStreamTask) Username() string {
	return t.req.Req.GetUsername()
}

func (t *QueryStreamTask) IsGpuIndex() bool {
	return false
}

// PreExecute the task, only call once.
func (t *QueryStreamTask) PreExecute() error {
	return nil
}

func (t *QueryStreamTask) Execute() error {
	retrievePlan, err := segcore.NewRetrievePlan(
		t.collection.GetCCollection(),
		t.req.Req.GetSerializedExprPlan(),
		t.req.Req.GetMvccTimestamp(),
		t.req.Req.Base.GetMsgID(),
	)
	if err != nil {
		return err
	}
	defer retrievePlan.Delete()

	srv := streamrpc.NewResultCacheServer(t.srv, t.minMsgSize, t.maxMsgSize)
	defer srv.Flush()

	segments, err := segments.RetrieveStream(t.ctx, t.segmentManager, retrievePlan, t.req, srv)
	defer t.segmentManager.Segment.Unpin(segments)
	if err != nil {
		return err
	}
	return nil
}

func (t *QueryStreamTask) Done(err error) {
	t.notifier <- err
}

func (t *QueryStreamTask) Canceled() error {
	return t.ctx.Err()
}

func (t *QueryStreamTask) Wait() error {
	return <-t.notifier
}

func (t *QueryStreamTask) NQ() int64 {
	return 1
}

func (t *QueryStreamTask) SearchResult() *internalpb.SearchResults {
	return nil
}
