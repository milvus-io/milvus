//go:build test && dynamic

package viewquery

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	readscheduler "github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	sharedviewquery "github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSchedulerSearchDelegatesQNTasks(t *testing.T) {
	executor := &fakeQNSegmentTaskExecutor{
		searchResult: &internalpb.SearchResults{NumQueries: 10},
	}
	scheduler := NewScheduler(executor)
	req := &internalpb.SearchRequest{CollectionID: 100}
	mvcc := &viewpb.QueryPlanMVCC{TransformingTimetick: 10}
	tasks := qnview.NewQNSearchSegmentTasks([]qnview.QNSearchSegmentTask{
		{Handle: fakeSealedSegmentHandle{id: 1}, Request: req, MVCC: mvcc},
		{Handle: fakeSealedSegmentHandle{id: 2}, Request: req, MVCC: mvcc},
	})

	options := &viewpb.SearchExecutionOptions{FilterOnly: true, EnableExprCache: true}
	result, err := scheduler.Search(context.Background(), &sharedviewquery.SearchTasksWithOptions{SearchSegmentTasks: tasks, Options: options})

	require.NoError(t, err)
	assert.Same(t, executor.searchResult, result)
	require.Len(t, executor.searchTasks, 2)
	assert.Equal(t, int64(1), executor.searchTasks[0].Handle.ID())
	assert.Equal(t, int64(2), executor.searchTasks[1].Handle.ID())
	assert.Same(t, options, executor.searchTasks[0].Options)
	assert.Same(t, req, executor.searchTasks[0].Request)
	assert.Same(t, mvcc, executor.searchTasks[0].MVCC)
}

func TestSchedulerSearchRejectsNonQNTask(t *testing.T) {
	executor := &fakeQNSegmentTaskExecutor{}
	scheduler := NewScheduler(executor)

	_, err := scheduler.Search(context.Background(), fakeSearchSegmentTasks{tasks: []sharedviewquery.SearchSegmentTask{struct{}{}}})

	require.Error(t, err)
	assert.Empty(t, executor.searchTasks)
}

func TestSchedulerQueryDelegatesQNTasks(t *testing.T) {
	executor := &fakeQNSegmentTaskExecutor{
		queryResult: &internalpb.RetrieveResults{AllRetrieveCount: 3},
	}
	scheduler := NewScheduler(executor)
	req := &internalpb.RetrieveRequest{CollectionID: 100}
	mvcc := &viewpb.QueryPlanMVCC{TransformingTimetick: 10}
	tasks := qnview.NewQNQuerySegmentTasks([]qnview.QNQuerySegmentTask{
		{Handle: fakeSealedSegmentHandle{id: 1}, Request: req, MVCC: mvcc},
		{Handle: fakeSealedSegmentHandle{id: 2}, Request: req, MVCC: mvcc},
	})

	result, err := scheduler.Query(context.Background(), tasks)

	require.NoError(t, err)
	assert.Same(t, executor.queryResult, result)
	require.Len(t, executor.queryTasks, 2)
	assert.Equal(t, int64(1), executor.queryTasks[0].Handle.ID())
	assert.Equal(t, int64(2), executor.queryTasks[1].Handle.ID())
	assert.Same(t, req, executor.queryTasks[0].Request)
	assert.Same(t, mvcc, executor.queryTasks[0].MVCC)
}

func TestSchedulerQueryRejectsNonQNTask(t *testing.T) {
	executor := &fakeQNSegmentTaskExecutor{}
	scheduler := NewScheduler(executor)

	_, err := scheduler.Query(context.Background(), fakeQuerySegmentTasks{tasks: []sharedviewquery.QuerySegmentTask{struct{}{}}})

	require.Error(t, err)
	assert.Empty(t, executor.queryTasks)
}

func TestSchedulerReturnsExecutorError(t *testing.T) {
	executorErr := errors.New("executor failed")
	executor := &fakeQNSegmentTaskExecutor{searchErr: executorErr}
	scheduler := NewScheduler(executor)
	tasks := qnview.NewQNSearchSegmentTasks([]qnview.QNSearchSegmentTask{
		{Handle: fakeSealedSegmentHandle{id: 1}, Request: &internalpb.SearchRequest{}, MVCC: &viewpb.QueryPlanMVCC{}},
	})

	_, err := scheduler.Search(context.Background(), tasks)

	require.ErrorIs(t, err, executorErr)
}

// Holding PreExecute stalls the consumer of the scheduler's unbuffered execChan.
type holdReadTask struct {
	readscheduler.Task
	release <-chan struct{}
}

func (t holdReadTask) PreExecute() error {
	select {
	case <-t.release:
		return nil
	case <-t.Context().Done():
		return t.Context().Err()
	}
}

func (holdReadTask) Execute() error { return nil }

type pinnedReadProvider struct {
	sharedviewquery.TaskProvider
	handle qnview.SealedSegmentHandle
}

func (p pinnedReadProvider) AcquireSearchSegmentTasks(_ context.Context, _ qviews.ShardID, _ qviews.QueryViewVersion, mvcc *viewpb.QueryPlanMVCC, req *internalpb.SearchRequest, _ *viewpb.SegmentSelection) (sharedviewquery.SearchSegmentTasks, error) {
	return qnview.NewQNSearchSegmentTasks([]qnview.QNSearchSegmentTask{{Handle: p.handle, MVCC: mvcc, Request: req}}), nil
}

func (p pinnedReadProvider) AcquireQuerySegmentTasks(_ context.Context, _ qviews.ShardID, _ qviews.QueryViewVersion, mvcc *viewpb.QueryPlanMVCC, req *internalpb.RetrieveRequest, _ *viewpb.SegmentSelection) (sharedviewquery.QuerySegmentTasks, error) {
	return qnview.NewQNQuerySegmentTasks([]qnview.QNQuerySegmentTask{{Handle: p.handle, MVCC: mvcc, Request: req}}), nil
}

type releaseCountingHandle struct {
	fakeSealedSegmentHandle
	releases int
}

func (h *releaseCountingHandle) Release() { h.releases++ }

func TestViewReadsRefusedByReadQueue(t *testing.T) {
	paramtable.Init()
	cfg := paramtable.Get()
	cfg.Save(cfg.QueryNodeCfg.MaxQueueWaitRatio.Key, "0")
	t.Cleanup(func() {
		cfg.Reset(cfg.QueryNodeCfg.MaxQueueWaitRatio.Key)
		cfg.Reset(cfg.QueryNodeCfg.MaxQueueWait.Key)
	})
	queue := readscheduler.NewScheduler("fifo")
	queue.Start()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	t.Cleanup(func() {
		close(release)
		queue.Stop()
	})
	newHeldTask := func() holdReadTask {
		return holdReadTask{Task: tasks.NewQueryTask(ctx, nil, nil, &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{}}), release: release}
	}
	require.NoError(t, queue.Add(newHeldTask()))
	// Once the first task reaches PreExecute, the second becomes the waiting head.
	require.Eventually(t, func() bool { return queue.GetWaitingTaskTotal() == 0 }, time.Second, time.Millisecond)
	require.NoError(t, queue.Add(newHeldTask()))
	// Synchronize with the scheduling loop after it picks the second task.
	_, err := queue.ClearQueued(ctx, func(readscheduler.Task) bool { return false }, "")
	require.NoError(t, err)
	cfg.Save(cfg.QueryNodeCfg.MaxQueueWait.Key, "1ns")
	cfg.Save(cfg.QueryNodeCfg.MaxQueueWaitRatio.Key, "0.1")

	handle := &releaseCountingHandle{fakeSealedSegmentHandle: fakeSealedSegmentHandle{id: 1, segment: fakeReadableSegment{id: 1, vchannel: "v1"}}}
	executor := NewDirectSegmentTaskExecutor(1, func(task readscheduler.Task) error {
		if err := queue.Add(task); err != nil {
			return err
		}
		return task.Wait()
	})
	server := sharedviewquery.NewServer(pinnedReadProvider{handle: handle}, NewScheduler(executor))
	shard := &viewpb.ShardID{Vchannel: "v1"}
	version := &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{}}
	mvcc := &viewpb.QueryPlanMVCC{TransformingTimetick: 10}
	_, err = server.SearchOnView(ctx, &viewpb.SearchOnViewRequest{ShardId: shard, Version: version, Mvcc: mvcc, LegacyReq: &internalpb.SearchRequest{Nq: 1}})
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.ErrorIs(t, viewerror.ConvertViewError("SearchOnView", viewerror.NewGRPCStatusFromError(err).Err()), merr.ErrServiceTooManyRequests)
	require.Equal(t, 1, handle.releases)
	_, err = server.QueryOnView(ctx, &viewpb.QueryOnViewRequest{ShardId: shard, Version: version, Mvcc: mvcc, LegacyReq: &internalpb.RetrieveRequest{}})
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.ErrorIs(t, viewerror.ConvertViewError("QueryOnView", viewerror.NewGRPCStatusFromError(err).Err()), merr.ErrServiceTooManyRequests)
	require.Equal(t, 2, handle.releases)
}

type fakeQNSegmentTaskExecutor struct {
	searchTasks  []qnview.QNSearchSegmentTask
	searchResult *internalpb.SearchResults
	searchErr    error
	queryTasks   []qnview.QNQuerySegmentTask
	queryResult  *internalpb.RetrieveResults
	queryErr     error
}

func (e *fakeQNSegmentTaskExecutor) Search(ctx context.Context, tasks []qnview.QNSearchSegmentTask) (*internalpb.SearchResults, error) {
	e.searchTasks = append([]qnview.QNSearchSegmentTask(nil), tasks...)
	return e.searchResult, e.searchErr
}

func (e *fakeQNSegmentTaskExecutor) Query(ctx context.Context, tasks []qnview.QNQuerySegmentTask) (*internalpb.RetrieveResults, error) {
	e.queryTasks = append([]qnview.QNQuerySegmentTask(nil), tasks...)
	return e.queryResult, e.queryErr
}

type fakeSealedSegmentHandle struct {
	id      int64
	segment qnview.TransformSegment
}

func (h fakeSealedSegmentHandle) ID() int64 {
	return h.id
}

func (h fakeSealedSegmentHandle) PartitionID() int64 {
	return 0
}

func (h fakeSealedSegmentHandle) Segment() qnview.TransformSegment {
	return h.segment
}

func (h fakeSealedSegmentHandle) Release() {}

type fakeSearchSegmentTasks struct {
	tasks []sharedviewquery.SearchSegmentTask
}

func (t fakeSearchSegmentTasks) Tasks() []sharedviewquery.SearchSegmentTask {
	return t.tasks
}

func (t fakeSearchSegmentTasks) Release() {}

type fakeQuerySegmentTasks struct {
	tasks []sharedviewquery.QuerySegmentTask
}

func (t fakeQuerySegmentTasks) Tasks() []sharedviewquery.QuerySegmentTask {
	return t.tasks
}

func (t fakeQuerySegmentTasks) Release() {}
