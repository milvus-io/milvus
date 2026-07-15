//go:build test && dynamic

package viewquery

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	sharedviewquery "github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
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

	result, err := scheduler.Search(context.Background(), tasks)

	require.NoError(t, err)
	assert.Same(t, executor.searchResult, result)
	require.Len(t, executor.searchTasks, 2)
	assert.Equal(t, int64(1), executor.searchTasks[0].Handle.ID())
	assert.Equal(t, int64(2), executor.searchTasks[1].Handle.ID())
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
