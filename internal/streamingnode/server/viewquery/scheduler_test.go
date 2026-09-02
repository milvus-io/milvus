//go:build test && dynamic

package viewquery

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	sharedviewquery "github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSchedulerSearchDelegatesSNTasks(t *testing.T) {
	executor := &fakeSNSegmentTaskExecutor{searchResult: &internalpb.SearchResults{NumQueries: 10}}
	scheduler := NewScheduler(executor)
	req := &internalpb.SearchRequest{CollectionID: 100}
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 10}
	tasks := snview.NewSNSearchSegmentTasks([]snview.SNSearchSegmentTask{
		{Handle: fakeGrowingSegmentHandle{id: 1}, Request: req, MVCC: mvcc, VChannel: "v1"},
		{Handle: fakeGrowingSegmentHandle{id: 2}, Request: req, MVCC: mvcc, VChannel: "v1"},
	})

	result, err := scheduler.Search(context.Background(), tasks)

	require.NoError(t, err)
	assert.Same(t, executor.searchResult, result)
	require.Len(t, executor.searchTasks, 2)
	assert.Equal(t, int64(1), executor.searchTasks[0].Handle.ID())
	assert.Same(t, req, executor.searchTasks[0].Request)
	assert.Same(t, mvcc, executor.searchTasks[0].MVCC)
}

func TestSchedulerQueryDelegatesSNTasks(t *testing.T) {
	executor := &fakeSNSegmentTaskExecutor{queryResult: &internalpb.RetrieveResults{AllRetrieveCount: 3}}
	scheduler := NewScheduler(executor)
	req := &internalpb.RetrieveRequest{CollectionID: 100}
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 10}
	tasks := snview.NewSNQuerySegmentTasks([]snview.SNQuerySegmentTask{
		{Handle: fakeGrowingSegmentHandle{id: 1}, Request: req, MVCC: mvcc, VChannel: "v1"},
		{Handle: fakeGrowingSegmentHandle{id: 2}, Request: req, MVCC: mvcc, VChannel: "v1"},
	})

	result, err := scheduler.Query(context.Background(), tasks)

	require.NoError(t, err)
	assert.Same(t, executor.queryResult, result)
	require.Len(t, executor.queryTasks, 2)
	assert.Equal(t, int64(1), executor.queryTasks[0].Handle.ID())
	assert.Same(t, req, executor.queryTasks[0].Request)
	assert.Same(t, mvcc, executor.queryTasks[0].MVCC)
}

func TestSchedulerRejectsNonSNTask(t *testing.T) {
	executor := &fakeSNSegmentTaskExecutor{}
	scheduler := NewScheduler(executor)

	_, err := scheduler.Search(context.Background(), fakeSearchSegmentTasks{tasks: []sharedviewquery.SearchSegmentTask{struct{}{}}})

	require.Error(t, err)
	assert.Empty(t, executor.searchTasks)
}

func TestSchedulerReturnsExecutorError(t *testing.T) {
	executorErr := errors.New("executor failed")
	executor := &fakeSNSegmentTaskExecutor{searchErr: executorErr}
	scheduler := NewScheduler(executor)
	tasks := snview.NewSNSearchSegmentTasks([]snview.SNSearchSegmentTask{
		{Handle: fakeGrowingSegmentHandle{id: 1}, Request: &internalpb.SearchRequest{}, MVCC: &viewpb.QueryPlanMVCC{}},
	})

	_, err := scheduler.Search(context.Background(), tasks)

	require.ErrorIs(t, err, executorErr)
}

type fakeSNSegmentTaskExecutor struct {
	searchTasks  []snview.SNSearchSegmentTask
	searchResult *internalpb.SearchResults
	searchErr    error
	queryTasks   []snview.SNQuerySegmentTask
	queryResult  *internalpb.RetrieveResults
	queryErr     error
}

func (e *fakeSNSegmentTaskExecutor) Search(ctx context.Context, tasks []snview.SNSearchSegmentTask) (*internalpb.SearchResults, error) {
	e.searchTasks = append([]snview.SNSearchSegmentTask(nil), tasks...)
	return e.searchResult, e.searchErr
}

func (e *fakeSNSegmentTaskExecutor) Query(ctx context.Context, tasks []snview.SNQuerySegmentTask) (*internalpb.RetrieveResults, error) {
	e.queryTasks = append([]snview.SNQuerySegmentTask(nil), tasks...)
	return e.queryResult, e.queryErr
}

type fakeSearchSegmentTasks struct {
	tasks []sharedviewquery.SearchSegmentTask
}

func (t fakeSearchSegmentTasks) Tasks() []sharedviewquery.SearchSegmentTask {
	return t.tasks
}

func (t fakeSearchSegmentTasks) Release() {}
