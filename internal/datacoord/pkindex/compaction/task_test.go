package compaction

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeAlloc struct {
	next int64
	err  error
}

func (a *fakeAlloc) AllocN(n int64) (int64, int64, error) {
	if a.err != nil {
		return 0, 0, a.err
	}
	b := a.next
	a.next += n
	return b, b + n, nil
}

type fakeCommitter struct {
	current  *pkindexpb.Manifest
	byTarget map[manifest.Target]*pkindexpb.Manifest
	commits  []manifest.Compaction
	// currentErr is returned by Current when set.
	currentErr error
	err        error
}

func (c *fakeCommitter) Current(_ context.Context, t manifest.Target) (*pkindexpb.Manifest, error) {
	if c.currentErr != nil {
		return nil, c.currentErr
	}
	if c.byTarget != nil {
		return c.byTarget[t], nil
	}
	return c.current, nil
}

func (c *fakeCommitter) CommitCompaction(_ context.Context, _ int64, _ string, comp manifest.Compaction) (*pkindexpb.Manifest, error) {
	c.commits = append(c.commits, comp)
	return nil, c.err
}

func newTestTask(t *testing.T, committer *fakeCommitter, onTerminal func(*Task)) *Task {
	t.Helper()
	p := plan{target: manifest.Target{CollectionID: 1, VChannel: "v"}, sstBasePath: "base", rule: ruleL0,
		inputs: []*pkindexpb.SSTRef{ref(9, 0, 100), ref(8, 0, 100)}, inputBytes: 200}
	return newTask(context.Background(), p, 100, 7, testConfig(), &fakeAlloc{next: 1000}, committer, onTerminal)
}

func TestTaskDispatchAndFinish(t *testing.T) {
	var done *Task
	committer := &fakeCommitter{}
	task := newTestTask(t, committer, func(tk *Task) { done = tk })
	cluster := session.NewMockCluster(t)

	var sent *pkindexpb.CompactionRequest
	cluster.EXPECT().CreatePKIndexCompaction(int64(5), mock.Anything, int64(4)).RunAndReturn(
		func(_ int64, req *pkindexpb.CompactionRequest, _ int64) error { sent = req; return nil }).Once()
	task.CreateTaskOnWorker(5, cluster)
	require.Equal(t, taskcommon.InProgress, task.GetTaskState())
	require.EqualValues(t, 100, sent.GetTaskId())
	require.EqualValues(t, 1, sent.GetCollectionId())
	require.Equal(t, "v", sent.GetVchannel())
	require.Equal(t, "base", sent.GetSstBasePath())
	require.EqualValues(t, 7, sent.GetRunId())
	require.EqualValues(t, 1000, sent.GetOutputIdBegin())
	require.EqualValues(t, 1000+outputIDCount(200, 128<<20), sent.GetOutputIdEnd())
	require.EqualValues(t, 128<<20, sent.GetTargetFileSize())
	begin, end := task.outputRange()
	require.Equal(t, []int64{1000, sent.GetOutputIdEnd()}, []int64{begin, end})

	outputs := []*pkindexpb.SSTRef{ref(1000, 7, 150)}
	cluster.EXPECT().QueryPKIndexCompaction(int64(5), int64(100)).Return(
		&pkindexpb.CompactionResult{Outputs: outputs, EntriesProcessed: 10}, taskcommon.Finished, "", nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.Finished, task.GetTaskState())
	require.Len(t, committer.commits, 1)
	require.Equal(t, []int64{9, 8}, committer.commits[0].Inputs)
	require.Equal(t, outputs, committer.commits[0].Outputs)
	require.Same(t, task, done)
}

func TestTaskCreateFailureStaysInit(t *testing.T) {
	task := newTestTask(t, &fakeCommitter{}, func(*Task) { t.Fatal("must not end") })
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreatePKIndexCompaction(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("rpc")).Once()
	task.CreateTaskOnWorker(5, cluster)
	require.Equal(t, taskcommon.Init, task.GetTaskState())
	begin, end := task.outputRange()
	require.Zero(t, begin)
	require.Zero(t, end)
}

func TestTaskCommitUnavailableBacksOff(t *testing.T) {
	committer := &fakeCommitter{err: errors.Mark(errors.New("etcd"), pkindex.ErrUnavailable)}
	task := newTestTask(t, committer, func(*Task) { t.Fatal("must not end") })
	clock := time.Unix(1000, 0)
	task.now = func() time.Time { return clock }
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreatePKIndexCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	task.CreateTaskOnWorker(5, cluster)
	cluster.EXPECT().QueryPKIndexCompaction(mock.Anything, mock.Anything).Return(
		&pkindexpb.CompactionResult{Outputs: []*pkindexpb.SSTRef{ref(1000, 7, 1)}}, taskcommon.Finished, "", nil)
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.InProgress, task.GetTaskState())
	require.Len(t, committer.commits, 1)

	clock = clock.Add(commitBackoffInitial / 2) // inside the backoff
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.InProgress, task.GetTaskState())
	require.Len(t, committer.commits, 1)

	clock = clock.Add(commitBackoffInitial) // past it
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.InProgress, task.GetTaskState())
	require.Len(t, committer.commits, 2)
	require.Equal(t, 2*commitBackoffInitial, task.commitBackoff) // doubled

	for i := 0; i < 10; i++ {
		clock = clock.Add(commitBackoffMax)
		task.QueryTaskOnWorker(cluster)
	}
	require.Equal(t, commitBackoffMax, task.commitBackoff) // capped
}

func TestTaskCommitDeadlineBacksOff(t *testing.T) {
	committer := &fakeCommitter{err: context.DeadlineExceeded}
	task := newTestTask(t, committer, func(*Task) { t.Fatal("must not end") })
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreatePKIndexCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	task.CreateTaskOnWorker(5, cluster)
	cluster.EXPECT().QueryPKIndexCompaction(mock.Anything, mock.Anything).Return(
		&pkindexpb.CompactionResult{Outputs: []*pkindexpb.SSTRef{ref(1000, 7, 1)}}, taskcommon.Finished, "", nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.InProgress, task.GetTaskState())
	require.Equal(t, commitBackoffInitial, task.commitBackoff)
}

func finishWith(t *testing.T, task *Task, cluster *session.MockCluster, outputs []*pkindexpb.SSTRef) {
	t.Helper()
	cluster.EXPECT().QueryPKIndexCompaction(mock.Anything, mock.Anything).Return(
		&pkindexpb.CompactionResult{Outputs: outputs, EntriesProcessed: 11}, taskcommon.Finished, "", nil).Once()
	task.QueryTaskOnWorker(cluster)
}

func TestTaskStaleResultResets(t *testing.T) {
	for name, out := range map[string]*pkindexpb.SSTRef{
		"id below range":  ref(999, 7, 1),
		"id above range":  ref(1000+outputIDCount(200, 128<<20), 7, 1),
		"wrong run id":    ref(1000, 6, 1),
		"second is stale": nil,
	} {
		t.Run(name, func(t *testing.T) {
			committer := &fakeCommitter{}
			task := newTestTask(t, committer, func(*Task) { t.Fatal("must not end") })
			cluster := session.NewMockCluster(t)
			cluster.EXPECT().CreatePKIndexCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			cluster.EXPECT().DropPKIndexCompaction(mock.Anything, mock.Anything).Return(nil).Once()
			task.CreateTaskOnWorker(5, cluster)
			outputs := []*pkindexpb.SSTRef{out}
			if out == nil {
				outputs = []*pkindexpb.SSTRef{ref(1000, 7, 1), ref(5000, 7, 1)}
			}
			finishWith(t, task, cluster, outputs)
			require.Equal(t, taskcommon.Init, task.GetTaskState())
			require.Equal(t, int64(nullNodeID), task.getNodeID())
			require.Empty(t, committer.commits)
		})
	}
}

func TestTaskEmptyResult(t *testing.T) {
	t.Run("live inputs fail", func(t *testing.T) {
		var done *Task
		committer := &fakeCommitter{}
		task := newTestTask(t, committer, func(tk *Task) { done = tk })
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreatePKIndexCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		task.CreateTaskOnWorker(5, cluster)
		finishWith(t, task, cluster, nil)
		require.Equal(t, taskcommon.Failed, task.GetTaskState())
		require.Empty(t, committer.commits)
		require.Same(t, task, done)
	})
	t.Run("all tombstones commit", func(t *testing.T) {
		committer := &fakeCommitter{}
		task := newTestTask(t, committer, nil)
		task.dropTombstones = true
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreatePKIndexCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		task.CreateTaskOnWorker(5, cluster)
		finishWith(t, task, cluster, nil)
		require.Equal(t, taskcommon.Finished, task.GetTaskState())
		require.Len(t, committer.commits, 1)
		require.Empty(t, committer.commits[0].Outputs)
		require.EqualValues(t, 11, task.progress())
	})
}

type failingAlloc struct{}

func (failingAlloc) AllocN(int64) (int64, int64, error) { return 0, 0, errors.New("alloc") }

func TestTaskAllocFailureKeepsRange(t *testing.T) {
	task := newTestTask(t, &fakeCommitter{}, nil)
	task.setOutputRange(1000, 1002)
	task.alloc = failingAlloc{}
	cluster := session.NewMockCluster(t) // no cluster call expected
	task.CreateTaskOnWorker(5, cluster)
	require.Equal(t, taskcommon.Init, task.GetTaskState())
	begin, end := task.outputRange()
	require.Equal(t, []int64{1000, 1002}, []int64{begin, end})
}

func TestTaskResetKeepsRange(t *testing.T) {
	task := newTestTask(t, &fakeCommitter{}, func(*Task) { t.Fatal("must not end") })
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreatePKIndexCompaction(int64(5), mock.Anything, mock.Anything).Return(nil).Once()
	task.CreateTaskOnWorker(5, cluster)
	begin, end := task.outputRange()

	// the drop RPC fails inside reset: still back to Init with no node
	cluster.EXPECT().QueryPKIndexCompaction(int64(5), int64(100)).Return(&pkindexpb.CompactionResult{}, taskcommon.None, "", nil).Once()
	cluster.EXPECT().DropPKIndexCompaction(int64(5), int64(100)).Return(errors.New("rpc")).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.Init, task.GetTaskState())
	require.Equal(t, int64(nullNodeID), task.getNodeID())
	b, e := task.outputRange()
	require.Equal(t, []int64{begin, end}, []int64{b, e})

	// a dispatch that fails after the reset keeps the old range
	cluster.EXPECT().CreatePKIndexCompaction(int64(6), mock.Anything, mock.Anything).Return(errors.New("rpc")).Once()
	task.CreateTaskOnWorker(6, cluster)
	require.Equal(t, taskcommon.Init, task.GetTaskState())
	b, e = task.outputRange()
	require.Equal(t, []int64{begin, end}, []int64{b, e})
}

func TestTaskStaleEditFails(t *testing.T) {
	var done *Task
	committer := &fakeCommitter{err: manifest.ErrStaleEdit}
	task := newTestTask(t, committer, func(tk *Task) { done = tk })
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreatePKIndexCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	task.CreateTaskOnWorker(5, cluster)
	cluster.EXPECT().QueryPKIndexCompaction(mock.Anything, mock.Anything).Return(
		&pkindexpb.CompactionResult{Outputs: []*pkindexpb.SSTRef{ref(1000, 7, 1)}}, taskcommon.Finished, "", nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.Failed, task.GetTaskState())
	require.Same(t, task, done)
}

func TestTaskPermanentFailure(t *testing.T) {
	var done *Task
	task := newTestTask(t, &fakeCommitter{}, func(tk *Task) { done = tk })
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreatePKIndexCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	task.CreateTaskOnWorker(5, cluster)
	cluster.EXPECT().QueryPKIndexCompaction(int64(5), int64(100)).Return(
		&pkindexpb.CompactionResult{Retriable: false}, taskcommon.Failed, "corrupt input", nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.Failed, task.GetTaskState())
	require.Same(t, task, done)
}

func TestTaskLostOrRetriableGoesBackToInit(t *testing.T) {
	committer := &fakeCommitter{}
	task := newTestTask(t, committer, func(*Task) { t.Fatal("must not end") })
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreatePKIndexCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(3)
	cluster.EXPECT().DropPKIndexCompaction(mock.Anything, mock.Anything).Return(nil).Times(3)

	// the node is gone
	task.CreateTaskOnWorker(5, cluster)
	cluster.EXPECT().QueryPKIndexCompaction(int64(5), int64(100)).Return(nil, taskcommon.None, "", merr.ErrNodeNotFound).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.Init, task.GetTaskState())

	// the node does not know the task: a new ID range on the next dispatch
	task.CreateTaskOnWorker(6, cluster)
	begin2, _ := task.outputRange()
	require.Greater(t, begin2, int64(1000))
	cluster.EXPECT().QueryPKIndexCompaction(int64(6), int64(100)).Return(&pkindexpb.CompactionResult{}, taskcommon.None, "", nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.Init, task.GetTaskState())

	// a retriable failure
	task.CreateTaskOnWorker(7, cluster)
	cluster.EXPECT().QueryPKIndexCompaction(int64(7), int64(100)).Return(
		&pkindexpb.CompactionResult{Retriable: true}, taskcommon.Failed, "disk", nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.Init, task.GetTaskState())
	require.Empty(t, committer.commits)
}

func TestTaskTransientQueryErrorKeepsRunning(t *testing.T) {
	task := newTestTask(t, &fakeCommitter{}, func(*Task) { t.Fatal("must not end") })
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreatePKIndexCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	task.CreateTaskOnWorker(7, cluster)
	cluster.EXPECT().QueryPKIndexCompaction(int64(7), int64(100)).Return(nil, taskcommon.None, "", errors.New("timeout")).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.InProgress, task.GetTaskState())
	cluster.EXPECT().QueryPKIndexCompaction(int64(7), int64(100)).Return(
		&pkindexpb.CompactionResult{EntriesProcessed: 42}, taskcommon.InProgress, "", nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, taskcommon.InProgress, task.GetTaskState())
	require.EqualValues(t, 42, task.progress())
}

func TestTaskDropOnWorker(t *testing.T) {
	task := newTestTask(t, &fakeCommitter{}, nil)
	cluster := session.NewMockCluster(t)
	task.DropTaskOnWorker(cluster) // no node: nothing to drop
	cluster.EXPECT().CreatePKIndexCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	task.CreateTaskOnWorker(5, cluster)
	cluster.EXPECT().DropPKIndexCompaction(int64(5), int64(100)).Return(nil).Once()
	task.DropTaskOnWorker(cluster)
	task.DropTaskOnWorker(cluster) // already dropped: no call
}

func TestTaskTableOneTaskPerVChannel(t *testing.T) {
	tbl := newTaskTable()
	a := newTestTask(t, &fakeCommitter{}, nil)
	b := newTestTask(t, &fakeCommitter{}, nil)
	b.taskID = 101
	require.True(t, tbl.add(a))
	require.False(t, tbl.add(b))
	require.True(t, tbl.has(a.target))
	require.Equal(t, 1, tbl.len())
	require.Nil(t, tbl.inFlightOutputs(a.target)) // not dispatched yet
	a.setOutputRange(1000, 1003)
	require.Equal(t, []int64{1000, 1001, 1002}, tbl.inFlightOutputs(a.target))
	tbl.remove(a.taskID)
	require.False(t, tbl.has(a.target))
	require.Nil(t, tbl.inFlightOutputs(a.target))
	require.Zero(t, tbl.len())
	tbl.remove(a.taskID) // idempotent
}
