package compaction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

func fourL0() *pkindexpb.Manifest {
	return &pkindexpb.Manifest{CollectionId: 1, Vchannel: "v", Version: 4, SstBasePath: "base",
		Ssts: []*pkindexpb.SSTRef{ref(4, 0, 1), ref(3, 0, 1), ref(2, 0, 1), ref(1, 0, 1)}}
}

func newTestInspector(t *testing.T, cfg Config, committer *fakeCommitter, sched *task.MockGlobalScheduler) *Inspector {
	t.Helper()
	return New(context.Background(), func() Config { return cfg }, &fakeAlloc{next: 100}, committer, sched)
}

func TestInspectorPlansOnceAndProtectsOutputs(t *testing.T) {
	committer := &fakeCommitter{current: fourL0()}
	sched := task.NewMockGlobalScheduler(t)
	insp := newTestInspector(t, testConfig(), committer, sched)

	var enqueued *Task
	sched.EXPECT().Enqueue(mock.Anything).Run(func(tk task.Task) { enqueued = tk.(*Task) }).Once()
	now := time.Now()
	insp.ManifestChanged(context.Background(), fourL0())
	insp.inspect(now)
	require.NotNil(t, enqueued)
	require.Equal(t, ruleL0, enqueued.rule)
	require.EqualValues(t, 100, enqueued.taskID)
	require.EqualValues(t, 101, enqueued.runID)
	require.Equal(t, 1, insp.table.len())

	// the vchannel is busy: no second task, even after another change
	insp.ManifestChanged(context.Background(), fourL0())
	insp.inspect(now.Add(time.Second))

	// the outputs are protected once dispatched
	ids, err := insp.InFlightOutputs(context.Background(), 1, "v")
	require.NoError(t, err)
	require.Nil(t, ids)
	enqueued.setOutputRange(500, 502)
	ids, err = insp.InFlightOutputs(context.Background(), 1, "v")
	require.NoError(t, err)
	require.Equal(t, []int64{500, 501}, ids)

	// after it ends, the vchannel is planned again
	enqueued.mu.Lock()
	enqueued.state = taskcommon.Finished
	enqueued.mu.Unlock()
	insp.onTerminal(enqueued)
	require.Zero(t, insp.table.len())
	ids, err = insp.InFlightOutputs(context.Background(), 1, "v")
	require.NoError(t, err)
	require.Nil(t, ids)
	sched.EXPECT().Enqueue(mock.Anything).Once()
	insp.inspect(now.Add(2 * time.Second))
}

func TestInspectorCooldownAfterFailure(t *testing.T) {
	committer := &fakeCommitter{current: fourL0()}
	sched := task.NewMockGlobalScheduler(t)
	cfg := testConfig()
	cfg.FailureCooldown = time.Minute
	insp := newTestInspector(t, cfg, committer, sched)

	var tk *Task
	sched.EXPECT().Enqueue(mock.Anything).Run(func(x task.Task) { tk = x.(*Task) }).Once()
	now := time.Now()
	insp.ManifestChanged(context.Background(), fourL0())
	insp.inspect(now)
	tk.mu.Lock()
	tk.state = taskcommon.Failed
	tk.mu.Unlock()
	insp.now = func() time.Time { return now }
	insp.onTerminal(tk)

	insp.inspect(now.Add(30 * time.Second)) // in cooldown: nothing
	sched.EXPECT().Enqueue(mock.Anything).Once()
	insp.inspect(now.Add(2 * time.Minute))
}

func TestInspectorHonorsMaxConcurrentAndOrdersByScore(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConcurrent = 1
	// "w" has five L0 tables (score 1.25), "v" four (score 1.0): "w" goes first
	w := fourL0()
	w.Vchannel = "w"
	w.Ssts = append([]*pkindexpb.SSTRef{ref(5, 0, 1)}, w.Ssts...)
	w.Version = 5
	committer := &fakeCommitter{}
	committer.byTarget = map[manifest.Target]*pkindexpb.Manifest{
		{CollectionID: 1, VChannel: "v"}: fourL0(),
		{CollectionID: 1, VChannel: "w"}: w,
	}
	sched := task.NewMockGlobalScheduler(t)
	insp := newTestInspector(t, cfg, committer, sched)
	var tk *Task
	sched.EXPECT().Enqueue(mock.Anything).Run(func(x task.Task) { tk = x.(*Task) }).Once()
	insp.ManifestChanged(context.Background(), fourL0())
	insp.ManifestChanged(context.Background(), w)
	insp.inspect(time.Now())
	require.Equal(t, 1, insp.table.len())
	require.Equal(t, "w", tk.target.VChannel)
	// "v" stays dirty and is launched once a slot frees up
	tk.mu.Lock()
	tk.state = taskcommon.Finished
	tk.mu.Unlock()
	insp.onTerminal(tk)
	// the commit of "w" would report its new manifest to the observer
	merged := fourL0()
	merged.Vchannel = "w"
	merged.Version = 6
	merged.Ssts = merged.Ssts[:1]
	committer.byTarget[manifest.Target{CollectionID: 1, VChannel: "w"}] = merged
	insp.ManifestChanged(context.Background(), merged)
	sched.EXPECT().Enqueue(mock.Anything).Run(func(x task.Task) { tk = x.(*Task) }).Once()
	insp.inspect(time.Now())
	require.Equal(t, "v", tk.target.VChannel)
}

func TestInspectorIgnoresStaleAndForgetsGone(t *testing.T) {
	committer := &fakeCommitter{current: fourL0()}
	sched := task.NewMockGlobalScheduler(t)
	insp := newTestInspector(t, testConfig(), committer, sched)
	target := manifest.Target{CollectionID: 1, VChannel: "v"}

	insp.ManifestChanged(context.Background(), fourL0())
	old := fourL0()
	old.Version = 3
	old.Ssts = old.Ssts[:1]
	insp.ManifestChanged(context.Background(), old)
	require.EqualValues(t, 4, insp.summaries[target].version)

	// nothing to do: the vchannel is no longer dirty
	quiet := fourL0()
	quiet.Version = 5
	quiet.Ssts = quiet.Ssts[:1]
	insp.ManifestChanged(context.Background(), quiet)
	insp.inspect(time.Now())
	_, dirty := insp.dirty[target]
	require.False(t, dirty)

	// the manifest disappeared between the change and the inspection
	again := fourL0()
	again.Version = 6 // newer than the quiet summary, so it is recorded and marked
	insp.ManifestChanged(context.Background(), again)
	committer.current = nil
	insp.inspect(time.Now())
	_, known := insp.summaries[target]
	require.False(t, known)
}

func TestInspectorStartAndClose(t *testing.T) {
	cfg := testConfig()
	cfg.InspectInterval = 10 * time.Millisecond
	committer := &fakeCommitter{current: fourL0()}
	sched := task.NewMockGlobalScheduler(t)
	insp := newTestInspector(t, cfg, committer, sched)
	done := make(chan struct{})
	sched.EXPECT().Enqueue(mock.Anything).Run(func(task.Task) { close(done) }).Once()
	insp.Start()
	insp.ManifestChanged(context.Background(), fourL0())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the inspector did not plan")
	}
	insp.Close()
}

func TestInspectorKeepsDirtyMarkOfNewerSummary(t *testing.T) {
	// Current is behind a commit that was reported meanwhile: the mark stays.
	committer := &fakeCommitter{}
	sched := task.NewMockGlobalScheduler(t)
	insp := newTestInspector(t, testConfig(), committer, sched)
	target := manifest.Target{CollectionID: 1, VChannel: "v"}
	newer := fourL0()
	newer.Version = 5
	insp.ManifestChanged(context.Background(), newer)
	stale := fourL0()
	stale.Version = 4
	stale.Ssts = stale.Ssts[:1]
	committer.current = stale
	insp.inspect(time.Now())
	_, dirty := insp.dirty[target]
	require.True(t, dirty)
	require.EqualValues(t, 5, insp.summaries[target].version)
}

func TestInspectorLaunchFailures(t *testing.T) {
	target := manifest.Target{CollectionID: 1, VChannel: "v"}
	cfg := testConfig()
	cfg.FailureCooldown = time.Minute

	t.Run("current fails", func(t *testing.T) {
		committer := &fakeCommitter{currentErr: errors.New("store down")}
		insp := newTestInspector(t, cfg, committer, task.NewMockGlobalScheduler(t))
		now := time.Now()
		insp.ManifestChanged(context.Background(), fourL0())
		insp.inspect(now)
		_, dirty := insp.dirty[target]
		require.True(t, dirty)
		require.Equal(t, now.Add(time.Minute), insp.cooldown[target])
	})
	t.Run("nothing to plan", func(t *testing.T) {
		quiet := fourL0()
		quiet.Ssts = quiet.Ssts[:1]
		committer := &fakeCommitter{current: quiet}
		insp := newTestInspector(t, cfg, committer, task.NewMockGlobalScheduler(t))
		// the cached summary is out of date: it scores, the manifest does not plan
		insp.ManifestChanged(context.Background(), fourL0())
		insp.inspect(time.Now())
		_, dirty := insp.dirty[target]
		require.False(t, dirty)
	})
	t.Run("alloc fails", func(t *testing.T) {
		committer := &fakeCommitter{current: fourL0()}
		insp := New(context.Background(), func() Config { return cfg }, &fakeAlloc{err: errors.New("alloc")}, committer, task.NewMockGlobalScheduler(t))
		insp.ManifestChanged(context.Background(), fourL0())
		insp.inspect(time.Now())
		_, dirty := insp.dirty[target]
		require.True(t, dirty)
		require.Zero(t, insp.table.len())
	})
}
