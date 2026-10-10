package task

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// gateSpy records the nodes it was asked about and answers what the test set.
type gateSpy struct {
	calls int
	nodes []int64
	err   error
}

func (g *gateSpy) gate(nodeID int64) error {
	g.calls++
	g.nodes = append(g.nodes, nodeID)
	return g.err
}

func TestExecutorWithoutGateRunsEveryActionAsBefore(t *testing.T) {
	paramtable.Init()
	ex := newTestExecutor(1)
	require.Nil(t, ex.fileResourceGate, "a stock executor holds no gate")

	for _, action := range []Action{
		NewSegmentAction(1, ActionTypeGrow, "shard-0", 100),
		NewChannelAction(1, ActionTypeGrow, "ch-0"),
		NewSegmentAction(1, ActionTypeReduce, "shard-0", 100),
	} {
		assert.NoError(t, ex.checkFileResourceReady(action),
			"a nil gate must let every action through untouched")
	}
}

func TestExecutorGateOnlyAppliesToGrowActionsThatPutDataOnTheNode(t *testing.T) {
	paramtable.Init()

	cases := []struct {
		name     string
		action   Action
		consults bool
	}{
		{"grow segment", NewSegmentAction(1, ActionTypeGrow, "shard-0", 100), true},
		{"grow channel", NewChannelAction(1, ActionTypeGrow, "ch-0"), true},
		{"reduce segment", NewSegmentAction(1, ActionTypeReduce, "shard-0", 100), false},
		{"reduce channel", NewChannelAction(1, ActionTypeReduce, "ch-0"), false},
		{"grow leader", NewLeaderAction(1, 2, ActionTypeGrow, "shard-0", 100, 1), false},
		{"update leader", NewLeaderAction(1, 2, ActionTypeUpdate, "shard-0", 100, 1), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spy := &gateSpy{err: merr.WrapErrServiceUnavailable("not synced")}
			ex := newTestExecutor(1)
			ex.fileResourceGate = spy.gate

			err := ex.checkFileResourceReady(tc.action)
			if tc.consults {
				assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
				assert.Equal(t, 1, spy.calls)
				assert.Equal(t, []int64{1}, spy.nodes, "the gate must be asked about this executor's node")
			} else {
				assert.NoError(t, err, "only a grow that puts data on the node may be held back")
				assert.Equal(t, 0, spy.calls, "the gate must not even be consulted")
			}
		})
	}
}

// A held-back task must be deferred, not failed and not counted: it has to be
// retriable, and it must not consume a slot of either execution pool while it
// waits.
func TestExecutorDefersAGrowActionRatherThanConsumingAPoolSlot(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	replica := newTestReplica(1000, 1)

	t.Run("channel task", func(t *testing.T) {
		spy := &gateSpy{err: merr.WrapErrServiceUnavailable("not synced")}
		ex := newTestExecutor(1)
		ex.fileResourceGate = spy.gate

		task, err := NewChannelTask(ctx, 10*time.Second, testSource("test"), 1000, replica,
			NewChannelAction(1, ActionTypeGrow, "ch-0"))
		require.NoError(t, err)
		task.SetID(1)

		assert.False(t, ex.Execute(task, 0), "a node that is behind must not run the action")
		assert.Equal(t, int32(0), ex.channelTaskNum.Load(),
			"a deferred task must not hold a channel pool slot")
		assert.Equal(t, int32(0), ex.nonChannelTaskNum.Load(),
			"a deferred task must not hold a non-channel pool slot")
		assert.False(t, ex.executingTasks.Contain(task.Index()),
			"a deferred task must be retriable, so it must not stay marked as executing")
		assert.NoError(t, task.Err(), "deferring is not failing")
	})

	t.Run("segment task", func(t *testing.T) {
		spy := &gateSpy{err: merr.WrapErrServiceUnavailable("not synced")}
		ex := newTestExecutor(1)
		ex.fileResourceGate = spy.gate

		task, err := NewSegmentTask(ctx, 10*time.Second, testSource("test"), 1000, replica, 0,
			NewSegmentAction(1, ActionTypeGrow, "shard-0", 100))
		require.NoError(t, err)
		task.SetID(2)

		assert.False(t, ex.Execute(task, 0))
		assert.Equal(t, int32(0), ex.nonChannelTaskNum.Load())
		assert.Equal(t, int32(0), ex.channelTaskNum.Load())
		assert.False(t, ex.executingTasks.Contain(task.Index()))
		assert.NoError(t, task.Err(), "deferring is not failing")
	})
}

func TestExecutorGateIsHandedToEveryExecutorTheSchedulerCreates(t *testing.T) {
	paramtable.Init()
	spy := &gateSpy{}
	scheduler := NewScheduler(context.Background(), nil, nil, nil, nil, nil, nil)
	scheduler.SetFileResourceGate(spy.gate)

	scheduler.AddExecutor(11)
	scheduler.AddExecutor(12)
	t.Cleanup(func() {
		scheduler.RemoveExecutor(11)
		scheduler.RemoveExecutor(12)
	})

	for _, nodeID := range []int64{11, 12} {
		executor, ok := scheduler.executors.Get(nodeID)
		require.True(t, ok)
		require.NotNil(t, executor.fileResourceGate,
			"an executor added after the gate was installed must carry it")
		assert.NoError(t, executor.fileResourceGate(nodeID))
	}
	assert.Equal(t, []int64{11, 12}, spy.nodes)
}

func TestSchedulerWithoutGateLeavesItsExecutorsUngated(t *testing.T) {
	paramtable.Init()
	scheduler := NewScheduler(context.Background(), nil, nil, nil, nil, nil, nil)
	scheduler.SetFileResourceGate(nil)

	scheduler.AddExecutor(21)
	t.Cleanup(func() { scheduler.RemoveExecutor(21) })

	executor, ok := scheduler.executors.Get(21)
	require.True(t, ok)
	assert.Nil(t, executor.fileResourceGate,
		"a stock binary must leave every executor on the native path")
}
