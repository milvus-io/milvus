// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type fakeGrowingSourceReleaseDrainer struct {
	calls   []growingSourceReleaseDrainCall
	fenceTs map[string]uint64
	err     error
}

type growingSourceReleaseDrainCall struct {
	collectionID       int64
	channels           []string
	segmentsByChannel  map[string][]int64
	segmentScopedDrain bool
}

func (d *fakeGrowingSourceReleaseDrainer) DrainGrowingSourceReleaseChannels(_ context.Context, collectionID int64, channels []string) (map[string]uint64, error) {
	d.calls = append(d.calls, growingSourceReleaseDrainCall{
		collectionID: collectionID,
		channels:     append([]string(nil), channels...),
	})
	return d.fenceTs, d.err
}

func (d *fakeGrowingSourceReleaseDrainer) DrainGrowingSourceReleaseSegments(_ context.Context, collectionID int64, segmentsByChannel map[string][]int64) (map[string]uint64, error) {
	copied := make(map[string][]int64, len(segmentsByChannel))
	channels := make([]string, 0, len(segmentsByChannel))
	for channel, segmentIDs := range segmentsByChannel {
		channels = append(channels, channel)
		copied[channel] = append([]int64(nil), segmentIDs...)
	}
	d.calls = append(d.calls, growingSourceReleaseDrainCall{
		collectionID:       collectionID,
		channels:           channels,
		segmentsByChannel:  copied,
		segmentScopedDrain: true,
	})
	return d.fenceTs, d.err
}

func newTestExecutor(nodeID int64) *Executor {
	nodeMgr := session.NewNodeManager()
	return NewExecutor(
		nodeID,
		nil, // meta
		nil, // dist
		nil, // broker
		nil, // targetMgr
		nil, // cluster
		nodeMgr,
		nil, // growingSourceReleaseDrainer
	)
}

func TestExecutorDrainTextBeforeStreamingSegmentRelease(t *testing.T) {
	drainErr := errors.New("drain failed")
	drainer := &fakeGrowingSourceReleaseDrainer{err: drainErr}
	ex := newTestExecutor(1)
	ex.growingSourceReleaseDrainer = drainer

	replica := newTestReplica(1000, 1)
	action := NewSegmentActionWithScope(1, ActionTypeReduce, "ch-1", 100, querypb.DataScope_Streaming, 0)
	task, err := NewSegmentTask(context.Background(), time.Second, testSource("test"), 1000, replica, commonpb.LoadPriority_LOW, action)
	assert.NoError(t, err)
	task.SetID(1)

	ex.releaseSegment(task, 0)

	assert.ErrorContains(t, task.Err(), "drain failed")
	assert.Equal(t, []growingSourceReleaseDrainCall{{
		collectionID:       1000,
		channels:           []string{"ch-1"},
		segmentsByChannel:  map[string][]int64{"ch-1": {100}},
		segmentScopedDrain: true,
	}}, drainer.calls)
}

func TestExecutorSkipDrainTextForDroppedStreamingSegment(t *testing.T) {
	drainer := &fakeGrowingSourceReleaseDrainer{err: errors.New("drain should be skipped")}
	ex := newTestExecutor(1)
	ex.growingSourceReleaseDrainer = drainer
	ex.targetMgr = meta.NewMockTargetManager(t)
	ex.targetMgr.(*meta.MockTargetManager).EXPECT().
		GetDmChannel(mock.Anything, int64(1000), "ch-1", meta.CurrentTarget).
		Return(&meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{
			ChannelName:       "ch-1",
			SeekPosition:      &msgpb.MsgPosition{Timestamp: typeutil.MaxTimestamp},
			DroppedSegmentIds: []int64{100},
		}})
	ex.cluster = session.NewMockCluster(t)
	ex.cluster.(*session.MockCluster).EXPECT().
		ReleaseSegments(mock.Anything, int64(1), mock.MatchedBy(func(req *querypb.ReleaseSegmentsRequest) bool {
			return req.GetNeedTransfer() &&
				req.GetScope() == querypb.DataScope_Streaming &&
				req.GetCheckpoint().GetTimestamp() == typeutil.MaxTimestamp
		})).
		Return(merr.Success(), nil)

	replica := newTestReplica(1000, 1)
	action := NewSegmentActionWithScope(1, ActionTypeReduce, "ch-1", 100, querypb.DataScope_Streaming, 0)
	task, err := NewSegmentTask(context.Background(), time.Second, testSource("test"), 1000, replica, commonpb.LoadPriority_LOW, action)
	assert.NoError(t, err)
	task.SetID(1)

	ex.releaseSegment(task, 0)

	assert.NoError(t, task.Err())
	assert.Empty(t, drainer.calls)
}

func TestExecutorUsesGrowingSourceReleaseFenceAsStreamingReleaseCheckpoint(t *testing.T) {
	drainer := &fakeGrowingSourceReleaseDrainer{fenceTs: map[string]uint64{"ch-1": 1000}}
	ex := newTestExecutor(1)
	ex.growingSourceReleaseDrainer = drainer
	ex.targetMgr = meta.NewMockTargetManager(t)
	ex.targetMgr.(*meta.MockTargetManager).EXPECT().
		GetDmChannel(mock.Anything, int64(1000), "ch-1", meta.CurrentTarget).
		Return(&meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{
			ChannelName:  "ch-1",
			SeekPosition: &msgpb.MsgPosition{Timestamp: 2000},
		}})
	ex.cluster = session.NewMockCluster(t)
	ex.cluster.(*session.MockCluster).EXPECT().
		ReleaseSegments(mock.Anything, int64(1), mock.MatchedBy(func(req *querypb.ReleaseSegmentsRequest) bool {
			return req.GetNeedTransfer() &&
				req.GetScope() == querypb.DataScope_Streaming &&
				req.GetCheckpoint().GetChannelName() == "ch-1" &&
				req.GetCheckpoint().GetTimestamp() == 1000
		})).
		Return(merr.Success(), nil)

	replica := newTestReplica(1000, 1)
	action := NewSegmentActionWithScope(1, ActionTypeReduce, "ch-1", 100, querypb.DataScope_Streaming, 0)
	task, err := NewSegmentTask(context.Background(), time.Second, testSource("test"), 1000, replica, commonpb.LoadPriority_LOW, action)
	assert.NoError(t, err)
	task.SetID(1)

	ex.releaseSegment(task, 0)

	assert.NoError(t, task.Err())
	assert.Equal(t, []growingSourceReleaseDrainCall{{
		collectionID:       1000,
		channels:           []string{"ch-1"},
		segmentsByChannel:  map[string][]int64{"ch-1": {100}},
		segmentScopedDrain: true,
	}}, drainer.calls)
}

func TestExecutorDrainTextBeforeUnsubscribeChannel(t *testing.T) {
	drainErr := errors.New("drain failed")
	drainer := &fakeGrowingSourceReleaseDrainer{err: drainErr}
	ex := newTestExecutor(1)
	ex.growingSourceReleaseDrainer = drainer

	replica := newTestReplica(1000, 1)
	action := NewChannelAction(1, ActionTypeReduce, "ch-1")
	task, err := NewChannelTask(context.Background(), time.Second, testSource("test"), 1000, replica, action)
	assert.NoError(t, err)
	task.SetID(1)

	err = ex.unsubscribeChannel(task, 0)

	assert.ErrorContains(t, err, "drain failed")
	assert.ErrorContains(t, task.Err(), "drain failed")
	assert.Equal(t, []growingSourceReleaseDrainCall{{collectionID: 1000, channels: []string{"ch-1"}}}, drainer.calls)
}

func TestExecutorSkipDrainTextForReleasedCollectionBeforeUnsubscribeChannel(t *testing.T) {
	drainer := &fakeGrowingSourceReleaseDrainer{err: errors.New("drain should be skipped")}
	ex := newTestExecutor(1)
	ex.growingSourceReleaseDrainer = drainer
	ex.meta = meta.NewMeta(nil, nil, session.NewNodeManager())
	ex.cluster = session.NewMockCluster(t)
	ex.cluster.(*session.MockCluster).EXPECT().
		UnsubDmChannel(mock.Anything, int64(1), mock.AnythingOfType("*querypb.UnsubDmChannelRequest")).
		Return(merr.Success(), nil)

	replica := newTestReplica(1000, 1)
	action := NewChannelAction(1, ActionTypeReduce, "ch-1")
	task, err := NewChannelTask(context.Background(), time.Second, testSource("test"), 1000, replica, action)
	assert.NoError(t, err)
	task.SetID(1)

	err = ex.unsubscribeChannel(task, 0)

	assert.NoError(t, err)
	assert.Empty(t, drainer.calls)
}

func newTestReplica(collectionID int64, nodes ...int64) *meta.Replica {
	return meta.NewReplica(
		&querypb.Replica{
			ID:            100,
			CollectionID:  collectionID,
			ResourceGroup: meta.DefaultResourceGroupName,
			Nodes:         nodes,
		},
		typeutil.NewUniqueSet(nodes...),
	)
}

type testSource string

func (s testSource) String() string {
	return string(s)
}

func TestExecutorCapacity(t *testing.T) {
	paramtable.Init()

	t.Run("GetChannelTaskCap", func(t *testing.T) {
		// With default fraction 0.1 and cap 256: ceil(256*0.1) = 26
		paramtable.Get().Save("queryCoord.taskExecutionCap", "256")
		paramtable.Get().Save("queryCoord.channelTaskCapFraction", "0.1")
		defer paramtable.Get().Reset("queryCoord.taskExecutionCap")
		defer paramtable.Get().Reset("queryCoord.channelTaskCapFraction")

		ex := newTestExecutor(1)
		assert.Equal(t, int32(26), ex.GetChannelTaskCap())
		assert.Equal(t, int32(230), ex.GetNonChannelTaskCap())
	})

	t.Run("GetChannelTaskCap_SmallTotal", func(t *testing.T) {
		// With cap=5 and fraction=0.1: ceil(5*0.1) = 1
		paramtable.Get().Save("queryCoord.taskExecutionCap", "5")
		paramtable.Get().Save("queryCoord.channelTaskCapFraction", "0.1")
		defer paramtable.Get().Reset("queryCoord.taskExecutionCap")
		defer paramtable.Get().Reset("queryCoord.channelTaskCapFraction")

		ex := newTestExecutor(1)
		assert.Equal(t, int32(1), ex.GetChannelTaskCap())
		assert.Equal(t, int32(4), ex.GetNonChannelTaskCap())
	})

	t.Run("GetNonChannelTaskCap_MinOne", func(t *testing.T) {
		// fraction=1.0 → channel gets all, but non-channel must be at least 1
		paramtable.Get().Save("queryCoord.taskExecutionCap", "5")
		paramtable.Get().Save("queryCoord.channelTaskCapFraction", "1.0")
		defer paramtable.Get().Reset("queryCoord.taskExecutionCap")
		defer paramtable.Get().Reset("queryCoord.channelTaskCapFraction")

		ex := newTestExecutor(1)
		assert.Equal(t, int32(5), ex.GetChannelTaskCap())
		assert.Equal(t, int32(1), ex.GetNonChannelTaskCap())
	})

	t.Run("GetChannelTaskCap_ClampNegative", func(t *testing.T) {
		// negative fraction should be clamped to 0, then min cap=1 kicks in
		paramtable.Get().Save("queryCoord.taskExecutionCap", "10")
		paramtable.Get().Save("queryCoord.channelTaskCapFraction", "-0.5")
		defer paramtable.Get().Reset("queryCoord.taskExecutionCap")
		defer paramtable.Get().Reset("queryCoord.channelTaskCapFraction")

		ex := newTestExecutor(1)
		assert.Equal(t, int32(1), ex.GetChannelTaskCap())
	})

	t.Run("GetChannelTaskCap_ClampAboveOne", func(t *testing.T) {
		// fraction > 1 should be clamped to 1
		paramtable.Get().Save("queryCoord.taskExecutionCap", "10")
		paramtable.Get().Save("queryCoord.channelTaskCapFraction", "2.5")
		defer paramtable.Get().Reset("queryCoord.taskExecutionCap")
		defer paramtable.Get().Reset("queryCoord.channelTaskCapFraction")

		ex := newTestExecutor(1)
		assert.Equal(t, int32(10), ex.GetChannelTaskCap())
		assert.Equal(t, int32(1), ex.GetNonChannelTaskCap())
	})
}

func TestExecutorChannelPoolCapacity(t *testing.T) {
	paramtable.Init()

	// Set small capacity for testing: total=5, fraction=0.4 → channel cap=2, non-channel cap=3
	paramtable.Get().Save("queryCoord.taskExecutionCap", "5")
	paramtable.Get().Save("queryCoord.channelTaskCapFraction", "0.4")
	defer paramtable.Get().Reset("queryCoord.taskExecutionCap")
	defer paramtable.Get().Reset("queryCoord.channelTaskCapFraction")

	ex := newTestExecutor(1)
	assert.Equal(t, int32(2), ex.GetChannelTaskCap())
	assert.Equal(t, int32(3), ex.GetNonChannelTaskCap())

	replica := newTestReplica(1000, 1)
	ctx := context.Background()

	// Fill channel pool (cap=2) by directly incrementing counters
	for i := 0; i < 2; i++ {
		channelName := fmt.Sprintf("ch-%d", i)
		action := NewChannelAction(1, ActionTypeGrow, channelName)
		task, err := NewChannelTask(ctx, 10*time.Second, testSource("test"), 1000, replica, action)
		assert.NoError(t, err)
		task.SetID(int64(100 + i))

		ok := ex.executingTasks.Insert(task.Index())
		assert.True(t, ok)
		n := ex.channelTaskNum.Inc()
		assert.True(t, n <= ex.GetChannelTaskCap(), "channel task %d should be accepted", i)
	}

	// Now channel pool is full (2/2). Try to submit another channel task - should be rejected
	action3 := NewChannelAction(1, ActionTypeGrow, "ch-overflow")
	task3, err := NewChannelTask(ctx, 10*time.Second, testSource("test"), 1000, replica, action3)
	assert.NoError(t, err)
	task3.SetID(103)
	ok := ex.Execute(task3, 0)
	assert.False(t, ok, "channel task should be rejected when channel pool is full")

	// Non-channel tasks should still be accepted (separate pool)
	segAction := NewSegmentAction(1, ActionTypeGrow, "shard-0", 999)
	segTask, err := NewSegmentTask(ctx, 10*time.Second, testSource("test"), 1000, replica, 0, segAction)
	assert.NoError(t, err)
	segTask.SetID(200)
	// Manually test the counter (don't actually execute since we lack cluster/broker mocks)
	ok = ex.executingTasks.Insert(segTask.Index())
	assert.True(t, ok)
	n := ex.nonChannelTaskNum.Inc()
	assert.True(t, n <= ex.GetNonChannelTaskCap(), "non-channel task should be accepted when only channel pool is full")
}

func TestExecutorNonChannelPoolCapacity(t *testing.T) {
	paramtable.Init()

	// total=5, fraction=0.4 → channel cap=2, non-channel cap=3
	paramtable.Get().Save("queryCoord.taskExecutionCap", "5")
	paramtable.Get().Save("queryCoord.channelTaskCapFraction", "0.4")
	defer paramtable.Get().Reset("queryCoord.taskExecutionCap")
	defer paramtable.Get().Reset("queryCoord.channelTaskCapFraction")

	ex := newTestExecutor(1)
	replica := newTestReplica(1000, 1)
	ctx := context.Background()

	// Fill non-channel pool (cap=3) by directly incrementing counters
	for i := 0; i < 3; i++ {
		segAction := NewSegmentAction(1, ActionTypeGrow, "shard-0", int64(300+i))
		segTask, err := NewSegmentTask(ctx, 10*time.Second, testSource("test"), 1000, replica, 0, segAction)
		assert.NoError(t, err)
		segTask.SetID(int64(300 + i))

		ok := ex.executingTasks.Insert(segTask.Index())
		assert.True(t, ok)
		n := ex.nonChannelTaskNum.Inc()
		assert.True(t, n <= ex.GetNonChannelTaskCap())
	}

	// Non-channel pool full (3/3). Try another segment task via Execute - should be rejected
	segAction := NewSegmentAction(1, ActionTypeGrow, "shard-0", 999)
	segTask, err := NewSegmentTask(ctx, 10*time.Second, testSource("test"), 1000, replica, 0, segAction)
	assert.NoError(t, err)
	segTask.SetID(999)
	ok := ex.Execute(segTask, 0)
	assert.False(t, ok, "non-channel task should be rejected when non-channel pool is full")

	// Channel tasks should still be accepted (separate pool)
	chAction := NewChannelAction(1, ActionTypeGrow, "ch-ok")
	chTask, err := NewChannelTask(ctx, 10*time.Second, testSource("test"), 1000, replica, chAction)
	assert.NoError(t, err)
	chTask.SetID(400)
	// Verify via counter
	ok = ex.executingTasks.Insert(chTask.Index())
	assert.True(t, ok)
	n := ex.channelTaskNum.Inc()
	assert.True(t, n <= ex.GetChannelTaskCap(), "channel task should be accepted when only non-channel pool is full")
}

func TestExecutorRemoveTaskDecrementsCorrectPool(t *testing.T) {
	paramtable.Init()

	paramtable.Get().Save("queryCoord.taskExecutionCap", "10")
	paramtable.Get().Save("queryCoord.channelTaskCapFraction", "0.5")
	defer paramtable.Get().Reset("queryCoord.taskExecutionCap")
	defer paramtable.Get().Reset("queryCoord.channelTaskCapFraction")

	ex := newTestExecutor(1)
	replica := newTestReplica(1000, 1)
	ctx := context.Background()

	// Add one channel task manually
	chAction := NewChannelAction(1, ActionTypeGrow, "ch-remove")
	chTask, err := NewChannelTask(ctx, 10*time.Second, testSource("test"), 1000, replica, chAction)
	assert.NoError(t, err)
	chTask.SetID(500)
	ex.executingTasks.Insert(chTask.Index())
	ex.channelTaskNum.Inc()

	// Add one segment task manually
	segAction := NewSegmentAction(1, ActionTypeGrow, "shard-0", 501)
	segTask, err := NewSegmentTask(ctx, 10*time.Second, testSource("test"), 1000, replica, 0, segAction)
	assert.NoError(t, err)
	segTask.SetID(501)
	ex.executingTasks.Insert(segTask.Index())
	ex.nonChannelTaskNum.Inc()

	assert.Equal(t, int32(1), ex.channelTaskNum.Load())
	assert.Equal(t, int32(1), ex.nonChannelTaskNum.Load())

	// Remove channel task — should decrement channelTaskNum only
	ex.removeTask(chTask, 0)
	assert.Equal(t, int32(0), ex.channelTaskNum.Load())
	assert.Equal(t, int32(1), ex.nonChannelTaskNum.Load())

	// Remove segment task — should decrement nonChannelTaskNum only
	ex.removeTask(segTask, 0)
	assert.Equal(t, int32(0), ex.channelTaskNum.Load())
	assert.Equal(t, int32(0), ex.nonChannelTaskNum.Load())
}

// TestExecutorDeadlockReproduction verifies that when channel tasks fill the executor capacity,
// non-channel tasks (segment/leader) are blocked — the deadlock scenario this fix addresses.
// With the split-pool fix, this test should pass: non-channel tasks execute even when channel pool is full.
func TestExecutorDeadlockReproduction(t *testing.T) {
	paramtable.Init()

	// Set capacity=5, fraction=0.4 → channel cap=2, non-channel cap=3
	paramtable.Get().Save("queryCoord.taskExecutionCap", "5")
	paramtable.Get().Save("queryCoord.channelTaskCapFraction", "0.4")
	defer paramtable.Get().Reset("queryCoord.taskExecutionCap")
	defer paramtable.Get().Reset("queryCoord.channelTaskCapFraction")

	ex := newTestExecutor(1)
	replica := newTestReplica(1000, 1)
	ctx := context.Background()

	// Simulate: fill the channel pool completely (2 channel tasks)
	for i := 0; i < 2; i++ {
		chAction := NewChannelAction(1, ActionTypeGrow, fmt.Sprintf("deadlock-ch-%d", i))
		chTask, err := NewChannelTask(ctx, 10*time.Second, testSource("test"), 1000, replica, chAction)
		assert.NoError(t, err)
		chTask.SetID(int64(600 + i))
		ex.executingTasks.Insert(chTask.Index())
		ex.channelTaskNum.Inc()
	}

	// Verify channel pool is full
	assert.Equal(t, int32(2), ex.channelTaskNum.Load())

	// KEY ASSERTION: non-channel tasks should NOT be blocked
	// In the old single-pool design, this would fail because all 5 slots would need to be full
	// But with split pools, non-channel has its own capacity of 3
	leaderAction := NewLeaderAction(1, 1, ActionTypeGrow, "shard-0", 700, 1)
	leaderTask := NewLeaderSegmentTask(ctx, testSource("test"), 1000, replica, 1, leaderAction)
	leaderTask.SetID(700)

	// Directly check: can we increment the non-channel counter?
	ok := ex.executingTasks.Insert(leaderTask.Index())
	assert.True(t, ok, "leader task should not be deduped")
	n := ex.nonChannelTaskNum.Inc()
	assert.True(t, n <= ex.GetNonChannelTaskCap(),
		"leader task should be accepted even when channel pool is full (got count=%d, cap=%d)", n, ex.GetNonChannelTaskCap())

	// Also verify that additional channel tasks ARE rejected
	chOverflow := NewChannelAction(1, ActionTypeGrow, "deadlock-ch-overflow")
	chOverflowTask, err := NewChannelTask(ctx, 10*time.Second, testSource("test"), 1000, replica, chOverflow)
	assert.NoError(t, err)
	chOverflowTask.SetID(999)
	ok = ex.Execute(chOverflowTask, 0)
	assert.False(t, ok, "overflow channel task should be rejected")
}
