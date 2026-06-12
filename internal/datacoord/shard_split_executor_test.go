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

package datacoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// fakeVChannelAllocator implements splitVChannelAllocator for tests.
type fakeVChannelAllocator struct {
	vchannels []string
	err       error
}

func (f *fakeVChannelAllocator) AllocVirtualChannels(ctx context.Context, param balancer.AllocVChannelParam) ([]string, error) {
	return f.vchannels, f.err
}

// fakeSplitPlanner implements splitPlanner for tests: it splits the key
// space in half and assigns segments by partition parity.
type fakeSplitPlanner struct {
	planErr   error
	assignErr error
}

func (f *fakeSplitPlanner) PlanTargets(ctx context.Context, collection *collectionInfo, sourceVChannel string, targetVChannels []string) ([]*datapb.SplitShardTaskTarget, error) {
	if f.planErr != nil {
		return nil, f.planErr
	}
	targets := make([]*datapb.SplitShardTaskTarget, 0, len(targetVChannels))
	for i, vchannel := range targetVChannels {
		target := &datapb.SplitShardTaskTarget{Vchannel: vchannel}
		if i == 0 {
			target.RoutingKeyUpper = []byte{0x80}
		} else {
			target.RoutingKeyLower = []byte{0x80}
		}
		targets = append(targets, target)
	}
	return targets, nil
}

func (f *fakeSplitPlanner) AssignSegment(segment *SegmentInfo, targets []*datapb.SplitShardTaskTarget) (int, error) {
	if f.assignErr != nil {
		return 0, f.assignErr
	}
	return int(segment.GetPartitionID() % int64(len(targets))), nil
}

// newSplitExecutorManager builds a manager with one persisted task and the
// given fakes; the catalog accepts every save.
func newSplitExecutorManager(t *testing.T, m *meta, task *datapb.SplitShardTask, wal streaming.WALAccesser, vchannelAllocator splitVChannelAllocator, planner splitPlanner) (*shardSplitManager, *mocks.DataCoordCatalog) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSplitShardTask(mock.Anything).Return([]*datapb.SplitShardTask{task}, nil).Once()
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(1000), nil).Maybe()
	if m != nil {
		// the redistribution path persists the relabeled segments through
		// the meta's own catalog.
		m.catalog = catalog
	}
	manager, err := newShardSplitManager(context.Background(), m, catalog, alloc, wal, vchannelAllocator, planner)
	assert.NoError(t, err)
	return manager, catalog
}

func preparingTask() *datapb.SplitShardTask {
	return &datapb.SplitShardTask{
		TaskId:         100,
		CollectionId:   1,
		SourceVchannel: "v0",
		State:          datapb.SplitShardTaskState_SplitShardTaskPreparing,
	}
}

func fencingTask() *datapb.SplitShardTask {
	return &datapb.SplitShardTask{
		TaskId:         100,
		CollectionId:   1,
		SourceVchannel: "v0",
		State:          datapb.SplitShardTaskState_SplitShardTaskFencing,
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: "v1", RoutingKeyUpper: []byte{0x80}},
			{Vchannel: "v2", RoutingKeyLower: []byte{0x80}},
		},
	}
}

// fakeCompactionPreempter records the channels preempted.
type fakeCompactionPreempter struct {
	channels []string
}

func (f *fakeCompactionPreempter) preemptTasksByChannel(channel string) {
	f.channels = append(f.channels, channel)
}

func TestAdvancePreparing(t *testing.T) {
	paramtable.Init()

	t.Run("targets planned, advance to fencing", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		manager, catalog := newSplitExecutorManager(t, m, preparingTask(),
			nil, &fakeVChannelAllocator{vchannels: []string{"v1", "v2"}}, &fakeSplitPlanner{})
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()
		preempter := &fakeCompactionPreempter{}
		manager.setCompactionPreempter(preempter)

		manager.advanceTasks()
		task := manager.mustGetTask(100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, task.GetState())
		assert.Len(t, task.GetTargets(), 2)
		assert.Equal(t, "v1", task.GetTargets()[0].GetVchannel())
		// the in-flight compaction of the source shard is preempted so the
		// split never waits behind it.
		assert.Equal(t, []string{"v0"}, preempter.channels)
	})

	t.Run("planner not ready, stay in preparing", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		manager, _ := newSplitExecutorManager(t, m, preparingTask(),
			nil, &fakeVChannelAllocator{vchannels: []string{"v1", "v2"}}, &fakeSplitPlanner{planErr: ErrSplitPlannerNotReady})

		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, manager.mustGetTask(100).GetState())
	})

	t.Run("vchannel allocation failure aborts before the fence", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		manager, catalog := newSplitExecutorManager(t, m, preparingTask(),
			nil, &fakeVChannelAllocator{err: errors.New("not enough pchannels")}, &fakeSplitPlanner{})
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()

		manager.advanceTasks()
		task := manager.mustGetTask(100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, task.GetState())
		assert.Contains(t, task.GetFailReason(), "not enough pchannels")
	})

	t.Run("collection dropped aborts before the fence", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80})
		task := preparingTask()
		task.CollectionId = 999
		manager, catalog := newSplitExecutorManager(t, m, task,
			nil, &fakeVChannelAllocator{vchannels: []string{"v1", "v2"}}, &fakeSplitPlanner{})
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()

		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, manager.mustGetTask(100).GetState())
	})
}

func TestAdvanceFencing(t *testing.T) {
	paramtable.Init()

	newManualFlushResult := func(t *testing.T) *types.AppendResult {
		extra, err := anypb.New(&messagespb.ManualFlushExtraResponse{SegmentIds: []int64{7}})
		assert.NoError(t, err)
		return &types.AppendResult{MessageID: rmq.NewRmqID(1), TimeTick: 1500, Extra: extra}
	}

	t.Run("fence and initialize targets", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		wal := mock_streaming.NewMockWALAccesser(t)
		// ManualFlush + SplitShard on the source vchannel.
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeManualFlush
		}), mock.Anything).Return(newManualFlushResult(t), nil).Once()
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeSplitShard
		})).Return(&types.AppendResult{MessageID: rmq.NewRmqID(2), TimeTick: 2000}, nil).Once()
		// one initialization message per target vchannel.
		initialized := make(map[string]uint64, 2)
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeCreateCollection
		}), mock.Anything).RunAndReturn(
			func(ctx context.Context, msg message.MutableMessage, opts ...streaming.AppendOption) (*types.AppendResult, error) {
				initialized[msg.VChannel()] = opts[0].BarrierTimeTick
				return &types.AppendResult{MessageID: rmq.NewRmqID(3), TimeTick: 2100}, nil
			}).Times(2)

		manager, catalog := newSplitExecutorManager(t, m, fencingTask(), wal, nil, &fakeSplitPlanner{})
		// two persists: T_switch first, then the state transition.
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Times(2)

		manager.advanceTasks()
		task := manager.mustGetTask(100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, task.GetState())
		assert.Equal(t, uint64(2000), task.GetSwitchTimeTick())
		// every target initialized with T_switch as the barrier.
		assert.Equal(t, map[string]uint64{"v1": 2000, "v2": 2000}, initialized)
	})

	t.Run("already fenced without recorded T_switch stays", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeManualFlush
		}), mock.Anything).Return(newManualFlushResult(t), nil).Once()
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeSplitShard
		})).Return(nil, status.NewShardFenced("v0")).Once()

		manager, _ := newSplitExecutorManager(t, m, fencingTask(), wal, nil, &fakeSplitPlanner{})
		manager.advanceTasks()
		task := manager.mustGetTask(100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, task.GetState())
		assert.Zero(t, task.GetSwitchTimeTick())
	})

	t.Run("resume after T_switch persisted only initializes targets", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		task := fencingTask()
		task.SwitchTimeTick = 2000
		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeCreateCollection
		}), mock.Anything).Return(&types.AppendResult{MessageID: rmq.NewRmqID(3), TimeTick: 2100}, nil).Times(2)

		manager, catalog := newSplitExecutorManager(t, m, task, wal, nil, &fakeSplitPlanner{})
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()

		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, manager.mustGetTask(100).GetState())
	})
}

func TestAdvanceRedistributing(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitRelabelBatchSize.Key, "2")
	defer params.Reset(params.DataCoordCfg.ShardSplitRelabelBatchSize.Key)

	task := fencingTask()
	task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	task.SwitchTimeTick = 2000

	// three segments on the source vchannel: partitions 10/11/12 spread to
	// the two targets by parity.
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 10, 11: 20, 12: 30})
	manager, catalog := newSplitExecutorManager(t, m, task, nil, nil, &fakeSplitPlanner{})
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Times(2)
	catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()

	// round 1: relabel a batch of two segments.
	manager.advanceTasks()
	assert.Len(t, m.GetSegmentsByChannel("v0"), 1)
	// round 2: relabel the last segment.
	manager.advanceTasks()
	assert.Len(t, m.GetSegmentsByChannel("v0"), 0)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, manager.mustGetTask(100).GetState())
	// the segments landed on the target decided by the planner.
	for _, segment := range append(m.GetSegmentsByChannel("v1"), m.GetSegmentsByChannel("v2")...) {
		expected := task.GetTargets()[segment.GetPartitionID()%2].GetVchannel()
		assert.Equal(t, expected, segment.GetInsertChannel())
	}
	// round 3: the source shard is empty, advance to adopting.
	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, manager.mustGetTask(100).GetState())

	// a compacting segment is skipped defensively and retried on the next
	// round once the flag is released.
	taskC := fencingTask()
	taskC.TaskId = 300
	taskC.SourceVchannel = "v8"
	taskC.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	taskC.SwitchTimeTick = 2000
	mC := newSplitTestMeta(true, "v8", map[int64]int64{10: 10})
	segmentID := mC.GetSegmentsByChannel("v8")[0].GetID()
	mC.segments.SetIsCompacting(segmentID, true)
	managerC, catalogC := newSplitExecutorManager(t, mC, taskC, nil, nil, &fakeSplitPlanner{})
	managerC.advanceTasks()
	assert.Len(t, mC.GetSegmentsByChannel("v8"), 1) // skipped, not relabeled
	mC.segments.SetIsCompacting(segmentID, false)
	catalogC.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()
	managerC.advanceTasks()
	assert.Len(t, mC.GetSegmentsByChannel("v8"), 0)

	// assignment failure keeps the round for the next tick.
	task2 := fencingTask()
	task2.TaskId = 200
	task2.SourceVchannel = "v9"
	task2.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	task2.SwitchTimeTick = 2000
	m2 := newSplitTestMeta(true, "v9", map[int64]int64{10: 10})
	manager2, _ := newSplitExecutorManager(t, m2, task2, nil, nil, &fakeSplitPlanner{assignErr: errors.New("mock assign error")})
	manager2.advanceTasks()
	assert.Len(t, m2.GetSegmentsByChannel("v9"), 1)
}

func TestAbortTaskRefusedPastFence(t *testing.T) {
	paramtable.Init()
	task := fencingTask()
	task.SwitchTimeTick = 2000
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80})
	manager, _ := newSplitExecutorManager(t, m, task, nil, nil, &fakeSplitPlanner{})

	manager.abortTask(manager.mustGetTask(100), "should be refused")
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, manager.mustGetTask(100).GetState())
}
