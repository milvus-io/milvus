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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
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

func (f *fakeSplitPlanner) AssignSegment(ctx context.Context, segment *SegmentInfo, targets []*datapb.SplitShardTaskTarget) (int, error) {
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
	// a permissive routing committer so the fence/adoption commits succeed;
	// tests that assert on the commit override it with their own mock.
	router := broker.NewMockBroker(t)
	router.EXPECT().CommitShardSplitRouting(mock.Anything, mock.Anything).Return(nil).Maybe()
	manager.setRoutingCommitter(router)
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

	t.Run("fence and initialize targets", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		wal := mock_streaming.NewMockWALAccesser(t)
		// a single SplitShard message fences the source vchannel (the source
		// streamingnode auto-flushes the growing segments, no ManualFlush).
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeSplitShard
		})).Return(&types.AppendResult{MessageID: rmq.NewRmqID(2), TimeTick: 2000}, nil).Once()
		// one CreateVChannel message per target vchannel.
		initialized := make(map[string]uint64, 2)
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeCreateVChannel
		}), mock.Anything).RunAndReturn(
			func(ctx context.Context, msg message.MutableMessage, opts ...streaming.AppendOption) (*types.AppendResult, error) {
				initialized[msg.VChannel()] = opts[0].BarrierTimeTick
				return &types.AppendResult{MessageID: rmq.NewRmqID(3), TimeTick: 2100, LastConfirmedMessageID: rmq.NewRmqID(9)}, nil
			}).Times(2)

		manager, catalog := newSplitExecutorManager(t, m, fencingTask(), wal, nil, &fakeSplitPlanner{})
		// three persists: the fenced flag, the target start positions, then the
		// state transition after the routing commit.
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Times(3)

		manager.advanceTasks()
		task := manager.mustGetTask(100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, task.GetState())
		assert.True(t, task.GetFenced())
		// every target is created with the freshly allocated barrier (1000 from
		// the mocked allocator), not T_switch.
		assert.Equal(t, map[string]uint64{"v1": 1000, "v2": 1000}, initialized)
		// the consume start position of each target is persisted.
		for _, target := range task.GetTargets() {
			assert.Equal(t, rmq.NewRmqID(9).Marshal(), target.GetStartPosition())
		}
	})

	t.Run("already fenced proceeds to create targets", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		wal := mock_streaming.NewMockWALAccesser(t)
		// the source was already fenced by a previous attempt: the retry returns
		// SHARD_FENCED, treated as success, and the task rolls forward.
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeSplitShard
		})).Return(nil, status.NewShardFenced("v0")).Once()
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeCreateVChannel
		}), mock.Anything).Return(&types.AppendResult{MessageID: rmq.NewRmqID(3), TimeTick: 2100, LastConfirmedMessageID: rmq.NewRmqID(9)}, nil).Times(2)

		manager, catalog := newSplitExecutorManager(t, m, fencingTask(), wal, nil, &fakeSplitPlanner{})
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Times(3)
		manager.advanceTasks()
		task := manager.mustGetTask(100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, task.GetState())
		assert.True(t, task.GetFenced())
	})

	t.Run("resume after T_switch persisted only initializes targets", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		task := fencingTask()
		task.Fenced = true
		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeCreateVChannel
		}), mock.Anything).Return(&types.AppendResult{MessageID: rmq.NewRmqID(3), TimeTick: 2100, LastConfirmedMessageID: rmq.NewRmqID(9)}, nil).Times(2)

		manager, catalog := newSplitExecutorManager(t, m, task, wal, nil, &fakeSplitPlanner{})
		// already fenced: persist the start positions, then the state transition.
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Times(2)

		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, manager.mustGetTask(100).GetState())
	})

	t.Run("collection dropped before the fence aborts", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80})
		task := fencingTask()
		task.CollectionId = 999 // not in meta
		wal := mock_streaming.NewMockWALAccesser(t)
		manager, catalog := newSplitExecutorManager(t, m, task, wal, nil, &fakeSplitPlanner{})
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()
		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, manager.mustGetTask(100).GetState())
	})

	t.Run("split shard append error stays in fencing", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80})
		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeSplitShard
		})).Return(nil, errors.New("mock append error")).Once()
		manager, _ := newSplitExecutorManager(t, m, fencingTask(), wal, nil, &fakeSplitPlanner{})
		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, manager.mustGetTask(100).GetState())
	})

	t.Run("create vchannel error keeps the fence and stays in fencing", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80})
		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeSplitShard
		})).Return(&types.AppendResult{MessageID: rmq.NewRmqID(2), TimeTick: 2000}, nil).Once()
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeCreateVChannel
		}), mock.Anything).Return(nil, errors.New("mock append error")).Once()
		manager, catalog := newSplitExecutorManager(t, m, fencingTask(), wal, nil, &fakeSplitPlanner{})
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once() // T_switch persist
		manager.advanceTasks()
		task := manager.mustGetTask(100)
		// the fence is persisted, but target creation failed: stay in fencing and retry.
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, task.GetState())
		assert.True(t, task.GetFenced())
	})

	t.Run("fenced-flag persist failure does not record the fence", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80})
		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeSplitShard
		})).Return(&types.AppendResult{MessageID: rmq.NewRmqID(2), TimeTick: 2000}, nil).Once()
		manager, catalog := newSplitExecutorManager(t, m, fencingTask(), wal, nil, &fakeSplitPlanner{})
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(errors.New("save failed")).Once()
		manager.advanceTasks()
		task := manager.mustGetTask(100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, task.GetState())
		assert.False(t, task.GetFenced())
	})

	t.Run("final persist failure stays in fencing", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeSplitShard
		})).Return(&types.AppendResult{MessageID: rmq.NewRmqID(2), TimeTick: 2000}, nil).Once()
		wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
			return msg.MessageType() == message.MessageTypeCreateVChannel
		}), mock.Anything).Return(&types.AppendResult{MessageID: rmq.NewRmqID(3), TimeTick: 2100, LastConfirmedMessageID: rmq.NewRmqID(9)}, nil).Times(2)
		manager, catalog := newSplitExecutorManager(t, m, fencingTask(), wal, nil, &fakeSplitPlanner{})
		// the fenced-flag and start-position persists succeed, the final
		// state-transition persist fails.
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Times(2)
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(errors.New("save failed")).Once()
		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, manager.mustGetTask(100).GetState())
	})
}

func TestAdvanceAdopting(t *testing.T) {
	paramtable.Init()

	adoptingTask := func() *datapb.SplitShardTask {
		task := fencingTask()
		task.State = datapb.SplitShardTaskState_SplitShardTaskAdopting
		task.Fenced = true
		return task
	}

	t.Run("adoption flip completes the task", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		wal := mock_streaming.NewMockWALAccesser(t)
		manager, catalog := newSplitExecutorManager(t, m, adoptingTask(), wal, nil, &fakeSplitPlanner{})
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()
		// the adoption commit drops the source and makes the targets Normal.
		router := broker.NewMockBroker(t)
		router.EXPECT().CommitShardSplitRouting(mock.Anything, mock.MatchedBy(func(req *rootcoordpb.CommitShardSplitRoutingRequest) bool {
			states := make(map[string]etcdpb.ShardState, len(req.GetVirtualChannelNames()))
			for i, vch := range req.GetVirtualChannelNames() {
				states[vch] = req.GetShardInfos()[i].GetState()
			}
			return req.GetRoutingMode() == etcdpb.RoutingMode_RoutingModeRange &&
				states["v0"] == etcdpb.ShardState_ShardDropped &&
				states["v1"] == etcdpb.ShardState_ShardNormal &&
				states["v2"] == etcdpb.ShardState_ShardNormal
		})).Return(nil).Once()
		manager.setRoutingCommitter(router)

		manager.advanceTasks()
		task := manager.mustGetTask(100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, task.GetState())
		assert.NotZero(t, task.GetEndTime())
	})

	t.Run("commit error stays in adopting", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80})
		wal := mock_streaming.NewMockWALAccesser(t)
		manager, _ := newSplitExecutorManager(t, m, adoptingTask(), wal, nil, &fakeSplitPlanner{})
		router := broker.NewMockBroker(t)
		router.EXPECT().CommitShardSplitRouting(mock.Anything, mock.Anything).Return(errors.New("commit failed")).Once()
		manager.setRoutingCommitter(router)

		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, manager.mustGetTask(100).GetState())
	})

	t.Run("collection dropped still completes the task", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80})
		task := adoptingTask()
		task.CollectionId = 999 // not in meta
		wal := mock_streaming.NewMockWALAccesser(t)
		manager, catalog := newSplitExecutorManager(t, m, task, wal, nil, &fakeSplitPlanner{})
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()
		// no routing commit is issued when the collection is gone.
		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, manager.mustGetTask(100).GetState())
	})
}

func TestAdvanceRedistributing(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitRelabelBatchSize.Key, "2")
	defer params.Reset(params.DataCoordCfg.ShardSplitRelabelBatchSize.Key)

	task := fencingTask()
	task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	task.Fenced = true

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
	taskC.Fenced = true
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
	task2.Fenced = true
	m2 := newSplitTestMeta(true, "v9", map[int64]int64{10: 10})
	manager2, _ := newSplitExecutorManager(t, m2, task2, nil, nil, &fakeSplitPlanner{assignErr: errors.New("mock assign error")})
	manager2.advanceTasks()
	assert.Len(t, m2.GetSegmentsByChannel("v9"), 1)
}

func TestAdvanceRedistributingSkipsImportingSegments(t *testing.T) {
	paramtable.Init()
	task := fencingTask()
	task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	task.Fenced = true

	// one flushed segment and one still importing on the source vchannel.
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 10})
	m.segments.SetSegment(5000, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: 5000, CollectionID: 1, PartitionID: 11, InsertChannel: "v0",
			State: commonpb.SegmentState_Importing, IsImporting: true,
			Binlogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogID: 5000, MemorySize: 100}}}},
		},
	})
	manager, catalog := newSplitExecutorManager(t, m, task, nil, nil, &fakeSplitPlanner{})
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()

	// only the flushed segment is relabeled; the importing one stays on source.
	manager.advanceTasks()
	remaining := m.GetSegmentsByChannel("v0")
	assert.Len(t, remaining, 1)
	assert.Equal(t, int64(5000), remaining[0].GetID())
	assert.True(t, remaining[0].GetIsImporting())
	// the source is not drained while the importing segment remains.
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, manager.mustGetTask(100).GetState())
}

func TestAdvanceRedistributingDrainWaitsForImportJob(t *testing.T) {
	paramtable.Init()
	task := fencingTask()
	task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	task.Fenced = true

	// no segment remains on the source vchannel.
	m := newSplitTestMeta(true, "v0", map[int64]int64{})
	manager, catalog := newSplitExecutorManager(t, m, task, nil, nil, &fakeSplitPlanner{})
	importMeta := NewMockImportMeta(t)
	manager.setImportMeta(importMeta)

	// an active import job still targets the source vchannel: stay redistributing
	// even though the segment scan is empty (the Pending job has no segment yet).
	importMeta.EXPECT().GetJobBy(mock.Anything, mock.Anything).Return([]ImportJob{
		&importJob{ImportJob: &datapb.ImportJob{Vchannels: []string{"v0"}, State: internalpb.ImportJobState_Pending}},
	}).Once()
	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, manager.mustGetTask(100).GetState())

	// once no active import job targets the source, the source is drained.
	importMeta.EXPECT().GetJobBy(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()
	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, manager.mustGetTask(100).GetState())
}

func TestAbortTaskRefusedPastFence(t *testing.T) {
	paramtable.Init()
	task := fencingTask()
	task.Fenced = true
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80})
	manager, _ := newSplitExecutorManager(t, m, task, nil, nil, &fakeSplitPlanner{})

	manager.abortTask(manager.mustGetTask(100), "should be refused")
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, manager.mustGetTask(100).GetState())
}

func TestAbortTaskPersistFailure(t *testing.T) {
	paramtable.Init()
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80})
	manager, catalog := newSplitExecutorManager(t, m, preparingTask(), nil, nil, &fakeSplitPlanner{})
	catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(errors.New("save failed")).Once()

	manager.abortTask(manager.mustGetTask(100), "reason")
	// the persist failed, so the task is not marked aborted in the cache.
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, manager.mustGetTask(100).GetState())
}
