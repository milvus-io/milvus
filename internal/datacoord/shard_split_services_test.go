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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	splitTestSource  = "by-dev-rootcoord-dml_0_1v0"
	splitTestTarget0 = "by-dev-rootcoord-dml_1_1v1"
	splitTestTarget1 = "by-dev-rootcoord-dml_2_1v2"
)

// newShardSplitTestServer builds a datacoord Server with a real in-memory meta
// and an empty split task store -- the state a secondary's datacoord is in when
// the SplitShard ack callback reaches it.
func newShardSplitTestServer(t *testing.T) *Server {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	s := &Server{meta: m, shardSplitTasks: newShardSplitTasks()}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	return s
}

func splitTestPosition(vchannel string, ts uint64) *msgpb.MsgPosition {
	return &msgpb.MsgPosition{
		ChannelName: vchannel,
		MsgID:       []byte(vchannel),
		WALName:     commonpb.WALName_Pulsar,
		Timestamp:   ts,
	}
}

func splitTestCommitRequest() *datapb.CommitShardSplitRequest {
	return &datapb.CommitShardSplitRequest{
		CollectionId: 100,
		SplitTaskId:  200,
		Sources: []*datapb.SplitShardTaskSource{
			{Vchannel: splitTestSource, SwitchTimeTick: 2000},
		},
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: splitTestTarget0, Buckets: []uint64{0}},
			{Vchannel: splitTestTarget1, Buckets: []uint64{1}},
		},
		TargetStartPositions: []*msgpb.MsgPosition{
			splitTestPosition(splitTestTarget0, 3000),
			splitTestPosition(splitTestTarget1, 3000),
		},
		RoutingModulus: 2,
	}
}

func TestCommitShardSplitCreatesTheTaskOnASecondary(t *testing.T) {
	// A secondary's datacoord never planned the split -- it learns of it only
	// through the ack callback, and the record it writes here is what makes the
	// drain check and the later adoption possible on that replica at all.
	svr := newShardSplitTestServer(t)

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	task, ok := svr.shardSplitTasks.get(200)
	require.True(t, ok)
	assert.Equal(t, int64(100), task.GetCollectionId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, task.GetState())
	assert.True(t, task.GetFenced())
	assert.Equal(t, uint64(2), task.GetRoutingModulus())
	require.Len(t, task.GetSources(), 1)
	assert.Equal(t, uint64(2000), task.GetSources()[0].GetSwitchTimeTick())
	assert.Len(t, task.GetTargets(), 2)

	// The record is durable: a datacoord restart must resume the same split.
	persisted, err := svr.meta.catalog.ListSplitShardTask(context.Background())
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	assert.Equal(t, int64(200), persisted[0].GetTaskId())

	// Each target's first checkpoint is seeded from its genesis position, so
	// the child delegators have somewhere to seek from.
	assert.Equal(t, uint64(3000), svr.meta.GetChannelCheckpoint(splitTestTarget0).GetTimestamp())
	assert.Equal(t, uint64(3000), svr.meta.GetChannelCheckpoint(splitTestTarget1).GetTimestamp())
}

func TestCommitShardSplitIsIdempotent(t *testing.T) {
	// The callback is delivered at least once to every replica, so a redelivery
	// must not create a second task nor re-seed a checkpoint that has since
	// advanced past its genesis.
	svr := newShardSplitTestServer(t)

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))
	first, ok := svr.shardSplitTasks.get(200)
	require.True(t, ok)

	seeded := mockey.Mock((*meta).UpdateChannelCheckpoints).To(func(_ *meta, _ context.Context, _ []*msgpb.MsgPosition) error {
		t.Fatal("a redelivered CommitShardSplit must not re-seed target checkpoints")
		return nil
	}).Build()
	defer seeded.UnPatch()

	status, err = svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	second, ok := svr.shardSplitTasks.get(200)
	require.True(t, ok)
	assert.Equal(t, first.GetTaskId(), second.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, second.GetState())
	assert.Len(t, second.GetTargets(), 2)
	assert.Equal(t, uint64(2000), second.GetSources()[0].GetSwitchTimeTick())

	persisted, err := svr.meta.catalog.ListSplitShardTask(context.Background())
	require.NoError(t, err)
	assert.Len(t, persisted, 1)
}

func TestCommitShardSplitTakesTheRequestSwitchTimeTick(t *testing.T) {
	// The primary wrote the task before it broadcast, so its T_switch is a
	// placeholder; the tick the broadcast actually landed on is what the write
	// path fenced at, and it is the one the drain must wait for.
	svr := newShardSplitTestServer(t)
	require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
		TaskId:       200,
		CollectionId: 100,
		State:        datapb.SplitShardTaskState_SplitShardTaskFencing,
		Sources:      []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource}},
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: splitTestTarget0, Buckets: []uint64{0}},
			{Vchannel: splitTestTarget1, Buckets: []uint64{1}},
		},
	}))

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	task, _ := svr.shardSplitTasks.get(200)
	assert.Equal(t, uint64(2000), task.GetSources()[0].GetSwitchTimeTick())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, task.GetState())
	assert.True(t, task.GetFenced())
}

func TestCommitShardSplitFillsATaskRecordedWithoutItsSource(t *testing.T) {
	// A task written before its fence was planned carries no source yet. The
	// commit supplies it, with the tick the fence landed on; the targets and the
	// modulus a task learned of late come from the request too.
	svr := newShardSplitTestServer(t)
	require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
		TaskId:       200,
		CollectionId: 100,
		State:        datapb.SplitShardTaskState_SplitShardTaskPreparing,
	}))

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	task, _ := svr.shardSplitTasks.get(200)
	assert.Equal(t, []string{splitTestSource}, splitSourceVChannels(task))
	assert.Equal(t, uint64(2000), task.GetSources()[0].GetSwitchTimeTick())
	assert.Len(t, task.GetTargets(), 2)
	assert.Equal(t, uint64(2), task.GetRoutingModulus())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, task.GetState())
}

func TestCommitShardSplitIgnoresPositionsForUnlistedChannels(t *testing.T) {
	// The positions are matched to targets by channel name. One naming a
	// channel this split does not own is not this split's business to seed --
	// it would plant a genesis checkpoint on somebody else's shard.
	svr := newShardSplitTestServer(t)
	req := splitTestCommitRequest()
	req.TargetStartPositions = append(req.TargetStartPositions,
		splitTestPosition("by-dev-rootcoord-dml_8_1v8", 3000))

	status, err := svr.CommitShardSplit(context.Background(), req)
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	assert.Nil(t, svr.meta.GetChannelCheckpoint("by-dev-rootcoord-dml_8_1v8"))
	assert.Equal(t, uint64(3000), svr.meta.GetChannelCheckpoint(splitTestTarget0).GetTimestamp())
}

func TestCommitShardSplitLeavesLaterStatesAlone(t *testing.T) {
	// The task rolls forward only. A callback redelivered after the split has
	// reached adoption must not drag it back into the redistribution window.
	svr := newShardSplitTestServer(t)
	require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
		TaskId:       200,
		CollectionId: 100,
		State:        datapb.SplitShardTaskState_SplitShardTaskAdopting,
		Sources:      []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource, SwitchTimeTick: 2000}},
	}))

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	task, _ := svr.shardSplitTasks.get(200)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, task.GetState())
}

func TestCommitShardSplitSeedsOnlyUnseededTargets(t *testing.T) {
	// A target that already reported a checkpoint has been consuming for a
	// while; overwriting it with the genesis position would rewind every child
	// delegator to the start of the WAL.
	svr := newShardSplitTestServer(t)
	require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(),
		[]*msgpb.MsgPosition{splitTestPosition(splitTestTarget0, 9000)}))

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	assert.Equal(t, uint64(9000), svr.meta.GetChannelCheckpoint(splitTestTarget0).GetTimestamp())
	assert.Equal(t, uint64(3000), svr.meta.GetChannelCheckpoint(splitTestTarget1).GetTimestamp())
}

func TestCommitShardSplitPersistFailureIsSystemError(t *testing.T) {
	svr := newShardSplitTestServer(t)
	save := mockey.Mock(mockey.GetMethod(svr.meta.catalog, "SaveSplitShardTask")).
		Return(errors.New("etcd down")).Build()
	defer save.UnPatch()

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(status), merr.ErrServiceInternal)
	_, ok := svr.shardSplitTasks.get(200)
	assert.False(t, ok)
}

func TestCommitShardSplitSeedFailureIsSystemError(t *testing.T) {
	svr := newShardSplitTestServer(t)
	seed := mockey.Mock((*meta).UpdateChannelCheckpoints).Return(errors.New("etcd down")).Build()
	defer seed.UnPatch()

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(status), merr.ErrServiceInternal)
}

// F7: a typed store failure keeps its code, so a transient one stays retriable
// on the wire; only a bare error, which carries no code, is relabeled System.
func TestCommitShardSplitStoreFailureKeepsItsCode(t *testing.T) {
	t.Run("persist", func(t *testing.T) {
		svr := newShardSplitTestServer(t)
		save := mockey.Mock(mockey.GetMethod(svr.meta.catalog, "SaveSplitShardTask")).
			Return(merr.WrapErrServiceUnavailableMsg("etcd leader changed")).Build()
		defer save.UnPatch()

		status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrServiceUnavailable)
		assert.True(t, status.GetRetriable())
		assert.Contains(t, status.GetReason(), "persist the committed shard split task 200")
	})

	t.Run("seed", func(t *testing.T) {
		svr := newShardSplitTestServer(t)
		seed := mockey.Mock((*meta).UpdateChannelCheckpoints).Return(merr.WrapErrServiceUnavailableMsg("etcd leader changed")).Build()
		defer seed.UnPatch()

		status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrServiceUnavailable)
		assert.True(t, status.GetRetriable())
	})

	t.Run("a timeout keeps the timeout code", func(t *testing.T) {
		err := shardSplitStoreError(context.DeadlineExceeded, "seed the split targets of task %d", 1)
		assert.Equal(t, merr.TimeoutCode, merr.Code(err))
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("a bare error is System, not Unexpected", func(t *testing.T) {
		err := shardSplitStoreError(errors.New("etcd down"), "persist the committed shard split task %d", 1)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.NotEqual(t, merr.InputError, merr.GetErrorType(err))
	})
}

func TestCommitShardSplitRefusesAMalformedRequest(t *testing.T) {
	// The sender is another coordinator, so anything wrong here is a Milvus
	// bug -- but a bug that would leave a durable record nothing can act on.
	// Nothing is written on any of these paths.
	assertRefused := func(t *testing.T, svr *Server, req *datapb.CommitShardSplitRequest) {
		t.Helper()
		status, err := svr.CommitShardSplit(context.Background(), req)
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrServiceInternal)
		persisted, err := svr.meta.catalog.ListSplitShardTask(context.Background())
		require.NoError(t, err)
		assert.Empty(t, persisted)
	}

	t.Run("task id zero is the fence's no-task sentinel", func(t *testing.T) {
		svr := newShardSplitTestServer(t)
		req := splitTestCommitRequest()
		req.SplitTaskId = 0
		assertRefused(t, svr, req)
		_, ok := svr.shardSplitTasks.get(0)
		assert.False(t, ok)
	})

	t.Run("no source shard would report drained immediately", func(t *testing.T) {
		svr := newShardSplitTestServer(t)
		req := splitTestCommitRequest()
		req.Sources = nil
		assertRefused(t, svr, req)
	})

	t.Run("a split fences exactly one source", func(t *testing.T) {
		svr := newShardSplitTestServer(t)
		req := splitTestCommitRequest()
		req.Sources = append(req.Sources, &datapb.SplitShardTaskSource{Vchannel: "by-dev-rootcoord-dml_9_1v9", SwitchTimeTick: 2000})
		assertRefused(t, svr, req)
	})

	t.Run("a split creates exactly two targets", func(t *testing.T) {
		for _, n := range []int{0, 1, 3} {
			svr := newShardSplitTestServer(t)
			req := splitTestCommitRequest()
			req.Targets = nil
			for i := 0; i < n; i++ {
				req.Targets = append(req.Targets, &datapb.SplitShardTaskTarget{
					Vchannel: fmt.Sprintf("by-dev-rootcoord-dml_%d_1v%d", 5+i, 5+i),
					Buckets:  []uint64{uint64(i)},
				})
			}
			assertRefused(t, svr, req)
		}
	})

	t.Run("a collection id that disagrees with the stored task", func(t *testing.T) {
		// mergeCommittedShardSplit never overwrites CollectionId, so without
		// this check the two coordinators would carry different records under
		// the same task id and never notice.
		svr := newShardSplitTestServer(t)
		require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
			TaskId:       200,
			CollectionId: 100,
			State:        datapb.SplitShardTaskState_SplitShardTaskFencing,
			Sources:      []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource}},
		}))
		req := splitTestCommitRequest()
		req.CollectionId = 999

		status, err := svr.CommitShardSplit(context.Background(), req)
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrServiceInternal)
		task, _ := svr.shardSplitTasks.get(200)
		assert.Equal(t, int64(100), task.GetCollectionId())
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, task.GetState())
	})
}

func TestCommitShardSplitRefusesAMalformedGenesisPosition(t *testing.T) {
	// meta.UpdateChannelCheckpoints filters these out, logs a warning and
	// returns nil -- so without the up-front check the target is left unseeded
	// while the callback reports success, which is exactly the state seeding
	// exists to prevent.
	t.Run("a nil message id on a non-WoodPecker WAL", func(t *testing.T) {
		svr := newShardSplitTestServer(t)
		req := splitTestCommitRequest()
		req.TargetStartPositions[0].MsgID = nil

		status, err := svr.CommitShardSplit(context.Background(), req)
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrServiceInternal)

		// Nothing was written: not the task, not the other target's checkpoint.
		persisted, err := svr.meta.catalog.ListSplitShardTask(context.Background())
		require.NoError(t, err)
		assert.Empty(t, persisted)
		_, ok := svr.shardSplitTasks.get(200)
		assert.False(t, ok)
		assert.Nil(t, svr.meta.GetChannelCheckpoint(splitTestTarget0))
		assert.Nil(t, svr.meta.GetChannelCheckpoint(splitTestTarget1))
	})

	t.Run("a position with no channel name", func(t *testing.T) {
		svr := newShardSplitTestServer(t)
		req := splitTestCommitRequest()
		req.TargetStartPositions[0].ChannelName = ""

		status, err := svr.CommitShardSplit(context.Background(), req)
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrServiceInternal)
		_, ok := svr.shardSplitTasks.get(200)
		assert.False(t, ok)
	})

	t.Run("a nil message id is fine on WoodPecker", func(t *testing.T) {
		// WoodPecker serializes a zero message id to nil, and
		// UpdateChannelCheckpoints accepts it, so refusing it would refuse a
		// legitimate genesis.
		svr := newShardSplitTestServer(t)
		req := splitTestCommitRequest()
		for _, position := range req.TargetStartPositions {
			position.MsgID = nil
			position.WALName = commonpb.WALName_WoodPecker
		}

		status, err := svr.CommitShardSplit(context.Background(), req)
		require.NoError(t, err)
		require.NoError(t, merr.Error(status))
		assert.Equal(t, uint64(3000), svr.meta.GetChannelCheckpoint(splitTestTarget0).GetTimestamp())
	})

	t.Run("a malformed position for a channel this split does not own is ignored", func(t *testing.T) {
		svr := newShardSplitTestServer(t)
		req := splitTestCommitRequest()
		req.TargetStartPositions = append(req.TargetStartPositions,
			&msgpb.MsgPosition{ChannelName: "by-dev-rootcoord-dml_8_1v8", Timestamp: 3000})

		status, err := svr.CommitShardSplit(context.Background(), req)
		require.NoError(t, err)
		require.NoError(t, merr.Error(status))
	})
}

func TestCommitShardSplitRefusesASourceTheRecordedTaskDoesNotFence(t *testing.T) {
	// A redelivery names the same source. A different one under the same task
	// id is two splits sharing an id -- appending it would make one task wait on
	// two sources, which a shard split never has -- so it is refused and the
	// record is left as it was.
	svr := newShardSplitTestServer(t)
	require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
		TaskId:       200,
		CollectionId: 100,
		State:        datapb.SplitShardTaskState_SplitShardTaskFencing,
		Sources:      []*datapb.SplitShardTaskSource{{Vchannel: "by-dev-rootcoord-dml_9_1v9"}},
	}))

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(status), merr.ErrServiceInternal)
	assert.ErrorContains(t, merr.Error(status), "recorded task fences")

	task, _ := svr.shardSplitTasks.get(200)
	assert.Equal(t, []string{"by-dev-rootcoord-dml_9_1v9"}, splitSourceVChannels(task))
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, task.GetState())
}

func TestCommitShardSplitRejectsAnUnhealthyServer(t *testing.T) {
	svr := newShardSplitTestServer(t)
	svr.UpdateStateCode(commonpb.StateCode_Abnormal)

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	assert.Error(t, merr.Error(status))
}

// drainedTestServer records a split whose single source is fenced at 2000.
func drainedTestServer(t *testing.T) *Server {
	svr := newShardSplitTestServer(t)
	require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
		TaskId:       200,
		CollectionId: 100,
		State:        datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Fenced:       true,
		Sources:      []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource, SwitchTimeTick: 2000}},
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: splitTestTarget0, Buckets: []uint64{0}},
			{Vchannel: splitTestTarget1, Buckets: []uint64{1}},
		},
	}))
	return svr
}

func putSourceSegment(svr *Server, state commonpb.SegmentState) {
	svr.meta.segments.SetSegment(9001, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            9001,
		CollectionID:  100,
		InsertChannel: splitTestSource,
		State:         state,
	}})
}

func idleImportMeta(t *testing.T) ImportMeta {
	im := NewMockImportMeta(t)
	im.EXPECT().GetJobBy(mock.Anything, mock.Anything).Return(nil).Maybe()
	return im
}

func activeImportMeta(t *testing.T, vchannel string) ImportMeta {
	im := NewMockImportMeta(t)
	im.EXPECT().GetJobBy(mock.Anything, mock.Anything).Return([]ImportJob{
		&importJob{ImportJob: &datapb.ImportJob{
			JobID:     7,
			State:     internalpb.ImportJobState_Pending,
			Vchannels: []string{vchannel},
		}},
	}).Maybe()
	return im
}

func TestCheckShardSplitDrainedThreeConjuncts(t *testing.T) {
	drainedCheckpoint := []*msgpb.MsgPosition{splitTestPosition(splitTestSource, 2500)}

	t.Run("a live segment on the source holds the drain open", func(t *testing.T) {
		// Data the targets have not taken yet: dropping the source now would
		// lose it.
		svr := drainedTestServer(t)
		svr.importMeta = idleImportMeta(t)
		require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(), drainedCheckpoint))
		putSourceSegment(svr, commonpb.SegmentState_Flushed)

		resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
			CollectionId: 100, SplitTaskId: 200,
		})
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		assert.False(t, resp.GetDrained())
	})

	t.Run("a checkpoint below T_switch holds the drain open", func(t *testing.T) {
		// The fence only appended a message; the flusher seals and reports the
		// sealed segments afterwards. Below T_switch those segments may not
		// have been reported yet, so an empty segment scan proves nothing.
		svr := drainedTestServer(t)
		svr.importMeta = idleImportMeta(t)
		require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(),
			[]*msgpb.MsgPosition{splitTestPosition(splitTestSource, 1999)}))

		resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
			CollectionId: 100, SplitTaskId: 200,
		})
		require.NoError(t, err)
		assert.False(t, resp.GetDrained())
	})

	t.Run("a missing checkpoint holds the drain open", func(t *testing.T) {
		svr := drainedTestServer(t)
		svr.importMeta = idleImportMeta(t)

		resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
			CollectionId: 100, SplitTaskId: 200,
		})
		require.NoError(t, err)
		assert.False(t, resp.GetDrained())
	})

	t.Run("an active import on the source holds the drain open", func(t *testing.T) {
		// A job still in Pending has registered no segment in meta, so the
		// segment scan cannot see it, and it would otherwise allocate onto the
		// just-retired shard after this check passed.
		svr := drainedTestServer(t)
		svr.importMeta = activeImportMeta(t, splitTestSource)
		require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(), drainedCheckpoint))

		resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
			CollectionId: 100, SplitTaskId: 200,
		})
		require.NoError(t, err)
		assert.False(t, resp.GetDrained())
	})

	t.Run("a source whose fence was never recorded is never drained", func(t *testing.T) {
		// T_switch zero means the fence is not on record, so the source may
		// still be accepting writes. Comparing a checkpoint against zero passes
		// vacuously and would collapse the predicate to the empty scan the
		// conjunct exists to close.
		svr := newShardSplitTestServer(t)
		require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
			TaskId:       200,
			CollectionId: 100,
			State:        datapb.SplitShardTaskState_SplitShardTaskFencing,
			Sources:      []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource}},
		}))
		svr.importMeta = idleImportMeta(t)
		// Everything else is clear: a checkpoint exists, no segment, no import.
		require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(), drainedCheckpoint))

		resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
			CollectionId: 100, SplitTaskId: 200,
		})
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		assert.False(t, resp.GetDrained())
	})

	t.Run("all three clear reports drained", func(t *testing.T) {
		svr := drainedTestServer(t)
		svr.importMeta = idleImportMeta(t)
		require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(), drainedCheckpoint))
		// A dropped segment is not data a reader can still be routed to.
		putSourceSegment(svr, commonpb.SegmentState_Dropped)

		resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
			CollectionId: 100, SplitTaskId: 200,
		})
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		assert.True(t, resp.GetDrained())
	})

	t.Run("an import on an unrelated vchannel does not hold the drain open", func(t *testing.T) {
		svr := drainedTestServer(t)
		svr.importMeta = activeImportMeta(t, "some-other-vchannel")
		require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(), drainedCheckpoint))

		resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
			CollectionId: 100, SplitTaskId: 200,
		})
		require.NoError(t, err)
		assert.True(t, resp.GetDrained())
	})

	t.Run("no import meta wired means no import conjunct", func(t *testing.T) {
		svr := drainedTestServer(t)
		require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(), drainedCheckpoint))

		resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
			CollectionId: 100, SplitTaskId: 200,
		})
		require.NoError(t, err)
		assert.True(t, resp.GetDrained())
	})
}

func TestCheckShardSplitDrainedUnknownTaskIsNotRecorded(t *testing.T) {
	// "Not drained" would be a lie and "drained" would be catastrophic, so a
	// task datacoord has no record of is neither: it is answered recorded=false,
	// which tells the caller the SplitShard callback has not run here yet.
	svr := newShardSplitTestServer(t)

	resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
		CollectionId: 100, SplitTaskId: 404,
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	assert.False(t, resp.GetRecorded())
	assert.False(t, resp.GetDrained())
	assert.Empty(t, resp.GetSourceVchannels())
	assert.Empty(t, resp.GetTargetVchannels())

	// A task only planned here (the primary's planner writes it before the
	// broadcast) is not recorded either: CommitShardSplit has not fenced it.
	svr.shardSplitTasks.cache(&datapb.SplitShardTask{
		TaskId: 405, CollectionId: 100, State: datapb.SplitShardTaskState_SplitShardTaskPreparing,
		Sources: []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource}},
	})
	resp, err = svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
		CollectionId: 100, SplitTaskId: 405,
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	assert.False(t, resp.GetRecorded())
	assert.Empty(t, resp.GetSourceVchannels())
}

// TestCheckShardSplitDrainedDescribesTheTask: the response names the shards the
// recorded task splits, which is what the adoption callback may retire and
// adopt; a task recorded against another collection is a System error, never
// an answer about this one.
func TestCheckShardSplitDrainedDescribesTheTask(t *testing.T) {
	svr := drainedTestServer(t)
	svr.importMeta = idleImportMeta(t)

	resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
		CollectionId: 100, SplitTaskId: 200,
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	assert.True(t, resp.GetRecorded())
	assert.Equal(t, []string{splitTestSource}, resp.GetSourceVchannels())
	assert.Equal(t, []string{splitTestTarget0, splitTestTarget1}, resp.GetTargetVchannels())

	resp, err = svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
		CollectionId: 101, SplitTaskId: 200,
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceInternal)
	assert.False(t, resp.GetRecorded())
	assert.False(t, resp.GetDrained())
}

func TestCheckShardSplitDrainedRejectsAnUnhealthyServer(t *testing.T) {
	svr := drainedTestServer(t)
	svr.UpdateStateCode(commonpb.StateCode_Abnormal)

	resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
		CollectionId: 100, SplitTaskId: 200,
	})
	require.NoError(t, err)
	assert.Error(t, merr.Error(resp.GetStatus()))
}

// splitRewriteTestSegment is one segment of a split task's source as a
// compaction-like rewrite leaves it: an input is on the source vchannel, a
// rewrite output is on a target vchannel with CompactionFrom naming its input,
// and a relabeled segment is the same id moved to a target by InsertChannel.
type splitRewriteTestSegment struct {
	id             int64
	vchannel       string
	state          commonpb.SegmentState
	compactionFrom []int64
}

// TestCheckShardSplitDrainedAfterACompactionLikeRewrite pins the contract the
// rewrite path relies on: the drain predicate is unchanged, and a rewrite that
// commits like a compaction --- its inputs Dropped on the source, its outputs
// Flushed on the targets with CompactionFrom lineage, in one commit --- drains
// the source exactly when nothing non-Dropped is left on it. The outputs on the
// targets neither help nor hinder: the predicate only reads the source.
func TestCheckShardSplitDrainedAfterACompactionLikeRewrite(t *testing.T) {
	cases := []struct {
		name             string
		segments         []splitRewriteTestSegment
		sourceCheckpoint uint64
		drained          bool
	}{
		{
			name: "every source segment rewritten drains",
			segments: []splitRewriteTestSegment{
				{id: 9001, vchannel: splitTestSource, state: commonpb.SegmentState_Dropped},
				{id: 9002, vchannel: splitTestSource, state: commonpb.SegmentState_Dropped},
				{id: 9101, vchannel: splitTestTarget0, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
				{id: 9102, vchannel: splitTestTarget1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
				{id: 9103, vchannel: splitTestTarget0, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9002}},
				{id: 9104, vchannel: splitTestTarget1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9002}},
			},
			sourceCheckpoint: 2000,
			drained:          true,
		},
		{
			name: "every source segment rewritten but the checkpoint below T_switch does not drain",
			segments: []splitRewriteTestSegment{
				{id: 9001, vchannel: splitTestSource, state: commonpb.SegmentState_Dropped},
				{id: 9101, vchannel: splitTestTarget0, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
				{id: 9102, vchannel: splitTestTarget1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
			},
			sourceCheckpoint: 1999,
			drained:          false,
		},
		{
			name: "a partial rewrite leaves a flushed source segment and does not drain",
			segments: []splitRewriteTestSegment{
				{id: 9001, vchannel: splitTestSource, state: commonpb.SegmentState_Dropped},
				{id: 9101, vchannel: splitTestTarget0, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
				{id: 9102, vchannel: splitTestTarget1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
				{id: 9002, vchannel: splitTestSource, state: commonpb.SegmentState_Flushed},
			},
			sourceCheckpoint: 2500,
			drained:          false,
		},
		{
			name: "outputs on the targets with their input not yet dropped do not drain",
			// A commit that added the outputs without dropping the input is not
			// a compaction-like commit; the input is still data a reader can be
			// routed to on the source.
			segments: []splitRewriteTestSegment{
				{id: 9001, vchannel: splitTestSource, state: commonpb.SegmentState_Flushed},
				{id: 9101, vchannel: splitTestTarget0, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
				{id: 9102, vchannel: splitTestTarget1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
			},
			sourceCheckpoint: 2500,
			drained:          false,
		},
		{
			name: "relabel and rewrite mixed drains once nothing non-dropped is on the source",
			segments: []splitRewriteTestSegment{
				// Relabeled: the same segment id now carried by a target.
				{id: 9003, vchannel: splitTestTarget0, state: commonpb.SegmentState_Flushed},
				{id: 9004, vchannel: splitTestTarget1, state: commonpb.SegmentState_Flushed},
				// Rewritten.
				{id: 9001, vchannel: splitTestSource, state: commonpb.SegmentState_Dropped},
				{id: 9101, vchannel: splitTestTarget0, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
				{id: 9102, vchannel: splitTestTarget1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
			},
			sourceCheckpoint: 2500,
			drained:          true,
		},
		{
			name: "relabel and rewrite mixed with a segment not yet relabeled does not drain",
			segments: []splitRewriteTestSegment{
				{id: 9003, vchannel: splitTestTarget0, state: commonpb.SegmentState_Flushed},
				{id: 9004, vchannel: splitTestSource, state: commonpb.SegmentState_Flushed},
				{id: 9001, vchannel: splitTestSource, state: commonpb.SegmentState_Dropped},
				{id: 9101, vchannel: splitTestTarget0, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
				{id: 9102, vchannel: splitTestTarget1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
			},
			sourceCheckpoint: 2500,
			drained:          false,
		},
		{
			name: "relabel and rewrite mixed with a rewrite input not yet dropped does not drain",
			segments: []splitRewriteTestSegment{
				{id: 9003, vchannel: splitTestTarget0, state: commonpb.SegmentState_Flushed},
				{id: 9004, vchannel: splitTestTarget1, state: commonpb.SegmentState_Flushed},
				{id: 9001, vchannel: splitTestSource, state: commonpb.SegmentState_Flushed},
				{id: 9101, vchannel: splitTestTarget0, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
				{id: 9102, vchannel: splitTestTarget1, state: commonpb.SegmentState_Flushed, compactionFrom: []int64{9001}},
			},
			sourceCheckpoint: 2500,
			drained:          false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svr := drainedTestServer(t)
			svr.importMeta = idleImportMeta(t)
			require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(),
				[]*msgpb.MsgPosition{splitTestPosition(splitTestSource, tc.sourceCheckpoint)}))
			for _, seg := range tc.segments {
				svr.meta.segments.SetSegment(seg.id, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
					ID:             seg.id,
					CollectionID:   100,
					InsertChannel:  seg.vchannel,
					State:          seg.state,
					CompactionFrom: seg.compactionFrom,
				}})
			}

			resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
				CollectionId: 100, SplitTaskId: 200,
			})
			require.NoError(t, err)
			require.NoError(t, merr.Error(resp.GetStatus()))
			assert.Equal(t, tc.drained, resp.GetDrained())
		})
	}
}

// flushStateSplitTestServer records task 200 fencing splitTestSource at
// switchTimeTick and gives the source the checkpoint sourceCheckpoint.
func flushStateSplitTestServer(t *testing.T, switchTimeTick, sourceCheckpoint uint64) *Server {
	svr := newShardSplitTestServer(t)
	require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
		TaskId:       200,
		CollectionId: 100,
		State:        datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Fenced:       true,
		Sources:      []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource, SwitchTimeTick: switchTimeTick}},
	}))
	require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(),
		[]*msgpb.MsgPosition{splitTestPosition(splitTestSource, sourceCheckpoint)}))
	return svr
}

func TestGetFlushStateCountsADrainedSplitSourceAsFlushed(t *testing.T) {
	const flushTs = uint64(5000)
	mockChannels := mockey.Mock((*Server).getChannelsByCollectionID).To(
		func(_ *Server, _ context.Context, collectionID int64) ([]RWChannel, error) {
			return []RWChannel{
				&channelMeta{Name: splitTestSource, CollectionID: collectionID},
				&channelMeta{Name: splitTestTarget0, CollectionID: collectionID},
			}, nil
		}).Build()
	defer mockChannels.UnPatch()

	getFlushState := func(t *testing.T, svr *Server) bool {
		resp, err := svr.GetFlushState(context.Background(), &datapb.GetFlushStateRequest{CollectionID: 100, FlushTs: flushTs})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		return resp.GetFlushed()
	}
	// The target keeps consuming, so its checkpoint passes the flush ts.
	advanceTarget := func(t *testing.T, svr *Server) {
		require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(),
			[]*msgpb.MsgPosition{splitTestPosition(splitTestTarget0, flushTs+1)}))
	}

	t.Run("a source checkpoint at or past T_switch counts even below the flush ts", func(t *testing.T) {
		svr := flushStateSplitTestServer(t, 2000, 2000)
		advanceTarget(t, svr)
		assert.True(t, getFlushState(t, svr))
	})

	t.Run("a source checkpoint below T_switch is not flushed", func(t *testing.T) {
		svr := flushStateSplitTestServer(t, 2000, 1999)
		advanceTarget(t, svr)
		assert.False(t, getFlushState(t, svr))
	})

	t.Run("a zero T_switch never counts", func(t *testing.T) {
		svr := flushStateSplitTestServer(t, 0, 2500)
		advanceTarget(t, svr)
		assert.False(t, getFlushState(t, svr))
	})

	t.Run("the rule does not stand in for the other vchannels", func(t *testing.T) {
		// The target is not a split source: it still has to reach the flush ts.
		svr := flushStateSplitTestServer(t, 2000, 2500)
		require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(),
			[]*msgpb.MsgPosition{splitTestPosition(splitTestTarget0, 2500)}))
		assert.False(t, getFlushState(t, svr))
	})
}

func TestChannelCheckpointCovers(t *testing.T) {
	t.Run("a vchannel in no split task keeps the plain comparison", func(t *testing.T) {
		svr := flushStateSplitTestServer(t, 2000, 2500)
		const other = "by-dev-rootcoord-dml_3_1v3"
		assert.False(t, svr.channelCheckpointCovers(other, nil, 1000))
		assert.False(t, svr.channelCheckpointCovers(other, splitTestPosition(other, 2500), 5000))
		assert.True(t, svr.channelCheckpointCovers(other, splitTestPosition(other, 5000), 5000))
	})

	t.Run("a source with no checkpoint is not flushed", func(t *testing.T) {
		svr := flushStateSplitTestServer(t, 2000, 2500)
		assert.False(t, svr.channelCheckpointCovers(splitTestSource, nil, 5000))
	})

	t.Run("a source whose checkpoint reached the flush ts needs no split record", func(t *testing.T) {
		svr := flushStateSplitTestServer(t, 0, 2500)
		assert.True(t, svr.channelCheckpointCovers(splitTestSource, splitTestPosition(splitTestSource, 2500), 2500))
	})

	t.Run("a server without a split task store keeps the plain comparison", func(t *testing.T) {
		svr := &Server{}
		assert.False(t, svr.channelCheckpointCovers(splitTestSource, splitTestPosition(splitTestSource, 2500), 5000))
		assert.True(t, svr.channelCheckpointCovers(splitTestSource, splitTestPosition(splitTestSource, 5000), 5000))
	})
}

func TestVerifyFlushAllStateCountsADrainedSplitSourceAsFlushed(t *testing.T) {
	pchannel := funcutil.ToPhysicalChannel(splitTestSource)
	flushAllTss := map[string]uint64{pchannel: 5000}

	t.Run("per-pchannel flush all ts", func(t *testing.T) {
		ok, err := flushStateSplitTestServer(t, 2000, 2000).
			verifyFlushAllStateByChannelFlushAllTs(context.Background(), splitTestSource, flushAllTss)
		require.NoError(t, err)
		assert.True(t, ok)

		ok, err = flushStateSplitTestServer(t, 2000, 1999).
			verifyFlushAllStateByChannelFlushAllTs(context.Background(), splitTestSource, flushAllTss)
		require.NoError(t, err)
		assert.False(t, ok)

		ok, err = flushStateSplitTestServer(t, 0, 2500).
			verifyFlushAllStateByChannelFlushAllTs(context.Background(), splitTestSource, flushAllTss)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("legacy flush all ts", func(t *testing.T) {
		assert.True(t, flushStateSplitTestServer(t, 2000, 2000).
			verifyFlushAllStateByLegacyFlushAllTs(context.Background(), splitTestSource, 5000))
		assert.False(t, flushStateSplitTestServer(t, 2000, 1999).
			verifyFlushAllStateByLegacyFlushAllTs(context.Background(), splitTestSource, 5000))
		assert.False(t, flushStateSplitTestServer(t, 0, 2500).
			verifyFlushAllStateByLegacyFlushAllTs(context.Background(), splitTestSource, 5000))
	})
}

// High-2: a target gets the channel-added mark a created collection's vchannels
// get from WatchChannels, and gets it before its checkpoint is seeded. The
// garbage collector's ChannelExists guard reads that mark; without it a
// compacted-away segment on the target loses its meta as soon as dropTolerance
// passes, and a recovery replaying from the still-behind checkpoint re-inserts
// its rows next to the compacted output.
func TestCommitShardSplitMarksTargetsAddedBeforeSeeding(t *testing.T) {
	svr := newShardSplitTestServer(t)
	ctx := context.Background()

	var seedOrigin func(*meta, context.Context, []*msgpb.MsgPosition) error
	seed := mockey.Mock((*meta).UpdateChannelCheckpoints).Origin(&seedOrigin).
		To(func(m *meta, ctx context.Context, positions []*msgpb.MsgPosition) error {
			for _, position := range positions {
				assert.True(t, svr.meta.catalog.ChannelExists(ctx, position.GetChannelName()),
					"target %s must be marked added before its checkpoint is seeded", position.GetChannelName())
			}
			return seedOrigin(m, ctx, positions)
		}).Build()
	defer seed.UnPatch()

	status, err := svr.CommitShardSplit(ctx, splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))
	assert.Equal(t, 1, seed.MockTimes())

	assert.True(t, svr.meta.catalog.ChannelExists(ctx, splitTestTarget0))
	assert.True(t, svr.meta.catalog.ChannelExists(ctx, splitTestTarget1))
	// The source was marked when its collection was created; the commit is
	// not what marks it.
	assert.False(t, svr.meta.catalog.ChannelExists(ctx, splitTestSource))
	assert.NotNil(t, svr.meta.GetChannelCheckpoint(splitTestTarget0))
	assert.NotNil(t, svr.meta.GetChannelCheckpoint(splitTestTarget1))
}

func TestCommitShardSplitRedeliveryKeepsTheTargetMarks(t *testing.T) {
	svr := newShardSplitTestServer(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		status, err := svr.CommitShardSplit(ctx, splitTestCommitRequest())
		require.NoError(t, err)
		require.NoError(t, merr.Error(status))
		assert.True(t, svr.meta.catalog.ChannelExists(ctx, splitTestTarget0))
		assert.True(t, svr.meta.catalog.ChannelExists(ctx, splitTestTarget1))
	}
	persisted, err := svr.meta.catalog.ListSplitShardTask(ctx)
	require.NoError(t, err)
	assert.Len(t, persisted, 1)
}

func TestCommitShardSplitMarkFailureIsSystemErrorAndSeedsNothing(t *testing.T) {
	svr := newShardSplitTestServer(t)
	mark := mockey.Mock(mockey.GetMethod(svr.meta.catalog, "MarkChannelAdded")).
		Return(errors.New("etcd down")).Build()
	defer mark.UnPatch()

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(status), merr.ErrServiceInternal)
	// The mark goes first, so a failed mark leaves the target unseeded and the
	// retry seeds it once the mark lands.
	assert.Nil(t, svr.meta.GetChannelCheckpoint(splitTestTarget0))
	assert.Nil(t, svr.meta.GetChannelCheckpoint(splitTestTarget1))
}

// A redelivery that arrives after the target's data has been dropped (its
// collection dropped, DropVirtualChannel ran) must not overwrite the removal
// tombstone with the added mark: that would revive the GC guard for a channel
// whose checkpoint is gone and pin its Dropped segments in meta forever.
func TestCommitShardSplitDoesNotReviveADroppedTarget(t *testing.T) {
	svr := newShardSplitTestServer(t)
	ctx := context.Background()
	require.NoError(t, svr.meta.UpdateDropChannelSegmentInfo(ctx, splitTestTarget0, nil))
	require.True(t, svr.meta.catalog.ShouldDropChannel(ctx, splitTestTarget0))

	status, err := svr.CommitShardSplit(ctx, splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	assert.True(t, svr.meta.catalog.ShouldDropChannel(ctx, splitTestTarget0))
	assert.False(t, svr.meta.catalog.ChannelExists(ctx, splitTestTarget0))
	assert.True(t, svr.meta.catalog.ChannelExists(ctx, splitTestTarget1))
}

// The guard itself, on a real catalog: a compacted-away segment on a committed
// target whose dml position is past the target's checkpoint stays in meta, as
// it would on any created vchannel; on an unmarked channel it would not.
func TestCommitShardSplitTargetIsProtectedByTheDroppedSegmentGCGuard(t *testing.T) {
	svr := newShardSplitTestServer(t)
	ctx := context.Background()
	gc := newGarbageCollector(svr.meta, newMockHandler(), GcOption{dropTolerance: 0})
	compactedAway := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            300,
		CollectionID:  100,
		InsertChannel: splitTestTarget0,
		State:         commonpb.SegmentState_Dropped,
		Compacted:     true,
		DroppedAt:     0,
		DmlPosition:   splitTestPosition(splitTestTarget0, 5000),
	}}
	// Before the commit the target is an unmarked channel: the guard does not
	// apply and the segment's meta would go at once.
	assert.True(t, gc.checkDroppedSegmentGC(compactedAway, nil, typeutil.NewUniqueSet(), 3000))

	status, err := svr.CommitShardSplit(ctx, splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	// Checkpoint 3000 < dml position 5000: a reader may still be behind the
	// segment, so its meta must stay until the checkpoint passes it.
	assert.False(t, gc.checkDroppedSegmentGC(compactedAway, nil, typeutil.NewUniqueSet(), 3000))
	assert.True(t, gc.checkDroppedSegmentGC(compactedAway, nil, typeutil.NewUniqueSet(), 5000))
}

// Medium-7: CommitShardSplit is a read-modify-write over the task store, and
// the merge keeps fields the record already carries (a non-zero routing
// modulus, targets). Two concurrent commits of the same task id must
// serialize, or the second's read predates the first's write and its upsert
// drops what the first merged in. The catalog save is held until both
// callers have entered it, which forces exactly that interleaving on an
// unlocked implementation.
func TestCommitShardSplitConcurrentCommitsOfOneTaskKeepBothWritersFields(t *testing.T) {
	svr := newShardSplitTestServer(t)
	ctx := context.Background()

	var (
		saveOrigin func(*datacoord.Catalog, context.Context, *datapb.SplitShardTask) error
		mu         sync.Mutex
		saves      int
	)
	save := mockey.Mock((*datacoord.Catalog).SaveSplitShardTask).Origin(&saveOrigin).
		To(func(c *datacoord.Catalog, ctx context.Context, task *datapb.SplitShardTask) error {
			// Give the other caller time to perform its read before this
			// write lands, so an unlocked implementation reads a stale record.
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			saves++
			mu.Unlock()
			return saveOrigin(c, ctx, task)
		}).Build()
	defer save.UnPatch()

	withModulus := splitTestCommitRequest()
	withModulus.RoutingModulus = 2
	withoutModulus := splitTestCommitRequest()
	withoutModulus.RoutingModulus = 0

	var wg sync.WaitGroup
	for _, req := range []*datapb.CommitShardSplitRequest{withModulus, withoutModulus} {
		wg.Add(1)
		go func(req *datapb.CommitShardSplitRequest) {
			defer wg.Done()
			status, err := svr.CommitShardSplit(ctx, req)
			assert.NoError(t, err)
			assert.NoError(t, merr.Error(status))
		}(req)
	}
	wg.Wait()

	task, ok := svr.shardSplitTasks.get(200)
	require.True(t, ok)
	assert.Equal(t, uint64(2), task.GetRoutingModulus(), "the modulus one writer carried must survive the other's upsert")
	assert.True(t, task.GetFenced())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, task.GetState())
	assert.Equal(t, uint64(2000), task.GetSources()[0].GetSwitchTimeTick())
	persisted, err := svr.meta.catalog.ListSplitShardTask(ctx)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	assert.Equal(t, uint64(2), persisted[0].GetRoutingModulus())
	assert.Equal(t, 2, saves)
}

// TestDropSegmentsByTimeOnAFencedSplitSource: a TruncateCollection issued inside
// the split window reaches the fenced source at a tick past T_switch. The
// source's data sync service is closed at the fence, so its checkpoint never
// reaches that tick; the wait is answered by channelCheckpointCovers, as the
// flush state is.
func TestDropSegmentsByTimeOnAFencedSplitSource(t *testing.T) {
	const truncateTs = uint64(5000)
	addSourceSegment := func(t *testing.T, svr *Server, id int64, dmlTs uint64) {
		require.NoError(t, svr.meta.AddSegment(context.Background(), &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            id,
			CollectionID:  100,
			InsertChannel: splitTestSource,
			State:         commonpb.SegmentState_Flushed,
			DmlPosition:   &msgpb.MsgPosition{Timestamp: dmlTs},
		}}))
	}
	dropByTime := func(svr *Server, vchannel string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		return svr.DropSegmentsByTime(ctx, 100, map[string]uint64{vchannel: truncateTs})
	}

	t.Run("a checkpoint frozen at T_switch lets the truncate proceed and drops every source segment", func(t *testing.T) {
		svr := flushStateSplitTestServer(t, 2000, 2000)
		addSourceSegment(t, svr, 1, 1500)
		addSourceSegment(t, svr, 2, 2000)
		require.NoError(t, dropByTime(svr, splitTestSource))
		for _, id := range []int64{1, 2} {
			assert.Equal(t, commonpb.SegmentState_Dropped, svr.meta.GetSegment(context.Background(), id).GetState())
		}
	})

	t.Run("a checkpoint below T_switch still waits", func(t *testing.T) {
		svr := flushStateSplitTestServer(t, 2000, 1999)
		addSourceSegment(t, svr, 1, 1500)
		require.ErrorIs(t, dropByTime(svr, splitTestSource), context.DeadlineExceeded)
		assert.Equal(t, commonpb.SegmentState_Flushed, svr.meta.GetSegment(context.Background(), 1).GetState())
	})

	t.Run("a T_switch of zero still waits", func(t *testing.T) {
		svr := flushStateSplitTestServer(t, 0, 2500)
		require.ErrorIs(t, dropByTime(svr, splitTestSource), context.DeadlineExceeded)
	})

	t.Run("a vchannel in no split task is unchanged", func(t *testing.T) {
		svr := flushStateSplitTestServer(t, 2000, 2000)
		const other = "by-dev-rootcoord-dml_3_100v3"
		require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(), []*msgpb.MsgPosition{splitTestPosition(other, 2500)}))
		require.ErrorIs(t, dropByTime(svr, other), context.DeadlineExceeded)
		require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(), []*msgpb.MsgPosition{splitTestPosition(other, truncateTs)}))
		require.NoError(t, dropByTime(svr, other))
	})

	t.Run("a wait that started before T_switch was recorded is woken by the commit's notification", func(t *testing.T) {
		svr := flushStateSplitTestServer(t, 0, 2000)
		done := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			done <- svr.DropSegmentsByTime(ctx, 100, map[string]uint64{splitTestSource: truncateTs})
		}()
		select {
		case err := <-done:
			t.Fatalf("the truncate must wait while no T_switch is recorded, got %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
			TaskId:       200,
			CollectionId: 100,
			State:        datapb.SplitShardTaskState_SplitShardTaskRedistributing,
			Fenced:       true,
			Sources:      []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource, SwitchTimeTick: 2000}},
		}))
		svr.meta.NotifyChannelCheckpointWatchers()
		require.NoError(t, <-done)
	})
}

// TestSplitSourceFenceRecorded: the append gate of a secondary asks it about a
// split whose broadcast task was already collected. Only a recorded T_switch --
// the fence landed in this cluster's WAL -- counts.
func TestSplitSourceFenceRecorded(t *testing.T) {
	ctx := context.Background()

	recorded, err := (&Server{}).splitSourceFenceRecorded(ctx, splitTestSource)
	require.NoError(t, err)
	assert.False(t, recorded, "a server without a task store records nothing")

	svr := flushStateSplitTestServer(t, 0, 1000)
	recorded, err = svr.splitSourceFenceRecorded(ctx, splitTestSource)
	require.NoError(t, err)
	assert.False(t, recorded, "a task whose fence is not on record does not count")

	svr = flushStateSplitTestServer(t, 2000, 1000)
	recorded, err = svr.splitSourceFenceRecorded(ctx, splitTestSource)
	require.NoError(t, err)
	assert.True(t, recorded)
	recorded, err = svr.splitSourceFenceRecorded(ctx, splitTestTarget0)
	require.NoError(t, err)
	assert.False(t, recorded, "a target is not a fenced source")

	// It is what the broadcaster asks once DataCoord has registered it.
	registry.ResetRegistration()
	defer registry.ResetRegistration()
	registry.RegisterAppendFirstReplicaRecordedChecker(svr.splitSourceFenceRecorded)
	recorded, err = registry.IsAppendFirstReplicaRecorded(ctx, splitTestSource)
	require.NoError(t, err)
	assert.True(t, recorded)
}
