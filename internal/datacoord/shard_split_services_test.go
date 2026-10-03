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
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
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
				assert.True(t, channelExists(t, svr.meta.catalog, position.GetChannelName()),
					"target %s must be marked added before its checkpoint is seeded", position.GetChannelName())
			}
			return seedOrigin(m, ctx, positions)
		}).Build()
	defer seed.UnPatch()

	status, err := svr.CommitShardSplit(ctx, splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))
	assert.Equal(t, 1, seed.MockTimes())

	assert.True(t, channelExists(t, svr.meta.catalog, splitTestTarget0))
	assert.True(t, channelExists(t, svr.meta.catalog, splitTestTarget1))
	// The source was marked when its collection was created; the commit is
	// not what marks it.
	assert.False(t, channelExists(t, svr.meta.catalog, splitTestSource))
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
		assert.True(t, channelExists(t, svr.meta.catalog, splitTestTarget0))
		assert.True(t, channelExists(t, svr.meta.catalog, splitTestTarget1))
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
	assert.False(t, channelExists(t, svr.meta.catalog, splitTestTarget0))
	assert.True(t, channelExists(t, svr.meta.catalog, splitTestTarget1))
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

// splitFenceTestServer records task 200 fencing splitTestSource at
// switchTimeTick. Zero means the fence is not on record yet.
func splitFenceTestServer(t *testing.T, switchTimeTick uint64) *Server {
	svr := newShardSplitTestServer(t)
	require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
		TaskId:       200,
		CollectionId: 100,
		State:        datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Fenced:       true,
		Sources:      []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource, SwitchTimeTick: switchTimeTick}},
	}))
	return svr
}

// TestSplitSourceFenceRecorded: the append gate of a secondary asks it about a
// split whose broadcast task was already collected. Only a recorded T_switch --
// the fence landed in this cluster's WAL -- counts.
func TestSplitSourceFenceRecorded(t *testing.T) {
	ctx := context.Background()

	recorded, err := (&Server{}).splitSourceFenceRecorded(ctx, splitTestSource)
	require.NoError(t, err)
	assert.False(t, recorded, "a server without a task store records nothing")

	svr := splitFenceTestServer(t, 0)
	recorded, err = svr.splitSourceFenceRecorded(ctx, splitTestSource)
	require.NoError(t, err)
	assert.False(t, recorded, "a task whose fence is not on record does not count")

	svr = splitFenceTestServer(t, 2000)
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

// A redelivered CommitShardSplit carries the source exactly as the SplitShard
// broadcast named it: a vchannel and its tick, nothing else. Whatever else the
// recorded source holds -- fields another writer of the record owns, or fields
// a newer build wrote that this one does not even know -- must survive the
// merge, or every redelivery silently erases them.
func TestCommitShardSplitRedeliveryKeepsTheRecordedSourceFields(t *testing.T) {
	svr := newShardSplitTestServer(t)
	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, merr.CheckRPCCall(status, err))

	// A field this build does not know, set on the recorded source the way a
	// newer writer would leave it (field 99, varint 7).
	recorded, ok := svr.shardSplitTasks.get(200)
	require.True(t, ok)
	withUnknown := proto.Clone(recorded).(*datapb.SplitShardTask)
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 7)
	withUnknown.GetSources()[0].ProtoReflect().SetUnknown(unknown)
	require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, withUnknown))

	// A redelivery that reports another tick does not move the recorded one:
	// T_switch is the tick of the task's FIRST fence (design doc §6.1 step 3),
	// and the drain may already have been judged against it.
	req := splitTestCommitRequest()
	req.Sources[0].SwitchTimeTick = 2500
	status, err = svr.CommitShardSplit(context.Background(), req)
	require.NoError(t, merr.CheckRPCCall(status, err))

	merged, ok := svr.shardSplitTasks.get(200)
	require.True(t, ok)
	require.Len(t, merged.GetSources(), 1)
	assert.Equal(t, splitTestSource, merged.GetSources()[0].GetVchannel())
	assert.Equal(t, uint64(2000), merged.GetSources()[0].GetSwitchTimeTick(), "the first recorded T_switch is kept")
	assert.Equal(t, unknown, []byte(merged.GetSources()[0].ProtoReflect().GetUnknown()),
		"the redelivery erased a field of the recorded source it does not carry")
}

// channelExists reads the catalog's channel mark, which master's GC guard now
// reports with a separate lookup error.
func channelExists(t *testing.T, catalog metastore.DataCoordCatalog, channel string) bool {
	t.Helper()
	exists, err := catalog.ChannelExists(context.TODO(), channel)
	require.NoError(t, err)
	return exists
}

func TestCommitShardSplitRollsAnAbortedTaskForward(t *testing.T) {
	// An abort is refused once a fence may be in the WAL, so an Aborted record
	// meeting its fence's commit is a bug. The fence has landed all the same:
	// the source is closed to writes, and only the split carries its rows on.
	// The task rolls forward into the window rather than staying Aborted.
	svr := newShardSplitTestServer(t)
	require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
		TaskId:       200,
		CollectionId: 100,
		State:        datapb.SplitShardTaskState_SplitShardTaskAborted,
		EndTime:      1234,
		FailReason:   "the write switch was refused",
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
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, task.GetState())
	assert.True(t, task.GetFenced())
	assert.Equal(t, uint64(2000), task.GetSources()[0].GetSwitchTimeTick())
	assert.Zero(t, task.GetEndTime())
	assert.Empty(t, task.GetFailReason())
	persisted, err := svr.meta.catalog.ListSplitShardTask(context.Background())
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, persisted[0].GetState())
}

// The drain predicate and the reason a stalled split logs are one function:
// each conjunct names itself, and the predicate holds exactly when no reason
// is left.
func TestSplitDrainBlockReasonNamesEachConjunct(t *testing.T) {
	ctx := context.Background()
	task := func(svr *Server) *datapb.SplitShardTask {
		task, ok := svr.shardSplitTasks.get(200)
		require.True(t, ok)
		return task
	}

	svr := drainedTestServer(t)
	svr.importMeta = activeImportMeta(t, splitTestSource)
	putSourceSegment(svr, commonpb.SegmentState_Flushed)
	assert.Contains(t, svr.splitDrainBlockReason(ctx, task(svr)), "still has a live segment 9001")
	assert.False(t, svr.splitSourcesDrained(ctx, task(svr)))

	putSourceSegment(svr, commonpb.SegmentState_Dropped)
	assert.Contains(t, svr.splitDrainBlockReason(ctx, task(svr)), "has no channel checkpoint yet")
	assert.Equal(t, svr.fenceFlushBlockReason(task(svr)), svr.splitDrainBlockReason(ctx, task(svr)))

	require.NoError(t, svr.meta.UpdateChannelCheckpoints(ctx, []*msgpb.MsgPosition{splitTestPosition(splitTestSource, 1999)}))
	assert.Contains(t, svr.splitDrainBlockReason(ctx, task(svr)), "checkpoint 1999 has not reached its switch time tick 2000")

	require.NoError(t, svr.meta.UpdateChannelCheckpoints(ctx, []*msgpb.MsgPosition{splitTestPosition(splitTestSource, 2000)}))
	assert.Empty(t, svr.fenceFlushBlockReason(task(svr)))
	assert.Equal(t, "an import is still in progress on a source", svr.splitDrainBlockReason(ctx, task(svr)))

	svr.importMeta = idleImportMeta(t)
	assert.Empty(t, svr.splitDrainBlockReason(ctx, task(svr)))
	assert.True(t, svr.splitSourcesDrained(ctx, task(svr)))

	unfenced := &datapb.SplitShardTask{Sources: []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource}}}
	assert.Contains(t, svr.fenceFlushBlockReason(unfenced), "fence not recorded yet")
	assert.False(t, svr.splitSourcesDrained(ctx, unfenced))
}

// Post-#53595 the checkpoint reported under a vchannel's name is the recovery
// checkpoint of its whole pchannel, so a neighbour vchannel of that pchannel
// can hold a split's drain back. The segment scan runs first, so when the
// checkpoint is the conjunct that blocks, the source itself has nothing left
// in meta -- the reason must therefore name the pchannel, which is the only
// place an operator can look next. There is no cross-check from datacoord: the
// min-growing-segment clamp in UpdateChannelCheckpoints applies to TEXT
// collections only, and a TEXT collection is refused a split outright.
func TestFenceFlushBlockReasonNamesThePChannelThatCanHoldItBack(t *testing.T) {
	ctx := context.Background()
	svr := drainedTestServer(t)
	svr.importMeta = idleImportMeta(t)
	putSourceSegment(svr, commonpb.SegmentState_Dropped)
	require.NoError(t, svr.meta.UpdateChannelCheckpoints(ctx, []*msgpb.MsgPosition{splitTestPosition(splitTestSource, 1500)}))

	task, ok := svr.shardSplitTasks.get(200)
	require.True(t, ok)
	reason := svr.splitDrainBlockReason(ctx, task)
	assert.Contains(t, reason, funcutil.ToPhysicalChannel(splitTestSource))
	assert.Contains(t, reason, "any vchannel of it can hold back")
	assert.Empty(t, svr.liveSegmentBlockReason(splitTestSource),
		"the segment scan must be the conjunct that passed, so the stall is not the source's own L1")
}

// A Growing segment of the source blocks the drain by name. Since #53595 a
// growing segment is published to datacoord with its binlogs before the Insert
// that filled it completes, so this conjunct also closes the window in which
// the source held L1 data datacoord could not see.
func TestLiveSegmentBlockReasonNamesAGrowingSourceSegment(t *testing.T) {
	ctx := context.Background()
	svr := drainedTestServer(t)
	svr.importMeta = idleImportMeta(t)
	require.NoError(t, svr.meta.UpdateChannelCheckpoints(ctx, []*msgpb.MsgPosition{splitTestPosition(splitTestSource, 9999)}))
	task, ok := svr.shardSplitTasks.get(200)
	require.True(t, ok)

	for _, state := range []commonpb.SegmentState{
		commonpb.SegmentState_Growing,
		commonpb.SegmentState_Sealed,
		commonpb.SegmentState_Flushed,
	} {
		putSourceSegment(svr, state)
		reason := svr.splitDrainBlockReason(ctx, task)
		assert.Contains(t, reason, "still has a live segment 9001", state.String())
		assert.Contains(t, reason, state.String())
		assert.False(t, svr.splitSourcesDrained(ctx, task), state.String())
	}

	// An unretired L0 of the source is the same conjunct: it is a non-Dropped
	// segment of the channel, so "the source's L0s are retired" needs no
	// separate check.
	svr.meta.segments.SetSegment(9002, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 9002, CollectionID: 100, InsertChannel: splitTestSource,
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0,
	}})
	putSourceSegment(svr, commonpb.SegmentState_Dropped)
	assert.Contains(t, svr.splitDrainBlockReason(ctx, task), "live segment 9002 (level L0")

	svr.meta.segments.SetSegment(9002, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 9002, CollectionID: 100, InsertChannel: splitTestSource,
		State: commonpb.SegmentState_Dropped, Level: datapb.SegmentLevel_L0,
	}})
	assert.True(t, svr.splitSourcesDrained(ctx, task))
}
