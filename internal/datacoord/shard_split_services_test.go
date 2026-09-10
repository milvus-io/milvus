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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
		Sources:      []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource, PendingSegments: []int64{7}}},
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
	// The task's own per-source work survives: only the fence tick is adopted.
	assert.Equal(t, []int64{7}, task.GetSources()[0].GetPendingSegments())
}

func TestCommitShardSplitAdoptsSourcesTheLocalTaskDoesNotKnow(t *testing.T) {
	// A rehash fences every shard of the collection; a coordinator whose record
	// predates the last planning round can be missing one. Dropping it would
	// retire that shard without ever waiting for its data.
	svr := newShardSplitTestServer(t)
	require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
		TaskId:       200,
		CollectionId: 100,
		State:        datapb.SplitShardTaskState_SplitShardTaskPreparing,
		Sources:      []*datapb.SplitShardTaskSource{{Vchannel: "by-dev-rootcoord-dml_9_1v9"}},
	}))

	status, err := svr.CommitShardSplit(context.Background(), splitTestCommitRequest())
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	task, _ := svr.shardSplitTasks.get(200)
	assert.ElementsMatch(t, []string{"by-dev-rootcoord-dml_9_1v9", splitTestSource}, splitSourceVChannels(task))
	// The targets and the modulus a task learned of late come from the request too.
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

func TestCommitShardSplitDoesNotDoubleAppendADuplicateSource(t *testing.T) {
	// A request that names one vchannel twice must not grow two entries for it:
	// the drain would then wait on it twice and the two copies would drift
	// apart on the next merge.
	svr := newShardSplitTestServer(t)
	require.NoError(t, svr.shardSplitTasks.upsert(context.Background(), svr.meta.catalog, &datapb.SplitShardTask{
		TaskId:       200,
		CollectionId: 100,
		State:        datapb.SplitShardTaskState_SplitShardTaskFencing,
		Sources:      []*datapb.SplitShardTaskSource{{Vchannel: "by-dev-rootcoord-dml_9_1v9"}},
	}))
	req := splitTestCommitRequest()
	req.Sources = append(req.Sources, &datapb.SplitShardTaskSource{Vchannel: splitTestSource, SwitchTimeTick: 2000})

	status, err := svr.CommitShardSplit(context.Background(), req)
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	task, _ := svr.shardSplitTasks.get(200)
	assert.Equal(t, []string{"by-dev-rootcoord-dml_9_1v9", splitTestSource}, splitSourceVChannels(task))
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

func TestCheckShardSplitDrainedUnknownTaskIsAnError(t *testing.T) {
	// "Not drained" would be a lie and "drained" would be catastrophic, so a
	// task datacoord has no record of is neither: it is a System error the
	// caller retries, because the record may simply not have arrived yet.
	svr := newShardSplitTestServer(t)

	resp, err := svr.CheckShardSplitDrained(context.Background(), &datapb.CheckShardSplitDrainedRequest{
		CollectionId: 100, SplitTaskId: 404,
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceInternal)
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

// newBroadcastShardSplitParam builds a valid one-source/two-target
// SplitShardParam for broadcastShardSplit tests: the residues exactly cover
// the source's, and the routing post-image names every target.
func newBroadcastShardSplitParam() streaming.SplitShardParam {
	return streaming.SplitShardParam{
		CollectionID:        100,
		DBID:                1,
		SplitTaskID:         200,
		SourceVChannels:     []string{splitTestSource},
		CollectionVChannels: []string{splitTestSource},
		RoutingModulus:      2,
		Targets: []*message.SplitShardTarget{
			{Vchannel: splitTestTarget0, Routing: &schemapb.HashRouting{Buckets: []uint64{0}}},
			{Vchannel: splitTestTarget1, Routing: &schemapb.HashRouting{Buckets: []uint64{1}}},
		},
		Schema:       &schemapb.CollectionSchema{Name: "col"},
		PartitionIDs: []int64{10, 11},
		Routing: &message.AlterCollectionMessageUpdates{
			VirtualChannelNames: []string{splitTestSource, splitTestTarget0, splitTestTarget1},
		},
		ControlChannel: "by-dev-rootcoord-dml_99_1v99",
	}
}

// broadcastShardSplitTestServer builds a datacoord Server wired with a mock
// broker, the shape broadcastShardSplit needs from startBroadcastWithCollectionID.
func broadcastShardSplitTestServer(t *testing.T) (*Server, *broker.MockBroker) {
	mockBroker := broker.NewMockBroker(t)
	s := &Server{broker: mockBroker}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	return s, mockBroker
}

func TestBroadcastShardSplitRefusesAnInvalidParamBeforeAnyBroadcast(t *testing.T) {
	// No broker/broadcaster is wired at all: a call that reaches past
	// Validate() here would panic on the nil broker, so a passing test is
	// itself proof the invalid param never gets that far.
	s := &Server{}
	param := newBroadcastShardSplitParam()
	param.CollectionID = 0 // invalid: Validate() requires a positive collection id

	err := s.broadcastShardSplit(context.Background(), param)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestBroadcastShardSplitPropagatesStartBroadcastFailure(t *testing.T) {
	s, mockBroker := broadcastShardSplitTestServer(t)
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).
		Return(nil, errors.New("collection not found"))

	err := s.broadcastShardSplit(context.Background(), newBroadcastShardSplitParam())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start broadcast for shard split")
}

func TestBroadcastShardSplitPropagatesBroadcastFailureAndStillCloses(t *testing.T) {
	s, mockBroker := broadcastShardSplitTestServer(t)
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).
		Return(&milvuspb.DescribeCollectionResponse{DbName: "test_db", CollectionName: "test_collection"}, nil)

	mockAPI := newMockBroadcastAPIImpl()
	mockAPI.broadcastErr = errors.New("wal append failed")
	mockStart := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return mockAPI, nil
		}).Build()
	defer mockStart.UnPatch()

	err := s.broadcastShardSplit(context.Background(), newBroadcastShardSplitParam())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to broadcast shard split")
	assert.True(t, mockAPI.closeCalled.Load(), "Close must still be called after a Broadcast failure")
}

func TestBroadcastShardSplitSuccess(t *testing.T) {
	s, mockBroker := broadcastShardSplitTestServer(t)
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).
		Return(&milvuspb.DescribeCollectionResponse{DbName: "test_db", CollectionName: "test_collection"}, nil)

	mockAPI := newMockBroadcastAPIImpl()
	mockAPI.broadcastResult = &types.BroadcastAppendResult{
		BroadcastID: 1,
		AppendResults: map[string]*types.AppendResult{
			"by-dev-rootcoord-dml_99_1v99": {TimeTick: 42},
		},
	}
	mockStart := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return mockAPI, nil
		}).Build()
	defer mockStart.UnPatch()

	param := newBroadcastShardSplitParam()
	err := s.broadcastShardSplit(context.Background(), param)
	require.NoError(t, err)
	require.True(t, mockAPI.closeCalled.Load())

	require.NotNil(t, mockAPI.capturedMsg)
	header := message.MustAsMutableSplitShardMessageV2(mockAPI.capturedMsg).Header()
	assert.Equal(t, param.SplitTaskID, header.GetSplitTaskId())
	assert.Equal(t, param.SourceVChannels, header.GetSourceVchannels())
	require.Len(t, header.GetTargets(), 2)
	assert.Equal(t, splitTestTarget0, header.GetTargets()[0].GetVchannel())
	assert.Equal(t, splitTestTarget1, header.GetTargets()[1].GetVchannel())
}
