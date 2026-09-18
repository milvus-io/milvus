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

package l0materializer

import (
	"context"
	"math"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func testMaterializer(t *testing.T, initial uint64, writeErrors ...*error) (*L0Materializer, *[]nodescheduler.Task, *[]MaterializeRequest) {
	t.Helper()
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	tasks := []nodescheduler.Task{}
	patch := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(task nodescheduler.Task) nodescheduler.TaskHandle {
		tasks = append(tasks, task)
		return nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	batches := []MaterializeRequest{}
	writer := &SyncMaterializer{}
	patchWriter := mockey.Mock((*SyncMaterializer).Materialize).To(func(_ *SyncMaterializer, _ context.Context, req MaterializeRequest) error {
		batches = append(batches, req)
		if len(writeErrors) > 0 {
			return *writeErrors[0]
		}
		return nil
	}).Build()
	t.Cleanup(func() { patchWriter.UnPatch() })
	m := New(Config{
		VChannel: "v1", MaterializedTimeTick: initial,
		Reader: walsummary.NewManager(walsummary.ManagerConfig{}), Materializer: writer,
		Runtime: moduleapi.Runtime{Scheduler: scheduler}, MaterializeMaxRows: 1, MaterializeMaxBytes: 1,
	})
	return m, &tasks, &batches
}

func observeDelete(t *testing.T, log *L0Materializer, timetick uint64) {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
		WithVChannel(log.vchannel).
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         1,
		}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{int64(timetick)}}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	owner := message.NewOwnedImmutableMessage(msg, nil)
	log.reader.(*walsummary.Manager).ObserveMessage(context.Background(), msg)
	log.ObserveMessage(msg)
	owner.Release()
}

func observeBarrier(t *testing.T, log *L0Materializer, timetick uint64) {
	t.Helper()
	mutableMsg := message.NewManualFlushMessageBuilderV2().
		WithVChannel(log.vchannel).
		WithHeader(&message.ManualFlushMessageHeader{}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	owner := message.NewOwnedImmutableMessage(msg, nil)
	log.reader.(*walsummary.Manager).ObserveMessage(context.Background(), msg)
	log.ObserveMessage(msg)
	owner.Release()
}

func TestWindowReadsSharedHotTailAndCaps(t *testing.T) {
	m, tasks, batches := testMaterializer(t, 0)
	m.materializeMaxRows = 1
	observeDelete(t, m, 100)
	observeDelete(t, m, 200)
	observeBarrier(t, m, 300)
	require.Len(t, *tasks, 1)
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(100), m.MaterializedTimeTick())
	require.Len(t, *tasks, 2)
	require.NoError(t, (*tasks)[1].Execute(context.Background()))
	require.Equal(t, uint64(300), m.MaterializedTimeTick())
	require.Len(t, *batches, 2)
	require.Len(t, (*batches)[1].Entries, 1)
	require.False(t, m.HasPendingMaterializeTask())
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Len(t, *batches, 2, "completed task cannot duplicate output")
}

func TestL1BoundAndCompletionWakeup(t *testing.T) {
	m, tasks, _ := testMaterializer(t, 0)
	m.materializeMaxRows = 10
	m.materializeMaxBytes = 1 << 20
	m.SetMaterializeUpperBound(150)
	observeDelete(t, m, 100)
	observeBarrier(t, m, 200)
	requestFlush(t, m, 200)
	require.Empty(t, *tasks, "explicit completion must wait for all earlier L1 commits")
	m.SetMaterializeUpperBound(199)
	require.Empty(t, *tasks)
	require.True(t, m.SetMaterializeUpperBound(300))
	require.Len(t, *tasks, 1)
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(200), m.MaterializedTimeTick())
	require.False(t, m.HasPendingMaterializeTask())
}

func TestRecoveryRequestsBacklogOnlyAfterBarrier(t *testing.T) {
	m, tasks, batches := testMaterializer(t, 50)
	m.materializeMaxRows = 10
	m.materializeMaxBytes = 1 << 20
	// Summary can contain Delete@100 before checkpoint@200. Recovery restores
	// M only: replaying a RecoveryBarrier must discover that older Delete.
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	summary := walsummary.NewManager(walsummary.ManagerConfig{PChannel: "p1", Term: 1, Store: walsummary.NewStore(cm, "p1", 1), Runtime: m.runtime})
	source := New(Config{VChannel: "v1", Reader: summary})
	observeDelete(t, source, 100)
	require.Empty(t, *tasks)
	summary.RequestFlushThrough(100)
	for i := 0; i < len(*tasks); i++ {
		require.NoError(t, (*tasks)[i].Execute(context.Background()))
	}
	require.Len(t, summary.Manifest().GetChunks(), 1)
	restored := walsummary.NewManager(walsummary.ManagerConfig{PChannel: "p1", Term: 2, Store: walsummary.NewStore(cm, "p1", 2)})
	require.NoError(t, restored.Restore(context.Background()))
	m.reader = restored
	*tasks = nil
	require.Equal(t, uint64(50), m.requestedThrough)
	mutable := message.NewRecoveryBarrierMessageBuilderV2().WithVChannel("").
		WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).MustBuildMutable()
	msg := mutable.WithTimeTick(250).WithLastConfirmed(walimplstest.NewTestMessageID(249)).IntoImmutableMessage(walimplstest.NewTestMessageID(250))
	m.reader.(*walsummary.Manager).ObserveMessage(context.Background(), msg)
	m.ObserveMessage(msg)
	require.Empty(t, *tasks, "recovery barrier alone does not force a small batch")
	m.RequestBacklogThrough(250)
	require.Len(t, *tasks, 1)
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(250), m.MaterializedTimeTick())
	require.Len(t, *batches, 1)
	require.Equal(t, uint64(100), (*batches)[0].Entries[0].GetTimeTick())
}

func TestReaderFailureAndIncompleteCoverageCannotCommit(t *testing.T) {
	m, tasks, batches := testMaterializer(t, 0)
	observeDelete(t, m, 100)
	read := mockey.Mock((*walsummary.Manager).ReadTransform).Return(walsummary.TransformBatch{}, context.DeadlineExceeded).Build()
	require.Error(t, (*tasks)[0].Execute(context.Background()))
	require.Zero(t, m.MaterializedTimeTick())
	require.Len(t, *tasks, 1)
	read.Return(walsummary.TransformBatch{}, nil)
	require.ErrorIs(t, (*tasks)[0].Execute(context.Background()), nodescheduler.ErrDelay)
	require.Empty(t, *batches)
	read.UnPatch()
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(100), m.MaterializedTimeTick())
}

func TestOutputFailureRetriesWithoutAdvancing(t *testing.T) {
	outputError := context.DeadlineExceeded
	m, tasks, _ := testMaterializer(t, 0, &outputError)
	observeDelete(t, m, 100)
	require.Error(t, (*tasks)[0].Execute(context.Background()))
	require.Zero(t, m.MaterializedTimeTick())
	require.Len(t, *tasks, 1)
	outputError = nil
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(100), m.MaterializedTimeTick())
}

func TestMissingWiringAndRetractedBound(t *testing.T) {
	m, tasks, _ := testMaterializer(t, 0)
	observeDelete(t, m, 100)
	reader := m.reader
	m.reader = nil
	require.Error(t, (*tasks)[0].Execute(context.Background()))
	m.reader = reader
	m.materializer = nil
	require.Error(t, (*tasks)[0].Execute(context.Background()))
	m.SetMaterializeUpperBound(0)
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.False(t, m.HasPendingMaterializeTask())
	require.Zero(t, m.MaterializedTimeTick())
}

func TestObservationClassificationAndEmptyWindows(t *testing.T) {
	m, tasks, batches := testMaterializer(t, 100)
	observeBarrier(t, m, 50)
	require.Empty(t, *tasks)
	insert := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{CollectionID: 1}).MustBuildMutable().WithTimeTick(150).IntoImmutableMessage(walimplstest.NewTestMessageID(150))
	m.ObserveMessage(insert)
	require.Empty(t, *tasks)
	foreign := message.NewManualFlushMessageBuilderV2().WithVChannel("v2").WithHeader(&message.ManualFlushMessageHeader{}).
		WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable().WithTimeTick(200).IntoImmutableMessage(walimplstest.NewTestMessageID(200))
	m.ObserveMessage(foreign)
	require.Empty(t, *tasks)
	observeBarrier(t, m, 300)
	require.Empty(t, *tasks)
	requestFlush(t, m, 300)
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(300), m.MaterializedTimeTick())
	require.Empty(t, *batches)
	require.False(t, m.SetMaterializeUpperBound(math.MaxUint64))
}

func TestConcurrentObservationDuringTaskCompletion(t *testing.T) {
	m, tasks, _ := testMaterializer(t, 0)
	m.SetMaterializeUpperBound(100)
	observeDelete(t, m, 100)
	entered, release := make(chan struct{}), make(chan struct{})
	m.onMaterialized = func(tt uint64) {
		if tt == 100 {
			close(entered)
			<-release
		}
	}
	done := make(chan error, 1)
	go func() { done <- (*tasks)[0].Execute(context.Background()) }()
	<-entered
	observeDelete(t, m, 200)
	m.SetMaterializeUpperBound(200)
	close(release)
	require.NoError(t, <-done)
	require.Len(t, *tasks, 2)
	require.NoError(t, (*tasks)[1].Execute(context.Background()))
	require.Equal(t, uint64(200), m.MaterializedTimeTick())
	require.False(t, m.HasPendingMaterializeTask())
}

func TestCapacityLeavesSmallTailAndIgnoresBarriers(t *testing.T) {
	m, tasks, batches := testMaterializer(t, 0)
	m.materializeMaxRows = 2
	m.materializeMaxBytes = 34
	observeDelete(t, m, 200)
	observeBarrier(t, m, 250)
	require.Empty(t, *tasks)
	observeDelete(t, m, 300)
	require.Len(t, *tasks, 1)
	// Arrivals during a capacity task cannot make it drain a small remainder.
	observeDelete(t, m, 400)
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(300), m.MaterializedTimeTick())
	require.Len(t, *batches, 1)
	require.Len(t, *tasks, 2, "a boundary probe resolves the remaining section")
	require.NoError(t, (*tasks)[1].Execute(context.Background()))
	require.Len(t, *batches, 1, "the small tail does not produce L0 output")
	require.False(t, m.HasPendingMaterializeTask())
	observeBarrier(t, m, 450)
	m.SetMaterializeUpperBound(500)
	require.Len(t, *tasks, 2, "barriers do not repeat an unsuccessful probe")
	observeDelete(t, m, 500)
	require.Len(t, *tasks, 3)
	require.NoError(t, (*tasks)[2].Execute(context.Background()))
	require.Equal(t, uint64(500), m.MaterializedTimeTick())
}

func TestCapacityUsesSafeDeleteBytes(t *testing.T) {
	m, tasks, _ := testMaterializer(t, 0)
	m.materializeMaxRows = 100
	m.materializeMaxBytes = 1
	m.SetMaterializeUpperBound(99)
	observeDelete(t, m, 100)
	observeBarrier(t, m, 200)
	require.Empty(t, *tasks, "Delete beyond L does not count toward capacity")
	require.True(t, m.SetMaterializeUpperBound(100))
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(100), m.MaterializedTimeTick())
	m.SetMaterializeUpperBound(math.MaxUint64)
	require.Len(t, *tasks, 1, "empty interval does not meet byte capacity")
}

func TestForcedGoalDoesNotChaseNewSmallTail(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "backlog", true: "explicit"}[explicit], func(t *testing.T) {
			m, tasks, batches := testMaterializer(t, 0)
			m.materializeMaxRows = 2
			m.materializeMaxBytes = 34
			m.SetMaterializeUpperBound(0)
			for _, tt := range []uint64{200, 300, 400} {
				observeDelete(t, m, tt)
			}
			if explicit {
				requestFlush(t, m, 400)
			} else {
				m.RequestBacklogThrough(400)
			}
			require.Empty(t, *tasks)
			m.SetMaterializeUpperBound(math.MaxUint64)
			require.Len(t, *tasks, 1)
			observeDelete(t, m, 500)
			require.NoError(t, (*tasks)[0].Execute(context.Background()))
			require.Equal(t, uint64(300), m.MaterializedTimeTick())
			require.Len(t, *tasks, 2)
			require.NoError(t, (*tasks)[1].Execute(context.Background()))
			require.Equal(t, uint64(400), m.MaterializedTimeTick())
			require.Len(t, *batches, 2)
			require.Len(t, (*batches)[1].Entries, 1, "forced goal includes a small tail")
			require.Len(t, *tasks, 3, "the remaining boundary section needs a capacity probe")
			require.NoError(t, (*tasks)[2].Execute(context.Background()))
			require.Len(t, *batches, 2, "new small arrivals do not extend the captured goal")
			require.False(t, m.HasPendingMaterializeTask())
		})
	}
}

func flushMessage(tt uint64) message.ImmutableMessage {
	return message.NewManualFlushMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable().
		WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(int64(tt - 1))).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt)))
}

func requestFlush(t *testing.T, m *L0Materializer, tt uint64) {
	t.Helper()
	owner := message.NewOwnedImmutableMessage(flushMessage(tt), nil)
	handle := owner.Clone()
	m.RequestFlush(handle)
	handle.Release()
	owner.Release()
}

func trackFlush(t *testing.T, m *L0Materializer, tracker *messageack.Tracker, tt uint64) {
	t.Helper()
	raw := flushMessage(tt)
	owner := tracker.Track(raw)
	retained := owner.Clone()
	m.reader.(*walsummary.Manager).ObserveMessage(context.Background(), raw)
	m.RequestFlush(retained)
	m.ObserveMessage(raw)
	retained.Release()
	owner.Release()
}

func TestFlushHandlesReleaseOnlyCoveredPrefixAfterDirtyCallback(t *testing.T) {
	outputErr := context.DeadlineExceeded
	m, tasks, _ := testMaterializer(t, 0, &outputErr)
	m.materializeMaxRows = 2
	m.materializeMaxBytes = 34
	m.SetMaterializeUpperBound(0)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	observeDelete(t, m, 100)
	trackFlush(t, m, tracker, 120)
	observeDelete(t, m, 150)
	observeDelete(t, m, 200)
	trackFlush(t, m, tracker, 250)
	m.SetMaterializeUpperBound(math.MaxUint64)
	require.Len(t, *tasks, 1)
	require.Error(t, (*tasks)[0].Execute(context.Background()))
	require.Zero(t, tracker.CompletedPoint().TimeTick, "failed output cannot release Flush")
	require.Len(t, m.pendingFlushes, 2)
	m.onMaterialized = func(tt uint64) {
		require.Less(t, tracker.CompletedPoint().TimeTick, tt, "install dirty metadata before releasing handles")
	}
	outputErr = nil
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(120), tracker.CompletedPoint().TimeTick)
	require.Len(t, m.pendingFlushes, 1, "only covered requests release after a partial batch")
	require.Len(t, *tasks, 2)
	outputErr = context.Canceled
	require.Error(t, (*tasks)[1].Execute(context.Background()))
	require.Equal(t, uint64(120), tracker.CompletedPoint().TimeTick, "cancellation leaves unfinished work replayable")
	outputErr = nil
	require.NoError(t, (*tasks)[1].Execute(context.Background()))
	require.Equal(t, uint64(250), tracker.CompletedPoint().TimeTick)
	require.Empty(t, m.pendingFlushes)
	require.NoError(t, (*tasks)[1].Execute(context.Background()))
}

func TestFlushWaitsForMetadataCallback(t *testing.T) {
	m, tasks, _ := testMaterializer(t, 0)
	m.materializeMaxRows = 10
	m.materializeMaxBytes = 1 << 20
	observeDelete(t, m, 100)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	trackFlush(t, m, tracker, 200)
	entered, release := make(chan struct{}), make(chan struct{})
	m.onMaterialized = func(tt uint64) {
		if tt == 200 {
			close(entered)
			<-release
		}
	}
	done := make(chan error, 1)
	go func() { done <- (*tasks)[0].Execute(context.Background()) }()
	<-entered
	// Output is complete, but metadata is still being installed. Later Flush
	// arrival must neither release this request early nor extend its active goal.
	trackFlush(t, m, tracker, 250)
	require.Zero(t, tracker.CompletedPoint().TimeTick)
	close(release)
	require.NoError(t, <-done)
	require.Equal(t, uint64(200), tracker.CompletedPoint().TimeTick)
	require.Len(t, m.pendingFlushes, 1)
	require.Len(t, *tasks, 2)
	require.NoError(t, (*tasks)[1].Execute(context.Background()))
	require.Equal(t, uint64(250), tracker.CompletedPoint().TimeTick)
	require.Empty(t, m.pendingFlushes)
}

func TestFlushReplayAfterPersistedMNeedsNoNewOutput(t *testing.T) {
	m, tasks, batches := testMaterializer(t, 200)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	trackFlush(t, m, tracker, 200)
	require.Equal(t, uint64(200), tracker.CompletedPoint().TimeTick)
	require.Empty(t, m.pendingFlushes)
	require.Empty(t, *tasks)
	require.Empty(t, *batches)
}

func TestFastForwardCannotCompleteUnmaterializedFlush(t *testing.T) {
	for _, capacity := range []bool{false, true} {
		t.Run(map[bool]string{false: "flush", true: "capacity-probe"}[capacity], func(t *testing.T) {
			m, tasks, batches := testMaterializer(t, 0)
			if !capacity {
				m.materializeMaxBytes = 1 << 20
			}
			observeDelete(t, m, 100)
			tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
			trackFlush(t, m, tracker, 200)
			if capacity {
				stats := mockey.Mock((*walsummary.Manager).TransformStats).Return(walsummary.TransformStats{UpperBytes: 100, LastTimeTick: 100}).Build()
				defer stats.UnPatch()
			}
			read := mockey.Mock((*walsummary.Manager).ReadTransform).Return(walsummary.TransformBatch{
				FastForwardTimeTick: 100, CoveredThrough: 200, ReadableThrough: 200,
			}, nil).Build()
			defer read.UnPatch()
			require.ErrorContains(t, (*tasks)[0].Execute(context.Background()), "precedes summary fast-forward")
			require.Zero(t, m.MaterializedTimeTick())
			require.Zero(t, tracker.CompletedPoint().TimeTick)
			require.Len(t, m.pendingFlushes, 1)
			require.Empty(t, *batches)
		})
	}
}

func TestFlushCompletesReadyPrefixBeforeLaterBlockedRequests(t *testing.T) {
	m, tasks, batches := testMaterializer(t, 0)
	m.materializeMaxBytes = 1 << 20
	m.SetMaterializeUpperBound(99)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	for _, tt := range []uint64{100, 120, 200} {
		trackFlush(t, m, tracker, tt)
	}
	require.Empty(t, *tasks)
	require.True(t, m.SetMaterializeUpperBound(150))
	require.Len(t, *tasks, 1)
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(120), m.MaterializedTimeTick())
	require.Equal(t, uint64(120), tracker.CompletedPoint().TimeTick)
	require.Len(t, m.pendingFlushes, 1)
	require.Len(t, *tasks, 1, "blocked later request does not poll")
	require.True(t, m.SetMaterializeUpperBound(200))
	require.Len(t, *tasks, 2)
	require.NoError(t, (*tasks)[1].Execute(context.Background()))
	require.Equal(t, uint64(200), tracker.CompletedPoint().TimeTick)
	require.Empty(t, m.pendingFlushes)
	require.Empty(t, *batches, "empty coverage completes without physical L0 output")
}
