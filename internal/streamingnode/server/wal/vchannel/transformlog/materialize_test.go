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

package transformlog

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type recordingScheduler struct {
	tasks []nodescheduler.Task
}

func (s *recordingScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return recordingTaskHandle{}
}

type recordingTaskHandle struct{}

func (recordingTaskHandle) Cancel() {}

func (recordingTaskHandle) Wait(context.Context) error { return nil }

// recordingMaterializer records every Materialize call.
type recordingMaterializer struct {
	mu      sync.Mutex
	batches []MaterializeRequest
}

func (m *recordingMaterializer) Materialize(_ context.Context, req MaterializeRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = append(m.batches, req)
	return nil
}

func (m *recordingMaterializer) calls() []MaterializeRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]MaterializeRequest(nil), m.batches...)
}

// failingMaterializer fails until released.
type failingMaterializer struct {
	mu    sync.Mutex
	fail  bool
	calls int
}

func (m *failingMaterializer) Materialize(_ context.Context, _ MaterializeRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.fail {
		return context.DeadlineExceeded
	}
	return nil
}

func (m *failingMaterializer) setFail(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = fail
}

func (m *failingMaterializer) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func newTestDeleteEntry(timetick uint64) *streamingpb.TransformLogEntry {
	return &streamingpb.TransformLogEntry{
		TimeTick: timetick,
		Entry: &streamingpb.TransformLogEntry_Delete{
			Delete: &streamingpb.TransformDeleteEntry{
				Blocks: []*streamingpb.TransformDeleteBlock{
					{
						PartitionId: 10,
						PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{Data: []int64{int64(timetick)}},
						}},
					},
				},
			},
		},
	}
}

// observeDelete observes one delete message of the vchannel.
func observeDelete(t *testing.T, log *TransformLog, timetick uint64) {
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
	retained := owner.Clone()
	log.ObserveMessage(retained)
	retained.Release()
	owner.Release()
}

func newTestTransformLog(t *testing.T, materializer Materializer, initialMaterialized uint64) (*TransformLog, *recordingScheduler, *recordingMaterializer) {
	t.Helper()
	scheduler := &recordingScheduler{}
	rec := &recordingMaterializer{}
	m := materializer
	if m == nil {
		m = rec
	}
	log := New(Config{
		VChannel:             "v1",
		MaterializedTimeTick: initialMaterialized,
		MaterializeMaxRows:   500000,
		MaterializeMaxBytes:  32 * 1024 * 1024,
		Materializer:         m,
		Runtime:              moduleapi.Runtime{Scheduler: scheduler},
	})
	return log, scheduler, rec
}

func TestTransformLogObserveSchedulesMaterialize(t *testing.T) {
	log, scheduler, rec := newTestTransformLog(t, nil, 0)
	observeDelete(t, log, 100)
	observeDelete(t, log, 200)
	observeDelete(t, log, 300)
	onMaterialized := []uint64{}
	log.onMaterialized = func(tt uint64) { onMaterialized = append(onMaterialized, tt) }

	// Observation schedules at most one task; the cap-batch continuation
	// inside materialize keeps the chain going.
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	// The first task was scheduled when the window frontier was 100; the
	// continuation catches the window up to 300.
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.Equal(t, uint64(300), log.MaterializedTimeTick())
	assert.Equal(t, []uint64{100, 300}, onMaterialized)

	calls := rec.calls()
	require.Len(t, calls, 2)
	assert.Equal(t, uint64(100), calls[0].TargetTimeTick)
	assert.Len(t, calls[0].Entries, 1)
	assert.Equal(t, uint64(300), calls[1].TargetTimeTick)
	assert.Len(t, calls[1].Entries, 2)

	// Nothing new observed: no further task is scheduled.
	assert.Len(t, scheduler.tasks, 2)
}

func TestTransformLogMaterializeCapsBatchAndContinues(t *testing.T) {
	log, scheduler, rec := newTestTransformLog(t, nil, 0)
	observeDelete(t, log, 100)
	observeDelete(t, log, 200)
	observeDelete(t, log, 300)
	log.materializeMaxRows = 1

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(100), log.MaterializedTimeTick())
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.Equal(t, uint64(200), log.MaterializedTimeTick())
	require.Len(t, scheduler.tasks, 3)
	require.NoError(t, scheduler.tasks[2].Execute(context.Background()))
	assert.Equal(t, uint64(300), log.MaterializedTimeTick())

	calls := rec.calls()
	require.Len(t, calls, 3)
	assert.Equal(t, uint64(100), calls[0].TargetTimeTick)
	assert.Equal(t, uint64(200), calls[1].TargetTimeTick)
	assert.Equal(t, uint64(300), calls[2].TargetTimeTick)
}

func TestTransformLogMaterializeRetriesOnFailure(t *testing.T) {
	fail := &failingMaterializer{}
	log, scheduler, _ := newTestTransformLog(t, fail, 0)
	observeDelete(t, log, 100)
	fail.setFail(true)

	require.Len(t, scheduler.tasks, 1)
	err := scheduler.tasks[0].Execute(context.Background())
	assert.Error(t, err)
	assert.False(t, scheduler.tasks[0].(*transformMaterializeTask).Done())
	assert.Equal(t, uint64(0), log.MaterializedTimeTick())

	fail.setFail(false)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, scheduler.tasks[0].(*transformMaterializeTask).Done())
	assert.Equal(t, uint64(100), log.MaterializedTimeTick())
}

func TestTransformLogMaterializeUpperBoundLimitsTarget(t *testing.T) {
	log, scheduler, rec := newTestTransformLog(t, nil, 0)
	observeDelete(t, log, 100)
	observeDelete(t, log, 200)
	observeDelete(t, log, 300)

	// The upper bound retracts the frontier before the task runs.
	log.SetMaterializeUpperBound(200)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.Equal(t, uint64(200), log.MaterializedTimeTick())

	// Advancing the bound continues materialization without a new WAL
	// trigger.
	log.SetMaterializeUpperBound(300)
	require.Len(t, scheduler.tasks, 3)
	require.NoError(t, scheduler.tasks[2].Execute(context.Background()))
	assert.Equal(t, uint64(300), log.MaterializedTimeTick())
	assert.Len(t, rec.calls(), 3)
}

func TestTransformLogMaterializesObservedRecordsImmediately(t *testing.T) {
	// The core of the decoupling: materialization consumes the observed
	// window directly and never waits for the summary to persist.
	log, scheduler, rec := newTestTransformLog(t, nil, 0)
	observeDelete(t, log, 100)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(100), log.MaterializedTimeTick())
	require.Len(t, rec.calls(), 1)
	assert.Equal(t, uint64(100), rec.calls()[0].TargetTimeTick)
}

func TestTransformLogRecoveryWindow(t *testing.T) {
	// Recovery loads the durable backlog into the initial window: records
	// after the restored frontier are materializable without any observation
	// or flush event.
	scheduler := &recordingScheduler{}
	rec := &recordingMaterializer{}
	log := New(Config{
		VChannel:             "v1",
		MaterializedTimeTick: 100,
		PendingEntries:       []*streamingpb.TransformLogEntry{newTestDeleteEntry(200)},
		MaterializeMaxRows:   500000,
		MaterializeMaxBytes:  32 * 1024 * 1024,
		Materializer:         rec,
		Runtime:              moduleapi.Runtime{Scheduler: scheduler},
	})

	// The loaded window alone is not materialized until a trigger arrives
	// (here, the upper bound publish on segment creation).
	log.SetMaterializeUpperBound(200)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(200), log.MaterializedTimeTick())
	assert.Empty(t, log.pending)
	require.Len(t, rec.calls(), 1)
}

func TestTransformLogObserveSkipsRecordsAtOrBelowLoadedThrough(t *testing.T) {
	// Recovery loaded (100, 200]; replay re-observes 200 and must not append
	// it again. Records past the loaded frontier are appended normally.
	scheduler := &recordingScheduler{}
	rec := &recordingMaterializer{}
	log := New(Config{
		VChannel:             "v1",
		MaterializedTimeTick: 100,
		PendingEntries:       []*streamingpb.TransformLogEntry{newTestDeleteEntry(200)},
		MaterializeMaxRows:   500000,
		MaterializeMaxBytes:  32 * 1024 * 1024,
		Materializer:         rec,
		Runtime:              moduleapi.Runtime{Scheduler: scheduler},
	})

	observeDelete(t, log, 200) // at the loaded frontier: skipped.
	require.Empty(t, scheduler.tasks)
	require.Len(t, log.pending, 1)

	observeDelete(t, log, 300) // past the frontier: appended and scheduled.
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(300), log.MaterializedTimeTick())
	assert.Empty(t, log.pending)
}

func TestTransformLogObserveIgnoresNonDelete(t *testing.T) {
	log, scheduler, _ := newTestTransformLog(t, nil, 0)
	insert := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{CollectionID: 1, PartitionID: 10}).
		MustBuildMutable()
	msg := insert.WithTimeTick(100).IntoImmutableMessage(walimplstest.NewTestMessageID(101))
	owner := message.NewOwnedImmutableMessage(msg, nil)
	retained := owner.Clone()
	log.ObserveMessage(retained)
	retained.Release()
	owner.Release()
	require.Empty(t, scheduler.tasks)
	require.Empty(t, log.pending)
}

// observeBarrier observes one ManualFlush message of the vchannel; it is
// classified as a transform barrier.
func observeBarrier(t *testing.T, log *TransformLog, timetick uint64) {
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
	retained := owner.Clone()
	log.ObserveMessage(retained)
	retained.Release()
	owner.Release()
}

func TestTransformLogBarrierAdvancesFrontier(t *testing.T) {
	// A flush barrier carries no delete data, but the frontier must be able
	// to reach it: on a vchannel that only ever saw inserts, the frontier
	// would otherwise stay at its initial value and pin the vchannel-level
	// flush checkpoint (min of the frontier and the growing segment
	// checkpoints) below the flush boundary forever.
	log, scheduler, _ := newTestTransformLog(t, nil, 0)
	observeBarrier(t, log, 200)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(200), log.MaterializedTimeTick())
	assert.Empty(t, log.pending)
}

func TestTransformLogBarrierAfterDeleteMaterializesBoth(t *testing.T) {
	// delete@100 then barrier@200: the first batch emits the delete as L0
	// output, the continuation reaches the barrier; no L0 output is produced
	// for the payload-free barrier itself.
	log, scheduler, rec := newTestTransformLog(t, nil, 0)
	observeDelete(t, log, 100)
	observeBarrier(t, log, 200)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(100), log.MaterializedTimeTick())
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.Equal(t, uint64(200), log.MaterializedTimeTick())
	assert.Empty(t, log.pending)
	require.Len(t, rec.calls(), 1)
}

func TestTransformLogBarrierBelowUpperBoundPinned(t *testing.T) {
	// The frontier must never pass the L1 materialization upper bound: a
	// barrier at 200 below an uncommitted L1 segment created at 150 pins the
	// delete data through 100; the barrier itself is reached only after the
	// bound is raised past it.
	log, scheduler, _ := newTestTransformLog(t, nil, 0)
	log.SetMaterializeUpperBound(150)
	observeDelete(t, log, 100)
	observeBarrier(t, log, 200)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(100), log.MaterializedTimeTick())
	// Raising the bound past the barrier lets the continuation chain reach
	// it (the pinned task's own continuation re-reads the bound).
	log.SetMaterializeUpperBound(300)
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.Equal(t, uint64(100), log.MaterializedTimeTick())
	require.Len(t, scheduler.tasks, 3)
	require.NoError(t, scheduler.tasks[2].Execute(context.Background()))
	assert.Equal(t, uint64(200), log.MaterializedTimeTick())
	assert.Empty(t, log.pending)
}

func TestTransformLogBarrierOfOtherVChannelIgnored(t *testing.T) {
	// A foreign-vchannel barrier carries no per-vchannel data boundary for
	// this vchannel; it must not move the frontier.
	log, scheduler, _ := newTestTransformLog(t, nil, 0)
	mutableMsg := message.NewManualFlushMessageBuilderV2().
		WithVChannel("other-vchannel").
		WithHeader(&message.ManualFlushMessageHeader{}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(200).
		WithLastConfirmed(walimplstest.NewTestMessageID(200)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(201))
	owner := message.NewOwnedImmutableMessage(msg, nil)
	retained := owner.Clone()
	log.ObserveMessage(retained)
	retained.Release()
	owner.Release()
	require.Empty(t, scheduler.tasks)
	assert.Equal(t, uint64(0), log.MaterializedTimeTick())
}

// TestTransformLogUpperBoundRaiseAfterCommitSchedulesSuccessor covers the
// window between a task's materialize commit and its Done flag: a bound raise
// landing there was previously swallowed (the pending task's presence made the
// scheduler think the frontier was still covered), stranding the
// (old bound, new bound] records in the window forever.
func TestTransformLogUpperBoundRaiseAfterCommitSchedulesSuccessor(t *testing.T) {
	log, scheduler, _ := newTestTransformLog(t, nil, 0)
	observeDelete(t, log, 100)
	observeDelete(t, log, 200)
	observeDelete(t, log, 300)
	log.SetMaterializeUpperBound(200)

	// task[0] materializes 100 and schedules the continuation to 200.
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	require.Len(t, scheduler.tasks, 2)

	// Simulate the window: the continuation already committed its batch
	// (materialized = 200) but has not flipped its Done flag yet — exactly the
	// state between materialize's commit and execute's done.Store.
	log.mu.Lock()
	log.materializedTimeTick = 200
	log.mu.Unlock()

	// A bound raise in that window must schedule a successor, not be swallowed.
	assert.True(t, log.SetMaterializeUpperBound(300))
	require.Len(t, scheduler.tasks, 3)
	// The successor chains behind the still-pending continuation: the
	// continuation finishes first (its already-committed batch makes it a
	// no-op), then the successor materializes the raised frontier.
	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	require.NoError(t, scheduler.tasks[2].Execute(context.Background()))
	assert.Equal(t, uint64(300), log.MaterializedTimeTick())
}
